// SAS-enabled container URL
// Example:
// const containerUrl = "https://aicodeledgerlineage.blob.core.windows.net/lineage?sv=xxxx&sig=xxxx";
const containerUrl = "https://aicodeledgerlineage.blob.core.windows.net/lineage?sp=rl&st=2025-11-18T01:29:42Z&se=2026-11-18T09:44:42Z&spr=https&sv=2024-11-04&sr=c&sig=X8%2BwAKmeKXetzbfcVWDcpTaipOiahwXZzfaEJ2Qh8%2BE%3D";

// Folder in blob storage
const prefix = "local_repo/models/";

async function listJsonFiles() {
    // Build list URL with prefix filter
    const listUrl = `${containerUrl}&restype=container&comp=list&prefix=${prefix}`;

    const res = await fetch(listUrl);
    const xmlText = await res.text();

    const xml = new DOMParser().parseFromString(xmlText, "application/xml");
    const blobs = [...xml.getElementsByTagName("Blob")];

    // Return blob names relative to prefix
    return blobs
        .map(b => b.getElementsByTagName("Name")[0].textContent)
        .filter(name => name.endsWith(".json"));
}

async function fetchJson(name) {
    // Real blob URL: base container URL prefix + blob name + SAS token
    const blobBase = containerUrl.split("?")[0];
    const sas = "?" + containerUrl.split("?")[1];

    const url = `${blobBase}/${name}${sas}`;

    return fetch(url).then(r => r.json());
}

function convertToCytoscape(model) {
    const target = model.target_table || model.file_name || "unknown_target";

    return {
        target,
        nodes: [{ data: { id: target, type: model.target_type, fullModel: model }}],
        edges: model.sources.map(src => {
            const srcKey = src.table || src.model;
            const id = `${src.name}.${srcKey}`;
            return {
                sourceNode: { data: { id: id, type: src.type }},
                edge: { data: { source: id, target: target }}
            };
        })
    };
}

// Add click handler after cytoscape() call:

cy.on('tap', 'node', evt => {
    const node = evt.target;
    const model = node.data('fullModel');

    if (!model || !model.columns) {
        document.getElementById("info-panel").style.display = "none";
        return;
    }

    const panel = document.getElementById("info-panel");
    const title = document.getElementById("panel-title");
    const content = document.getElementById("panel-content");

    title.innerText = model.target_table || node.id();

    let html = "";

    for (const [col, meta] of Object.entries(model.columns)) {
        html += `<div style="margin-bottom: 10px;">
            <strong>${col}</strong><br>
            <em>Sources:</em><br>`;

        if (meta.source) {
            if (Array.isArray(meta.source)) {
                html += meta.source.map(s => `- ${s}`).join("<br>");
            } else {
                html += `- ${meta.source}`;
            }
        } else {
            html += `<span style="color:#888;">(none)</span>`;
        }

        if (meta.transformation) {
            html += `<br><em>Transformation:</em><br>${meta.transformation}`;
        }

        html += `<hr></div>`;
    }

    content.innerHTML = html;
    panel.style.display = "block";
});

(async () => {
    const files = await listJsonFiles();

    let allElements = [];

    for (const file of files) {
        const modelJson = await fetchJson(file);
        const cyData = convertToCytoscape(modelJson);
        allElements.push(...cyData.nodes, ...cyData.edges);
    }

    cytoscape({
        container: document.getElementById("cy"),
        elements: allElements,
        layout: { name: "cose", padding: 30 },
        style: [
            {
                selector: 'node[type="table"]',
                style: { 'background-color': '#6cca98', 'label': 'data(id)' }
            },
            {
                selector: 'node[type="dbt_source"]',
                style: { 'background-color': '#003057', 'label': 'data(id)' }
            },
            {
                selector: 'node[type="ref"]',
                style: { 'background-color': '#27a878', 'label': 'data(id)' }
            },
            {
                selector: 'edge',
                style: {
                    'width': 2,
                    'line-color': '#777',
                    'target-arrow-color': '#777',
                    'target-arrow-shape': 'triangle'
                }
            }
        ]
    });
})();
