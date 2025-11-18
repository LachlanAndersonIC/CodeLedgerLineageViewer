// SAS-enabled container URL
// Example:
// const containerUrl = "https://aicodeledgerlineage.blob.core.windows.net/lineage?sv=xxxx&sig=xxxx";
const containerUrl = "https://aicodeledgerlineage.blob.core.windows.net/lineage?sp=rl&st=2025-11-18T01:29:42Z&se=2026-11-18T09:44:42Z&spr=https&sv=2024-11-04&sr=c&sig=X8%2BwAKmeKXetzbfcVWDcpTaipOiahwXZzfaEJ2Qh8%2BE%3D";

// Folder in blob storage
const prefix = "local_repo/models/";

async function listJsonFiles() {
    const listUrl = `${containerUrl}&restype=container&comp=list&prefix=${prefix}`;
    const res = await fetch(listUrl);
    const xmlText = await res.text();

    const xml = new DOMParser().parseFromString(xmlText, "application/xml");
    const blobs = [...xml.getElementsByTagName("Blob")];

    return blobs
        .map(b => b.getElementsByTagName("Name")[0].textContent)
        .filter(name => name.endsWith(".json"));
}

async function fetchJson(name) {
    const blobBase = containerUrl.split("?")[0];
    const sas = "?" + containerUrl.split("?")[1];
    const url = `${blobBase}/${name}${sas}`;
    return fetch(url).then(r => r.json());
}

function convertToCytoscape(model) {
    const target = model.target_table || model.file_name || "unknown_target";

    let nodes = [];
    let edges = [];

    nodes.push({
        data: {
            id: target,
            type: model.target_type,
            fullModel: model
        }
    });

    for (const src of model.sources || []) {
        const srcKey = src.table || src.model || "unknown_source";
        const srcId = `${src.name}.${srcKey}`;

        nodes.push({ data: { id: srcId, type: src.type } });

        edges.push({
            data: { source: srcId, target: target }
        });
    }

    return { nodes, edges };
}

// MAIN EXECUTION
(async () => {
    const files = await listJsonFiles();

    let allElements = [];

    for (const file of files) {
        const modelJson = await fetchJson(file);
        const cyData = convertToCytoscape(modelJson);
        allElements.push(...cyData.nodes, ...cyData.edges);
    }

    // IMPORTANT: SAVE INSTANCE
    const cy = cytoscape({
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

    // ADD CLICK HANDLER AFTER cy EXISTS
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

            let sources = [];

            if (meta.source) {
                sources = Array.isArray(meta.source) ? meta.source : [meta.source];
            }

            if (sources.length > 0) {
                html += sources.map(s => `- ${s}`).join("<br>");
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

})();
