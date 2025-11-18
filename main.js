// SAS-enabled container URL
// Example:
// const containerUrl = "https://aicodeledgerlineage.blob.core.windows.net/lineage?sv=xxxx&sig=xxxx";
const containerUrl = "https://aicodeledgerlineage.blob.core.windows.net/lineage?sp=rl&st=2025-11-18T01:29:42Z&se=2026-11-18T09:44:42Z&spr=https&sv=2024-11-04&sr=c&sig=X8%2BwAKmeKXetzbfcVWDcpTaipOiahwXZzfaEJ2Qh8%2BE%3D";
const prefix = "local_repo/models/";


// ---------------------------------------------------------------------------
// LIST JSON FILES IN BLOB STORAGE
// ---------------------------------------------------------------------------
async function listJsonFiles() {
    const listUrl = `${containerUrl}&restype=container&comp=list&prefix=${prefix}`;
    console.log("Listing URL:", listUrl);

    const res = await fetch(listUrl);
    const xmlText = await res.text();
    const xml = new DOMParser().parseFromString(xmlText, "application/xml");

    const blobs = [...xml.getElementsByTagName("Blob")];

    return blobs
        .map(b => b.getElementsByTagName("Name")[0].textContent)
        .filter(name => name.endsWith(".json"));
}


// ---------------------------------------------------------------------------
// FETCH INDIVIDUAL JSON FILE
// ---------------------------------------------------------------------------
async function fetchJson(name) {
    const blobBase = containerUrl.split("?")[0];
    const sas = "?" + containerUrl.split("?")[1];

    const url = `${blobBase}/${name}${sas}`;
    console.log("Fetching:", url);

    const res = await fetch(url);
    return res.json();
}


// ---------------------------------------------------------------------------
// CONVERT ONE MODEL JSON → Cytoscape Nodes + Edges
// ---------------------------------------------------------------------------
function convertToCytoscape(model) {
    // Determine the model ID (unique identifier)
    const target =
        model.model_name ||
        model.target_table ||
        (model.file_name ? model.file_name.replace(".sql", "") : null);

    if (!target) {
        console.warn("❗ Model has no usable identifier:", model);
        return { nodes: [], edges: [] };
    }

    let nodes = [];
    let edges = [];

    // MODEL NODE (green)
    nodes.push({
        data: {
            id: target,
            label: target,
            type: "model",
            entity: "model",
            fullModel: model
        }
    });

    // SOURCE / REF NODES (blue)
    for (const src of model.sources || []) {
        const srcKey = src.table || src.model || "unknown_source";
        const srcId = `${src.name}.${srcKey}`;

        nodes.push({
            data: {
                id: srcId,
                label: srcId,
                type: src.type,
                entity: "source"
            }
        });

        edges.push({
            data: {
                source: srcId,
                target: target
            }
        });
    }

    return { nodes, edges };
}


// ---------------------------------------------------------------------------
// MAIN EXECUTION
// ---------------------------------------------------------------------------
(async () => {
    console.log("Loading JSON files…");

    const files = await listJsonFiles();
    console.log("Files found:", files);

    let allElements = [];

    for (const file of files) {
        const modelJson = await fetchJson(file);
        const { nodes, edges } = convertToCytoscape(modelJson);
        allElements.push(...nodes, ...edges);
    }

    console.log("Elements prepared:", allElements.length);

    // ----------------------------------------------------------------------
    // CYTOSCAPE INITIALISATION
    // ----------------------------------------------------------------------
    const cy = cytoscape({
        container: document.getElementById("cy"),
        elements: allElements,

        layout: {
            name: "cose",
            padding: 50
        },

        style: [
            // MODEL NODES
            {
                selector: 'node[entity="model"]',
                style: {
                    "background-color": "#6cca98",
                    "border-width": 3,
                    "border-color": "#003057",
                    "label": "data(label)",
                    "font-size": 10,
                    "text-valign": "center",
                    "text-halign": "center"
                }
            },

            // SOURCE / REF NODES
            {
                selector: 'node[entity="source"]',
                style: {
                    "background-color": "#003057",
                    "color": "#ffffff",
                    "label": "data(label)",
                    "font-size": 9,
                    "text-valign": "center",
                    "text-halign": "center"
                }
            },

            // EDGES
            {
                selector: "edge",
                style: {
                    "width": 2,
                    "line-color": "#777",
                    "target-arrow-color": "#777",
                    "target-arrow-shape": "triangle"
                }
            }
        ]
    });

    console.log("Cytoscape initialised.");


    // ----------------------------------------------------------------------
    // NODE CLICK HANDLER — SHOW COLUMN LINEAGE ONLY FOR MODEL NODES
    // ----------------------------------------------------------------------
    cy.on("tap", "node", evt => {
        const node = evt.target;

        // NOT A MODEL → close panel
        if (node.data("entity") !== "model") {
            document.getElementById("info-panel").style.display = "none";
            return;
        }

        const model = node.data("fullModel");
        if (!model) {
            console.warn("No fullModel for node:", node.id());
            return;
        }

        const columns = model.columns;
        if (!columns || Object.keys(columns).length === 0) {
            console.warn("Model has no columns:", model);
            document.getElementById("info-panel").style.display = "none";
            return;
        }

        // SHOW PANEL
        const panel = document.getElementById("info-panel");
        const title = document.getElementById("panel-title");
        const content = document.getElementById("panel-content");

        title.innerText = model.target_table || model.model_name;

        let html = "";
        for (const [col, meta] of Object.entries(columns)) {
            html += `<div style="margin-bottom: 12px;">
                <strong>${col}</strong><br>
                <em>Sources:</em><br>`;

            // Accepts "source", "derived_from", etc
            const sources =
                (Array.isArray(meta.source) && meta.source) ||
                (meta.source ? [meta.source] : null) ||
                meta.derived_from ||
                [];

            if (sources.length) {
                html += sources.map(s => `- ${s}`).join("<br>");
            } else {
                html += `<span style="color:#888">(none)</span>`;
            }

            if (meta.transform || meta.transformation) {
                html += `<br><em>Transformation:</em><br>${meta.transform || meta.transformation}`;
            }

            html += `<hr></div>`;
        }

        content.innerHTML = html;
        panel.style.display = "block";
    });

})();
