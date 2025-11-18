// SAS-enabled container URL
// Example:
// const containerUrl = "https://aicodeledgerlineage.blob.core.windows.net/lineage?sv=xxxx&sig=xxxx";
const containerUrl = "https://aicodeledgerlineage.blob.core.windows.net/lineage?sp=rl&st=2025-11-18T01:29:42Z&se=2026-11-18T09:44:42Z&spr=https&sv=2024-11-04&sr=c&sig=X8%2BwAKmeKXetzbfcVWDcpTaipOiahwXZzfaEJ2Qh8%2BE%3D";
const prefix = "local_repo/models/";


// ---------------------------------------------------------------------------
// LIST JSON FILES
// ---------------------------------------------------------------------------
async function listJsonFiles() {
  const listUrl = `${containerUrl}&restype=container&comp=list&prefix=${prefix}`;
  const res = await fetch(listUrl);
  const xmlText = await res.text();
  const xml = new DOMParser().parseFromString(xmlText, "application/xml");

  const blobs = [...xml.getElementsByTagName("Blob")];
  return blobs
    .map((b) => b.getElementsByTagName("Name")[0].textContent)
    .filter((n) => n.endsWith(".json"));
}


// ---------------------------------------------------------------------------
// FETCH JSON
// ---------------------------------------------------------------------------
async function fetchJson(name) {
  const blobBase = containerUrl.split("?")[0];
  const sas = "?" + containerUrl.split("?")[1];
  const url = `${blobBase}/${name}${sas}`;
  const res = await fetch(url);
  return res.json();
}


// ---------------------------------------------------------------------------
// CONVERT JSON MODEL TO CYTOSCAPE ELEMENTS
// ---------------------------------------------------------------------------
function convertToCytoscape(model) {

  // ALWAYS use model_name as model node ID (stable, correct, non-null)
  const target = model.model_name;

  if (!target) {
    console.warn("Missing model_name:", model);
    return { nodes: [], edges: [] };
  }

  let nodes = [];
  let edges = [];

  // MODEL NODE
  nodes.push({
    data: {
      id: target,
      label: target,
      entity: "model",
      fullModel: model
    }
  });

  // SOURCE / REF NODES
  for (const src of model.sources || []) {
    const srcKey = src.table || src.model || "unknown_source";
    const srcId = `${src.name}.${srcKey}`;

    nodes.push({
      data: {
        id: srcId,
        label: srcId,
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
// MAIN
// ---------------------------------------------------------------------------
(async () => {
  const files = await listJsonFiles();

  let elements = [];

  for (const file of files) {
    const modelJson = await fetchJson(file);
    const { nodes, edges } = convertToCytoscape(modelJson);
    elements.push(...nodes, ...edges);
  }

  // -------------------------------------------------------
  // INITIALIZE CYTOSCAPE WITH DAG LAYOUT
  // -------------------------------------------------------
  const cy = cytoscape({
    container: document.getElementById("cy"),
    elements,

    layout: {
      name: "breadthfirst",   // more readable than cose
      directed: true,
      roots: elements.filter(e => e.data?.entity === "model").map(e => e.data.id),
      padding: 50,
      spacingFactor: 1.2
    },

    style: [
      // MODEL NODES = green
      {
        selector: 'node[entity="model"]',
        style: {
          "background-color": "#6cca98",
          "border-color": "#003057",
          "border-width": 3,
          "font-size": "12px",
          "label": "data(label)",
          "text-valign": "center",
          "text-halign": "center",
          "width": 50,
          "height": 50
        }
      },
      // SOURCE NODES = blue
      {
        selector: 'node[entity="source"]',
        style: {
          "background-color": "#003057",
          "color": "#ffffff",
          "font-size": "10px",
          "label": "data(label)",
          "width": 35,
          "height": 35
        }
      },
      // edges
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

  // -------------------------------------------------------
  // NODE CLICK HANDLER (MODEL NODES ONLY)
  // -------------------------------------------------------
  cy.on("tap", "node", (evt) => {
    const n = evt.target;

    if (n.data("entity") !== "model") {
      document.getElementById("info-panel").style.display = "none";
      return;
    }

    const model = n.data("fullModel");
    const cols = model.columns;

    if (!cols) {
      document.getElementById("info-panel").style.display = "none";
      return;
    }

    const panel = document.getElementById("info-panel");
    const title = document.getElementById("panel-title");
    const content = document.getElementById("panel-content");

    title.innerText = model.model_name;

    let html = "";
    for (const [col, meta] of Object.entries(cols)) {
      const srcs =
        meta.source ||
        meta.derived_from ||
        [];

      html += `
        <div>
          <strong>${col}</strong><br>
          <em>Sources:</em><br>
          ${Array.isArray(srcs)
            ? srcs.map(s => "- " + s).join("<br>")
            : "- " + srcs}
          <br>
          ${meta.transform ? `<em>Transform:</em><br>${meta.transform}` : ""}
          ${meta.transformation ? `<em>Transform:</em><br>${meta.transformation}` : ""}
          <hr>
        </div>`;
    }

    content.innerHTML = html;
    panel.style.display = "block";
  });
})();
