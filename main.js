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
    .map((b) => b.getElementsByTagName("Name")[0].textContent)
    .filter((name) => name.endsWith(".json"));
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
// HELPERS
// ---------------------------------------------------------------------------
function getModelId(model) {
  // Stable ID for model nodes
  return (
    model.model_name ||
    model.target_table ||
    (model.file_name ? model.file_name.replace(".sql", "") : null)
  );
}

function ensureNode(nodeMap, id, entity) {
  let node = nodeMap.get(id);
  if (!node) {
    node = {
      id,
      label: id,
      entity,       // "model" or "source"
      columns: {},  // will be filled with real / inferred columns
    };
    nodeMap.set(id, node);
  }
  return node;
}


// ---------------------------------------------------------------------------
// MAIN EXECUTION
// ---------------------------------------------------------------------------
(async () => {
  console.log("Loading JSON files…");

  const files = await listJsonFiles();
  console.log("Files found:", files);

  const models = [];
  for (const file of files) {
    const modelJson = await fetchJson(file);
    models.push(modelJson);
  }

  // Node registry and edge list
  const nodeMap = new Map();   // id -> { id, label, entity, fullModel?, columns }
  const edges = [];            // { source, target }

  // For inferring source columns from lineage
  const sourceColumnIndex = {}; // tableId -> colName -> { usedBy: [{ model, column }] }

  // ---------------------------------------------------------
  // PASS 1: REGISTER MODEL NODES + THEIR COLUMNS
  // ---------------------------------------------------------
  for (const model of models) {
    const targetId = getModelId(model);
    if (!targetId) {
      console.warn("❗ Model has no usable identifier:", model);
      continue;
    }

    // Ensure a model node
    const modelNode = ensureNode(nodeMap, targetId, "model");
    modelNode.label = targetId;
    modelNode.fullModel = model;

    // Attach model's own columns, if present
    if (model.columns) {
      modelNode.columns = model.columns;

      // For each column, inspect sources to infer source-table columns
      for (const [colName, meta] of Object.entries(model.columns)) {
        const allSourceRefs = [];

        // source may be string or array
        if (Array.isArray(meta.source)) {
          allSourceRefs.push(...meta.source);
        } else if (meta.source) {
          allSourceRefs.push(meta.source);
        }

        // also support "derived_from" if present
        if (Array.isArray(meta.derived_from)) {
          allSourceRefs.push(...meta.derived_from);
        } else if (meta.derived_from) {
          allSourceRefs.push(meta.derived_from);
        }

        for (const ref of allSourceRefs) {
          if (typeof ref !== "string") continue;

          const parts = ref.split(".");
          if (parts.length < 2) continue;

          const srcTableId = parts.slice(0, -1).join(".");
          const srcColName = parts[parts.length - 1];

          if (!sourceColumnIndex[srcTableId]) {
            sourceColumnIndex[srcTableId] = {};
          }
          if (!sourceColumnIndex[srcTableId][srcColName]) {
            sourceColumnIndex[srcTableId][srcColName] = { usedBy: [] };
          }

          sourceColumnIndex[srcTableId][srcColName].usedBy.push({
            model: targetId,
            column: colName,
          });
        }
      }
    }

    // -------------------------------------------------------
    // PASS 1b: REGISTER SOURCE NODES + EDGES FOR THIS MODEL
    // -------------------------------------------------------
    for (const src of model.sources || []) {
      const srcKey = src.table || src.model || "unknown_source";
      const srcId = `${src.name}.${srcKey}`;

      const srcNode = ensureNode(nodeMap, srcId, "source");
      srcNode.label = srcId;

      edges.push({ source: srcId, target: targetId });
    }
  }

  // ---------------------------------------------------------
  // PASS 2: APPLY INFERRED COLUMNS TO SOURCE NODES
  // ---------------------------------------------------------
  for (const [tableId, cols] of Object.entries(sourceColumnIndex)) {
    const srcNode = ensureNode(nodeMap, tableId, "source");
    if (!srcNode.columns) srcNode.columns = {};

    for (const [colName, info] of Object.entries(cols)) {
      if (!srcNode.columns[colName]) {
        srcNode.columns[colName] = {};
      }
      // attach "usedBy" info so we know downstream usage
      srcNode.columns[colName].usedBy = info.usedBy;
    }
  }

  // ---------------------------------------------------------
  // BUILD CYTOSCAPE ELEMENTS
  // ---------------------------------------------------------
  const allElements = [];

  // Nodes
  for (const node of nodeMap.values()) {
    allElements.push({
      data: {
        id: node.id,
        label: node.label,
        entity: node.entity,
        hasColumns: Object.keys(node.columns).length > 0
      },
      scratch: {
        columns: node.columns,     // <-- SAFE STORAGE
        fullModel: node.fullModel  // optional
      }
    });
  }

  // Edges
  for (const e of edges) {
    allElements.push({ data: { source: e.source, target: e.target } });
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
      padding: 50,
    },

    style: [
      // MODEL NODES (targets)
      {
        selector: 'node[entity="model"]',
        style: {
          "background-color": "#6cca98",
          "border-width": 3,
          "border-color": "#003057",
          "label": "data(label)",
          "font-size": 10,
          "text-valign": "center",
          "text-halign": "center",
        },
      },

      // SOURCE NODES (inferred columns)
      {
        selector: 'node[entity="source"]',
        style: {
          "background-color": "#003057",
          "color": "#f0f4ff",     // softer light text
          "label": "data(label)",
          "font-size": 9,
          "text-valign": "center",
          "text-halign": "center",
        },
      },

      // EDGES
      {
        selector: "edge",
        style: {
          "width": 2,
          "line-color": "#777",
          "target-arrow-color": "#777",
          "target-arrow-shape": "triangle",
        },
      },
    ],
  });

  window.cy = cy;

  console.log("Cytoscape initialised.");


  // ----------------------------------------------------------------------
  // NODE CLICK HANDLER — SHOW COLUMN LINEAGE FOR ANY NODE
  // ----------------------------------------------------------------------
  cy.on("tap", "node", (evt) => {
    const node = evt.target;
    const data = node.data();
    const columns = node.scratch('columns');

    const panel = document.getElementById("info-panel");
    const title = document.getElementById("panel-title");
    const content = document.getElementById("panel-content");

    if (!columns || Object.keys(columns).length === 0) {
      panel.style.display = "none";
      return;
    }

    title.innerText = data.id;
    let html = "";

    for (const [colName, meta] of Object.entries(columns)) {
      html += `<div style="margin-bottom: 12px;">
        <strong>${colName}</strong><br>`;

      // For models: show source lineage + transform
      if (data.entity === "model") {
        html += `<em>Sources:</em><br>`;

        const srcs = [];
        if (Array.isArray(meta.source)) {
          srcs.push(...meta.source);
        } else if (meta.source) {
          srcs.push(meta.source);
        }
        if (Array.isArray(meta.derived_from)) {
          srcs.push(...meta.derived_from);
        } else if (meta.derived_from) {
          srcs.push(meta.derived_from);
        }

        if (srcs.length) {
          html += srcs.map((s) => `- ${s}`).join("<br>");
        } else {
          html += `<span style="color:#888">(none)</span>`;
        }

        if (meta.transform || meta.transformation) {
          html += `<br><em>Transform:</em><br>${meta.transform || meta.transformation}`;
        }
      }

      // For sources: show which model/columns consume them
      if (data.entity === "source") {
        const usedBy = meta.usedBy || [];
        html += `<em>Used by:</em><br>`;
        if (usedBy.length) {
          html += usedBy
            .map((u) => `- ${u.model}.${u.column}`)
            .join("<br>");
        } else {
          html += `<span style="color:#888">(not referenced yet)</span>`;
        }
      }

      html += `<hr></div>`;
    }

    content.innerHTML = html;
    panel.style.display = "block";
  });
})();
