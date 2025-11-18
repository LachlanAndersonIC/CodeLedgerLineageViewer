// SAS-enabled container URL
// Example:
// const containerUrl = "https://aicodeledgerlineage.blob.core.windows.net/lineage?sv=xxxx&sig=xxxx";
const containerUrl = "https://aicodeledgerlineage.blob.core.windows.net/lineage?sp=rl&st=2025-11-18T01:29:42Z&se=2026-11-18T09:44:42Z&spr=https&sv=2024-11-04&sr=c&sig=X8%2BwAKmeKXetzbfcVWDcpTaipOiahwXZzfaEJ2Qh8%2BE%3D";
const prefix = "local_repo/models/";

async function listJsonFiles() {
  const listUrl = `${containerUrl}&restype=container&comp=list&prefix=${prefix}`;
  const res = await fetch(listUrl);
  const xml = new DOMParser().parseFromString(await res.text(), "application/xml");
  return [...xml.getElementsByTagName("Blob")]
    .map((b) => b.getElementsByTagName("Name")[0].textContent)
    .filter((name) => name.endsWith(".json"));
}

async function fetchJson(name) {
  const base = containerUrl.split("?")[0];
  const sas = "?" + containerUrl.split("?")[1];
  const res = await fetch(`${base}/${name}${sas}`);
  return res.json();
}

function getModelId(model) {
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
      entity,
      columns: {},
      fullModel: null,
    };
    nodeMap.set(id, node);
  }
  return node;
}

(async () => {
  const files = await listJsonFiles();
  const models = [];
  for (const file of files) {
    models.push(await fetchJson(file));
  }

  const nodeMap = new Map();
  const edges = [];
  const sourceColumnIndex = {};

  // Pass 1 — construct nodes & collect model columns
  for (const model of models) {
    const targetId = getModelId(model);
    if (!targetId) continue;

    const modelNode = ensureNode(nodeMap, targetId, "model");
    modelNode.label = targetId;
    modelNode.fullModel = model;

    if (model.columns) {
      modelNode.columns = model.columns;

      for (const [colName, meta] of Object.entries(model.columns)) {
        const refs = [];

        if (Array.isArray(meta.source)) refs.push(...meta.source);
        else if (meta.source) refs.push(meta.source);

        if (Array.isArray(meta.derived_from)) refs.push(...meta.derived_from);
        else if (meta.derived_from) refs.push(meta.derived_from);

        for (const ref of refs) {
          if (typeof ref !== "string") continue;
          const parts = ref.split(".");
          if (parts.length < 2) continue;

          const srcTable = parts.slice(0, -1).join(".");
          const srcCol = parts[parts.length - 1];

          sourceColumnIndex[srcTable] ??= {};
          sourceColumnIndex[srcTable][srcCol] ??= { usedBy: [] };
          sourceColumnIndex[srcTable][srcCol].usedBy.push({
            model: targetId,
            column: colName,
          });
        }
      }
    }

    for (const src of model.sources || []) {
      const srcKey = src.table || src.model || "unknown_source";
      const srcId = `${src.name}.${srcKey}`;
      ensureNode(nodeMap, srcId, "source");
      edges.push({ source: srcId, target: targetId });
    }
  }

  // Pass 2 — apply inferred columns to sources
  for (const [tbl, cols] of Object.entries(sourceColumnIndex)) {
    const srcNode = ensureNode(nodeMap, tbl, "source");
    srcNode.columns ??= {};
    for (const [colName, info] of Object.entries(cols)) {
      srcNode.columns[colName] ??= {};
      srcNode.columns[colName].usedBy = info.usedBy;
    }
  }

  // Build Cytoscape elements
  const allElements = [];
  for (const node of nodeMap.values()) {
    allElements.push({
      data: {
        id: node.id,
        label: node.label,
        entity: node.entity,
        hasColumns: Object.keys(node.columns).length > 0,
      },
      scratch: {
        columns: node.columns,
        fullModel: node.fullModel,
      },
    });
  }

  for (const e of edges) {
    allElements.push({ data: { source: e.source, target: e.target } });
  }

  const cy = cytoscape({
    container: document.getElementById("cy"),
    elements: allElements,
    layout: {
      name: "cose",
      padding: 50,
    },
    style: [
      {
        selector: 'node[entity="model"]',
        style: {
          "background-color": "#6cca98",
          "border-width": 0,
          "label": "data(label)",
          "font-size": 11,
          "text-valign": "center",
          "text-halign": "center",
          "color": "#003057",
          "text-outline-width": 2,
          "text-outline-color": "#ffffff",
        },
      },
      {
        selector: 'node[entity="source"]',
        style: {
          "background-color": "#003057",
          "color": "#ffffff",
          "label": "data(label)",
          "font-size": 10,
          "text-valign": "center",
          "text-halign": "center",
          "text-outline-width": 2,
          "text-outline-color": "#003057",
        },
      },
      {
        selector: "edge",
        style: {
          "width": 2,
          "line-color": "#999",
          "target-arrow-color": "#999",
          "target-arrow-shape": "triangle",
          "curve-style": "bezier",
        },
      },
    ],
  });

  window.cy = cy;

  // Ensure scratch is attached (Cytoscape ignores scratch in JSON init)
  cy.nodes().forEach((ele) => {
    const id = ele.data("id");
    const original = nodeMap.get(id);
    if (original) {
      ele.scratch("columns", original.columns);
      ele.scratch("fullModel", original.fullModel);
    }
  });

  // Click handler
  cy.on("tap", "node", (evt) => {
    const node = evt.target;
    const data = node.data();
    const columns = node.scratch("columns");

    const panel = document.getElementById("info-panel");
    const title = document.getElementById("panel-title");
    const content = document.getElementById("panel-content");

    if (!columns || Object.keys(columns).length === 0) {
      panel.style.display = "none";
      return;
    }

    panel.style.display = "block";
    title.innerText = data.id;

    let html = "";
    for (const [colName, meta] of Object.entries(columns)) {
      html += `<div class="column-card">
        <strong>${colName}</strong><br>`;

      if (data.entity === "model") {
        html += `<em>Sources:</em><br>`;
        const srcs = [];

        if (meta.source) srcs.push(...(Array.isArray(meta.source) ? meta.source : [meta.source]));
        if (meta.derived_from) srcs.push(...(Array.isArray(meta.derived_from) ? meta.derived_from : [meta.derived_from]));

        html += srcs.length
          ? srcs.map((s) => `- ${s}`).join("<br>")
          : `<span style='color:#888'>(none)</span>`;

        if (meta.transform || meta.transformation) {
          html += `<br><em>Transform:</em><br>${meta.transform || meta.transformation}`;
        }
      }

      if (data.entity === "source") {
        const used = meta.usedBy || [];
        html += `<em>Used by:</em><br>`;
        html += used.length
          ? used.map((u) => `- ${u.model}.${u.column}`).join("<br>")
          : `<span style='color:#888'>(not referenced)</span>`;
      }

      html += `</div>`;
    }

    content.innerHTML = html;
  });
})();
