// SAS-enabled container URL
// Example:
// const containerUrl = "https://aicodeledgerlineage.blob.core.windows.net/lineage?sv=xxxx&sig=xxxx";
const containerUrl = "https://aicodeledgerlineage.blob.core.windows.net/lineage?sp=rl&st=2025-11-18T01:29:42Z&se=2026-11-18T09:44:42Z&spr=https&sv=2024-11-04&sr=c&sig=X8%2BwAKmeKXetzbfcVWDcpTaipOiahwXZzfaEJ2Qh8%2BE%3D";
?")[0];
  const sas = "?" + containerUrl.split("?")[1];
  const url = `${blobBase}/${name}${sas}`;
  const res = await fetch(url);
  return res.json();
}


// ---------------------------------------------------------------------------
// HELPERS
// ---------------------------------------------------------------------------
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
      _meta: { columns: {} }
    };
    nodeMap.set(id, node);
  }
  return node;
}


// ---------------------------------------------------------------------------
// MAIN
// ---------------------------------------------------------------------------
(async () => {
  const files = await listJsonFiles();
  const models = [];

  for (const f of files) {
    models.push(await fetchJson(f));
  }

  const nodeMap = new Map();
  const edges = [];
  const sourceColumnIndex = {};

  // ---------------------------------------------------------
  // PASS 1: REGISTER MODELS + THEIR COLUMNS
  // ---------------------------------------------------------
  for (const model of models) {
    const targetId = getModelId(model);
    if (!targetId) continue;

    const modelNode = ensureNode(nodeMap, targetId, "model");
    modelNode._meta.fullModel = model;

    if (model.columns) {
      modelNode._meta.columns = model.columns;

      for (const [col, meta] of Object.entries(model.columns)) {
        const refs = [];

        if (Array.isArray(meta.source)) refs.push(...meta.source);
        else if (meta.source) refs.push(meta.source);

        if (Array.isArray(meta.derived_from)) refs.push(...meta.derived_from);
        else if (meta.derived_from) refs.push(meta.derived_from);

        for (const r of refs) {
          const p = r.split(".");
          const srcTable = p.slice(0, -1).join(".");
          const srcCol = p[p.length - 1];

          if (!sourceColumnIndex[srcTable]) {
            sourceColumnIndex[srcTable] = {};
          }
          if (!sourceColumnIndex[srcTable][srcCol]) {
            sourceColumnIndex[srcTable][srcCol] = { usedBy: [] };
          }

          sourceColumnIndex[srcTable][srcCol].usedBy.push({
            model: targetId,
            column: col
          });
        }
      }
    }

    // Register all sources
    for (const src of model.sources || []) {
      const srcKey = src.table || src.model || "unknown_source";
      const srcId = `${src.name}.${srcKey}`;
      ensureNode(nodeMap, srcId, "source");

      edges.push({ source: srcId, target: targetId });
    }
  }

  // ---------------------------------------------------------
  // PASS 2: APPLY INFERRED SOURCE COLUMNS
  // ---------------------------------------------------------
  for (const [tblId, cols] of Object.entries(sourceColumnIndex)) {
    const srcNode = ensureNode(nodeMap, tblId, "source");

    for (const [col, info] of Object.entries(cols)) {
      if (!srcNode._meta.columns[col]) {
        srcNode._meta.columns[col] = {};
      }
      srcNode._meta.columns[col].usedBy = info.usedBy;
    }
  }

  // ---------------------------------------------------------
  // BUILD CY ELEMENTS
  // ---------------------------------------------------------
  const elements = [];

  for (const node of nodeMap.values()) {
    elements.push({ data: node });
  }

  for (const e of edges) {
    elements.push({ data: { source: e.source, target: e.target } });
  }

  // ---------------------------------------------------------
  // INIT CY
  // ---------------------------------------------------------
  const cy = cytoscape({
    container: document.getElementById("cy"),
    elements,
    layout: { name: "cose", padding: 50 },

    style: [
      {
        selector: 'node[entity="model"]',
        style: {
          "background-color": "#6cca98",
          "border-width": 3,
          "border-color": "#003057",
          "label": "data(label)",
          "font-size": 10
        }
      },
      {
        selector: 'node[entity="source"]',
        style: {
          "background-color": "#003057",
          "color": "#e6efff",
          "label": "data(label)",
          "font-size": 9
        }
      },
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

  window.cy = cy;

  // ---------------------------------------------------------
  // CLICK HANDLER — ANY NODE CAN SHOW COLUMNS NOW
  // ---------------------------------------------------------
  cy.on("tap", "node", (evt) => {
    const n = evt.target;
    const meta = n.data("_meta");

    if (!meta || !meta.columns || !Object.keys(meta.columns).length) {
      document.getElementById("info-panel").style.display = "none";
      return;
    }

    const panel = document.getElementById("info-panel");
    const title = document.getElementById("panel-title");
    const content = document.getElementById("panel-content");

    title.innerText = n.id();

    let html = "";
    for (const [col, details] of Object.entries(meta.columns)) {
      html += `<div style="margin-bottom: 12px;">
        <strong>${col}</strong><br>`;

      if (details.usedBy) {
        html += `<em>Used By:</em><br>`;
        html += details.usedBy.map(u => `- ${u.model}.${u.column}`).join("<br>");
      }

      if (details.source || details.derived_from || details.transform || details.transformation) {
        html += `<em>Lineage:</em><br>`;
        const s = [];

        if (Array.isArray(details.source)) s.push(...details.source);
        else if (details.source) s.push(details.source);

        if (Array.isArray(details.derived_from)) s.push(...details.derived_from);
        else if (details.derived_from) s.push(details.derived_from);

        html += s.map(x => `- ${x}`).join("<br>");
      }

      html += `<hr></div>`;
    }

    content.innerHTML = html;
    panel.style.display = "block";
  });
})();
