export function readSavedEngines(database = indexedDB) {
  return new Promise((resolve, reject) => {
    const request = database.open("degoog");
    let absent = false;
    request.onupgradeneeded = () => {
      absent = true;
      request.transaction.abort();
    };
    request.onblocked = () => reject(new Error("Cannot read engine preferences"));
    request.onerror = () => absent ? resolve({}) : reject(new Error("Cannot read engine preferences"));
    request.onsuccess = () => {
      const db = request.result;
      try {
        if (!db.objectStoreNames.contains("settings")) {
          db.close();
          reject(new Error("Unknown Degoog preference schema"));
          return;
        }
        const tx = db.transaction("settings", "readonly");
        const get = tx.objectStore("settings").get("engines");
        get.onsuccess = () => resolve(get.result ?? {});
        get.onerror = () => reject(new Error("Cannot read engine preferences"));
        tx.oncomplete = tx.onabort = () => db.close();
      } catch (error) { db.close(); reject(error); }
    };
  });
}

export function enabledWebEngines(registry, saved) {
  if (!Array.isArray(registry.engines) || !saved || typeof saved !== "object")
    throw new Error("Cannot resolve engine preferences");
  return registry.engines.filter(engine => {
    const types = engine.searchTypes?.length ? engine.searchTypes : [engine.primaryType || "web"];
    return types.includes("web") && (saved[engine.id] ?? registry.defaults?.[engine.id] ?? true);
  }).map(engine => engine.id);
}

export async function researchEngines(base = "", fetcher = fetch, database = indexedDB) {
  const [response, saved] = await Promise.all([
    fetcher(`${base}/api/engines`), readSavedEngines(database),
  ]);
  if (!response.ok) throw new Error("Cannot load engine registry");
  return enabledWebEngines(await response.json(), saved);
}
