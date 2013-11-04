/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.script;

import com.google.common.base.Charsets;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableMap;
import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.fielddata.IndexFieldDataService;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.watcher.FileChangesListener;
import org.elasticsearch.watcher.FileWatcher;
import org.elasticsearch.watcher.ResourceWatcherService;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class ScriptService extends AbstractComponent {

    private final String defaultLang;

    private final ImmutableMap<String, ScriptEngineService> scriptEngines;

    private final ConcurrentMap<String, CompiledScript> staticCache = ConcurrentCollections.newConcurrentMap();

    private final Cache<CacheKey, CompiledScript> cache;
    private final File scriptsDirectory;

    private final boolean disableDynamic;

    @Inject
    public ScriptService(Settings settings, Environment env, Set<ScriptEngineService> scriptEngines,
                         ResourceWatcherService resourceWatcherService) {
        super(settings);

        int cacheMaxSize = componentSettings.getAsInt("cache.max_size", 500);
        TimeValue cacheExpire = componentSettings.getAsTime("cache.expire", null);
        logger.debug("using script cache with max_size [{}], expire [{}]", cacheMaxSize, cacheExpire);

        this.defaultLang = componentSettings.get("default_lang", "mvel");
        this.disableDynamic = componentSettings.getAsBoolean("disable_dynamic", false);

        CacheBuilder cacheBuilder = CacheBuilder.newBuilder();
        if (cacheMaxSize >= 0) {
            cacheBuilder.maximumSize(cacheMaxSize);
        }
        if (cacheExpire != null) {
            cacheBuilder.expireAfterAccess(cacheExpire.nanos(), TimeUnit.NANOSECONDS);
        }
        this.cache = cacheBuilder.build();

        ImmutableMap.Builder<String, ScriptEngineService> builder = ImmutableMap.builder();
        for (ScriptEngineService scriptEngine : scriptEngines) {
            for (String type : scriptEngine.types()) {
                builder.put(type, scriptEngine);
            }
        }
        this.scriptEngines = builder.build();

        // put some default optimized scripts
        staticCache.put("doc.score", new CompiledScript("native", new DocScoreNativeScriptFactory()));

        // add file watcher for static scripts
        scriptsDirectory = new File(env.configFile(), "scripts");
        FileWatcher fileWatcher = new FileWatcher(scriptsDirectory);
        fileWatcher.addListener(new ScriptChangesListener());

        if (componentSettings.getAsBoolean("auto_reload_enabled", true)) {
            // automatic reload is enabled - register scripts
            resourceWatcherService.add(fileWatcher);
        } else {
            // automatic reload is disable just load scripts once
            fileWatcher.init();
        }
    }

    public void close() {
        for (ScriptEngineService engineService : scriptEngines.values()) {
            engineService.close();
        }
    }

    public CompiledScript compile(String script) {
        return compile(defaultLang, script);
    }

    public CompiledScript compile(String lang, String script) {
        CompiledScript compiled = staticCache.get(script);
        if (compiled != null) {
            return compiled;
        }
        if (lang == null) {
            lang = defaultLang;
        }
        if (dynamicScriptDisabled(lang)) {
            throw new ScriptException("dynamic scripting disabled");
        }
        CacheKey cacheKey = new CacheKey(lang, script);
        compiled = cache.getIfPresent(cacheKey);
        if (compiled != null) {
            return compiled;
        }
        // not the end of the world if we compile it twice...
        ScriptEngineService service = scriptEngines.get(lang);
        if (service == null) {
            throw new ElasticSearchIllegalArgumentException("script_lang not supported [" + lang + "]");
        }
        compiled = new CompiledScript(lang, service.compile(script));
        cache.put(cacheKey, compiled);
        return compiled;
    }

    public ExecutableScript executable(String lang, String script, Map vars) {
        return executable(compile(lang, script), vars);
    }

    public ExecutableScript executable(CompiledScript compiledScript, Map vars) {
        return scriptEngines.get(compiledScript.lang()).executable(compiledScript.compiled(), vars);
    }

    public SearchScript search(CompiledScript compiledScript, SearchLookup lookup, @Nullable Map<String, Object> vars) {
        return scriptEngines.get(compiledScript.lang()).search(compiledScript.compiled(), lookup, vars);
    }

    public SearchScript search(SearchLookup lookup, String lang, String script, @Nullable Map<String, Object> vars) {
        return search(compile(lang, script), lookup, vars);
    }

    public SearchScript search(MapperService mapperService, IndexFieldDataService fieldDataService, String lang, String script, @Nullable Map<String, Object> vars) {
        return search(compile(lang, script), new SearchLookup(mapperService, fieldDataService, null), vars);
    }

    public Object execute(CompiledScript compiledScript, Map vars) {
        return scriptEngines.get(compiledScript.lang()).execute(compiledScript.compiled(), vars);
    }

    public void clear() {
        cache.invalidateAll();
    }

    private boolean dynamicScriptDisabled(String lang) {
        if (!disableDynamic) {
            return false;
        }
        // we allow "native" executions since they register through plugins, so they are "allowed"
        return !"native".equals(lang);
    }

    private class ScriptChangesListener extends FileChangesListener {

        private Tuple<String, String> scriptNameExt(File file) {
            String scriptPath = scriptsDirectory.toURI().relativize(file.toURI()).getPath();
            int extIndex = scriptPath.lastIndexOf('.');
            if (extIndex != -1) {
                String ext = scriptPath.substring(extIndex + 1);
                String scriptName = scriptPath.substring(0, extIndex).replace(File.separatorChar, '_');
                return new Tuple<String, String>(scriptName, ext);
            } else {
                return null;
            }
        }

        @Override
        public void onFileInit(File file) {
            Tuple<String, String> scriptNameExt = scriptNameExt(file);
            if (scriptNameExt != null) {
                boolean found = false;
                for (ScriptEngineService engineService : scriptEngines.values()) {
                    for (String s : engineService.extensions()) {
                        if (s.equals(scriptNameExt.v2())) {
                            found = true;
                            try {
                                logger.trace("compiling script file " + file.getAbsolutePath());
                                String script = Streams.copyToString(new InputStreamReader(new FileInputStream(file), Charsets.UTF_8));
                                staticCache.put(scriptNameExt.v1(), new CompiledScript(engineService.types()[0], engineService.compile(script)));
                            } catch (Throwable e) {
                                logger.warn("failed to load/compile script [{}]", e, scriptNameExt.v1());
                            }
                            break;
                        }
                    }
                    if (found) {
                        break;
                    }
                }
                if (!found) {
                    logger.warn("no script engine found for [{}]", scriptNameExt.v2());
                }
            }
        }

        @Override
        public void onFileCreated(File file) {
            onFileInit(file);
        }

        @Override
        public void onFileDeleted(File file) {
            Tuple<String, String> scriptNameExt = scriptNameExt(file);
            logger.trace("removing script file " + file.getAbsolutePath());
            staticCache.remove(scriptNameExt.v1());
        }

        @Override
        public void onFileChanged(File file) {
            onFileInit(file);
        }

    }

    public static class CacheKey {
        public final String lang;
        public final String script;

        public CacheKey(String lang, String script) {
            this.lang = lang;
            this.script = script;
        }

        @Override
        public boolean equals(Object o) {
            CacheKey other = (CacheKey) o;
            return lang.equals(other.lang) && script.equals(other.script);
        }

        @Override
        public int hashCode() {
            return lang.hashCode() + 31 * script.hashCode();
        }
    }

    public static class DocScoreNativeScriptFactory implements NativeScriptFactory {
        @Override
        public ExecutableScript newScript(@Nullable Map<String, Object> params) {
            return new DocScoreSearchScript();
        }
    }

    public static class DocScoreSearchScript extends AbstractFloatSearchScript {
        @Override
        public float runAsFloat() {
            try {
                return doc().score();
            } catch (IOException e) {
                return 0;
            }
        }
    }
}
