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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.cache.field.data.FieldDataCache;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.script.mvel.MvelScriptEngineService;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

/**
 *
 */
public class ScriptService extends AbstractComponent {

    private final String defaultLang;

    private final ImmutableMap<String, ScriptEngineService> scriptEngines;

    private final ConcurrentMap<String, CompiledScript> staticCache = ConcurrentCollections.newConcurrentMap();

    // TODO expose some cache aspects like expiration and max size
    private final Cache<CacheKey, CompiledScript> cache = CacheBuilder.newBuilder().build();

    private final boolean disableDynamic;

    public ScriptService(Settings settings) {
        this(settings, new Environment(), ImmutableSet.<ScriptEngineService>builder()
                .add(new MvelScriptEngineService(settings))
                .build()
        );
    }

    @Inject
    public ScriptService(Settings settings, Environment env, Set<ScriptEngineService> scriptEngines) {
        super(settings);

        this.defaultLang = componentSettings.get("default_lang", "mvel");
        this.disableDynamic = componentSettings.getAsBoolean("disable_dynamic", false);

        ImmutableMap.Builder<String, ScriptEngineService> builder = ImmutableMap.builder();
        for (ScriptEngineService scriptEngine : scriptEngines) {
            for (String type : scriptEngine.types()) {
                builder.put(type, scriptEngine);
            }
        }
        this.scriptEngines = builder.build();

        // put some default optimized scripts
        staticCache.put("doc.score", new CompiledScript("native", new DocScoreNativeScriptFactory()));

        // compile static scripts
        File scriptsFile = new File(env.configFile(), "scripts");
        if (scriptsFile.exists()) {
            processScriptsDirectory("", scriptsFile);
        }
    }

    private void processScriptsDirectory(String prefix, File dir) {
        for (File file : dir.listFiles()) {
            if (file.isDirectory()) {
                processScriptsDirectory(prefix + file.getName() + "_", file);
            } else {
                int extIndex = file.getName().lastIndexOf('.');
                if (extIndex != -1) {
                    String ext = file.getName().substring(extIndex + 1);
                    String scriptName = prefix + file.getName().substring(0, extIndex);
                    boolean found = false;
                    for (ScriptEngineService engineService : scriptEngines.values()) {
                        for (String s : engineService.extensions()) {
                            if (s.equals(ext)) {
                                found = true;
                                try {
                                    String script = Streams.copyToString(new InputStreamReader(new FileInputStream(file), "UTF-8"));
                                    staticCache.put(scriptName, new CompiledScript(engineService.types()[0], engineService.compile(script)));
                                } catch (Exception e) {
                                    logger.warn("failed to load/compile script [{}]", e, scriptName);
                                }
                                break;
                            }
                        }
                        if (found) {
                            break;
                        }
                    }
                    if (!found) {
                        logger.warn("no script engine found for [{}]", ext);
                    }
                }
            }
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

    public SearchScript search(MapperService mapperService, FieldDataCache fieldDataCache, String lang, String script, @Nullable Map<String, Object> vars) {
        return search(compile(lang, script), new SearchLookup(mapperService, fieldDataCache, null), vars);
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
