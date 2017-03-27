/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import org.apache.lucene.util.IOUtils;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.storedscripts.DeleteStoredScriptRequest;
import org.elasticsearch.action.admin.cluster.storedscripts.DeleteStoredScriptResponse;
import org.elasticsearch.action.admin.cluster.storedscripts.GetStoredScriptRequest;
import org.elasticsearch.action.admin.cluster.storedscripts.PutStoredScriptRequest;
import org.elasticsearch.action.admin.cluster.storedscripts.PutStoredScriptResponse;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.watcher.ResourceWatcherService;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableMap;

/**
 * Source of scripts that are run in various parts of Elasticsearch.
 */
public class ScriptService extends AbstractComponent implements Closeable, ClusterStateListener {

    static final String DISABLE_DYNAMIC_SCRIPTING_SETTING = "script.disable_dynamic";

    public static final Setting<Integer> SCRIPT_CACHE_SIZE_SETTING =
        Setting.intSetting("script.cache.max_size", 100, 0, Property.NodeScope);
    public static final Setting<TimeValue> SCRIPT_CACHE_EXPIRE_SETTING =
        Setting.positiveTimeSetting("script.cache.expire", TimeValue.timeValueMillis(0), Property.NodeScope);
    public static final Setting<Boolean> SCRIPT_AUTO_RELOAD_ENABLED_SETTING =
        Setting.boolSetting("script.auto_reload_enabled", true, Property.NodeScope);
    public static final Setting<Integer> SCRIPT_MAX_SIZE_IN_BYTES =
        Setting.intSetting("script.max_size_in_bytes", 65535, Property.NodeScope);
    public static final Setting<Integer> SCRIPT_MAX_COMPILATIONS_PER_MINUTE =
        Setting.intSetting("script.max_compilations_per_minute", 15, 0, Property.Dynamic, Property.NodeScope);

    private final Collection<ScriptEngineService> scriptEngines;
    private final Map<String, ScriptEngineService> scriptEnginesByLang;
    private final Map<String, ScriptEngineService> scriptEnginesByExt;

    private final ScriptMetrics scriptMetrics;
    private final ScriptPermits scriptPermits;
    private final CachingCompiler<CacheKey> compiler;
    private final int maxScriptSizeInBytes;

    /**
     * Build the service.
     *
     * @param settings common settings loaded at node startup
     * @param env environment in which the node is running. Used to resolve the
     *        {@code config/scripts} directory that is scanned periodically for scripts.
     * @param resourceWatcherService Scans the {@code config/scripts} directory.
     * @param scriptEngineRegistry all {@link ScriptEngineService}s that we support. This delegates
     *        to those engines to build the actual executable.
     * @param scriptContextRegistry all {@link ScriptContext}s that we support.
     * @param scriptSettings settings for scripts
     * @param scriptMetrics compilation metrics for scripts. This should be shared between
     *        {@link ScriptService} and {@link TemplateService}
     * @throws IOException If there is an error scanning the {@code config/scripts} directory.
     */
    public ScriptService(Settings settings, Environment env,
            ResourceWatcherService resourceWatcherService,
            ScriptEngineRegistry scriptEngineRegistry, ScriptContextRegistry scriptContextRegistry,
            ScriptSettings scriptSettings, ScriptMetrics scriptMetrics) throws IOException {
        super(settings);
        if (Strings.hasLength(settings.get(DISABLE_DYNAMIC_SCRIPTING_SETTING))) {
            throw new IllegalArgumentException(DISABLE_DYNAMIC_SCRIPTING_SETTING + " is not a supported setting, replace with "
                    + "fine-grained script settings. Dynamic scripts can be enabled for all languages and all operations by replacing "
                    + "`script.disable_dynamic: false` with `script.inline: true` and `script.stored: true` in elasticsearch.yml");
        }

        Objects.requireNonNull(scriptEngineRegistry);
        this.scriptEngines = scriptEngineRegistry.getRegisteredLanguages().values();
        Objects.requireNonNull(scriptContextRegistry);

        Map<String, ScriptEngineService> enginesByLangBuilder = new HashMap<>();
        Map<String, ScriptEngineService> enginesByExtBuilder = new HashMap<>();
        for (ScriptEngineService scriptEngine : scriptEngines) {
            String language = scriptEngineRegistry.getLanguage(scriptEngine.getClass());
            enginesByLangBuilder.put(language, scriptEngine);
            enginesByExtBuilder.put(scriptEngine.getExtension(), scriptEngine);
        }
        this.scriptEnginesByLang = unmodifiableMap(enginesByLangBuilder);
        this.scriptEnginesByExt = unmodifiableMap(enginesByExtBuilder);

        maxScriptSizeInBytes = ScriptService.SCRIPT_MAX_SIZE_IN_BYTES.get(settings);
        this.scriptMetrics = scriptMetrics;
        this.scriptPermits = new ScriptPermits(settings, scriptSettings, scriptContextRegistry);
        this.compiler = new CachingCompiler<CacheKey>(settings, env, resourceWatcherService, scriptMetrics, "script") {
            @Override
            protected CacheKey cacheKeyForFile(String baseName, String extension) {
                if (extension.equals("mustache")) {
                    // For backwards compatibility mustache templates are in the scripts directory and we must ignore them here
                    return null;
                }
                ScriptEngineService engine = scriptEnginesByExt.get(extension);
                if (engine == null) {
                    logger.warn("script file extension not supported [{}.{}]", baseName, extension);
                    return null;
                }
                return new CacheKey(engine.getType(), baseName, null);
            }

            @Override
            protected CacheKey cacheKeyFromClusterState(StoredScriptSource scriptMetadata) {
                return new CacheKey(scriptMetadata.getLang(), scriptMetadata.getCode(), scriptMetadata.getOptions());
            }

            @Override
            protected StoredScriptSource lookupStoredScript(ScriptMetaData scriptMetaData, CacheKey cacheKey) {
                if (cacheKey.lang != null && isLangSupported(cacheKey.lang) == false) {
                    throw new IllegalArgumentException("unable to get stored script with unsupported lang [" + cacheKey.lang + "]");
                }

                String id = cacheKey.idOrCode;
                String[] path = id.split("/");

                if (path.length != 1) {
                    throw new IllegalArgumentException("illegal stored script format [" + id + "] use only <id>");
                }

                return scriptMetaData.getStoredScript(id, cacheKey.lang);
            }

            @Override
            protected boolean anyScriptContextsEnabled(CacheKey cacheKey, ScriptType scriptType) {
                for (ScriptContext scriptContext : scriptContextRegistry.scriptContexts()) {
                    if (scriptPermits.checkContextPermissions(cacheKey.lang, scriptType, scriptContext)) {
                        return true;
                    }
                }
                return false;
            }

            @Override
            protected void checkContextPermissions(CacheKey cacheKey, ScriptType scriptType, ScriptContext scriptContext) {
                if (isLangSupported(cacheKey.lang) == false) {
                    throw new IllegalArgumentException("script_lang not supported [" + cacheKey.lang + "]");
                }
                // TODO: fix this through some API or something, this is a silly way to do this
                // special exception to prevent expressions from compiling as update or mapping scripts
                boolean expression = "expression".equals(cacheKey.lang);
                boolean notSupported = scriptContext.getKey().equals(ScriptContext.Standard.UPDATE.getKey());
                if (expression && notSupported) {
                    throw new UnsupportedOperationException("scripts of type [" + scriptType + "]," +
                        " operation [" + scriptContext.getKey() + "] and lang [" + cacheKey.lang + "] are not supported");
                }
                if (scriptPermits.checkContextPermissions(cacheKey.lang, scriptType, scriptContext) == false) {
                    throw new IllegalStateException("scripts of type [" + scriptType + "]," +
                            " operation [" + scriptContext.getKey() + "] and lang [" + cacheKey.lang + "] are disabled");
                }
            }

            @Override
            protected void checkCompilationLimit() {
                scriptPermits.checkCompilationLimit();
            }

            @Override
            protected CompiledScript compile(ScriptType scriptType, CacheKey cacheKey) {
                return compile(scriptType, cacheKey, cacheKey.idOrCode, null);
            }

            @Override
            protected CompiledScript compileFileScript(CacheKey cacheKey, String body, Path file) {
                // pass the actual file name to the compiler (for script engines that care about this)
                return compile(ScriptType.FILE, cacheKey, body, file.getFileName().toString());
            }

            private CompiledScript compile(ScriptType scriptType, CacheKey cacheKey, String body, String fileName) {
                ScriptEngineService engine = getScriptEngineServiceForLang(cacheKey.lang);
                Object executable = engine.compile(fileName, body, cacheKey.options);
                return new CompiledScript(scriptType, body, engine.getType(), executable);
            }
        };
    }

    @Override
    public void close() throws IOException {
        IOUtils.close(scriptEngines);
    }

    public void registerClusterSettingsListeners(ClusterSettings clusterSettings) {
        scriptPermits.registerClusterSettingsListeners(clusterSettings);
    }

    private ScriptEngineService getScriptEngineServiceForLang(String lang) {
        ScriptEngineService scriptEngineService = scriptEnginesByLang.get(lang);
        if (scriptEngineService == null) {
            throw new IllegalArgumentException("script_lang not supported [" + lang + "]");
        }
        return scriptEngineService;
    }

    /**
     * Checks if a script can be executed and compiles it if needed, or returns the previously compiled and cached script.
     */
    public CompiledScript compile(Script script, ScriptContext scriptContext) {
        Objects.requireNonNull(script);
        Objects.requireNonNull(scriptContext);

        CacheKey cacheKey = new CacheKey(script.getLang(), script.getIdOrCode(), script.getOptions());
        return compiler.getScript(cacheKey, script.getType(), scriptContext);
    }

    public boolean isLangSupported(String lang) {
        Objects.requireNonNull(lang);

        return scriptEnginesByLang.containsKey(lang);
    }

    /**
     * Compiles (or retrieves from cache) and executes the provided script
     */
    public ExecutableScript executable(Script script, ScriptContext scriptContext) {
        return executable(compile(script, scriptContext), script.getParams());
    }

    /**
     * Executes a previously compiled script provided as an argument
     */
    public ExecutableScript executable(CompiledScript compiledScript, Map<String, Object> params) {
        return getScriptEngineServiceForLang(compiledScript.lang()).executable(compiledScript, params);
    }

    /**
     * Compiles (or retrieves from cache) and executes the provided search script
     */
    public SearchScript search(SearchLookup lookup, Script script, ScriptContext scriptContext) {
        CompiledScript compiledScript = compile(script, scriptContext);
        return search(lookup, compiledScript, script.getParams());
    }

    /**
     * Binds provided parameters to a compiled script returning a
     * {@link SearchScript} ready for execution
     */
    public SearchScript search(SearchLookup lookup, CompiledScript compiledScript,  Map<String, Object> params) {
        return getScriptEngineServiceForLang(compiledScript.lang()).search(compiledScript, lookup, params);
    }

    public ScriptStats stats() {
        return scriptMetrics.stats();
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        compiler.clusterChanged(event);
    }

    public final StoredScriptSource getStoredScript(ClusterState state, GetStoredScriptRequest request) {
        ScriptMetaData scriptMetadata = state.metaData().custom(ScriptMetaData.TYPE);

        if (scriptMetadata != null) {
            return scriptMetadata.getStoredScript(request.id(), request.lang());
        } else {
            return null;
        }
    }

    public void putStoredScript(ClusterService clusterService, PutStoredScriptRequest request,
            ActionListener<PutStoredScriptResponse> listener) {
        if (request.content().length() > maxScriptSizeInBytes) {
            throw new IllegalArgumentException("exceeded max allowed stored script size in bytes [" + maxScriptSizeInBytes
                    + "] with size [" + request.content().length() + "] for script [" + request.id() + "]");
        }

        StoredScriptSource source = StoredScriptSource.parse(request.lang(), request.content(), request.xContentType());
        try {
            compiler.checkCompileBeforeStore(source);
        } catch (IllegalArgumentException | ScriptException e) {
            throw new IllegalArgumentException("failed to parse/compile stored script [" + request.id() + "]" +
                    (source.getCode() == null ? "" : " using code [" + source.getCode() + "]"), e);
        }
        clusterService.submitStateUpdateTask("put-script-" + request.id(),
                new AckedClusterStateUpdateTask<PutStoredScriptResponse>(request, listener) {

                @Override
                protected PutStoredScriptResponse newResponse(boolean acknowledged) {
                    return new PutStoredScriptResponse(acknowledged);
                }

                @Override
                public ClusterState execute(ClusterState currentState) throws Exception {
                    ScriptMetaData smd = currentState.metaData().custom(ScriptMetaData.TYPE);
                    smd = ScriptMetaData.putStoredScript(smd, request.id(), source);
                    MetaData.Builder mdb = MetaData.builder(currentState.getMetaData()).putCustom(ScriptMetaData.TYPE, smd);

                    return ClusterState.builder(currentState).metaData(mdb).build();
                }
            });
    }

    public void deleteStoredScript(ClusterService clusterService, DeleteStoredScriptRequest request,
            ActionListener<DeleteStoredScriptResponse> listener) {
        if (request.lang() != null && isLangSupported(request.lang()) == false) {
            throw new IllegalArgumentException("unable to delete stored script with unsupported lang [" + request.lang() + "]");
        }

        clusterService.submitStateUpdateTask("delete-script-" + request.id(),
                new AckedClusterStateUpdateTask<DeleteStoredScriptResponse>(request, listener) {
                    @Override
                    protected DeleteStoredScriptResponse newResponse(boolean acknowledged) {
                        return new DeleteStoredScriptResponse(acknowledged);
                    }
        
                    @Override
                    public ClusterState execute(ClusterState currentState) throws Exception {
                        ScriptMetaData smd = currentState.metaData().custom(ScriptMetaData.TYPE);
                        smd = ScriptMetaData.deleteStoredScript(smd, request.id(), request.lang());
                        MetaData.Builder mdb = MetaData.builder(currentState.getMetaData()).putCustom(ScriptMetaData.TYPE, smd);
        
                        return ClusterState.builder(currentState).metaData(mdb).build();
                    }
                });
    }

    private static final class CacheKey {
        final String lang;
        final String idOrCode;
        final Map<String, String> options;

        private CacheKey(String lang, String idOrCode, Map<String, String> options) {
            this.lang = lang;
            this.idOrCode = idOrCode;
            this.options = options == null ? emptyMap() : options;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            CacheKey cacheKey = (CacheKey)o;

            if (lang != null ? !lang.equals(cacheKey.lang) : cacheKey.lang != null) return false;
            if (!idOrCode.equals(cacheKey.idOrCode)) return false;
            return options.equals(cacheKey.options);

        }

        @Override
        public int hashCode() {
            int result = lang != null ? lang.hashCode() : 0;
            result = 31 * result + idOrCode.hashCode();
            result = 31 * result + options.hashCode();
            return result;
        }

        @Override
        public String toString() {
            String result = "lang=" + lang + ", id=" + idOrCode;
            if (false == options.isEmpty()) {
                result += ", options " + options; 
            }
            return result;
        }
    }
}
