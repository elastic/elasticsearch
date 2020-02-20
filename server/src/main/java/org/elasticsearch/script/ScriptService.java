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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.storedscripts.DeleteStoredScriptRequest;
import org.elasticsearch.action.admin.cluster.storedscripts.GetStoredScriptRequest;
import org.elasticsearch.action.admin.cluster.storedscripts.PutStoredScriptRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateApplier;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.core.internal.io.IOUtils;

import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public class ScriptService implements Closeable, ClusterStateApplier {

    private static final Logger logger = LogManager.getLogger(ScriptService.class);

    static final String DISABLE_DYNAMIC_SCRIPTING_SETTING = "script.disable_dynamic";

    // a parsing function that requires a non negative int and a timevalue as arguments split by a slash
    // this allows you to easily define rates
    static final Function<String, Tuple<Integer, TimeValue>> MAX_COMPILATION_RATE_FUNCTION =
            (String value) -> {
                if (value.contains("/") == false || value.startsWith("/") || value.endsWith("/")) {
                    throw new IllegalArgumentException("parameter must contain a positive integer and a timevalue, i.e. 10/1m, but was [" +
                            value + "]");
                }
                int idx = value.indexOf("/");
                String count = value.substring(0, idx);
                String time = value.substring(idx + 1);
                try {

                    int rate = Integer.parseInt(count);
                    if (rate < 0) {
                        throw new IllegalArgumentException("rate [" + rate + "] must be positive");
                    }
                    TimeValue timeValue = TimeValue.parseTimeValue(time, "script.max_compilations_rate");
                    if (timeValue.nanos() <= 0) {
                        throw new IllegalArgumentException("time value [" + time + "] must be positive");
                    }
                    // protect against a too hard to check limit, like less than a minute
                    if (timeValue.seconds() < 60) {
                        throw new IllegalArgumentException("time value [" + time + "] must be at least on a one minute resolution");
                    }
                    return Tuple.tuple(rate, timeValue);
                } catch (NumberFormatException e) {
                    // the number format exception message is so confusing, that it makes more sense to wrap it with a useful one
                    throw new IllegalArgumentException("could not parse [" + count + "] as integer in value [" + value + "]", e);
                }
            };

    public static final Setting<Integer> SCRIPT_CACHE_SIZE_SETTING =
        Setting.intSetting("script.cache.max_size", 100, 0, Property.NodeScope);
    public static final Setting<TimeValue> SCRIPT_CACHE_EXPIRE_SETTING =
        Setting.positiveTimeSetting("script.cache.expire", TimeValue.timeValueMillis(0), Property.NodeScope);
    public static final Setting<Integer> SCRIPT_MAX_SIZE_IN_BYTES =
        Setting.intSetting("script.max_size_in_bytes", 65535, 0, Property.Dynamic, Property.NodeScope);
    public static final Setting<Tuple<Integer, TimeValue>> SCRIPT_MAX_COMPILATIONS_RATE =
            new Setting<>("script.max_compilations_rate", "75/5m", MAX_COMPILATION_RATE_FUNCTION, Property.Dynamic, Property.NodeScope);

    public static final String ALLOW_NONE = "none";

    public static final Setting<List<String>> TYPES_ALLOWED_SETTING =
        Setting.listSetting("script.allowed_types", Collections.emptyList(), Function.identity(), Setting.Property.NodeScope);
    public static final Setting<List<String>> CONTEXTS_ALLOWED_SETTING =
        Setting.listSetting("script.allowed_contexts", Collections.emptyList(), Function.identity(), Setting.Property.NodeScope);

    private final Settings settings;
    private final Set<String> typesAllowed;
    private final Set<String> contextsAllowed;

    final ContextCompiler compiler;

    private final Map<String, ScriptEngine> engines;
    private final Map<String, ScriptContext<?>> contexts;

    private ClusterState clusterState;

    private int maxSizeInBytes;

    public ScriptService(Settings settings, Map<String, ScriptEngine> engines, Map<String, ScriptContext<?>> contexts) {
        this.settings = Objects.requireNonNull(settings);
        this.engines = Objects.requireNonNull(engines);
        this.contexts = Objects.requireNonNull(contexts);

        if (Strings.hasLength(settings.get(DISABLE_DYNAMIC_SCRIPTING_SETTING))) {
            throw new IllegalArgumentException(DISABLE_DYNAMIC_SCRIPTING_SETTING + " is not a supported setting, replace with " +
                    "fine-grained script settings. \n Dynamic scripts can be enabled for all languages and all operations not " +
                    "using `script.disable_dynamic: false` in elasticsearch.yml");
        }

        this.typesAllowed = TYPES_ALLOWED_SETTING.exists(settings) ? new HashSet<>() : null;

        if (this.typesAllowed != null) {
            List<String> typesAllowedList = TYPES_ALLOWED_SETTING.get(settings);

            if (typesAllowedList.isEmpty()) {
                throw new IllegalArgumentException(
                    "must specify at least one script type or none for setting [" + TYPES_ALLOWED_SETTING.getKey() + "].");
            }

            for (String settingType : typesAllowedList) {
                if (ALLOW_NONE.equals(settingType)) {
                    if (typesAllowedList.size() != 1) {
                        throw new IllegalArgumentException("cannot specify both [" + ALLOW_NONE + "]" +
                            " and other script types for setting [" + TYPES_ALLOWED_SETTING.getKey() + "].");
                    } else {
                        break;
                    }
                }

                boolean found = false;

                for (ScriptType scriptType : ScriptType.values()) {
                    if (scriptType.getName().equals(settingType)) {
                        found = true;
                        this.typesAllowed.add(settingType);

                        break;
                    }
                }

                if (found == false) {
                    throw new IllegalArgumentException(
                        "unknown script type [" + settingType + "] found in setting [" + TYPES_ALLOWED_SETTING.getKey() + "].");
                }
            }
        }

        this.contextsAllowed = CONTEXTS_ALLOWED_SETTING.exists(settings) ? new HashSet<>() : null;

        if (this.contextsAllowed != null) {
            List<String> contextsAllowedList = CONTEXTS_ALLOWED_SETTING.get(settings);

            if (contextsAllowedList.isEmpty()) {
                throw new IllegalArgumentException(
                    "must specify at least one script context or none for setting [" + CONTEXTS_ALLOWED_SETTING.getKey() + "].");
            }

            for (String settingContext : contextsAllowedList) {
                if (ALLOW_NONE.equals(settingContext)) {
                    if (contextsAllowedList.size() != 1) {
                        throw new IllegalArgumentException("cannot specify both [" + ALLOW_NONE + "]" +
                            " and other script contexts for setting [" + CONTEXTS_ALLOWED_SETTING.getKey() + "].");
                    } else {
                        break;
                    }
                }

                if (contexts.containsKey(settingContext)) {
                    this.contextsAllowed.add(settingContext);
                } else {
                    throw new IllegalArgumentException(
                        "unknown script context [" + settingContext + "] found in setting [" + CONTEXTS_ALLOWED_SETTING.getKey() + "].");
                }
            }
        }

        this.setMaxSizeInBytes(SCRIPT_MAX_SIZE_IN_BYTES.get(settings));
        compiler = new ContextCompiler(
                    SCRIPT_CACHE_SIZE_SETTING.get(settings),
                    SCRIPT_CACHE_EXPIRE_SETTING.get(settings),
                    compilationLimitsEnabled() ?
                        SCRIPT_MAX_COMPILATIONS_RATE.get(settings):
                        new Tuple<>(0, TimeValue.ZERO)
                    );
    }

    /**
     * This is overridden in tests to disable compilation rate limiting.
     */
    boolean compilationLimitsEnabled() {
        return true;
    }

    void registerClusterSettingsListeners(ClusterSettings clusterSettings) {
        clusterSettings.addSettingsUpdateConsumer(SCRIPT_MAX_SIZE_IN_BYTES, this::setMaxSizeInBytes);
        clusterSettings.addSettingsUpdateConsumer(SCRIPT_MAX_COMPILATIONS_RATE, compiler::setMaxCompilationRate);
    }

    @Override
    public void close() throws IOException {
        IOUtils.close(engines.values());
    }

    private ScriptEngine getEngine(String lang) {
        ScriptEngine scriptEngine = engines.get(lang);
        if (scriptEngine == null) {
            throw new IllegalArgumentException("script_lang not supported [" + lang + "]");
        }
        return scriptEngine;
    }

    /**
     * Changes the maximum number of bytes a script's source is allowed to have.
     * @param newMaxSizeInBytes The new maximum number of bytes.
     */
    void setMaxSizeInBytes(int newMaxSizeInBytes) {
        for (Map.Entry<String, StoredScriptSource> source : getScriptsFromClusterState().entrySet()) {
            if (source.getValue().getSource().getBytes(StandardCharsets.UTF_8).length > newMaxSizeInBytes) {
                throw new IllegalArgumentException("script.max_size_in_bytes cannot be set to [" + newMaxSizeInBytes + "], " +
                        "stored script [" + source.getKey() + "] exceeds the new value with a size of " +
                        "[" + source.getValue().getSource().getBytes(StandardCharsets.UTF_8).length + "]");
            }
        }

        maxSizeInBytes = newMaxSizeInBytes;
    }

    /*
     * Compiles a script using the given context.
     *
     * @return a compiled script which may be used to construct instances of a script for the given context
     */
    public <FactoryType> FactoryType compile(Script script, ScriptContext<FactoryType> context) {
        Objects.requireNonNull(script);
        Objects.requireNonNull(context);

        ScriptType type = script.getType();
        String lang = script.getLang();
        String idOrCode = script.getIdOrCode();
        Map<String, String> options = script.getOptions();

        String id = idOrCode;

        if (type == ScriptType.STORED) {
            // * lang and options will both be null when looking up a stored script,
            // so we must get the source to retrieve them before checking if the
            // context is supported
            // * a stored script must be pulled from the cluster state every time in case
            // the script has been updated since the last compilation
            StoredScriptSource source = getScriptFromClusterState(id);
            lang = source.getLang();
            idOrCode = source.getSource();
            options = source.getOptions();
        }

        ScriptEngine scriptEngine = getEngine(lang);

        if (isTypeEnabled(type) == false) {
            throw new IllegalArgumentException("cannot execute [" + type + "] scripts");
        }

        if (contexts.containsKey(context.name) == false) {
            throw new IllegalArgumentException("script context [" + context.name + "] not supported");
        }

        if (isContextEnabled(context) == false) {
            throw new IllegalArgumentException("cannot execute scripts using [" + context.name + "] context");
        }

        if (type == ScriptType.INLINE) {
            if (idOrCode.getBytes(StandardCharsets.UTF_8).length > maxSizeInBytes) {
                throw new IllegalArgumentException("exceeded max allowed inline script size in bytes [" + maxSizeInBytes + "] " +
                    "with size [" + idOrCode.getBytes(StandardCharsets.UTF_8).length + "] for script [" + idOrCode + "]");
            }
        }

        if (logger.isTraceEnabled()) {
            logger.trace("compiling lang: [{}] type: [{}] script: {}", lang, type, idOrCode);
        }

        return compiler.compile(context, scriptEngine, id, idOrCode, type, options);
    }

    public boolean isLangSupported(String lang) {
        Objects.requireNonNull(lang);
        return engines.containsKey(lang);
    }

    public boolean isTypeEnabled(ScriptType scriptType) {
        return typesAllowed == null || typesAllowed.contains(scriptType.getName());
    }

    public boolean isContextEnabled(ScriptContext<?> scriptContext) {
        return contextsAllowed == null || contextsAllowed.contains(scriptContext.name);
    }

    public boolean isAnyContextEnabled() {
        return contextsAllowed == null || contextsAllowed.isEmpty() == false;
    }

    Map<String, StoredScriptSource> getScriptsFromClusterState() {
        if (clusterState == null) {
            return Collections.emptyMap();
        }

        ScriptMetaData scriptMetadata = clusterState.metaData().custom(ScriptMetaData.TYPE);

        if (scriptMetadata == null) {
            return Collections.emptyMap();
        }

        return scriptMetadata.getStoredScripts();
    }

    StoredScriptSource getScriptFromClusterState(String id) {
        ScriptMetaData scriptMetadata = clusterState.metaData().custom(ScriptMetaData.TYPE);

        if (scriptMetadata == null) {
            throw new ResourceNotFoundException("unable to find script [" + id + "] in cluster state");
        }

        StoredScriptSource source = scriptMetadata.getStoredScript(id);

        if (source == null) {
            throw new ResourceNotFoundException("unable to find script [" + id + "] in cluster state");
        }

        return source;
    }

    public void putStoredScript(ClusterService clusterService, PutStoredScriptRequest request,
                                ActionListener<AcknowledgedResponse> listener) {
        if (request.content().length() > maxSizeInBytes) {
            throw new IllegalArgumentException("exceeded max allowed stored script size in bytes [" + maxSizeInBytes + "] with size [" +
                request.content().length() + "] for script [" + request.id() + "]");
        }

        StoredScriptSource source = request.source();

        if (isLangSupported(source.getLang()) == false) {
            throw new IllegalArgumentException("unable to put stored script with unsupported lang [" + source.getLang() + "]");
        }

        try {
            ScriptEngine scriptEngine = getEngine(source.getLang());

            if (isTypeEnabled(ScriptType.STORED) == false) {
                throw new IllegalArgumentException(
                    "cannot put [" + ScriptType.STORED + "] script, [" + ScriptType.STORED + "] scripts are not enabled");
            } else if (isAnyContextEnabled() == false) {
                throw new IllegalArgumentException(
                    "cannot put [" + ScriptType.STORED + "] script, no script contexts are enabled");
            } else if (request.context() != null) {
                ScriptContext<?> context = contexts.get(request.context());
                if (context == null) {
                    throw new IllegalArgumentException("Unknown context [" + request.context() + "]");
                }
                scriptEngine.compile(request.id(), source.getSource(), context, Collections.emptyMap());
            }
        } catch (ScriptException good) {
            throw good;
        } catch (Exception exception) {
            throw new IllegalArgumentException("failed to parse/compile stored script [" + request.id() + "]", exception);
        }

        clusterService.submitStateUpdateTask("put-script-" + request.id(),
            new AckedClusterStateUpdateTask<AcknowledgedResponse>(request, listener) {

            @Override
            protected AcknowledgedResponse newResponse(boolean acknowledged) {
                return new AcknowledgedResponse(acknowledged);
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
                                   ActionListener<AcknowledgedResponse> listener) {
        clusterService.submitStateUpdateTask("delete-script-" + request.id(),
            new AckedClusterStateUpdateTask<AcknowledgedResponse>(request, listener) {

            @Override
            protected AcknowledgedResponse newResponse(boolean acknowledged) {
                return new AcknowledgedResponse(acknowledged);
            }

            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                ScriptMetaData smd = currentState.metaData().custom(ScriptMetaData.TYPE);
                smd = ScriptMetaData.deleteStoredScript(smd, request.id());
                MetaData.Builder mdb = MetaData.builder(currentState.getMetaData()).putCustom(ScriptMetaData.TYPE, smd);

                return ClusterState.builder(currentState).metaData(mdb).build();
            }
        });
    }

    public StoredScriptSource getStoredScript(ClusterState state, GetStoredScriptRequest request) {
        ScriptMetaData scriptMetadata = state.metaData().custom(ScriptMetaData.TYPE);

        if (scriptMetadata != null) {
            return scriptMetadata.getStoredScript(request.id());
        } else {
            return null;
        }
    }

    public Set<ScriptContextInfo> getContextInfos() {
        Set<ScriptContextInfo> infos = new HashSet<ScriptContextInfo>(contexts.size());
        for (ScriptContext<?> context : contexts.values()) {
            infos.add(new ScriptContextInfo(context.name, context.instanceClazz));
        }
        return infos;
    }

    public ScriptLanguagesInfo getScriptLanguages() {
        Set<String> types = typesAllowed;
        if (types == null) {
            types = new HashSet<>();
            for (ScriptType type: ScriptType.values()) {
                types.add(type.getName());
            }
        }

        final Set<String> contexts = contextsAllowed != null ? contextsAllowed : this.contexts.keySet();
        Map<String,Set<String>> languageContexts = new HashMap<>();
        engines.forEach(
            (key, value) -> languageContexts.put(
                key,
                value.getSupportedContexts().stream().map(c -> c.name).filter(contexts::contains).collect(Collectors.toSet())
            )
        );
        return new ScriptLanguagesInfo(types, languageContexts);
    }

    public ScriptStats stats() {
        return compiler.stats();
    }

    @Override
    public void applyClusterState(ClusterChangedEvent event) {
        clusterState = event.state();
    }
}
