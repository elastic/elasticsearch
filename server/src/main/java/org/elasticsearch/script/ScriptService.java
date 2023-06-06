/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.TimeValue;

import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;

public class ScriptService implements Closeable, ClusterStateApplier, ScriptCompiler {

    private static final Logger logger = LogManager.getLogger(ScriptService.class);
    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(ScriptService.class);

    static final String DISABLE_DYNAMIC_SCRIPTING_SETTING = "script.disable_dynamic";

    // Special setting value for SCRIPT_GENERAL_MAX_COMPILATIONS_RATE to indicate the script service should use context
    // specific caches
    static final ScriptCache.CompilationRate USE_CONTEXT_RATE_VALUE = new ScriptCache.CompilationRate(-1, TimeValue.MINUS_ONE);
    static final String USE_CONTEXT_RATE_KEY = "use-context";

    public static final Setting<Integer> SCRIPT_GENERAL_CACHE_SIZE_SETTING = Setting.intSetting(
        "script.cache.max_size",
        3000,
        0,
        Property.Dynamic,
        Property.NodeScope
    );
    public static final Setting<TimeValue> SCRIPT_GENERAL_CACHE_EXPIRE_SETTING = Setting.positiveTimeSetting(
        "script.cache.expire",
        TimeValue.timeValueMillis(0),
        Property.Dynamic,
        Property.NodeScope
    );
    public static final Setting<Integer> SCRIPT_MAX_SIZE_IN_BYTES = Setting.intSetting(
        "script.max_size_in_bytes",
        65535,
        0,
        Property.Dynamic,
        Property.NodeScope
    );
    public static final Setting<ScriptCache.CompilationRate> SCRIPT_GENERAL_MAX_COMPILATIONS_RATE_SETTING = new Setting<>(
        "script.max_compilations_rate",
        "150/5m",
        (String value) -> value.equals(USE_CONTEXT_RATE_KEY) ? USE_CONTEXT_RATE_VALUE : new ScriptCache.CompilationRate(value),
        Property.Dynamic,
        Property.NodeScope
    );

    public static final String USE_CONTEXT_RATE_KEY_DEPRECATION_MESSAGE = "["
        + USE_CONTEXT_RATE_KEY
        + "] is deprecated for the setting ["
        + SCRIPT_GENERAL_MAX_COMPILATIONS_RATE_SETTING.getKey()
        + "] as system scripts are now exempt from the rate limit. "
        + "Set to a value such as [150/5m] (a rate of 150 compilations per five minutes) to rate limit user scripts in case the "
        + "script cache ["
        + SCRIPT_GENERAL_CACHE_SIZE_SETTING.getKey()
        + "] is undersized causing script compilation thrashing.";

    // Per-context settings
    static final String CONTEXT_PREFIX = "script.context.";

    // script.context.<context-name>.{cache_max_size, cache_expire, max_compilations_rate}

    public static final Setting.AffixSetting<Integer> SCRIPT_CACHE_SIZE_SETTING = Setting.affixKeySetting(
        CONTEXT_PREFIX,
        "cache_max_size",
        key -> Setting.intSetting(
            key,
            SCRIPT_GENERAL_CACHE_SIZE_SETTING,
            0,
            Property.NodeScope,
            Property.Dynamic,
            Property.DeprecatedWarning
        )
    );

    public static final Setting.AffixSetting<TimeValue> SCRIPT_CACHE_EXPIRE_SETTING = Setting.affixKeySetting(
        CONTEXT_PREFIX,
        "cache_expire",
        key -> Setting.positiveTimeSetting(
            key,
            SCRIPT_GENERAL_CACHE_EXPIRE_SETTING,
            TimeValue.timeValueMillis(0),
            Property.NodeScope,
            Property.Dynamic,
            Property.DeprecatedWarning
        )
    );

    // Unlimited compilation rate for context-specific script caches
    static final String UNLIMITED_COMPILATION_RATE_KEY = "unlimited";

    public static final Setting.AffixSetting<ScriptCache.CompilationRate> SCRIPT_MAX_COMPILATIONS_RATE_SETTING = Setting.affixKeySetting(
        CONTEXT_PREFIX,
        "max_compilations_rate",
        key -> new Setting<ScriptCache.CompilationRate>(
            key,
            "75/5m",
            (String value) -> value.equals(UNLIMITED_COMPILATION_RATE_KEY)
                ? ScriptCache.UNLIMITED_COMPILATION_RATE
                : new ScriptCache.CompilationRate(value),
            Property.NodeScope,
            Property.Dynamic,
            Property.DeprecatedWarning
        )
    );

    private static final ScriptCache.CompilationRate SCRIPT_COMPILATION_RATE_ZERO = new ScriptCache.CompilationRate(0, TimeValue.ZERO);

    public static final Setting<Boolean> SCRIPT_DISABLE_MAX_COMPILATIONS_RATE_SETTING = Setting.boolSetting(
        "script.disable_max_compilations_rate",
        false,
        Property.NodeScope
    );

    public static final String ALLOW_NONE = "none";

    public static final Setting<List<String>> TYPES_ALLOWED_SETTING = Setting.stringListSetting(
        "script.allowed_types",
        Setting.Property.NodeScope
    );
    public static final Setting<List<String>> CONTEXTS_ALLOWED_SETTING = Setting.stringListSetting(
        "script.allowed_contexts",
        Setting.Property.NodeScope
    );

    private final Set<String> typesAllowed;
    private final Set<String> contextsAllowed;

    private final Map<String, ScriptEngine> engines;
    private final Map<String, ScriptContext<?>> contexts;
    private final LongSupplier timeProvider;

    private ClusterState clusterState;

    private int maxSizeInBytes;

    // package private for tests
    final AtomicReference<CacheHolder> cacheHolder = new AtomicReference<>();

    public ScriptService(
        Settings settings,
        Map<String, ScriptEngine> engines,
        Map<String, ScriptContext<?>> contexts,
        LongSupplier timeProvider
    ) {
        this.engines = Collections.unmodifiableMap(Objects.requireNonNull(engines));
        this.contexts = Collections.unmodifiableMap(Objects.requireNonNull(contexts));

        if (Strings.hasLength(settings.get(DISABLE_DYNAMIC_SCRIPTING_SETTING))) {
            throw new IllegalArgumentException(
                DISABLE_DYNAMIC_SCRIPTING_SETTING
                    + " is not a supported setting, replace with "
                    + "fine-grained script settings. \n Dynamic scripts can be enabled for all languages and all operations not "
                    + "using `script.disable_dynamic: false` in elasticsearch.yml"
            );
        }

        this.typesAllowed = TYPES_ALLOWED_SETTING.exists(settings) ? new HashSet<>() : null;

        if (this.typesAllowed != null) {
            List<String> typesAllowedList = TYPES_ALLOWED_SETTING.get(settings);

            if (typesAllowedList.isEmpty()) {
                throw new IllegalArgumentException(
                    "must specify at least one script type or none for setting [" + TYPES_ALLOWED_SETTING.getKey() + "]."
                );
            }

            for (String settingType : typesAllowedList) {
                if (ALLOW_NONE.equals(settingType)) {
                    if (typesAllowedList.size() != 1) {
                        throw new IllegalArgumentException(
                            "cannot specify both ["
                                + ALLOW_NONE
                                + "]"
                                + " and other script types for setting ["
                                + TYPES_ALLOWED_SETTING.getKey()
                                + "]."
                        );
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
                        "unknown script type [" + settingType + "] found in setting [" + TYPES_ALLOWED_SETTING.getKey() + "]."
                    );
                }
            }
        }

        this.contextsAllowed = CONTEXTS_ALLOWED_SETTING.exists(settings) ? new HashSet<>() : null;

        if (this.contextsAllowed != null) {
            List<String> contextsAllowedList = CONTEXTS_ALLOWED_SETTING.get(settings);

            if (contextsAllowedList.isEmpty()) {
                throw new IllegalArgumentException(
                    "must specify at least one script context or none for setting [" + CONTEXTS_ALLOWED_SETTING.getKey() + "]."
                );
            }

            for (String settingContext : contextsAllowedList) {
                if (ALLOW_NONE.equals(settingContext)) {
                    if (contextsAllowedList.size() != 1) {
                        throw new IllegalArgumentException(
                            "cannot specify both ["
                                + ALLOW_NONE
                                + "]"
                                + " and other script contexts for setting ["
                                + CONTEXTS_ALLOWED_SETTING.getKey()
                                + "]."
                        );
                    } else {
                        break;
                    }
                }

                if (contexts.containsKey(settingContext)) {
                    this.contextsAllowed.add(settingContext);
                } else {
                    throw new IllegalArgumentException(
                        "unknown script context [" + settingContext + "] found in setting [" + CONTEXTS_ALLOWED_SETTING.getKey() + "]."
                    );
                }
            }
        }

        this.setMaxSizeInBytes(SCRIPT_MAX_SIZE_IN_BYTES.get(settings));
        this.timeProvider = timeProvider;

        // Validation requires knowing which contexts exist.
        this.validateCacheSettings(settings);
        this.setCacheHolder(settings);
    }

    public static boolean isUseContextCacheSet(Settings settings) {
        return SCRIPT_GENERAL_MAX_COMPILATIONS_RATE_SETTING.get(settings).equals(USE_CONTEXT_RATE_VALUE);
    }

    public static boolean isImplicitContextCacheSet(Settings settings) {
        return new ScriptService.ContextSettings(settings).implicitContextCache();
    }

    public static String contextDeprecationMessage(Settings settings) {
        return new ScriptService.ContextSettings(settings).deprecationMessage();
    }

    /**
     * This is overridden in tests to disable compilation rate limiting.
     */
    boolean compilationLimitsEnabled() {
        return true;
    }

    void registerClusterSettingsListeners(ClusterSettings clusterSettings) {
        clusterSettings.addSettingsUpdateConsumer(SCRIPT_MAX_SIZE_IN_BYTES, this::setMaxSizeInBytes);

        // Handle all updatable per-context settings at once for each context.
        for (ScriptContext<?> context : contexts.values()) {
            clusterSettings.addSettingsUpdateConsumer(
                (settings) -> cacheHolder.get().set(context.name, contextCache(settings, context)),
                Arrays.asList(
                    SCRIPT_CACHE_SIZE_SETTING.getConcreteSettingForNamespace(context.name),
                    SCRIPT_CACHE_EXPIRE_SETTING.getConcreteSettingForNamespace(context.name),
                    SCRIPT_MAX_COMPILATIONS_RATE_SETTING.getConcreteSettingForNamespace(context.name),
                    SCRIPT_GENERAL_CACHE_EXPIRE_SETTING,
                    // general settings used for fallbacks
                    SCRIPT_GENERAL_CACHE_SIZE_SETTING
                )
            );
        }

        // Handle all settings for context and general caches, this flips between general and context caches.
        clusterSettings.addSettingsUpdateConsumer(
            this::setCacheHolder,
            Arrays.asList(
                SCRIPT_GENERAL_MAX_COMPILATIONS_RATE_SETTING,
                SCRIPT_GENERAL_CACHE_EXPIRE_SETTING,
                SCRIPT_GENERAL_CACHE_SIZE_SETTING,
                SCRIPT_MAX_COMPILATIONS_RATE_SETTING,
                SCRIPT_DISABLE_MAX_COMPILATIONS_RATE_SETTING,
                SCRIPT_CACHE_EXPIRE_SETTING,
                SCRIPT_CACHE_SIZE_SETTING
            ),
            this::validateCacheSettings
        );
    }

    /**
     * Throw an IllegalArgumentException if any per-context setting does not match a context or if per-context settings are configured
     * when using the general cache.
     */
    void validateCacheSettings(Settings settings) {
        ContextSettings contextSettings = new ContextSettings(settings, contexts.keySet());
        if (contextSettings.useContextSet) {
            deprecationLogger.warn(DeprecationCategory.SCRIPTING, "scripting-context-cache", USE_CONTEXT_RATE_KEY_DEPRECATION_MESSAGE);
        } else if (contextSettings.hasContextSettings()) {
            deprecationLogger.warn(DeprecationCategory.SCRIPTING, "scripting-context-cache", contextSettings.deprecationMessage());
        }
        if (contextSettings.incompatibleSettings()) {
            throw new IllegalArgumentException(contextSettings.incompatibleSettingsMessage());
        }
        if (SCRIPT_DISABLE_MAX_COMPILATIONS_RATE_SETTING.get(settings)) {
            if (contextSettings.compilationContexts.size() > 0) {
                throw new IllegalArgumentException(
                    "Cannot set custom context compilation rates ["
                        + String.join(", ", contextSettings.contextCompilationKeys())
                        + "] if compile rates disabled via ["
                        + SCRIPT_DISABLE_MAX_COMPILATIONS_RATE_SETTING.getKey()
                        + "]"
                );
            }
            if (contextSettings.useContextSet == false && contextSettings.isGeneralCompilationRateSet) {
                throw new IllegalArgumentException(
                    "Cannot set custom general compilation rates ["
                        + SCRIPT_GENERAL_MAX_COMPILATIONS_RATE_SETTING.getKey()
                        + "] to ["
                        + SCRIPT_GENERAL_MAX_COMPILATIONS_RATE_SETTING.get(settings)
                        + "] if compile rates disabled via ["
                        + SCRIPT_DISABLE_MAX_COMPILATIONS_RATE_SETTING.getKey()
                        + "]"
                );
            }
        }
    }

    /**
     * Collect settings related to script context and general caches.
     *
     * The general cache is used by default.
     * The context cache is used if {@code script.max_compilations_rate} is {@code "use-context"}, a deprecated value.
     * The context cache is used implicitly if {@code script.max_compilations_rate} is unset and any of the context
     * cache family of settings is used:
     *   {@code script.context.*.max_compilations_rate}, {@link ScriptService#SCRIPT_MAX_COMPILATIONS_RATE_SETTING}
     *   {@code script.context.*.cache_max_size}, {@link ScriptService#SCRIPT_CACHE_SIZE_SETTING}
     *   {@code script.context.*.cache_expire}, {@link ScriptService#SCRIPT_CACHE_EXPIRE_SETTING}
     */
    public static class ContextSettings {
        public final Settings settings;
        public final boolean useContextSet;
        public final boolean isGeneralCompilationRateSet;
        public final ScriptCache.CompilationRate generalCompilationRate;
        public final List<String> compilationContexts;
        public final List<String> sizeContexts;
        public final List<String> expireContexts;

        public ContextSettings(Settings settings, Set<String> contexts) {
            this.settings = settings;
            generalCompilationRate = SCRIPT_GENERAL_MAX_COMPILATIONS_RATE_SETTING.get(settings);
            useContextSet = generalCompilationRate.equals(USE_CONTEXT_RATE_VALUE);
            isGeneralCompilationRateSet = SCRIPT_GENERAL_MAX_COMPILATIONS_RATE_SETTING.exists(settings);
            compilationContexts = getContexts(SCRIPT_MAX_COMPILATIONS_RATE_SETTING, settings, contexts);
            sizeContexts = getContexts(SCRIPT_CACHE_SIZE_SETTING, settings, contexts);
            expireContexts = getContexts(SCRIPT_CACHE_EXPIRE_SETTING, settings, contexts);
        }

        public ContextSettings(Settings settings) {
            this(settings, Collections.emptySet());
        }

        protected static List<String> getContexts(Setting.AffixSetting<?> setting, Settings settings, Set<String> contexts) {
            List<String> contextSettings = new ArrayList<>();
            for (String context : setting.getAsMap(settings).keySet()) {
                if (contexts.isEmpty() == false && contexts.contains(context) == false) {
                    String settingKey = setting.getConcreteSettingForNamespace(context).getKey();
                    throw new IllegalArgumentException("Context [" + context + "] doesn't exist for setting [" + settingKey + "]");
                }
                contextSettings.add(context);
            }
            contextSettings.sort(Comparator.naturalOrder());
            return contextSettings;
        }

        /** Are there any context specific settings */
        public boolean hasContextSettings() {
            return compilationContexts.isEmpty() == false || expireContexts.isEmpty() == false || sizeContexts.isEmpty() == false;
        }

        /** deprecation message for implicitly using the context cache */
        public String deprecationMessage() {
            // Implicitly using the script context cache is deprecated, remove the following deprecated settings to use the script general
            // cache.
            if (hasContextSettings() == false) {
                return "";
            }
            List<String> settingsKeys = new ArrayList<>();
            settingsKeys.addAll(fullKeys(SCRIPT_MAX_COMPILATIONS_RATE_SETTING, compilationContexts));
            settingsKeys.addAll(fullKeys(SCRIPT_CACHE_SIZE_SETTING, sizeContexts));
            settingsKeys.addAll(fullKeys(SCRIPT_CACHE_EXPIRE_SETTING, expireContexts));
            settingsKeys.sort(Comparator.naturalOrder());
            return "Implicitly using the script context cache is deprecated, remove settings "
                + "["
                + String.join(", ", settingsKeys)
                + "] to use the script general cache.";
        }

        /** the context specific max compilation keys */
        public List<String> contextCompilationKeys() {
            return fullKeys(SCRIPT_MAX_COMPILATIONS_RATE_SETTING, compilationContexts);
        }

        /** the full keys for the contexts in the context affix setting */
        protected static List<String> fullKeys(Setting.AffixSetting<?> affix, List<String> contexts) {
            return contexts.stream().map(ctx -> affix.getConcreteSettingForNamespace(ctx).getKey()).toList();
        }

        /**
         *  Should the context cache be used?  This is true if "use-context" is set explicitly or implicitly, see above for implicit
         *  definition.
         */
        public boolean useContextCache() {
            return useContextSet || implicitContextCache();
        }

        /**
         * Implicitly use the script context cache.  False if context cache is explicitly used as well as context cache is unused.
         */
        public boolean implicitContextCache() {
            return useContextSet == false && hasContextSettings() && isGeneralCompilationRateSet == false;
        }

        /**
         * Is the set of settings incompatible?  This is the case if:
         * 1) {@code script.max_compilations_rate}, {@link ScriptService#SCRIPT_GENERAL_MAX_COMPILATIONS_RATE_SETTING} is set but not
         *    set to "use-context".
         * 2) Any of the context cache family of settings is set.
         */
        public boolean incompatibleSettings() {
            return useContextSet == false && hasContextSettings() && isGeneralCompilationRateSet;
        }

        /**
         * All context specific settings
         */
        public List<String> contextSettings() {
            List<String> contextSettings = new ArrayList<>(fullKeys(SCRIPT_MAX_COMPILATIONS_RATE_SETTING, compilationContexts));
            contextSettings.addAll(fullKeys(SCRIPT_CACHE_SIZE_SETTING, sizeContexts));
            contextSettings.addAll(fullKeys(SCRIPT_CACHE_EXPIRE_SETTING, expireContexts));
            return contextSettings;
        }

        /**
         * Error message if there are incompatible settings.
         */
        public String incompatibleSettingsMessage() {
            if (incompatibleSettings() == false) {
                return "";
            }
            List<String> incompatible = contextSettings();
            return "Context cache settings ["
                + String.join(",", incompatible)
                + "] are incompatible with ["
                + SCRIPT_GENERAL_MAX_COMPILATIONS_RATE_SETTING.getKey()
                + "] set to non-default value ["
                + generalCompilationRate
                + "]."
                + " Either remove the incompatible settings (recommended) or set ["
                + SCRIPT_GENERAL_MAX_COMPILATIONS_RATE_SETTING.getKey()
                + "] to ["
                + USE_CONTEXT_RATE_KEY
                + "] to use per-context settings";
        }
    }

    @Override
    public void close() throws IOException {
        IOUtils.close(engines.values());
    }

    /**
     * @return an unmodifiable {@link Map} of available script context names to {@link ScriptContext}s
     */
    public Map<String, ScriptContext<?>> getScriptContexts() {
        return contexts;
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
                throw new IllegalArgumentException(
                    "script.max_size_in_bytes cannot be set to ["
                        + newMaxSizeInBytes
                        + "], "
                        + "stored script ["
                        + source.getKey()
                        + "] exceeds the new value with a size of "
                        + "["
                        + source.getValue().getSource().getBytes(StandardCharsets.UTF_8).length
                        + "]"
                );
            }
        }

        maxSizeInBytes = newMaxSizeInBytes;
    }

    /**
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
                throw new IllegalArgumentException(
                    "exceeded max allowed inline script size in bytes ["
                        + maxSizeInBytes
                        + "] "
                        + "with size ["
                        + idOrCode.getBytes(StandardCharsets.UTF_8).length
                        + "] for script ["
                        + idOrCode
                        + "]"
                );
            }
        }

        if (logger.isTraceEnabled()) {
            logger.trace("compiling lang: [{}] type: [{}] script: {}", lang, type, idOrCode);
        }

        ScriptCache scriptCache = cacheHolder.get().get(context.name);
        assert scriptCache != null : "script context [" + context.name + "] has no script cache";
        return scriptCache.compile(context, scriptEngine, id, idOrCode, type, options);
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

        ScriptMetadata scriptMetadata = clusterState.metadata().custom(ScriptMetadata.TYPE);

        if (scriptMetadata == null) {
            return Collections.emptyMap();
        }

        return scriptMetadata.getStoredScripts();
    }

    protected StoredScriptSource getScriptFromClusterState(String id) {
        ScriptMetadata scriptMetadata = clusterState.metadata().custom(ScriptMetadata.TYPE);

        if (scriptMetadata == null) {
            throw new ResourceNotFoundException("unable to find script [" + id + "] in cluster state");
        }

        StoredScriptSource source = scriptMetadata.getStoredScript(id);

        if (source == null) {
            throw new ResourceNotFoundException("unable to find script [" + id + "] in cluster state");
        }

        return source;
    }

    public void putStoredScript(
        ClusterService clusterService,
        PutStoredScriptRequest request,
        ActionListener<AcknowledgedResponse> listener
    ) {
        if (request.content().length() > maxSizeInBytes) {
            throw new IllegalArgumentException(
                "exceeded max allowed stored script size in bytes ["
                    + maxSizeInBytes
                    + "] with size ["
                    + request.content().length()
                    + "] for script ["
                    + request.id()
                    + "]"
            );
        }

        StoredScriptSource source = request.source();

        if (isLangSupported(source.getLang()) == false) {
            throw new IllegalArgumentException("unable to put stored script with unsupported lang [" + source.getLang() + "]");
        }

        try {
            ScriptEngine scriptEngine = getEngine(source.getLang());

            if (isTypeEnabled(ScriptType.STORED) == false) {
                throw new IllegalArgumentException(
                    "cannot put [" + ScriptType.STORED + "] script, [" + ScriptType.STORED + "] scripts are not enabled"
                );
            } else if (isAnyContextEnabled() == false) {
                throw new IllegalArgumentException("cannot put [" + ScriptType.STORED + "] script, no script contexts are enabled");
            } else if (request.context() != null) {
                ScriptContext<?> context = contexts.get(request.context());
                if (context == null) {
                    throw new IllegalArgumentException("Unknown context [" + request.context() + "]");
                }
                if (context.allowStoredScript == false) {
                    throw new IllegalArgumentException("cannot store a script for context [" + request.context() + "]");
                }
                scriptEngine.compile(request.id(), source.getSource(), context, Collections.emptyMap());
            }
        } catch (ScriptException good) {
            throw good;
        } catch (Exception exception) {
            throw new IllegalArgumentException("failed to parse/compile stored script [" + request.id() + "]", exception);
        }

        submitUnbatchedTask(clusterService, "put-script-" + request.id(), new AckedClusterStateUpdateTask(request, listener) {
            @Override
            public ClusterState execute(ClusterState currentState) {
                ScriptMetadata smd = currentState.metadata().custom(ScriptMetadata.TYPE);
                smd = ScriptMetadata.putStoredScript(smd, request.id(), source);
                Metadata.Builder mdb = Metadata.builder(currentState.getMetadata()).putCustom(ScriptMetadata.TYPE, smd);

                return ClusterState.builder(currentState).metadata(mdb).build();
            }
        });
    }

    public static void deleteStoredScript(
        ClusterService clusterService,
        DeleteStoredScriptRequest request,
        ActionListener<AcknowledgedResponse> listener
    ) {
        submitUnbatchedTask(clusterService, "delete-script-" + request.id(), new AckedClusterStateUpdateTask(request, listener) {
            @Override
            public ClusterState execute(ClusterState currentState) {
                ScriptMetadata smd = currentState.metadata().custom(ScriptMetadata.TYPE);
                smd = ScriptMetadata.deleteStoredScript(smd, request.id());
                Metadata.Builder mdb = Metadata.builder(currentState.getMetadata()).putCustom(ScriptMetadata.TYPE, smd);

                return ClusterState.builder(currentState).metadata(mdb).build();
            }
        });
    }

    @SuppressForbidden(reason = "legacy usage of unbatched task") // TODO add support for batching here
    private static void submitUnbatchedTask(
        ClusterService clusterService,
        @SuppressWarnings("SameParameterValue") String source,
        ClusterStateUpdateTask task
    ) {
        clusterService.submitUnbatchedStateUpdateTask(source, task);
    }

    public static StoredScriptSource getStoredScript(ClusterState state, GetStoredScriptRequest request) {
        ScriptMetadata scriptMetadata = state.metadata().custom(ScriptMetadata.TYPE);

        if (scriptMetadata != null) {
            return scriptMetadata.getStoredScript(request.id());
        } else {
            return null;
        }
    }

    public Set<ScriptContextInfo> getContextInfos() {
        Set<ScriptContextInfo> infos = Sets.newHashSetWithExpectedSize(contexts.size());
        for (ScriptContext<?> context : contexts.values()) {
            infos.add(new ScriptContextInfo(context.name, context.instanceClazz));
        }
        return infos;
    }

    public ScriptLanguagesInfo getScriptLanguages() {
        Set<String> types = typesAllowed;
        if (types == null) {
            types = new HashSet<>();
            for (ScriptType type : ScriptType.values()) {
                types.add(type.getName());
            }
        }

        final Set<String> contexts = contextsAllowed != null ? contextsAllowed : this.contexts.keySet();
        Map<String, Set<String>> languageContexts = new HashMap<>();
        engines.forEach(
            (key, value) -> languageContexts.put(
                key,
                value.getSupportedContexts().stream().map(c -> c.name).filter(contexts::contains).collect(Collectors.toSet())
            )
        );
        return new ScriptLanguagesInfo(types, languageContexts);
    }

    public ScriptStats stats() {
        return cacheHolder.get().stats();
    }

    public ScriptCacheStats cacheStats() {
        return cacheHolder.get().cacheStats();
    }

    @Override
    public void applyClusterState(ClusterChangedEvent event) {
        clusterState = event.state();
    }

    void setCacheHolder(Settings settings) {
        CacheHolder current = cacheHolder.get();
        ContextSettings contextSettings = new ContextSettings(settings, contexts.keySet());

        if (current == null) {
            if (contextSettings.useContextCache()) {
                cacheHolder.set(contextCacheHolder(settings));
            } else {
                cacheHolder.set(generalCacheHolder(settings));
            }
            return;
        }

        // Update
        if (contextSettings.useContextCache()) {
            if (current.general != null) {
                // Flipping to context specific
                cacheHolder.set(contextCacheHolder(settings));
            }
        } else if (current.general == null) {
            // Flipping to general
            cacheHolder.set(generalCacheHolder(settings));
        } else if (current.general.rate.equals(SCRIPT_GENERAL_MAX_COMPILATIONS_RATE_SETTING.get(settings)) == false
            || current.general.cacheExpire.equals(SCRIPT_GENERAL_CACHE_EXPIRE_SETTING.get(settings)) == false
            || current.general.cacheSize != SCRIPT_GENERAL_CACHE_SIZE_SETTING.get(settings)) {
                // General compilation rate, cache expiration or cache size changed
                cacheHolder.set(generalCacheHolder(settings));
            }
    }

    CacheHolder generalCacheHolder(Settings settings) {
        ScriptCache.CompilationRate rate = SCRIPT_GENERAL_MAX_COMPILATIONS_RATE_SETTING.get(settings);
        if (SCRIPT_DISABLE_MAX_COMPILATIONS_RATE_SETTING.get(settings) || compilationLimitsEnabled() == false) {
            rate = SCRIPT_COMPILATION_RATE_ZERO;
        }
        return new CacheHolder(
            SCRIPT_GENERAL_CACHE_SIZE_SETTING.get(settings),
            SCRIPT_GENERAL_CACHE_EXPIRE_SETTING.get(settings),
            rate,
            SCRIPT_GENERAL_MAX_COMPILATIONS_RATE_SETTING.getKey(),
            timeProvider
        );
    }

    CacheHolder contextCacheHolder(Settings settings) {
        Map<String, ScriptCache> contextCache = Maps.newMapWithExpectedSize(contexts.size());
        contexts.forEach((k, v) -> contextCache.put(k, contextCache(settings, v)));
        return new CacheHolder(contextCache);
    }

    ScriptCache contextCache(Settings settings, ScriptContext<?> context) {
        Setting<Integer> cacheSizeSetting = SCRIPT_CACHE_SIZE_SETTING.getConcreteSettingForNamespace(context.name);
        int cacheSize = cacheSizeSetting.existsOrFallbackExists(settings) ? cacheSizeSetting.get(settings) : context.cacheSizeDefault;

        Setting<TimeValue> cacheExpireSetting = SCRIPT_CACHE_EXPIRE_SETTING.getConcreteSettingForNamespace(context.name);
        TimeValue cacheExpire = cacheExpireSetting.existsOrFallbackExists(settings)
            ? cacheExpireSetting.get(settings)
            : context.cacheExpireDefault;

        Setting<ScriptCache.CompilationRate> rateSetting = SCRIPT_MAX_COMPILATIONS_RATE_SETTING.getConcreteSettingForNamespace(
            context.name
        );
        ScriptCache.CompilationRate rate;
        if (SCRIPT_DISABLE_MAX_COMPILATIONS_RATE_SETTING.get(settings)
            || compilationLimitsEnabled() == false
            || context.compilationRateLimited == false) {
            rate = SCRIPT_COMPILATION_RATE_ZERO;
        } else if (rateSetting.existsOrFallbackExists(settings)) {
            rate = rateSetting.get(settings);
        } else {
            rate = new ScriptCache.CompilationRate(ScriptContext.DEFAULT_COMPILATION_RATE_LIMIT);
        }

        return new ScriptCache(cacheSize, cacheExpire, rate, rateSetting.getKey(), timeProvider);
    }

    /**
     * Container for the ScriptCache(s).  This class operates in two modes:
     * 1) general mode, if the general script cache is configured.  There are no context caches in this case.
     * 2) context mode, if the context script cache is configured.  There is no general cache in this case.
     */
    static class CacheHolder {
        final ScriptCache general;
        final Map<String, AtomicReference<ScriptCache>> contextCache;

        CacheHolder(
            int cacheMaxSize,
            TimeValue cacheExpire,
            ScriptCache.CompilationRate maxCompilationRate,
            String contextRateSetting,
            LongSupplier timeProvider
        ) {
            contextCache = null;
            general = new ScriptCache(cacheMaxSize, cacheExpire, maxCompilationRate, contextRateSetting, timeProvider);
        }

        CacheHolder(Map<String, ScriptCache> context) {
            Map<String, AtomicReference<ScriptCache>> refs = Maps.newMapWithExpectedSize(context.size());
            context.forEach((k, v) -> refs.put(k, new AtomicReference<>(v)));
            contextCache = Collections.unmodifiableMap(refs);
            general = null;
        }

        /**
         * get the cache appropriate for the context.  If in general mode, return the general cache.  Otherwise return the ScriptCache for
         * the given context. Returns null in context mode if the requested context does not exist.
         */
        ScriptCache get(String context) {
            if (general != null) {
                return general;
            }
            AtomicReference<ScriptCache> ref = contextCache.get(context);
            if (ref == null) {
                return null;
            }
            return ref.get();
        }

        ScriptStats stats() {
            if (general != null) {
                return general.stats();
            }
            List<ScriptContextStats> contextStats = new ArrayList<>(contextCache.size());
            for (Map.Entry<String, AtomicReference<ScriptCache>> entry : contextCache.entrySet()) {
                ScriptCache cache = entry.getValue().get();
                contextStats.add(cache.stats(entry.getKey()));
            }
            return new ScriptStats(contextStats);
        }

        ScriptCacheStats cacheStats() {
            if (general != null) {
                return new ScriptCacheStats(general.stats());
            }
            Map<String, ScriptStats> context = Maps.newMapWithExpectedSize(contextCache.size());
            for (String name : contextCache.keySet()) {
                context.put(name, contextCache.get(name).get().stats());
            }
            return new ScriptCacheStats(context);
        }

        /**
         * Update a single context cache if we're in the context cache mode otherwise no-op.
         */
        void set(String name, ScriptCache cache) {
            if (general != null) {
                return;
            }
            AtomicReference<ScriptCache> ref = contextCache.get(name);
            assert ref != null : "expected script cache to exist for context [" + name + "]";
            ScriptCache oldCache = ref.get();
            assert oldCache != null : "expected script cache to be non-null for context [" + name + "]";
            ref.set(cache);
            logger.debug("Replaced context [" + name + "] with new settings");
        }
    }
}
