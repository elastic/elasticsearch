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
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.storedscripts.DeleteStoredScriptRequest;
import org.elasticsearch.action.admin.cluster.storedscripts.DeleteStoredScriptResponse;
import org.elasticsearch.action.admin.cluster.storedscripts.GetStoredScriptRequest;
import org.elasticsearch.action.admin.cluster.storedscripts.PutStoredScriptRequest;
import org.elasticsearch.action.admin.cluster.storedscripts.PutStoredScriptResponse;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.common.cache.RemovalListener;
import org.elasticsearch.common.cache.RemovalNotification;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.query.TemplateQueryBuilder;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.watcher.FileChangesListener;
import org.elasticsearch.watcher.FileWatcher;
import org.elasticsearch.watcher.ResourceWatcherService;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import static java.util.Collections.unmodifiableMap;

public class ScriptService extends AbstractComponent implements Closeable {

    static final String DISABLE_DYNAMIC_SCRIPTING_SETTING = "script.disable_dynamic";

    public static final Setting<Integer> SCRIPT_CACHE_SIZE_SETTING =
        Setting.intSetting("script.cache.max_size", 100, 0, Property.NodeScope);
    public static final Setting<TimeValue> SCRIPT_CACHE_EXPIRE_SETTING =
        Setting.positiveTimeSetting("script.cache.expire", TimeValue.timeValueMillis(0), Property.NodeScope);
    public static final Setting<Boolean> SCRIPT_AUTO_RELOAD_ENABLED_SETTING =
        Setting.boolSetting("script.auto_reload_enabled", true, Property.NodeScope);
    public static final Setting<Integer> SCRIPT_MAX_SIZE_IN_BYTES =
        Setting.intSetting("script.max_size_in_bytes", 65535, Property.NodeScope);

    private final String defaultLang;

    private final Collection<ScriptEngineService> scriptEngines;
    private final Map<String, ScriptEngineService> scriptEnginesByLang;
    private final Map<String, ScriptEngineService> scriptEnginesByExt;

    private final ConcurrentMap<CacheKey, CompiledScript> staticCache = ConcurrentCollections.newConcurrentMap();

    private final Cache<CacheKey, CompiledScript> cache;
    private final Path scriptsDirectory;

    private final ScriptModes scriptModes;
    private final ScriptContextRegistry scriptContextRegistry;

    private final ParseFieldMatcher parseFieldMatcher;
    private final ScriptMetrics scriptMetrics = new ScriptMetrics();

    /**
     * @deprecated Use {@link org.elasticsearch.script.Script.ScriptField} instead. This should be removed in
     *             2.0
     */
    @Deprecated
    public static final ParseField SCRIPT_LANG = new ParseField("lang","script_lang");
    /**
     * @deprecated Use {@link ScriptType#getParseField()} instead. This should
     *             be removed in 2.0
     */
    @Deprecated
    public static final ParseField SCRIPT_FILE = new ParseField("script_file");
    /**
     * @deprecated Use {@link ScriptType#getParseField()} instead. This should
     *             be removed in 2.0
     */
    @Deprecated
    public static final ParseField SCRIPT_ID = new ParseField("script_id");
    /**
     * @deprecated Use {@link ScriptType#getParseField()} instead. This should
     *             be removed in 2.0
     */
    @Deprecated
    public static final ParseField SCRIPT_INLINE = new ParseField("script");

    @Inject
    public ScriptService(Settings settings, Environment env,
                         ResourceWatcherService resourceWatcherService, ScriptEngineRegistry scriptEngineRegistry,
                         ScriptContextRegistry scriptContextRegistry, ScriptSettings scriptSettings) throws IOException {
        super(settings);
        Objects.requireNonNull(scriptEngineRegistry);
        Objects.requireNonNull(scriptContextRegistry);
        Objects.requireNonNull(scriptSettings);
        this.parseFieldMatcher = new ParseFieldMatcher(settings);
        if (Strings.hasLength(settings.get(DISABLE_DYNAMIC_SCRIPTING_SETTING))) {
            throw new IllegalArgumentException(DISABLE_DYNAMIC_SCRIPTING_SETTING + " is not a supported setting, replace with fine-grained script settings. \n" +
                    "Dynamic scripts can be enabled for all languages and all operations by replacing `script.disable_dynamic: false` with `script.inline: true` and `script.stored: true` in elasticsearch.yml");
        }

        this.scriptEngines = scriptEngineRegistry.getRegisteredLanguages().values();
        this.scriptContextRegistry = scriptContextRegistry;
        int cacheMaxSize = SCRIPT_CACHE_SIZE_SETTING.get(settings);

        this.defaultLang = scriptSettings.getDefaultScriptLanguageSetting().get(settings);

        CacheBuilder<CacheKey, CompiledScript> cacheBuilder = CacheBuilder.builder();
        if (cacheMaxSize >= 0) {
            cacheBuilder.setMaximumWeight(cacheMaxSize);
        }

        TimeValue cacheExpire = SCRIPT_CACHE_EXPIRE_SETTING.get(settings);
        if (cacheExpire.getNanos() != 0) {
            cacheBuilder.setExpireAfterAccess(cacheExpire.nanos());
        }

        logger.debug("using script cache with max_size [{}], expire [{}]", cacheMaxSize, cacheExpire);
        this.cache = cacheBuilder.removalListener(new ScriptCacheRemovalListener()).build();

        Map<String, ScriptEngineService> enginesByLangBuilder = new HashMap<>();
        Map<String, ScriptEngineService> enginesByExtBuilder = new HashMap<>();
        for (ScriptEngineService scriptEngine : scriptEngines) {
            String language = scriptEngineRegistry.getLanguage(scriptEngine.getClass());
            enginesByLangBuilder.put(language, scriptEngine);
            enginesByExtBuilder.put(scriptEngine.getExtension(), scriptEngine);
        }
        this.scriptEnginesByLang = unmodifiableMap(enginesByLangBuilder);
        this.scriptEnginesByExt = unmodifiableMap(enginesByExtBuilder);

        this.scriptModes = new ScriptModes(scriptSettings, settings);

        // add file watcher for static scripts
        scriptsDirectory = env.scriptsFile();
        if (logger.isTraceEnabled()) {
            logger.trace("Using scripts directory [{}] ", scriptsDirectory);
        }
        FileWatcher fileWatcher = new FileWatcher(scriptsDirectory);
        fileWatcher.addListener(new ScriptChangesListener());

        if (SCRIPT_AUTO_RELOAD_ENABLED_SETTING.get(settings)) {
            // automatic reload is enabled - register scripts
            resourceWatcherService.add(fileWatcher);
        } else {
            // automatic reload is disable just load scripts once
            fileWatcher.init();
        }
    }

    @Override
    public void close() throws IOException {
        IOUtils.close(scriptEngines);
    }

    private ScriptEngineService getScriptEngineServiceForLang(String lang) {
        ScriptEngineService scriptEngineService = scriptEnginesByLang.get(lang);
        if (scriptEngineService == null) {
            throw new IllegalArgumentException("script_lang not supported [" + lang + "]");
        }
        return scriptEngineService;
    }

    private ScriptEngineService getScriptEngineServiceForFileExt(String fileExtension) {
        ScriptEngineService scriptEngineService = scriptEnginesByExt.get(fileExtension);
        if (scriptEngineService == null) {
            throw new IllegalArgumentException("script file extension not supported [" + fileExtension + "]");
        }
        return scriptEngineService;
    }



    /**
     * Checks if a script can be executed and compiles it if needed, or returns the previously compiled and cached script.
     */
    public CompiledScript compile(Script script, ScriptContext scriptContext, Map<String, String> params, ClusterState state) {
        if (script == null) {
            throw new IllegalArgumentException("The parameter script (Script) must not be null.");
        }
        if (scriptContext == null) {
            throw new IllegalArgumentException("The parameter scriptContext (ScriptContext) must not be null.");
        }

        String lang = script.getLang();

        if (lang == null) {
            lang = defaultLang;
        }

        ScriptEngineService scriptEngineService = getScriptEngineServiceForLang(lang);
        if (canExecuteScript(lang, scriptEngineService, script.getType(), scriptContext) == false) {
            throw new IllegalStateException("scripts of type [" + script.getType() + "], operation [" + scriptContext.getKey() + "] and lang [" + lang + "] are disabled");
        }

        // TODO: fix this through some API or something, that's wrong
        // special exception to prevent expressions from compiling as update or mapping scripts
        boolean expression = "expression".equals(script.getLang());
        boolean notSupported = scriptContext.getKey().equals(ScriptContext.Standard.UPDATE.getKey());
        if (expression && notSupported) {
            throw new UnsupportedOperationException("scripts of type [" + script.getType() + "]," +
                    " operation [" + scriptContext.getKey() + "] and lang [" + lang + "] are not supported");
        }

        return compileInternal(script, params, state);
    }

    /**
     * Compiles a script straight-away, or returns the previously compiled and cached script,
     * without checking if it can be executed based on settings.
     */
    CompiledScript compileInternal(Script script, Map<String, String> params, ClusterState state) {
        if (script == null) {
            throw new IllegalArgumentException("The parameter script (Script) must not be null.");
        }

        String lang = script.getLang() == null ? defaultLang : script.getLang();
        ScriptType type = script.getType();
        //script.getScript() could return either a name or code for a script,
        //but we check for a file script name first and an indexed script name second
        String name = script.getScript();

        if (logger.isTraceEnabled()) {
            logger.trace("Compiling lang: [{}] type: [{}] script: {}", lang, type, name);
        }

        ScriptEngineService scriptEngineService = getScriptEngineServiceForLang(lang);

        if (type == ScriptType.FILE) {
            CacheKey cacheKey = new CacheKey(scriptEngineService, name, null, params);
            //On disk scripts will be loaded into the staticCache by the listener
            CompiledScript compiledScript = staticCache.get(cacheKey);

            if (compiledScript == null) {
                throw new IllegalArgumentException("Unable to find on disk file script [" + name + "] using lang [" + lang + "]");
            }

            return compiledScript;
        }

        //script.getScript() will be code if the script type is inline
        String code = script.getScript();

        if (type == ScriptType.STORED) {
            //The look up for an indexed script must be done every time in case
            //the script has been updated in the index since the last look up.
            final IndexedScript indexedScript = new IndexedScript(lang, name);
            name = indexedScript.id;
            code = getScriptFromClusterState(state, indexedScript.lang, indexedScript.id);
        }

        CacheKey cacheKey = new CacheKey(scriptEngineService, type == ScriptType.INLINE ? null : name, code, params);
        CompiledScript compiledScript = cache.get(cacheKey);

        if (compiledScript == null) {
            //Either an un-cached inline script or indexed script
            //If the script type is inline the name will be the same as the code for identification in exceptions
            try {
                // but give the script engine the chance to be better, give it separate name + source code
                // for the inline case, then its anonymous: null.
                String actualName = (type == ScriptType.INLINE) ? null : name;
                compiledScript = new CompiledScript(type, name, lang, scriptEngineService.compile(actualName, code, params));
            } catch (ScriptException good) {
                // TODO: remove this try-catch completely, when all script engines have good exceptions!
                throw good; // its already good
            } catch (Exception exception) {
                throw new GeneralScriptException("Failed to compile " + type + " script [" + name + "] using lang [" + lang + "]", exception);
            }

            //Since the cache key is the script content itself we don't need to
            //invalidate/check the cache if an indexed script changes.
            scriptMetrics.onCompilation();
            cache.put(cacheKey, compiledScript);
        }

        return compiledScript;
    }

    private String validateScriptLanguage(String scriptLang) {
        if (scriptLang == null) {
            scriptLang = defaultLang;
        } else if (scriptEnginesByLang.containsKey(scriptLang) == false) {
            throw new IllegalArgumentException("script_lang not supported [" + scriptLang + "]");
        }
        return scriptLang;
    }

    String getScriptFromClusterState(ClusterState state, String scriptLang, String id) {
        scriptLang = validateScriptLanguage(scriptLang);
        ScriptMetaData scriptMetadata = state.metaData().custom(ScriptMetaData.TYPE);
        if (scriptMetadata == null) {
            throw new ResourceNotFoundException("Unable to find script [" + scriptLang + "/" + id + "] in cluster state");
        }

        String script = scriptMetadata.getScript(scriptLang, id);
        if (script == null) {
            throw new ResourceNotFoundException("Unable to find script [" + scriptLang + "/" + id + "] in cluster state");
        }
        return script;
    }

    void validate(String id, String scriptLang, BytesReference scriptBytes) {
        validateScriptSize(id, scriptBytes.length());
        try (XContentParser parser = XContentFactory.xContent(scriptBytes).createParser(scriptBytes)) {
            parser.nextToken();
            Template template = TemplateQueryBuilder.parse(scriptLang, parser, parseFieldMatcher, "params", "script", "template");
            if (Strings.hasLength(template.getScript())) {
                //Just try and compile it
                try {
                    ScriptEngineService scriptEngineService = getScriptEngineServiceForLang(scriptLang);
                    //we don't know yet what the script will be used for, but if all of the operations for this lang with
                    //indexed scripts are disabled, it makes no sense to even compile it.
                    if (isAnyScriptContextEnabled(scriptLang, scriptEngineService, ScriptType.STORED)) {
                        Object compiled = scriptEngineService.compile(id, template.getScript(), Collections.emptyMap());
                        if (compiled == null) {
                            throw new IllegalArgumentException("Unable to parse [" + template.getScript() +
                                    "] lang [" + scriptLang + "] (ScriptService.compile returned null)");
                        }
                    } else {
                        logger.warn(
                                "skipping compile of script [{}], lang [{}] as all scripted operations are disabled for indexed scripts",
                                template.getScript(), scriptLang);
                    }
                } catch (ScriptException good) {
                    // TODO: remove this when all script engines have good exceptions!
                    throw good; // its already good!
                } catch (Exception e) {
                    throw new IllegalArgumentException("Unable to parse [" + template.getScript() +
                            "] lang [" + scriptLang + "]", e);
                }
            } else {
                throw new IllegalArgumentException("Unable to find script in : " + scriptBytes.toUtf8());
            }
        } catch (IOException e) {
            throw new IllegalArgumentException("failed to parse template script", e);
        }
    }

    public void storeScript(ClusterService clusterService, PutStoredScriptRequest request, ActionListener<PutStoredScriptResponse> listener) {
        String scriptLang = validateScriptLanguage(request.scriptLang());
        //verify that the script compiles
        validate(request.id(), scriptLang, request.script());
        clusterService.submitStateUpdateTask("put-script-" + request.id(), new AckedClusterStateUpdateTask<PutStoredScriptResponse>(request, listener) {

            @Override
            protected PutStoredScriptResponse newResponse(boolean acknowledged) {
                return new PutStoredScriptResponse(acknowledged);
            }

            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                return innerStoreScript(currentState, scriptLang, request);
            }
        });
    }

    static ClusterState innerStoreScript(ClusterState currentState, String validatedScriptLang, PutStoredScriptRequest request) {
        ScriptMetaData scriptMetadata = currentState.metaData().custom(ScriptMetaData.TYPE);
        ScriptMetaData.Builder scriptMetadataBuilder = new ScriptMetaData.Builder(scriptMetadata);
        scriptMetadataBuilder.storeScript(validatedScriptLang, request.id(), request.script());
        MetaData.Builder metaDataBuilder = MetaData.builder(currentState.getMetaData())
                .putCustom(ScriptMetaData.TYPE, scriptMetadataBuilder.build());
        return ClusterState.builder(currentState).metaData(metaDataBuilder).build();
    }

    public void deleteStoredScript(ClusterService clusterService, DeleteStoredScriptRequest request, ActionListener<DeleteStoredScriptResponse> listener) {
        String scriptLang = validateScriptLanguage(request.scriptLang());
        clusterService.submitStateUpdateTask("delete-script-" + request.id(), new AckedClusterStateUpdateTask<DeleteStoredScriptResponse>(request, listener) {

            @Override
            protected DeleteStoredScriptResponse newResponse(boolean acknowledged) {
                return new DeleteStoredScriptResponse(acknowledged);
            }

            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                return innerDeleteScript(currentState, scriptLang, request);
            }
        });
    }

    static ClusterState innerDeleteScript(ClusterState currentState, String validatedLang, DeleteStoredScriptRequest request) {
        ScriptMetaData scriptMetadata = currentState.metaData().custom(ScriptMetaData.TYPE);
        ScriptMetaData.Builder scriptMetadataBuilder = new ScriptMetaData.Builder(scriptMetadata);
        scriptMetadataBuilder.deleteScript(validatedLang, request.id());
        MetaData.Builder metaDataBuilder = MetaData.builder(currentState.getMetaData())
                .putCustom(ScriptMetaData.TYPE, scriptMetadataBuilder.build());
        return ClusterState.builder(currentState).metaData(metaDataBuilder).build();
    }

    public String getStoredScript(ClusterState state, GetStoredScriptRequest request) {
        ScriptMetaData scriptMetadata = state.metaData().custom(ScriptMetaData.TYPE);
        if (scriptMetadata != null) {
            return scriptMetadata.getScript(request.lang(), request.id());
        } else {
            return null;
        }
    }

    /**
     * Compiles (or retrieves from cache) and executes the provided script
     */
    public ExecutableScript executable(Script script, ScriptContext scriptContext, Map<String, String> params, ClusterState state) {
        return executable(compile(script, scriptContext, params, state), script.getParams());
    }

    /**
     * Executes a previously compiled script provided as an argument
     */
    public ExecutableScript executable(CompiledScript compiledScript, Map<String, Object> vars) {
        return getScriptEngineServiceForLang(compiledScript.lang()).executable(compiledScript, vars);
    }

    /**
     * Compiles (or retrieves from cache) and executes the provided search script
     */
    public SearchScript search(SearchLookup lookup, Script script, ScriptContext scriptContext, Map<String, String> params, ClusterState state) {
        CompiledScript compiledScript = compile(script, scriptContext, params, state);
        return getScriptEngineServiceForLang(compiledScript.lang()).search(compiledScript, lookup, script.getParams());
    }

    private boolean isAnyScriptContextEnabled(String lang, ScriptEngineService scriptEngineService, ScriptType scriptType) {
        for (ScriptContext scriptContext : scriptContextRegistry.scriptContexts()) {
            if (canExecuteScript(lang, scriptEngineService, scriptType, scriptContext)) {
                return true;
            }
        }
        return false;
    }

    private boolean canExecuteScript(String lang, ScriptEngineService scriptEngineService, ScriptType scriptType, ScriptContext scriptContext) {
        assert lang != null;
        if (scriptContextRegistry.isSupportedContext(scriptContext) == false) {
            throw new IllegalArgumentException("script context [" + scriptContext.getKey() + "] not supported");
        }
        return scriptModes.getScriptEnabled(lang, scriptType, scriptContext);
    }

    public ScriptStats stats() {
        return scriptMetrics.stats();
    }

    private void validateScriptSize(String identifier, int scriptSizeInBytes) {
        int allowedScriptSizeInBytes = SCRIPT_MAX_SIZE_IN_BYTES.get(settings);
        if (scriptSizeInBytes > allowedScriptSizeInBytes) {
            String message = LoggerMessageFormat.format(
                    "Limit of script size in bytes [{}] has been exceeded for script [{}] with size [{}]",
                    allowedScriptSizeInBytes,
                    identifier,
                    scriptSizeInBytes
            );
            throw new IllegalArgumentException(message);
        }
    }

    /**
     * A small listener for the script cache that calls each
     * {@code ScriptEngineService}'s {@code scriptRemoved} method when the
     * script has been removed from the cache
     */
    private class ScriptCacheRemovalListener implements RemovalListener<CacheKey, CompiledScript> {
        @Override
        public void onRemoval(RemovalNotification<CacheKey, CompiledScript> notification) {
            scriptMetrics.onCacheEviction();
            for (ScriptEngineService service : scriptEngines) {
                try {
                    service.scriptRemoved(notification.getValue());
                } catch (Exception e) {
                    logger.warn("exception calling script removal listener for script service", e);
                    // We don't rethrow because Guava would just catch the
                    // exception and log it, which we have already done
                }
            }
        }
    }

    private class ScriptChangesListener extends FileChangesListener {

        private Tuple<String, String> getScriptNameExt(Path file) {
            Path scriptPath = scriptsDirectory.relativize(file);
            int extIndex = scriptPath.toString().lastIndexOf('.');
            if (extIndex <= 0) {
                return null;
            }

            String ext = scriptPath.toString().substring(extIndex + 1);
            if (ext.isEmpty()) {
                return null;
            }

            String scriptName = scriptPath.toString().substring(0, extIndex).replace(scriptPath.getFileSystem().getSeparator(), "_");
            return new Tuple<>(scriptName, ext);
        }

        @Override
        public void onFileInit(Path file) {
            Tuple<String, String> scriptNameExt = getScriptNameExt(file);
            if (scriptNameExt == null) {
                logger.debug("Skipped script with invalid extension : [{}]", file);
                return;
            }
            if (logger.isTraceEnabled()) {
                logger.trace("Loading script file : [{}]", file);
            }

            ScriptEngineService engineService = getScriptEngineServiceForFileExt(scriptNameExt.v2());
            if (engineService == null) {
                logger.warn("No script engine found for [{}]", scriptNameExt.v2());
            } else {
                try {
                    //we don't know yet what the script will be used for, but if all of the operations for this lang
                    // with file scripts are disabled, it makes no sense to even compile it and cache it.
                    if (isAnyScriptContextEnabled(engineService.getType(), engineService, ScriptType.FILE)) {
                        logger.info("compiling script file [{}]", file.toAbsolutePath());
                        try (InputStreamReader reader = new InputStreamReader(Files.newInputStream(file), StandardCharsets.UTF_8)) {
                            String script = Streams.copyToString(reader);
                            String name = scriptNameExt.v1();
                            CacheKey cacheKey = new CacheKey(engineService, name, null, Collections.emptyMap());
                            // pass the actual file name to the compiler (for script engines that care about this)
                            Object executable = engineService.compile(file.getFileName().toString(), script, Collections.emptyMap());
                            CompiledScript compiledScript = new CompiledScript(ScriptType.FILE, name, engineService.getType(), executable);
                            staticCache.put(cacheKey, compiledScript);
                            scriptMetrics.onCompilation();
                        }
                    } else {
                        logger.warn("skipping compile of script file [{}] as all scripted operations are disabled for file scripts", file.toAbsolutePath());
                    }
                } catch (Throwable e) {
                    logger.warn("failed to load/compile script [{}]", e, scriptNameExt.v1());
                }
            }
        }

        @Override
        public void onFileCreated(Path file) {
            onFileInit(file);
        }

        @Override
        public void onFileDeleted(Path file) {
            Tuple<String, String> scriptNameExt = getScriptNameExt(file);
            if (scriptNameExt != null) {
                ScriptEngineService engineService = getScriptEngineServiceForFileExt(scriptNameExt.v2());
                assert engineService != null;
                logger.info("removing script file [{}]", file.toAbsolutePath());
                staticCache.remove(new CacheKey(engineService, scriptNameExt.v1(), null, Collections.emptyMap()));
            }
        }

        @Override
        public void onFileChanged(Path file) {
            onFileInit(file);
        }

    }

    /**
     * The type of a script, more specifically where it gets loaded from:
     * - provided dynamically at request time
     * - loaded from an index
     * - loaded from file
     */
    public enum ScriptType {

        INLINE(0, "inline", "inline", false),
        STORED(1, "id", "stored", false),
        FILE(2, "file", "file", true);

        private final int val;
        private final ParseField parseField;
        private final String scriptType;
        private final boolean defaultScriptEnabled;

        public static ScriptType readFrom(StreamInput in) throws IOException {
            int scriptTypeVal = in.readVInt();
            for (ScriptType type : values()) {
                if (type.val == scriptTypeVal) {
                    return type;
                }
            }
            throw new IllegalArgumentException("Unexpected value read for ScriptType got [" + scriptTypeVal + "] expected one of ["
                    + INLINE.val + "," + FILE.val + "," + STORED.val + "]");
        }

        public static void writeTo(ScriptType scriptType, StreamOutput out) throws IOException{
            if (scriptType != null) {
                out.writeVInt(scriptType.val);
            } else {
                out.writeVInt(INLINE.val); //Default to inline
            }
        }

        ScriptType(int val, String name, String scriptType, boolean defaultScriptEnabled) {
            this.val = val;
            this.parseField = new ParseField(name);
            this.scriptType = scriptType;
            this.defaultScriptEnabled = defaultScriptEnabled;
        }

        public ParseField getParseField() {
            return parseField;
        }

        public boolean getDefaultScriptEnabled() {
            return defaultScriptEnabled;
        }

        public String getScriptType() {
            return scriptType;
        }

        @Override
        public String toString() {
            return name().toLowerCase(Locale.ROOT);
        }

    }

    private static final class CacheKey {
        final String lang;
        final String name;
        final String code;
        final Map<String, String> params;

        private CacheKey(final ScriptEngineService service, final String name, final String code, final Map<String, String> params) {
            this.lang = service.getType();
            this.name = name;
            this.code = code;
            this.params = params;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            CacheKey cacheKey = (CacheKey)o;

            if (!lang.equals(cacheKey.lang)) return false;
            if (name != null ? !name.equals(cacheKey.name) : cacheKey.name != null) return false;
            if (code != null ? !code.equals(cacheKey.code) : cacheKey.code != null) return false;
            return params.equals(cacheKey.params);

        }

        @Override
        public int hashCode() {
            int result = lang.hashCode();
            result = 31 * result + (name != null ? name.hashCode() : 0);
            result = 31 * result + (code != null ? code.hashCode() : 0);
            result = 31 * result + params.hashCode();
            return result;
        }
    }


    private static class IndexedScript {
        private final String lang;
        private final String id;

        IndexedScript(String lang, String script) {
            this.lang = lang;
            final String[] parts = script.split("/");
            if (parts.length == 1) {
                this.id = script;
            } else {
                if (parts.length != 3) {
                    throw new IllegalArgumentException("Illegal index script format [" + script + "]" +
                            " should be /lang/id");
                } else {
                    if (!parts[1].equals(this.lang)) {
                        throw new IllegalStateException("Conflicting script language, found [" + parts[1] + "] expected + ["+ this.lang + "]");
                    }
                    this.id = parts[2];
                }
            }
        }
    }
}
