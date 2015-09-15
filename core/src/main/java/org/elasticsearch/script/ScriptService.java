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

import java.nio.charset.StandardCharsets;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.collect.ImmutableMap;

import org.apache.lucene.util.IOUtils;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.indexedscripts.delete.DeleteIndexedScriptRequest;
import org.elasticsearch.action.indexedscripts.get.GetIndexedScriptRequest;
import org.elasticsearch.action.indexedscripts.put.PutIndexedScriptRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.HasContextAndHeaders;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.query.TemplateQueryParser;
import org.elasticsearch.script.expression.ExpressionScriptEngineService;
import org.elasticsearch.script.groovy.GroovyScriptEngineService;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.watcher.FileChangesListener;
import org.elasticsearch.watcher.FileWatcher;
import org.elasticsearch.watcher.ResourceWatcherService;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class ScriptService extends AbstractComponent implements Closeable {

    static final String DISABLE_DYNAMIC_SCRIPTING_SETTING = "script.disable_dynamic";

    public static final String DEFAULT_SCRIPTING_LANGUAGE_SETTING = "script.default_lang";
    public static final String SCRIPT_CACHE_SIZE_SETTING = "script.cache.max_size";
    public static final int SCRIPT_CACHE_SIZE_DEFAULT = 100;
    public static final String SCRIPT_CACHE_EXPIRE_SETTING = "script.cache.expire";
    public static final String SCRIPT_INDEX = ".scripts";
    public static final String DEFAULT_LANG = GroovyScriptEngineService.NAME;
    public static final String SCRIPT_AUTO_RELOAD_ENABLED_SETTING = "script.auto_reload_enabled";

    private final String defaultLang;

    private final Set<ScriptEngineService> scriptEngines;
    private final ImmutableMap<String, ScriptEngineService> scriptEnginesByLang;
    private final ImmutableMap<String, ScriptEngineService> scriptEnginesByExt;

    private final ConcurrentMap<String, CompiledScript> staticCache = ConcurrentCollections.newConcurrentMap();

    private final Cache<String, CompiledScript> cache;
    private final Path scriptsDirectory;

    private final ScriptModes scriptModes;
    private final ScriptContextRegistry scriptContextRegistry;

    private final ParseFieldMatcher parseFieldMatcher;

    private Client client = null;

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
    public ScriptService(Settings settings, Environment env, Set<ScriptEngineService> scriptEngines,
                         ResourceWatcherService resourceWatcherService, ScriptContextRegistry scriptContextRegistry) throws IOException {
        super(settings);
        this.parseFieldMatcher = new ParseFieldMatcher(settings);
        if (Strings.hasLength(settings.get(DISABLE_DYNAMIC_SCRIPTING_SETTING))) {
            throw new IllegalArgumentException(DISABLE_DYNAMIC_SCRIPTING_SETTING + " is not a supported setting, replace with fine-grained script settings. \n" +
                    "Dynamic scripts can be enabled for all languages and all operations by replacing `script.disable_dynamic: false` with `script.inline: on` and `script.indexed: on` in elasticsearch.yml");
        }

        this.scriptEngines = scriptEngines;
        this.scriptContextRegistry = scriptContextRegistry;
        int cacheMaxSize = settings.getAsInt(SCRIPT_CACHE_SIZE_SETTING, SCRIPT_CACHE_SIZE_DEFAULT);
        TimeValue cacheExpire = settings.getAsTime(SCRIPT_CACHE_EXPIRE_SETTING, null);
        logger.debug("using script cache with max_size [{}], expire [{}]", cacheMaxSize, cacheExpire);

        this.defaultLang = settings.get(DEFAULT_SCRIPTING_LANGUAGE_SETTING, DEFAULT_LANG);

        CacheBuilder cacheBuilder = CacheBuilder.newBuilder();
        if (cacheMaxSize >= 0) {
            cacheBuilder.maximumSize(cacheMaxSize);
        }
        if (cacheExpire != null) {
            cacheBuilder.expireAfterAccess(cacheExpire.nanos(), TimeUnit.NANOSECONDS);
        }
        this.cache = cacheBuilder.removalListener(new ScriptCacheRemovalListener()).build();

        ImmutableMap.Builder<String, ScriptEngineService> enginesByLangBuilder = ImmutableMap.builder();
        ImmutableMap.Builder<String, ScriptEngineService> enginesByExtBuilder = ImmutableMap.builder();
        for (ScriptEngineService scriptEngine : scriptEngines) {
            for (String type : scriptEngine.types()) {
                enginesByLangBuilder.put(type, scriptEngine);
            }
            for (String ext : scriptEngine.extensions()) {
                enginesByExtBuilder.put(ext, scriptEngine);
            }
        }
        this.scriptEnginesByLang = enginesByLangBuilder.build();
        this.scriptEnginesByExt = enginesByExtBuilder.build();

        this.scriptModes = new ScriptModes(this.scriptEnginesByLang, scriptContextRegistry, settings);

        // add file watcher for static scripts
        scriptsDirectory = env.scriptsFile();
        if (logger.isTraceEnabled()) {
            logger.trace("Using scripts directory [{}] ", scriptsDirectory);
        }
        FileWatcher fileWatcher = new FileWatcher(scriptsDirectory);
        fileWatcher.addListener(new ScriptChangesListener());

        if (settings.getAsBoolean(SCRIPT_AUTO_RELOAD_ENABLED_SETTING, true)) {
            // automatic reload is enabled - register scripts
            resourceWatcherService.add(fileWatcher);
        } else {
            // automatic reload is disable just load scripts once
            fileWatcher.init();
        }
    }

    //This isn't set in the ctor because doing so creates a guice circular
    @Inject(optional=true)
    public void setClient(Client client) {
        this.client = client;
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
    public CompiledScript compile(Script script, ScriptContext scriptContext, HasContextAndHeaders headersContext) {
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
            throw new ScriptException("scripts of type [" + script.getType() + "], operation [" + scriptContext.getKey() + "] and lang [" + lang + "] are disabled");
        }

        // special exception to prevent expressions from compiling as update or mapping scripts
        boolean expression = scriptEngineService instanceof ExpressionScriptEngineService;
        boolean notSupported = scriptContext.getKey().equals(ScriptContext.Standard.UPDATE.getKey()) ||
                               scriptContext.getKey().equals(ScriptContext.Standard.MAPPING.getKey());
        if (expression && notSupported) {
            throw new ScriptException("scripts of type [" + script.getType() + "]," +
                    " operation [" + scriptContext.getKey() + "] and lang [" + lang + "] are not supported");
        }

        return compileInternal(script, headersContext);
    }

    /**
     * Compiles a script straight-away, or returns the previously compiled and cached script,
     * without checking if it can be executed based on settings.
     */
    public CompiledScript compileInternal(Script script, HasContextAndHeaders context) {
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
            String cacheKey = getCacheKey(scriptEngineService, name, null);
            //On disk scripts will be loaded into the staticCache by the listener
            CompiledScript compiledScript = staticCache.get(cacheKey);

            if (compiledScript == null) {
                throw new IllegalArgumentException("Unable to find on disk file script [" + name + "] using lang [" + lang + "]");
            }

            return compiledScript;
        }

        //script.getScript() will be code if the script type is inline
        String code = script.getScript();

        if (type == ScriptType.INDEXED) {
            //The look up for an indexed script must be done every time in case
            //the script has been updated in the index since the last look up.
            final IndexedScript indexedScript = new IndexedScript(lang, name);
            name = indexedScript.id;
            code = getScriptFromIndex(indexedScript.lang, indexedScript.id, context);
        }

        String cacheKey = getCacheKey(scriptEngineService, type == ScriptType.INLINE ? null : name, code);
        CompiledScript compiledScript = cache.getIfPresent(cacheKey);

        if (compiledScript == null) {
            //Either an un-cached inline script or indexed script
            //If the script type is inline the name will be the same as the code for identification in exceptions
            try {
                compiledScript = new CompiledScript(type, name, lang, scriptEngineService.compile(code));
            } catch (Exception exception) {
                throw new ScriptException("Failed to compile " + type + " script [" + name + "] using lang [" + lang + "]", exception);
            }

            //Since the cache key is the script content itself we don't need to
            //invalidate/check the cache if an indexed script changes.
            scriptMetrics.onCompilation();
            cache.put(cacheKey, compiledScript);
        }

        return compiledScript;
    }

    public void queryScriptIndex(GetIndexedScriptRequest request, final ActionListener<GetResponse> listener) {
        String scriptLang = validateScriptLanguage(request.scriptLang());
        GetRequest getRequest = new GetRequest(request, SCRIPT_INDEX).type(scriptLang).id(request.id())
                .version(request.version()).versionType(request.versionType())
                .preference("_local"); //Set preference for no forking
        client.get(getRequest, listener);
    }

    private String validateScriptLanguage(String scriptLang) {
        if (scriptLang == null) {
            scriptLang = defaultLang;
        } else if (scriptEnginesByLang.containsKey(scriptLang) == false) {
            throw new IllegalArgumentException("script_lang not supported [" + scriptLang + "]");
        }
        return scriptLang;
    }

    String getScriptFromIndex(String scriptLang, String id, HasContextAndHeaders context) {
        if (client == null) {
            throw new IllegalArgumentException("Got an indexed script with no Client registered.");
        }
        scriptLang = validateScriptLanguage(scriptLang);
        GetRequest getRequest = new GetRequest(SCRIPT_INDEX, scriptLang, id);
        getRequest.copyContextAndHeadersFrom(context);
        GetResponse responseFields = client.get(getRequest).actionGet();
        if (responseFields.isExists()) {
            return getScriptFromResponse(responseFields);
        }
        throw new IllegalArgumentException("Unable to find script [" + SCRIPT_INDEX + "/"
                + scriptLang + "/" + id + "]");
    }

    private void validate(BytesReference scriptBytes, String scriptLang) {
        try {
            XContentParser parser = XContentFactory.xContent(scriptBytes).createParser(scriptBytes);
            parser.nextToken();
            Template template = TemplateQueryParser.parse(scriptLang, parser, parseFieldMatcher, "params", "script", "template");
            if (Strings.hasLength(template.getScript())) {
                //Just try and compile it
                try {
                    ScriptEngineService scriptEngineService = getScriptEngineServiceForLang(scriptLang);
                    //we don't know yet what the script will be used for, but if all of the operations for this lang with
                    //indexed scripts are disabled, it makes no sense to even compile it.
                    if (isAnyScriptContextEnabled(scriptLang, scriptEngineService, ScriptType.INDEXED)) {
                        Object compiled = scriptEngineService.compile(template.getScript());
                        if (compiled == null) {
                            throw new IllegalArgumentException("Unable to parse [" + template.getScript() +
                                    "] lang [" + scriptLang + "] (ScriptService.compile returned null)");
                        }
                    } else {
                        logger.warn(
                                "skipping compile of script [{}], lang [{}] as all scripted operations are disabled for indexed scripts",
                                template.getScript(), scriptLang);
                    }
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

    public void putScriptToIndex(PutIndexedScriptRequest request, ActionListener<IndexResponse> listener) {
        String scriptLang = validateScriptLanguage(request.scriptLang());
        //verify that the script compiles
        validate(request.source(), scriptLang);

        IndexRequest indexRequest = new IndexRequest(request).index(SCRIPT_INDEX).type(scriptLang).id(request.id())
                .version(request.version()).versionType(request.versionType())
                .source(request.source()).opType(request.opType()).refresh(true); //Always refresh after indexing a template
        client.index(indexRequest, listener);
    }

    public void deleteScriptFromIndex(DeleteIndexedScriptRequest request, ActionListener<DeleteResponse> listener) {
        String scriptLang = validateScriptLanguage(request.scriptLang());
        DeleteRequest deleteRequest = new DeleteRequest(request).index(SCRIPT_INDEX).type(scriptLang).id(request.id())
                .refresh(true).version(request.version()).versionType(request.versionType());
        client.delete(deleteRequest, listener);
    }

    @SuppressWarnings("unchecked")
    public static String getScriptFromResponse(GetResponse responseFields) {
        Map<String, Object> source = responseFields.getSourceAsMap();
        if (source.containsKey("template")) {
            try {
                XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
                Object template = source.get("template");
                if (template instanceof Map ){
                    builder.map((Map<String, Object>)template);
                    return builder.string();
                } else {
                    return template.toString();
                }
            } catch (IOException | ClassCastException e) {
                throw new IllegalStateException("Unable to parse "  + responseFields.getSourceAsString() + " as json", e);
            }
        } else  if (source.containsKey("script")) {
            return source.get("script").toString();
        } else {
            try {
                XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
                builder.map(responseFields.getSource());
                return builder.string();
            } catch (IOException|ClassCastException e) {
                throw new IllegalStateException("Unable to parse "  + responseFields.getSourceAsString() + " as json", e);
            }
        }
    }

    /**
     * Compiles (or retrieves from cache) and executes the provided script
     */
    public ExecutableScript executable(Script script, ScriptContext scriptContext, HasContextAndHeaders headersContext) {
        return executable(compile(script, scriptContext, headersContext), script.getParams());
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
    public SearchScript search(SearchLookup lookup, Script script, ScriptContext scriptContext) {
        CompiledScript compiledScript = compile(script, scriptContext, SearchContext.current());
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
        ScriptMode mode = scriptModes.getScriptMode(lang, scriptType, scriptContext);
        switch (mode) {
            case ON:
                return true;
            case OFF:
                return false;
            case SANDBOX:
                return scriptEngineService.sandboxed();
            default:
                throw new IllegalArgumentException("script mode [" + mode + "] not supported");
        }
    }

    public ScriptStats stats() {
        return scriptMetrics.stats();
    }

    /**
     * A small listener for the script cache that calls each
     * {@code ScriptEngineService}'s {@code scriptRemoved} method when the
     * script has been removed from the cache
     */
    private class ScriptCacheRemovalListener implements RemovalListener<String, CompiledScript> {

        @Override
        public void onRemoval(RemovalNotification<String, CompiledScript> notification) {
            if (logger.isDebugEnabled()) {
                logger.debug("notifying script services of script removal due to: [{}]", notification.getCause());
            }
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

        private Tuple<String, String> scriptNameExt(Path file) {
            Path scriptPath = scriptsDirectory.relativize(file);
            int extIndex = scriptPath.toString().lastIndexOf('.');
            if (extIndex != -1) {
                String ext = scriptPath.toString().substring(extIndex + 1);
                String scriptName = scriptPath.toString().substring(0, extIndex).replace(scriptPath.getFileSystem().getSeparator(), "_");
                return new Tuple<>(scriptName, ext);
            } else {
                return null;
            }
        }

        @Override
        public void onFileInit(Path file) {
            if (logger.isTraceEnabled()) {
                logger.trace("Loading script file : [{}]", file);
            }
            Tuple<String, String> scriptNameExt = scriptNameExt(file);
            if (scriptNameExt != null) {
                ScriptEngineService engineService = getScriptEngineServiceForFileExt(scriptNameExt.v2());
                if (engineService == null) {
                    logger.warn("no script engine found for [{}]", scriptNameExt.v2());
                } else {
                    try {
                        //we don't know yet what the script will be used for, but if all of the operations for this lang
                        // with file scripts are disabled, it makes no sense to even compile it and cache it.
                        if (isAnyScriptContextEnabled(engineService.types()[0], engineService, ScriptType.FILE)) {
                            logger.info("compiling script file [{}]", file.toAbsolutePath());
                            try(InputStreamReader reader = new InputStreamReader(Files.newInputStream(file), StandardCharsets.UTF_8)) {
                                String script = Streams.copyToString(reader);
                                String cacheKey = getCacheKey(engineService, scriptNameExt.v1(), null);
                                staticCache.put(cacheKey, new CompiledScript(ScriptType.FILE, scriptNameExt.v1(), engineService.types()[0], engineService.compile(script)));
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
        }

        @Override
        public void onFileCreated(Path file) {
            onFileInit(file);
        }

        @Override
        public void onFileDeleted(Path file) {
            Tuple<String, String> scriptNameExt = scriptNameExt(file);
            if (scriptNameExt != null) {
                ScriptEngineService engineService = getScriptEngineServiceForFileExt(scriptNameExt.v2());
                assert engineService != null;
                logger.info("removing script file [{}]", file.toAbsolutePath());
                staticCache.remove(getCacheKey(engineService, scriptNameExt.v1(), null));
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
    public static enum ScriptType {

        INLINE(0, "inline"),
        INDEXED(1, "id"),
        FILE(2, "file");

        private final int val;
        private final ParseField parseField;

        public static ScriptType readFrom(StreamInput in) throws IOException {
            int scriptTypeVal = in.readVInt();
            for (ScriptType type : values()) {
                if (type.val == scriptTypeVal) {
                    return type;
                }
            }
            throw new IllegalArgumentException("Unexpected value read for ScriptType got [" + scriptTypeVal + "] expected one of ["
                    + INLINE.val + "," + FILE.val + "," + INDEXED.val + "]");
        }

        public static void writeTo(ScriptType scriptType, StreamOutput out) throws IOException{
            if (scriptType != null) {
                out.writeVInt(scriptType.val);
            } else {
                out.writeVInt(INLINE.val); //Default to inline
            }
        }

        private ScriptType(int val, String name) {
            this.val = val;
            this.parseField = new ParseField(name);
        }

        public ParseField getParseField() {
            return parseField;
        }

        @Override
        public String toString() {
            return name().toLowerCase(Locale.ROOT);
        }
    }

    private static String getCacheKey(ScriptEngineService scriptEngineService, String name, String code) {
        String lang = scriptEngineService.types()[0];
        return lang + ":" + (name != null ? ":" + name : "") + (code != null ? ":" + code : "");
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
