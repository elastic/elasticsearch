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

import com.google.common.base.Charsets;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableMap;
import org.elasticsearch.ElasticsearchIllegalArgumentException;

import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.fielddata.IndexFieldDataService;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.TemplateQueryParser;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.watcher.FileChangesListener;
import org.elasticsearch.watcher.FileWatcher;
import org.elasticsearch.watcher.ResourceWatcherService;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class ScriptService extends AbstractComponent {

    public static final String DEFAULT_SCRIPTING_LANGUAGE_SETTING = "script.default_lang";
    public static final String DISABLE_DYNAMIC_SCRIPTING_SETTING = "script.disable_dynamic";
    public static final String DISABLE_DYNAMIC_SCRIPTING_DEFAULT = "sandbox";
    public static final String SCRIPT_INDEX = ".scripts";

    //Make static so that it has visibility in IndexedScript
    //Looked up from settings in ctor
    private static String defaultLang = "groovy";

    private final ImmutableMap<String, ScriptEngineService> scriptEngines;

    private final ConcurrentMap<String, CompiledScript> staticCache = ConcurrentCollections.newConcurrentMap();

    private final Cache<CacheKey, CompiledScript> cache;
    private final File scriptsDirectory;

    private final DynamicScriptDisabling dynamicScriptingDisabled;

    private Client client = null;

    /**
     * Enum defining the different dynamic settings for scripting, either
     * ONLY_DISK_ALLOWED (scripts must be placed on disk), EVERYTHING_ALLOWED
     * (all dynamic scripting is enabled), or SANDBOXED_ONLY (only sandboxed
     * scripting languages are allowed)
     */
    enum DynamicScriptDisabling {
        EVERYTHING_ALLOWED,
        ONLY_DISK_ALLOWED,
        SANDBOXED_ONLY;

        public static final DynamicScriptDisabling parse(String s) {
            switch (s.toLowerCase(Locale.ROOT)) {
                // true for "disable_dynamic" means only on-disk scripts are enabled
                case "true":
                case "all":
                    return ONLY_DISK_ALLOWED;
                // false for "disable_dynamic" means all scripts are enabled
                case "false":
                case "none":
                    return EVERYTHING_ALLOWED;
                // only sandboxed scripting is enabled
                case "sandbox":
                case "sandboxed":
                    return SANDBOXED_ONLY;
                default:
                    throw new ElasticsearchIllegalArgumentException("Unrecognized script allowance setting: [" + s + "]");
            }
        }
    }

    public static final ParseField SCRIPT_LANG = new ParseField("lang","script_lang");

    public static final ParseField VALUE_SCRIPT_FILE = new ParseField("value_script_file");
    public static final ParseField VALUE_SCRIPT_ID = new ParseField("value_script_id");
    public static final ParseField VALUE_SCRIPT_INLINE = new ParseField("value_script");

    public static final ParseField KEY_SCRIPT_FILE = new ParseField("key_script_file");
    public static final ParseField KEY_SCRIPT_ID = new ParseField("key_script_id");
    public static final ParseField KEY_SCRIPT_INLINE = new ParseField("key_script");

    public static final ParseField SCRIPT_FILE = new ParseField("script_file","file");
    public static final ParseField SCRIPT_ID = new ParseField("script_id", "id");
    public static final ParseField SCRIPT_INLINE = new ParseField("script","scriptField");

    public static enum ScriptType {

        INLINE,
        INDEXED,
        FILE;

        private static final int INLINE_VAL = 0;
        private static final int INDEXED_VAL = 1;
        private static final int FILE_VAL = 2;

        public static ScriptType readFrom(StreamInput in) throws IOException {
            int scriptTypeVal = in.readVInt();
            switch (scriptTypeVal) {
                case INDEXED_VAL:
                    return INDEXED;
                case INLINE_VAL:
                    return INLINE;
                case FILE_VAL:
                    return FILE;
                default:
                    throw new ElasticsearchIllegalArgumentException("Unexpected value read for ScriptType got [" + scriptTypeVal +
                            "] expected one of ["+INLINE_VAL +"," + INDEXED_VAL + "," + FILE_VAL+"]");
            }

    }

        public static void writeTo(ScriptType scriptType, StreamOutput out) throws IOException{
            if (scriptType != null) {
                switch (scriptType){
                    case INDEXED:
                        out.writeVInt(INDEXED_VAL);
                        return;
                    case INLINE:
                        out.writeVInt(INLINE_VAL);
                        return;
                    case FILE:
                        out.writeVInt(FILE_VAL);
                        return;
                    default:
                        throw new ElasticsearchIllegalStateException("Unknown ScriptType " + scriptType);
                }
            } else {
                out.writeVInt(INLINE_VAL); //Default to inline
            }
        }
    }

    static class IndexedScript {
        String lang;
        String id;

        IndexedScript(String lang, String script) {
            this.lang = lang;
            final String[] parts = script.split("/");
            if (parts.length == 1) {
                this.id = script;
                if (this.lang == null){
                    this.lang = defaultLang;
                }
            } else {
                if (parts.length != 3) {
                    throw new ElasticsearchIllegalArgumentException("Illegal index script format [" + script + "]" +
                            " should be /lang/id");
                } else {
                    if (!parts[1].equals(this.lang)) {
                        throw new ElasticsearchIllegalStateException("Conflicting script language, found [" + parts[1] + "] expected + ["+ this.lang + "]");
                    }
                    this.id = parts[2];
                }
            }
        }
    }

    @Inject
    public ScriptService(Settings settings, Environment env, Set<ScriptEngineService> scriptEngines,
                         ResourceWatcherService resourceWatcherService) {
        super(settings);

        int cacheMaxSize = componentSettings.getAsInt("cache.max_size", 500);
        TimeValue cacheExpire = componentSettings.getAsTime("cache.expire", null);
        logger.debug("using script cache with max_size [{}], expire [{}]", cacheMaxSize, cacheExpire);

        this.defaultLang = settings.get(DEFAULT_SCRIPTING_LANGUAGE_SETTING, "mvel");
        this.dynamicScriptingDisabled = DynamicScriptDisabling.parse(settings.get(DISABLE_DYNAMIC_SCRIPTING_SETTING, DISABLE_DYNAMIC_SCRIPTING_DEFAULT));

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
        if (logger.isTraceEnabled()) {
            logger.trace("Using scripts directory [{}] ", scriptsDirectory);
        }
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

    //This isn't set in the ctor because doing so creates a guice circular
    @Inject(optional=true)
    public void setClient(Client client) {
        this.client = client;
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
        return compile(lang, script, ScriptType.INLINE);
    }

    public CompiledScript compile(String lang,  String script, ScriptType scriptType) {
        if (logger.isTraceEnabled()) {
            logger.trace("Compiling lang: [{}] type: [{}] script: {}", lang, scriptType, script);
        }

        CacheKey cacheKey;
        CompiledScript compiled;

        if(scriptType == ScriptType.INDEXED) {
            if (client == null) {
                throw new ElasticsearchIllegalArgumentException("Got an indexed script with no Client registered.");
            }

            final IndexedScript indexedScript = new IndexedScript(lang,script);

            if (lang != null && !lang.equals(indexedScript.lang)) {
                logger.trace("Overriding lang to " + indexedScript.lang);
                lang = indexedScript.lang;
            }

            verifyDynamicScripting(indexedScript.lang); //Since anyone can index a script, disable indexed scripting
                                          // if dynamic scripting is disabled, perhaps its own setting ?

            script = getScriptFromIndex(client, indexedScript.lang, indexedScript.id);
        } else if (scriptType == ScriptType.FILE) {

            compiled = staticCache.get(script); //On disk scripts will be loaded into the staticCache by the listener

            if (compiled != null) {
                return compiled;
            } else {
                throw new ElasticsearchIllegalArgumentException("Unable to find on disk script " + script);
            }
        }

        if (scriptType != ScriptType.INDEXED) {
            //For backwards compat attempt to load from disk
            compiled = staticCache.get(script); //On disk scripts will be loaded into the staticCache by the listener

            if (compiled != null) {
                return compiled;
            }
        }

        if (lang == null) {
            lang = defaultLang;
        }

        //This is an inline script check to see if we have it in the cache
        verifyDynamicScripting(lang);

        cacheKey = new CacheKey(lang, script);

        compiled = cache.getIfPresent(cacheKey);
        if (compiled != null) {
            return compiled;
        }

        //Either an un-cached inline script or an indexed script

        if (!dynamicScriptEnabled(lang)) {
            throw new ScriptException("dynamic scripting for [" + lang + "] disabled");
        }

        if (cacheKey == null) {
            cacheKey = new CacheKey(lang, script);
        }
        // not the end of the world if we compile it twice...
        compiled = getCompiledScript(lang, script);
        //Since the cache key is the script content itself we don't need to
        //invalidate/check the cache if an indexed script changes.
        cache.put(cacheKey, compiled);

        return compiled;
    }

    private CompiledScript getCompiledScript(String lang, String script) {
        CompiledScript compiled;ScriptEngineService service = scriptEngines.get(lang);
        if (service == null) {
            throw new ElasticsearchIllegalArgumentException("script_lang not supported [" + lang + "]");
        }

        compiled = new CompiledScript(lang, service.compile(script));
        return compiled;
    }

    private void verifyDynamicScripting(String lang) {
        if (!dynamicScriptEnabled(lang)) {
            throw new ScriptException("dynamic scripting for [" + lang + "] disabled");
        }
    }

    public GetResponse queryScriptIndex(Client client, String scriptLang, String id){
        return queryScriptIndex(client, scriptLang, id, Versions.MATCH_ANY, VersionType.INTERNAL);
    }

    public GetResponse queryScriptIndex(Client client, String scriptLang, String id,
                                        long version, VersionType versionType){
        scriptLang = validateScriptLanguage(scriptLang);
        return client.prepareGet(SCRIPT_INDEX, scriptLang, id)
                .setVersion(version)
                .setVersionType(versionType)
                .get();
    }

    private String validateScriptLanguage(String scriptLang) {
        if (scriptLang == null){
            scriptLang = defaultLang;
        } else if (!scriptEngines.containsKey(scriptLang)){
            throw new ElasticsearchIllegalArgumentException("script_lang not supported ["+scriptLang+"]");
        }
        return scriptLang;
    }

    private String getScriptFromIndex(Client client, String scriptLang, String id) {
        GetResponse responseFields = queryScriptIndex(client,scriptLang,id);
        if (responseFields.isExists()) {
            return getScriptFromResponse(responseFields);
        }
        throw new ElasticsearchIllegalArgumentException("Unable to find script [" + SCRIPT_INDEX + "/"
                + scriptLang + "/" + id + "]");
    }

    private void validate(BytesReference scriptBytes, String scriptLang) throws IOException{
        XContentParser parser = XContentFactory.xContent(scriptBytes).createParser(scriptBytes);
        TemplateQueryParser.TemplateContext context = TemplateQueryParser.parse(parser, "params", "script", "template");
        if (Strings.hasLength(context.template()) == true){
            //Just try and compile it
            //This will have the benefit of also adding the script to the cache if it compiles
            try {
                CompiledScript compiledScript = compile(scriptLang, context.template(), ScriptService.ScriptType.INLINE);
                if (compiledScript == null) {
                    throw new ElasticsearchIllegalArgumentException("Unable to parse [" + context.template() +
                            "] lang [" + scriptLang + "] (ScriptService.compile returned null)");
                }
            } catch (Exception e) {
                throw new ElasticsearchIllegalArgumentException("Unable to parse [" + context.template() +
                        "] lang [" + scriptLang + "]", e);
            }
        } else {
            throw new ElasticsearchIllegalArgumentException("Unable to find script in : " + scriptBytes.toUtf8());
        }

    }

    public void putScriptToIndex(Client client, BytesReference scriptBytes, @Nullable String scriptLang, String id,
                                 @Nullable TimeValue timeout, @Nullable String sOpType, long version,
                                 VersionType versionType, ActionListener<IndexResponse> listener) {
        try {
            scriptLang = validateScriptLanguage(scriptLang);

            //verify that the script compiles
            validate(scriptBytes, scriptLang);

            IndexRequest indexRequest = new IndexRequest(SCRIPT_INDEX, scriptLang, id);
            indexRequest.listenerThreaded(false);
            indexRequest.operationThreaded(false);
            indexRequest.version(version);
            indexRequest.versionType(versionType);
            indexRequest.refresh(true); //Always refresh after indexing a template

            indexRequest.source(scriptBytes, true);
            if (timeout != null) {
                indexRequest.timeout(timeout);
            }

            if (sOpType != null) {
                indexRequest.opType(IndexRequest.OpType.fromString(sOpType));
            }

            client.index(indexRequest, listener);
        } catch (Throwable t){
            listener.onFailure(t);
        }
    }

    public void deleteScriptFromIndex(Client client, @Nullable String scriptLang, String id,
                                      long version, ActionListener<DeleteResponse> listener) {
        scriptLang = validateScriptLanguage(scriptLang);
        client.delete((new DeleteRequest(SCRIPT_INDEX,scriptLang,id)).refresh(true).version(version), listener);
    }

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
                throw new ElasticsearchIllegalStateException("Unable to parse "  + responseFields.getSourceAsString() + " as json", e);
            }
        } else  if (source.containsKey("script")) {
            return source.get("script").toString();
        } else {
            try {
                XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
                builder.map(responseFields.getSource());
                return builder.string();
            } catch (IOException|ClassCastException e) {
                throw new ElasticsearchIllegalStateException("Unable to parse "  + responseFields.getSourceAsString() + " as json", e);
            }
        }
    }

    public ExecutableScript executable(String lang, String script, ScriptType scriptType, Map vars) {
        return executable(compile(lang, script, scriptType), vars);
    }

    public ExecutableScript executable(CompiledScript compiledScript, Map vars) {
        return scriptEngines.get(compiledScript.lang()).executable(compiledScript.compiled(), vars);
    }

    public SearchScript search(CompiledScript compiledScript, SearchLookup lookup, @Nullable Map<String, Object> vars) {
        return scriptEngines.get(compiledScript.lang()).search(compiledScript.compiled(), lookup, vars);
    }

    public SearchScript search(SearchLookup lookup, String lang, String script, ScriptType scriptType, @Nullable Map<String, Object> vars) {
        return search(compile(lang, script, scriptType), lookup, vars);
    }

    public SearchScript search(MapperService mapperService, IndexFieldDataService fieldDataService, String lang, String script, ScriptType scriptType, @Nullable Map<String, Object> vars) {
        return search(compile(lang, script), new SearchLookup(mapperService, fieldDataService, null), vars);
    }

    public Object execute(CompiledScript compiledScript, Map vars) {
        return scriptEngines.get(compiledScript.lang()).execute(compiledScript.compiled(), vars);
    }

    public void clear() {
        cache.invalidateAll();
    }

    private boolean dynamicScriptEnabled(String lang) {
        ScriptEngineService service = scriptEngines.get(lang);
        if (service == null) {
            throw new ElasticsearchIllegalArgumentException("script_lang not supported [" + lang + "]");
        }

        // Templating languages (mustache) and native scripts are always
        // allowed, "native" executions are registered through plugins
        if (this.dynamicScriptingDisabled == DynamicScriptDisabling.EVERYTHING_ALLOWED || "native".equals(lang) || "mustache".equals(lang)) {
            return true;
        } else if (this.dynamicScriptingDisabled == DynamicScriptDisabling.ONLY_DISK_ALLOWED) {
            return false;
        } else {
            return service.sandboxed();
        }
    }

    private class ScriptChangesListener extends FileChangesListener {

        private Tuple<String, String> scriptNameExt(File file) {
            String scriptPath = scriptsDirectory.toURI().relativize(file.toURI()).getPath();
            int extIndex = scriptPath.lastIndexOf('.');
            if (extIndex != -1) {
                String ext = scriptPath.substring(extIndex + 1);
                String scriptName = scriptPath.substring(0, extIndex).replace(File.separatorChar, '_');
                return new Tuple<>(scriptName, ext);
            } else {
                return null;
            }
        }

        @Override
        public void onFileInit(File file) {
            if (logger.isTraceEnabled()) {
                logger.trace("Loading script file : [{}]", file);
            }
            Tuple<String, String> scriptNameExt = scriptNameExt(file);
            if (scriptNameExt != null) {
                boolean found = false;
                for (ScriptEngineService engineService : scriptEngines.values()) {
                    for (String s : engineService.extensions()) {
                        if (s.equals(scriptNameExt.v2())) {
                            found = true;
                            try {
                                logger.info("compiling script file [{}]", file.getAbsolutePath());
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
            logger.info("removing script file [{}]", file.getAbsolutePath());
            staticCache.remove(scriptNameExt.v1());
        }

        @Override
        public void onFileChanged(File file) {
            onFileInit(file);
        }

    }

    public final static class CacheKey {
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
