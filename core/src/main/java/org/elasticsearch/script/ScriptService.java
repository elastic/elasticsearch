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

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.ResourceNotFoundException;
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
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.common.cache.RemovalListener;
import org.elasticsearch.common.cache.RemovalNotification;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.env.Environment;
import org.elasticsearch.script.Script.FileScriptLookup;
import org.elasticsearch.script.Script.InlineScriptLookup;
import org.elasticsearch.script.Script.ScriptType;
import org.elasticsearch.script.Script.StoredScriptLookup;
import org.elasticsearch.script.Script.StoredScriptSource;
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
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentMap;

import static java.util.Collections.unmodifiableMap;
import static org.elasticsearch.script.Script.DEFAULT_SCRIPT_NAME;
import static org.elasticsearch.script.Script.ScriptType.FILE;
import static org.elasticsearch.script.Script.ScriptType.INLINE;
import static org.elasticsearch.script.Script.ScriptType.STORED;

public class ScriptService extends AbstractComponent implements Closeable, ClusterStateListener {

    private class ScriptChangesListener implements FileChangesListener {

        private class FileScriptSource {
            private final ScriptEngineService engine;
            private final String id;

            private FileScriptSource(ScriptEngineService engine, String id) {
                this.engine = engine;
                this.id = id;
            }
        }

        private FileScriptSource splitScriptPath(Path file) {
            Path scriptPath = fileScriptsDirectory.relativize(file);

            int extIndex = scriptPath.toString().lastIndexOf('.');

            if (extIndex <= 0) {
                return null;
            }

            String ext = scriptPath.toString().substring(extIndex + 1);

            if (ext.isEmpty()) {
                return null;
            }

            ScriptEngineService engine = getScriptEngineServiceForExt(ext);

            String id = scriptPath.toString().substring(0, extIndex).replace(scriptPath.getFileSystem().getSeparator(), "_");

            if (id.isEmpty()) {
                return null;
            }

            return new FileScriptSource(engine, id);
        }

        @Override
        public void onFileInit(Path file) {
            try {
                FileScriptSource source = splitScriptPath(file);

                if (source == null) {
                    logger.debug("skipped loading [{}] script with invalid an id/extension [{}]", FILE.name, file);

                    return;
                }

                if (logger.isTraceEnabled()) {
                    logger.trace("loading [{}] script [{}]", FILE.name, file);
                }

                canExecuteScriptInAnyContext(FILE, source.engine.getType());

                logger.info("compiling [{}] script [{}]", FILE.name, file.toAbsolutePath());

                InputStreamReader reader = new InputStreamReader(Files.newInputStream(file), StandardCharsets.UTF_8);
                String code = Streams.copyToString(reader);
                CompiledScript compiled = compile(false, FILE, source.id, source.engine.getType(), code, Collections.emptyMap());

                FileScriptLookup key = new FileScriptLookup(source.id);
                fileCache.put(key, compiled);
            } catch (Exception exception) {
                logger.warn((Supplier<?>) () -> new ParameterizedMessage("failed to load [{}] script [{}]", FILE.name, file), exception);
            }
        }

        @Override
        public void onFileCreated(Path file) {
            onFileInit(file);
        }

        @Override
        public void onFileChanged(Path file) {
            onFileInit(file);
        }

        @Override
        public void onFileDeleted(Path file) {
            try {
                FileScriptSource source = splitScriptPath(file);

                if (source == null) {
                    logger.debug("skipped removing [{}] script with invalid an id/extension [{}]", FILE.name, file);

                    return;
                }

                logger.info("removing [{}] script [{}]", FILE.name, file.toAbsolutePath());

                FileScriptLookup key = new FileScriptLookup(source.id);
                fileCache.remove(key);
            } catch (Exception exception) {
                logger.warn((Supplier<?>) () -> new ParameterizedMessage("failed to remove [{}] script [{}]", FILE.name, file), exception);
            }
        }
    }

    private static class StoredScriptCacheKey {
        private final String id;
        private final String code;

        private StoredScriptCacheKey(String id, String code) {
            this.id = id;
            this.code = code;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            StoredScriptCacheKey that = (StoredScriptCacheKey)o;

            if (!id.equals(that.id)) return false;
            return code.equals(that.code);

        }

        @Override
        public int hashCode() {
            int result = id.hashCode();
            result = 31 * result + code.hashCode();
            return result;
        }
    }

    private class StoredCacheRemovalListener implements RemovalListener<StoredScriptCacheKey, CompiledScript> {
        @Override
        public void onRemoval(RemovalNotification<StoredScriptCacheKey, CompiledScript> notification) {
            if (logger.isDebugEnabled()) {
                logger.debug("removed [{}] from stored cache, reason [{}]", notification.getValue(), notification.getRemovalReason());
            }

            scriptMetrics.onCacheEviction();
        }
    }

    private class InlineCacheRemovalListener implements RemovalListener<InlineScriptLookup, CompiledScript> {
        @Override
        public void onRemoval(RemovalNotification<InlineScriptLookup, CompiledScript> notification) {
            if (logger.isDebugEnabled()) {
                logger.debug("removed [{}] from inline cache, reason [{}]", notification.getValue(), notification.getRemovalReason());
            }

            scriptMetrics.onCacheEviction();
        }
    }

    public static final Setting<Integer> SCRIPT_CACHE_SIZE_SETTING =
        Setting.intSetting("script.cache.max_size", 100, 0, Property.NodeScope);
    public static final Setting<TimeValue> SCRIPT_CACHE_EXPIRE_SETTING =
        Setting.positiveTimeSetting("script.cache.expire", TimeValue.timeValueMillis(0), Property.NodeScope);
    public static final Setting<Boolean> SCRIPT_AUTO_RELOAD_ENABLED_SETTING =
        Setting.boolSetting("script.auto_reload_enabled", true, Property.NodeScope);
    public static final Setting<Integer> SCRIPT_MAX_SIZE_IN_LENGTH =
        Setting.intSetting("script.max_size_in_length", 16384, Property.NodeScope);
    public static final Setting<Integer> SCRIPT_MAX_COMPILATIONS_PER_MINUTE =
        Setting.intSetting("script.max_compilations_per_minute", 15, 0, Property.Dynamic, Property.NodeScope);

    public final ScriptModes scriptModes;
    private final ScriptContextRegistry scriptContextRegistry;
    private final ScriptMetrics scriptMetrics = new ScriptMetrics();

    private final Collection<ScriptEngineService> scriptEngines;
    private final Map<String, ScriptEngineService> scriptEnginesByLang;
    private final Map<String, ScriptEngineService> scriptEnginesByExt;

    private final Path fileScriptsDirectory;
    private final ConcurrentMap<FileScriptLookup, CompiledScript> fileCache = ConcurrentCollections.newConcurrentMap();

    private final Cache<StoredScriptCacheKey, CompiledScript> storedCache;
    private final Cache<InlineScriptLookup, CompiledScript> inlineCache;

    private ClusterState clusterState;

    private int totalCompilesPerMinute;
    private long lastInlineCompileTime;
    private double scriptsPerMinCounter;
    private double compilesAllowedPerNano;

    public ScriptService(Settings settings, Environment env,
                         ResourceWatcherService resourceWatcherService, ScriptEngineRegistry scriptEngineRegistry,
                         ScriptContextRegistry scriptContextRegistry, ScriptSettings scriptSettings) throws IOException {
        super(settings);

        Objects.requireNonNull(scriptEngineRegistry);
        Objects.requireNonNull(scriptContextRegistry);
        Objects.requireNonNull(scriptSettings);

        this.scriptModes = new ScriptModes(scriptSettings, settings);
        this.scriptContextRegistry = scriptContextRegistry;

        this.scriptEngines = scriptEngineRegistry.getRegisteredLanguages().values();

        Map<String, ScriptEngineService> enginesByLangBuilder = new HashMap<>();
        Map<String, ScriptEngineService> enginesByExtBuilder = new HashMap<>();

        for (ScriptEngineService engine : this.scriptEngines) {
            enginesByLangBuilder.put(engine.getType(), engine);
            enginesByExtBuilder.put(engine.getExtension(), engine);
        }

        this.scriptEnginesByLang = unmodifiableMap(enginesByLangBuilder);
        this.scriptEnginesByExt = unmodifiableMap(enginesByExtBuilder);

        // add file watcher for static scripts
        this.fileScriptsDirectory = env.scriptsFile();

        if (logger.isTraceEnabled()) {
            logger.trace("using scripts directory [{}] ", this.fileScriptsDirectory);
        }

        FileWatcher fileWatcher = new FileWatcher(fileScriptsDirectory);
        fileWatcher.addListener(new ScriptChangesListener());

        if (SCRIPT_AUTO_RELOAD_ENABLED_SETTING.get(settings)) {
            // automatic reload is enabled, register scripts
            resourceWatcherService.add(fileWatcher);
        } else {
            // automatic reload is disabled, just load scripts once
            fileWatcher.init();
        }

        CacheBuilder<StoredScriptCacheKey, CompiledScript> storedCacheBuilder = CacheBuilder.builder();
        CacheBuilder<InlineScriptLookup, CompiledScript> inlineCacheBuilder = CacheBuilder.builder();
        int cacheMaxSize = SCRIPT_CACHE_SIZE_SETTING.get(settings);
        TimeValue cacheExpire = SCRIPT_CACHE_EXPIRE_SETTING.get(settings);

        logger.debug("using stored and inline caches with max size [{}], expire [{}]", cacheMaxSize, cacheExpire);

        if (cacheMaxSize >= 0) {
            storedCacheBuilder.setMaximumWeight(cacheMaxSize);
            inlineCacheBuilder.setMaximumWeight(cacheMaxSize);
        }

        if (cacheExpire.getNanos() != 0) {
            storedCacheBuilder.setExpireAfterAccess(cacheExpire.nanos());
            inlineCacheBuilder.setExpireAfterAccess(cacheExpire.nanos());
        }

        this.storedCache = storedCacheBuilder.removalListener(new StoredCacheRemovalListener()).build();
        this.inlineCache = inlineCacheBuilder.removalListener(new InlineCacheRemovalListener()).build();

        this.lastInlineCompileTime = System.nanoTime();
        this.setMaxCompilationsPerMinute(SCRIPT_MAX_COMPILATIONS_PER_MINUTE.get(settings));
    }

    @Override
    public void close() throws IOException {
        IOUtils.close(scriptEngines);
    }

    void registerClusterSettingsListeners(ClusterSettings clusterSettings) {
        clusterSettings.addSettingsUpdateConsumer(SCRIPT_MAX_COMPILATIONS_PER_MINUTE, this::setMaxCompilationsPerMinute);
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        clusterState = event.state();
    }

    public ScriptStats stats() {
        return scriptMetrics.stats();
    }

    private ScriptEngineService getScriptEngineServiceForLang(String lang) {
        ScriptEngineService engine = scriptEnginesByLang.get(lang);

        if (engine == null) {
            throw new ResourceNotFoundException("script lang [" + lang + "] does not exist");
        }

        return engine;
    }

    private ScriptEngineService getScriptEngineServiceForExt(String ext) {
        ScriptEngineService engine = scriptEnginesByExt.get(ext);

        if (engine == null) {
            throw new ResourceNotFoundException("script ext [" + ext + "] does not exist");
        }

        return engine;
    }

    private void canExecuteScriptInAnyContext(ScriptType type, String lang) {
        for (ScriptContext context : scriptContextRegistry.scriptContexts()) {
            try {
                canExecuteScriptInSpecificContext(context, type, lang);

                return;
            } catch (IllegalStateException exception) {
                // do nothing
            }
        }

        throw new IllegalStateException("cannot execute [" + type.name + "] script using lang [" + lang + "] under any context");
    }

    private void canExecuteScriptInSpecificContext(ScriptContext context, ScriptType type, String lang) {
        if (!scriptContextRegistry.isSupportedContext(context)) {
            throw new IllegalArgumentException("script context [" + context.getKey() + "] does not exist");
        }

        if (!scriptModes.getScriptEnabled(lang, type, context)) {
            throw new IllegalStateException(
                "[" + type.name + "] scripts using lang [" + lang + "] with operation [" + context + "] are disabled");
        }
    }

    void setMaxCompilationsPerMinute(Integer newMaxPerMinute) {
        this.totalCompilesPerMinute = newMaxPerMinute;
        // Reset the counter to allow new compilations
        this.scriptsPerMinCounter = totalCompilesPerMinute;
        this.compilesAllowedPerNano = ((double) totalCompilesPerMinute) / TimeValue.timeValueMinutes(1).nanos();
    }

    /**
     * Check whether there have been too many compilations within the last minute, throwing a circuit breaking exception if so.
     * This is a variant of the token bucket algorithm: https://en.wikipedia.org/wiki/Token_bucket
     *
     * It can be thought of as a bucket with water, every time the bucket is checked, water is added proportional to the amount of time that
     * elapsed since the last time it was checked. If there is enough water, some is removed and the request is allowed. If there is not
     * enough water the request is denied. Just like a normal bucket, if water is added that overflows the bucket, the extra water/capacity
     * is discarded - there can never be more water in the bucket than the size of the bucket.
     */
    void checkCompilationLimit() {
        long now = System.nanoTime();
        long timePassed = now - lastInlineCompileTime;
        lastInlineCompileTime = now;

        scriptsPerMinCounter += (timePassed) * compilesAllowedPerNano;

        // It's been over the time limit anyway, readjust the bucket to be level
        if (scriptsPerMinCounter > totalCompilesPerMinute) {
            scriptsPerMinCounter = totalCompilesPerMinute;
        }

        // If there is enough tokens in the bucket, allow the request and decrease the tokens by 1
        if (scriptsPerMinCounter >= 1) {
            scriptsPerMinCounter -= 1.0;
        } else {
            // Otherwise reject the request
            throw new CircuitBreakingException("[script] Too many dynamic script compilations within one minute, max: [" +
                            totalCompilesPerMinute + "/min]; please use on-disk, indexed, or scripts with parameters instead; " +
                            "this limit can be changed by the [" + SCRIPT_MAX_COMPILATIONS_PER_MINUTE.getKey() + "] setting");
        }
    }

    public void putStoreScript(ClusterService clusterService, PutStoredScriptRequest request,
                               ActionListener<PutStoredScriptResponse> listener) {
        String id = request.id();
        StoredScriptSource source = request.source();

        int max = SCRIPT_MAX_SIZE_IN_LENGTH.get(settings);

        if (source.code.length() > max) {
            throw new IllegalArgumentException("limit of script size in length [" + max + "]" +
                " has been exceeded for script [" + id + "] with size [" + source.code.length() +"]");
        } else if (Strings.isEmpty(source.code)) {
            throw new IllegalArgumentException("cannot have an empty script [" + id + "]");
        }

        getScriptEngineServiceForLang(source.lang);
        canExecuteScriptInAnyContext(STORED, source.lang);
        compile(true, STORED, id, source.lang, source.code, source.options);

        clusterService.submitStateUpdateTask("put-script-" + request.id(),
            new AckedClusterStateUpdateTask<PutStoredScriptResponse>(request, listener) {

            @Override
            protected PutStoredScriptResponse newResponse(boolean acknowledged) {
                return new PutStoredScriptResponse(acknowledged);
            }

            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                return ScriptMetaData.storeScript(currentState, request.id(), request.source());
            }
        });
    }

    public void deleteStoredScript(ClusterService clusterService, DeleteStoredScriptRequest request,
                                   ActionListener<DeleteStoredScriptResponse> listener) {
        clusterService.submitStateUpdateTask("delete-script-" + request.id(),
            new AckedClusterStateUpdateTask<DeleteStoredScriptResponse>(request, listener) {

            @Override
            protected DeleteStoredScriptResponse newResponse(boolean acknowledged) {
                return new DeleteStoredScriptResponse(acknowledged);
            }

            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                return ScriptMetaData.deleteScript(currentState, request.id());
            }
        });
    }

    public StoredScriptSource getStoredScript(ClusterState state, GetStoredScriptRequest request) {
        return ScriptMetaData.getScript(state, request.id());
    }

    protected CompiledScript getFileScript(ScriptContext context, FileScriptLookup lookup) {
        CompiledScript compiled = fileCache.get(lookup);

        if (compiled == null) {
            throw new ResourceNotFoundException("file script [" + lookup.id + "] does not exist");
        }

        canExecuteScriptInSpecificContext(context, FILE, compiled.lang());

        return compiled;
    }

    protected CompiledScript getStoredScript(ScriptContext context, StoredScriptLookup lookup) {
        StoredScriptSource source = ScriptMetaData.getScript(clusterState, lookup.id);

        if (source == null) {
            throw new ResourceNotFoundException("stored script [" + lookup.id + "] does not exist");
        }

        canExecuteScriptInSpecificContext(context, STORED, source.lang);

        StoredScriptCacheKey key = new StoredScriptCacheKey(lookup.id, source.code);
        CompiledScript compiled = storedCache.get(key);

        if (compiled == null) {
            synchronized(this) {
                compiled = storedCache.get(key);

                if (compiled == null) {
                    compiled = compile(true, STORED, lookup.id, source.lang, source.code, source.options);
                    storedCache.put(key, compiled);
                }
            }
        }

        return compiled;
    }

    protected CompiledScript getInlineScript(ScriptContext context, InlineScriptLookup lookup) {
        canExecuteScriptInSpecificContext(context, INLINE, lookup.lang);

        CompiledScript compiled = inlineCache.get(lookup);

        if (compiled == null) {
            synchronized (this) {
                compiled = inlineCache.get(lookup);

                if (compiled == null) {
                    compiled = compile(true, INLINE, DEFAULT_SCRIPT_NAME, lookup.lang, lookup.code, lookup.options);
                    inlineCache.put(lookup, compiled);
                }
            }
        }

        return compiled;
    }

    private CompiledScript compile(boolean limit, ScriptType type,
                                   String id, String lang, String code, Map<String, String> options) {
        if (logger.isTraceEnabled()) {
            logger.trace("compiling script with type [{}], lang [{}], options [{}]", type, lang, options);
        }

        try {
            if (limit) {
                checkCompilationLimit();
            }

            ScriptEngineService engine = getScriptEngineServiceForLang(lang);
            Object compiled = engine.compile(id, code, options);
            scriptMetrics.onCompilation();

            return new CompiledScript(type, id, engine, compiled);
        } catch (ScriptException exception) {
            // TODO: remove this try-catch completely, when all script engines have good exceptions!
            throw exception; // its already good
        } catch (Exception exception) {
            throw new GeneralScriptException("failed to compile " + type + " script [" + id + "] using lang [" + lang + "]", exception);
        }
    }
}
