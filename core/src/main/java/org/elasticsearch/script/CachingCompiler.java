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

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.common.cache.RemovalListener;
import org.elasticsearch.common.cache.RemovalNotification;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.env.Environment;
import org.elasticsearch.watcher.FileChangesListener;
import org.elasticsearch.watcher.FileWatcher;
import org.elasticsearch.watcher.ResourceWatcherService;

import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.concurrent.ConcurrentMap;

/**
 * Manages caching, resource watching, permissions checking, and compilation of scripts (or templates).
 */
public abstract class CachingCompiler<CacheKeyT> implements ClusterStateListener {
    private static final Logger logger = ESLoggerFactory.getLogger(CachingCompiler.class);

    /**
     * Compiled file scripts (or templates). Modified by the file watching process.
     */
    private final ConcurrentMap<CacheKeyT, CompiledScript> fileScripts = ConcurrentCollections.newConcurrentMap();

    /**
     * Cache of compiled dynamic scripts (or templates).
     */
    private final Cache<CacheKeyT, CompiledScript> cache;

    private final Path scriptsDirectory;

    private final ScriptMetrics scriptMetrics;

    private volatile ClusterState clusterState;

    public CachingCompiler(Settings settings, ScriptSettings scriptSettings, Environment env,
            ResourceWatcherService resourceWatcherService, ScriptMetrics scriptMetrics) throws IOException {
        int cacheMaxSize = ScriptService.SCRIPT_CACHE_SIZE_SETTING.get(settings);

        CacheBuilder<CacheKeyT, CompiledScript> cacheBuilder = CacheBuilder.builder();
        if (cacheMaxSize >= 0) {
            cacheBuilder.setMaximumWeight(cacheMaxSize);
        }

        TimeValue cacheExpire = ScriptService.SCRIPT_CACHE_EXPIRE_SETTING.get(settings);
        if (cacheExpire.getNanos() != 0) {
            cacheBuilder.setExpireAfterAccess(cacheExpire);
        }

        logger.debug("using script cache with max_size [{}], expire [{}]", cacheMaxSize, cacheExpire);
        this.cache = cacheBuilder.removalListener(new CacheRemovalListener()).build();
        
        // add file watcher for file scripts
        scriptsDirectory = env.scriptsFile();
        if (logger.isTraceEnabled()) {
            logger.trace("Using scripts directory [{}] ", scriptsDirectory);
        }
        FileWatcher fileWatcher = new FileWatcher(scriptsDirectory);
        fileWatcher.addListener(new DirectoryChangesListener());
        if (ScriptService.SCRIPT_AUTO_RELOAD_ENABLED_SETTING.get(settings)) {
            // automatic reload is enabled - register scripts
            resourceWatcherService.add(fileWatcher);
        } else {
            // automatic reload is disabled just load scripts once
            fileWatcher.init();
        }

        this.scriptMetrics = scriptMetrics;
    }

    /**
     * Build the cache key for a file name and its extension. Return null to indicate that the file type is not supported.
     */
    protected abstract CacheKeyT cacheKeyForFile(String baseName, String extension);
    protected abstract CacheKeyT cacheKeyFromClusterState(StoredScriptSource scriptMetadata);
    protected abstract StoredScriptSource lookupStoredScript(ClusterState clusterState, CacheKeyT cacheKey);
    /**
     * Are any script contexts enabled for the given {@code cacheKey} and {@code scriptType}? Used to reject compilation if all script
     * contexts are disabled and produce a nice error message earlier rather than later.
     */ // NOCOMMIT make sure we have tests for cases where we use this (known cases are files and cluster state)
    protected abstract boolean areAnyScriptContextsEnabled(CacheKeyT cacheKey, ScriptType scriptType);
    /**
     * Check if a script can be executed.
     */
    protected abstract void checkCanExecuteScript(CacheKeyT cacheKey, ScriptType scriptType, ScriptContext scriptContext);
    /**
     * Check if too many scripts (or templates) have been compiled recently.
     */
    protected abstract void checkCompilationLimit();
    // NOCOMMIT document
    protected abstract CompiledScript compile(ScriptType scriptType, CacheKeyT cacheKey);
    protected abstract CompiledScript compileFileScript(CacheKeyT cacheKey, String body, Path file);

    public final CompiledScript getScript(CacheKeyT cacheKey, ScriptType scriptType, ScriptContext scriptContext) {
        Objects.requireNonNull(cacheKey);

        // First resolve stored scripts so so we have accurate parameters for checkCanExecuteScript
        if (scriptType == ScriptType.STORED) {
            cacheKey = getScriptFromClusterState(cacheKey);
        }

        // Validate that we can execute the script
        checkCanExecuteScript(cacheKey, scriptType, scriptContext);

        // Lookup file scripts from the map we maintain by watching the directory
        if (scriptType == ScriptType.FILE) {
            CompiledScript compiled = fileScripts.get(cacheKey);
            if (compiled == null) {
                throw new IllegalArgumentException("unable to find file script " + cacheKey);
            }
            return compiled;
        }

        // Other scripts are compiled lazily when needed so check the cache first
        CompiledScript compiledScript = cache.get(cacheKey);
        if (compiledScript != null) {
            return compiledScript;
        }

        // Synchronize so we don't compile scripts many times during multiple shards all compiling a script
        synchronized (this) {
            // Double check in case it was compiled while we were waiting for the monitor
            compiledScript = cache.get(cacheKey);
            if (compiledScript != null) {
                return compiledScript;
            }

            try {
                if (logger.isTraceEnabled()) {
                    logger.trace("compiling [{}]", cacheKey);
                }
                // Check whether too many compilations have happened
                checkCompilationLimit();
                compiledScript = compile(scriptType, cacheKey);
            } catch (ScriptException good) {
                // TODO: remove this try-catch completely, when all script engines have good exceptions!
                throw good; // its already good
            } catch (Exception exception) {
                throw new GeneralScriptException("Failed to compile " + cacheKey, exception);
            }
            scriptMetrics.onCompilation();
            cache.put(cacheKey, compiledScript);
            return compiledScript;
        }
    }

    private CacheKeyT getScriptFromClusterState(CacheKeyT cacheKey) {
        StoredScriptSource source = lookupStoredScript(clusterState, cacheKey);

        if (source == null) {
            throw new ResourceNotFoundException("unable to find script [" + cacheKey + "] in cluster state");
        }
        return cacheKeyFromClusterState(source);
    }

    /**
     * Check that a script compiles before attempting to store it.
     */
    public final void checkCompileBeforeStore(StoredScriptSource source) {
        CacheKeyT cacheKey = cacheKeyFromClusterState(source);
        try {
            if (areAnyScriptContextsEnabled(cacheKey, ScriptType.STORED)) {
                Object compiled = compile(ScriptType.STORED, cacheKey);

                if (compiled == null) {
                    throw new IllegalArgumentException("failed to parse/compile");
                }
            } else {
                throw new IllegalArgumentException("cannot be run under any context");
            }
        } catch (ScriptException good) {
            throw good;
        } catch (Exception exception) {
            throw new IllegalArgumentException("failed to parse/compile", exception);
        }
    }

    @Override
    public final void clusterChanged(ClusterChangedEvent event) {
        clusterState = event.state();
    }

    /**
     * Listener to manage metrics for the script cache.
     */
    private class CacheRemovalListener implements RemovalListener<CacheKeyT, CompiledScript> {
        @Override
        public void onRemoval(RemovalNotification<CacheKeyT, CompiledScript> notification) {
            if (logger.isDebugEnabled()) {
                logger.debug("removed {} from cache, reason: {}", notification.getValue(), notification.getRemovalReason());
            }
            scriptMetrics.onCacheEviction();
        }
    }

    private class DirectoryChangesListener implements FileChangesListener {
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

            CacheKeyT cacheKey = cacheKeyForFile(scriptNameExt.v1(), scriptNameExt.v2());
            if (cacheKey == null) {
                logger.warn("No script engine found for [{}]", scriptNameExt.v2());
                return;
            }
            try {
                /* we don't know yet what the script will be used for, but if all of the operations for this lang with file scripts are 
                 * disabled, it makes no sense to even compile it and cache it. */
                if (areAnyScriptContextsEnabled(cacheKey, ScriptType.FILE)) {
                    logger.info("compiling script file [{}]", file.toAbsolutePath());
                    try (InputStreamReader reader = new InputStreamReader(Files.newInputStream(file), StandardCharsets.UTF_8)) {
                        String body = Streams.copyToString(reader);
                        fileScripts.put(cacheKey, compileFileScript(cacheKey, body, file));
                        scriptMetrics.onCompilation();
                    }
                } else {
                    logger.warn("skipping compile of script file [{}] as all scripted operations are disabled for file scripts",
                            
                            file.toAbsolutePath());
                }
            } catch (ScriptException e) {
                /* Attempt to extract a concise error message using the xcontent generation mechanisms and log that. */
                try (XContentBuilder builder = JsonXContent.contentBuilder()) {
                    builder.prettyPrint();
                    builder.startObject();
                    ElasticsearchException.generateThrowableXContent(builder, ToXContent.EMPTY_PARAMS, e);
                    builder.endObject();
                    logger.warn("failed to load/compile script [{}]: {}", scriptNameExt.v1(), builder.string());
                } catch (IOException ioe) {
                    ioe.addSuppressed(e);
                    logger.warn((Supplier<?>) () -> new ParameterizedMessage(
                            "failed to log an appropriate warning after failing to load/compile script [{}]", scriptNameExt.v1()), ioe);
                }
                /* Log at the whole exception at the debug level as well just in case the stack trace is important. That way you can
                 * turn on the stack trace if you need it. */
                logger.debug((Supplier<?>) () -> new ParameterizedMessage("failed to load/compile script [{}]. full exception:",
                        scriptNameExt.v1()), e);
            } catch (Exception e) {
                logger.warn((Supplier<?>) () -> new ParameterizedMessage("failed to load/compile script [{}]", scriptNameExt.v1()), e);
            }
        }

        @Override
        public void onFileCreated(Path file) {
            onFileInit(file);
        }

        @Override
        public void onFileDeleted(Path file) {
            Tuple<String, String> scriptNameExt = getScriptNameExt(file);
            if (scriptNameExt == null) {
                return;
            }
            CacheKeyT cacheKey = cacheKeyForFile(scriptNameExt.v1(), scriptNameExt.v2());
            if (cacheKey == null) {
                return;
            }
            logger.info("removing script file [{}]", file.toAbsolutePath());
            fileScripts.remove(cacheKey);
        }

        @Override
        public void onFileChanged(Path file) {
            onFileInit(file);
        }
    }
}
