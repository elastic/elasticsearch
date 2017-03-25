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
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.watcher.ResourceWatcherService;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;

/**
 * Source of templates (think Mustache or StringTemplate).
 */
public class TemplateService implements ClusterStateListener {
    public interface Backend extends ScriptEngineService {} // TODO customize this for templates

    private static final String DEFAULT_CONTENT_TYPE = "text/plain";
    private static final Logger logger = ESLoggerFactory.getLogger(TemplateService.class);
    private static final DeprecationLogger deprecationLogger = new DeprecationLogger(logger);
    private final Backend backend;
    private final ScriptPermits scriptPermits;
    private final CachingCompiler<CacheKey> compiler;

    public TemplateService(Settings settings, Environment env,
            ResourceWatcherService resourceWatcherService, Backend backend,
            ScriptContextRegistry scriptContextRegistry, ScriptSettings scriptSettings,
            ScriptMetrics scriptMetrics) throws IOException {
        Objects.requireNonNull(scriptContextRegistry);

        this.backend = backend;
        this.scriptPermits = new ScriptPermits(settings, scriptSettings, scriptContextRegistry);
        this.compiler = new CachingCompiler<CacheKey>(settings, env, resourceWatcherService,
                scriptMetrics, "template") {
            @Override
            protected CacheKey cacheKeyForFile(String baseName, String extension) {
                if (false == backend.getType().equals(extension)) {
                    /* For backwards compatibility templates are in the scripts directory and we
                     * must ignore everything but templates. */
                    return null;
                }
                return new CacheKey(baseName, DEFAULT_CONTENT_TYPE);
            }

            @Override
            protected CacheKey cacheKeyFromClusterState(StoredScriptSource scriptMetadata) {
                String contentType = DEFAULT_CONTENT_TYPE;
                if (scriptMetadata.getOptions() != null) {
                    contentType = scriptMetadata.getOptions().get(Script.CONTENT_TYPE_OPTION);
                    if (contentType == null) {
                        contentType = DEFAULT_CONTENT_TYPE;
                    }
                }
                return new CacheKey(scriptMetadata.getCode(), contentType);
            }

            @Override
            protected StoredScriptSource lookupStoredScript(ClusterState clusterState,
                    CacheKey cacheKey) {
                if (clusterState == null) {
                    return null;
                }
                MetaData metaData = clusterState.metaData();
                if (metaData == null) {
                    return null;
                }
                ScriptMetaData scriptMetaData = clusterState.metaData().custom(ScriptMetaData.TYPE);
                if (scriptMetaData == null) {
                    return null;
                }

                /* This process throws away contentType which is maybe bad, but it is the behavior
                 * that we've had in templates for a long time. */
                String id = cacheKey.idOrCode;
                // search template requests can possibly pass in the entire path instead
                // of just an id for looking up a stored script, so we parse the path and
                // check for appropriate errors
                String[] path = id.split("/");

                if (path.length == 3) {
                    id = path[2];
                    deprecationLogger.deprecated("use of </lang/id> [{}] for looking up stored "
                            + "scripts/templates has been deprecated, use only <id> [{}] instead",
                            cacheKey, id);
                } else if (path.length != 1) {
                    throw new IllegalArgumentException( "illegal stored script format [" + id
                            + "] use only <id>");
                }

                return scriptMetaData.getStoredScript(id, backend.getExtension());
            }

            @Override
            protected boolean areAnyScriptContextsEnabled(CacheKey cacheKey, ScriptType scriptType) {
                for (ScriptContext scriptContext : scriptContextRegistry.scriptContexts()) {
                    if (scriptPermits.checkContextPermissions(backend.getType(), scriptType,
                            scriptContext)) {
                        return true;
                    }
                }
                return false;
            }

            @Override
            protected void checkContextPermissions(CacheKey cacheKey, ScriptType scriptType,
                    ScriptContext scriptContext) {
                if (scriptPermits.checkContextPermissions(backend.getType(), scriptType,
                        scriptContext) == false) {
                    throw new IllegalStateException("templates of [" + scriptType + "],"
                            + " operation [" + scriptContext.getKey() + "] are disabled");
                }
            }

            @Override
            protected void checkCompilationLimit() {
                scriptPermits.checkCompilationLimit();
            }

            @Override
            protected CompiledScript compile(ScriptType scriptType, CacheKey cacheKey) {
                Object compiled = backend.compile(null, cacheKey.idOrCode,
                        singletonMap(Script.CONTENT_TYPE_OPTION, cacheKey.contentType));
                return new CompiledScript(
                        scriptType, cacheKey.idOrCode,backend.getType(), compiled);
            }

            @Override
            protected CompiledScript compileFileScript(CacheKey cacheKey, String body, Path file) {
                Object compiled = backend.compile(file.getFileName().toString(), body,
                        emptyMap());
                return new CompiledScript(ScriptType.FILE, body, backend.getType(), compiled);
            }
        };
    }

    /**
     * Lookup and/or compile a template.
     *
     * @param idOrCode template to look up and/or compile
     * @param type whether to compile ({link ScriptType#INLINE}), lookup from cluster state
     *        ({@link ScriptType#STORED}), or lookup from disk ({@link ScriptType#FILE})
     * @param context context in which the template is being run
     * @param contentType content type passed to the template backend during compilation
     * @return the template
     */
    public Function<Map<String, Object>, BytesReference> template(String idOrCode,
            ScriptType type, ScriptContext context, @Nullable String contentType) {
        contentType = contentType == null ? DEFAULT_CONTENT_TYPE : contentType;
        CacheKey cacheKey = new CacheKey(idOrCode, contentType);
        CompiledScript compiled = compiler.getScript(cacheKey, type, context);
        return params -> {
            ExecutableScript executable = backend.executable(compiled, params);
            return (BytesReference) executable.run();
        };
    }

    /**
     * The language name that templates have when stored in {@link ScriptMetaData}.
     */
    public String getTemplateLanguage() {
        return backend.getType();
    }

    public void checkCompileBeforeStore(StoredScriptSource source) {
        compiler.checkCompileBeforeStore(source);
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        compiler.clusterChanged(event);
    }

    private static final class CacheKey {
        final String idOrCode;
        final String contentType;

        private CacheKey(String idOrCode, String contentType) {
            this.idOrCode = Objects.requireNonNull(idOrCode);
            this.contentType = Objects.requireNonNull(contentType);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            CacheKey cacheKey = (CacheKey)o;

            if (!idOrCode.equals(cacheKey.idOrCode)) return false;
            return contentType.equals(cacheKey.contentType);

        }

        @Override
        public int hashCode() {
            int result = idOrCode.hashCode();
            result = 31 * result + contentType.hashCode();
            return result;
        }

        @Override
        public String toString() {
            return "id=" + idOrCode + ", contentType=" + contentType;
        }
    }
}
