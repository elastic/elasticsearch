/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.logstashbridge.script;

import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.core.FixForMultiProject;
import org.elasticsearch.ingest.common.ProcessorsWhitelistExtension;
import org.elasticsearch.logstashbridge.StableBridgeAPI;
import org.elasticsearch.logstashbridge.common.SettingsBridge;
import org.elasticsearch.painless.PainlessPlugin;
import org.elasticsearch.painless.PainlessScriptEngine;
import org.elasticsearch.painless.spi.PainlessExtension;
import org.elasticsearch.painless.spi.Whitelist;
import org.elasticsearch.plugins.ExtensiblePlugin;
import org.elasticsearch.script.IngestConditionalScript;
import org.elasticsearch.script.IngestScript;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptEngine;
import org.elasticsearch.script.ScriptModule;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.mustache.MustacheScriptEngine;
import org.elasticsearch.xpack.constantkeyword.ConstantKeywordPainlessExtension;
import org.elasticsearch.xpack.spatial.SpatialPainlessExtension;
import org.elasticsearch.xpack.wildcard.WildcardPainlessExtension;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.LongSupplier;

/**
 * An external bridge for {@link ScriptService}
 */
public interface ScriptServiceBridge extends StableBridgeAPI<ScriptService>, Closeable {

    static ScriptServiceBridge fromInternal(final ScriptService delegate) {
        return new ScriptServiceBridge.ProxyInternal(delegate);
    }

    static ScriptServiceBridge create(final SettingsBridge bridgedSettings, final LongSupplier timeProvider) throws IOException {
        final ScriptService scriptService = ProxyInternal.getScriptService(bridgedSettings.toInternal(), timeProvider);
        return fromInternal(scriptService);
    }

    /**
     * An implementation of {@link ScriptServiceBridge} that proxies calls through
     * to an internal {@link ScriptService}.
     * @see StableBridgeAPI.ProxyInternal
     */
    final class ProxyInternal extends StableBridgeAPI.ProxyInternal<ScriptService> implements ScriptServiceBridge {

        ProxyInternal(ScriptService delegate) {
            super(delegate);
        }

        private static ScriptService getScriptService(final Settings settings, final LongSupplier timeProvider) throws IOException {
            final List<Whitelist> painlessBaseWhitelist = getPainlessBaseWhiteList();
            final Map<ScriptContext<?>, List<Whitelist>> scriptContexts = Map.of(
                IngestScript.CONTEXT,
                painlessBaseWhitelist,
                IngestConditionalScript.CONTEXT,
                painlessBaseWhitelist
            );
            final Map<String, ScriptEngine> scriptEngines = Map.of(
                PainlessScriptEngine.NAME,
                getPainlessScriptEngine(settings),
                MustacheScriptEngine.NAME,
                new MustacheScriptEngine(settings)
            );

            return new ScriptService(
                settings,
                scriptEngines,
                ScriptModule.CORE_CONTEXTS,
                timeProvider,
                LockdownOnlyDefaultProjectIdResolver.INSTANCE
            );
        }

        private static List<Whitelist> getPainlessBaseWhiteList() {
            return PainlessPlugin.baseWhiteList();
        }

        /**
         * @param settings the Elasticsearch settings object
         * @return a {@link ScriptEngine} for painless scripts for use in {@link IngestScript} and
         * {@link IngestConditionalScript} contexts, including all available {@link PainlessExtension}s.
         * @throws IOException when the underlying script engine cannot be created
         */
        private static ScriptEngine getPainlessScriptEngine(final Settings settings) throws IOException {
            try (PainlessPlugin painlessPlugin = new PainlessPlugin()) {
                painlessPlugin.loadExtensions(new ExtensiblePlugin.ExtensionLoader() {
                    @Override
                    @SuppressWarnings("unchecked")
                    public <T> List<T> loadExtensions(Class<T> extensionPointType) {
                        if (extensionPointType.isAssignableFrom(PainlessExtension.class)) {
                            final List<PainlessExtension> extensions = new ArrayList<>();

                            extensions.add(new ConstantKeywordPainlessExtension());  // module: constant-keyword
                            extensions.add(new ProcessorsWhitelistExtension());      // module: ingest-common
                            extensions.add(new SpatialPainlessExtension());          // module: spatial
                            extensions.add(new WildcardPainlessExtension());         // module: wildcard

                            return (List<T>) extensions;
                        } else {
                            return List.of();
                        }
                    }
                });

                return painlessPlugin.getScriptEngine(settings, Set.of(IngestScript.CONTEXT, IngestConditionalScript.CONTEXT));
            }
        }

        @Override
        public void close() throws IOException {
            this.internalDelegate.close();
        }

        @FixForMultiProject
        // Logstash resolves and runs ingest pipelines based on the datastream.
        // How should LockdownOnlyDefaultProjectIdResolver behave in this case?
        // In other words, it looks we need to find a way to figure out which ingest pipeline belongs to which project.
        static class LockdownOnlyDefaultProjectIdResolver implements ProjectResolver {

            public static final LockdownOnlyDefaultProjectIdResolver INSTANCE = new LockdownOnlyDefaultProjectIdResolver();

            @Override
            public ProjectId getProjectId() {
                return ProjectId.DEFAULT;
            }

            @Override
            public <E extends Exception> void executeOnProject(ProjectId projectId, CheckedRunnable<E> body) throws E {
                if (projectId.equals(ProjectId.DEFAULT)) {
                    body.run();
                } else {
                    throw new IllegalArgumentException("Cannot execute on a project other than [" + ProjectId.DEFAULT + "]");
                }
            }
        }
    }
}
