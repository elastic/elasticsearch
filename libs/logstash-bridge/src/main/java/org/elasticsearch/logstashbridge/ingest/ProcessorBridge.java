/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.logstashbridge.ingest;

import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.core.FixForMultiProject;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.IngestService;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.logstashbridge.StableBridgeAPI;
import org.elasticsearch.logstashbridge.env.EnvironmentBridge;
import org.elasticsearch.logstashbridge.script.ScriptServiceBridge;
import org.elasticsearch.logstashbridge.threadpool.ThreadPoolBridge;

import java.util.Map;
import java.util.function.BiConsumer;

/**
 * An external bridge for {@link Processor}
 */
public interface ProcessorBridge extends StableBridgeAPI<Processor> {

    String getType();

    String getTag();

    String getDescription();

    boolean isAsync();

    void execute(IngestDocumentBridge ingestDocumentBridge, BiConsumer<IngestDocumentBridge, Exception> handler);

    static ProcessorBridge fromInternal(final Processor internalProcessor) {
        if (internalProcessor instanceof AbstractExternal.ProxyExternal externalProxy) {
            return externalProxy.getProcessorBridge();
        }
        return new ProxyInternal(internalProcessor);
    }

    /**
     * The {@code ProcessorBridge.AbstractExternal} is an abstract base class for implementing
     * the {@link ProcessorBridge} externally to the Elasticsearch code-base. It takes care of
     * the details of maintaining a singular internal-form implementation of {@link Processor}
     * that proxies calls through the external implementation.
     */
    abstract class AbstractExternal implements ProcessorBridge {
        private ProxyExternal internalProcessor;

        public Processor toInternal() {
            if (internalProcessor == null) {
                internalProcessor = new ProxyExternal();
            }
            return internalProcessor;
        }

        private class ProxyExternal implements Processor {

            @Override
            public String getType() {
                return AbstractExternal.this.getType();
            }

            @Override
            public String getTag() {
                return AbstractExternal.this.getTag();
            }

            @Override
            public String getDescription() {
                return AbstractExternal.this.getDescription();
            }

            @Override
            public void execute(IngestDocument ingestDocument, BiConsumer<IngestDocument, Exception> handler) {
                AbstractExternal.this.execute(
                    IngestDocumentBridge.fromInternalNullable(ingestDocument),
                    (idb, e) -> handler.accept(idb.toInternal(), e)
                );
            }

            @Override
            public boolean isAsync() {
                return AbstractExternal.this.isAsync();
            }

            private AbstractExternal getProcessorBridge() {
                return AbstractExternal.this;
            }
        }
    }

    /**
     * An implementation of {@link ProcessorBridge} that proxies to an internal {@link Processor}
     */
    class ProxyInternal extends StableBridgeAPI.ProxyInternal<Processor> implements ProcessorBridge {
        public ProxyInternal(final Processor delegate) {
            super(delegate);
        }

        @Override
        public String getType() {
            return toInternal().getType();
        }

        @Override
        public String getTag() {
            return toInternal().getTag();
        }

        @Override
        public String getDescription() {
            return toInternal().getDescription();
        }

        @Override
        public boolean isAsync() {
            return toInternal().isAsync();
        }

        @Override
        public void execute(final IngestDocumentBridge ingestDocumentBridge, final BiConsumer<IngestDocumentBridge, Exception> handler) {
            internalDelegate.execute(
                StableBridgeAPI.toInternalNullable(ingestDocumentBridge),
                (id, e) -> handler.accept(IngestDocumentBridge.fromInternalNullable(id), e)
            );
        }
    }

    /**
     * An external bridge for {@link Processor.Parameters}
     */
    class Parameters extends StableBridgeAPI.ProxyInternal<Processor.Parameters> {

        public Parameters(
            final EnvironmentBridge environmentBridge,
            final ScriptServiceBridge scriptServiceBridge,
            final ThreadPoolBridge threadPoolBridge
        ) {
            this(
                new Processor.Parameters(
                    environmentBridge.toInternal(),
                    scriptServiceBridge.toInternal(),
                    null,
                    threadPoolBridge.toInternal().getThreadContext(),
                    threadPoolBridge.toInternal()::relativeTimeInMillis,
                    (delay, command) -> threadPoolBridge.toInternal()
                        .schedule(command, TimeValue.timeValueMillis(delay), threadPoolBridge.toInternal().generic()),
                    null,
                    null,
                    threadPoolBridge.toInternal().generic()::execute,
                    IngestService.createGrokThreadWatchdog(environmentBridge.toInternal(), threadPoolBridge.toInternal())
                )
            );
        }

        private Parameters(final Processor.Parameters delegate) {
            super(delegate);
        }

        @Override
        public Processor.Parameters toInternal() {
            return this.internalDelegate;
        }
    }

    /**
     * An external bridge for {@link Processor.Factory}
     */
    interface Factory extends StableBridgeAPI<Processor.Factory> {
        ProcessorBridge create(
            Map<String, ProcessorBridge.Factory> registry,
            String processorTag,
            String description,
            Map<String, Object> config
        ) throws Exception;

        static Factory fromInternal(final Processor.Factory delegate) {
            return new ProxyInternal(delegate);
        }

        @Override
        default Processor.Factory toInternal() {
            final Factory stableAPIFactory = this;
            return (registry, tag, description, config, projectId) -> stableAPIFactory.create(
                StableBridgeAPI.fromInternal(registry, Factory::fromInternal),
                tag,
                description,
                config
            ).toInternal();
        }

        /**
         * An implementation of {@link ProcessorBridge.Factory} that proxies to an internal {@link Processor.Factory}
         */
        class ProxyInternal extends StableBridgeAPI.ProxyInternal<Processor.Factory> implements Factory {
            private ProxyInternal(final Processor.Factory delegate) {
                super(delegate);
            }

            @FixForMultiProject(description = "should we pass a non-null project ID here?")
            @Override
            public ProcessorBridge create(
                final Map<String, Factory> registry,
                final String processorTag,
                final String description,
                final Map<String, Object> config
            ) throws Exception {
                return ProcessorBridge.fromInternal(
                    this.internalDelegate.create(StableBridgeAPI.toInternal(registry), processorTag, description, config, ProjectId.DEFAULT)
                );
            }

            @Override
            public Processor.Factory toInternal() {
                return this.internalDelegate;
            }
        }
    }

}
