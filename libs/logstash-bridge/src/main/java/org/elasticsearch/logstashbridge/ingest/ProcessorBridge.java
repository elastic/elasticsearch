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

public interface ProcessorBridge extends StableBridgeAPI<Processor> {
    String getType();

    String getTag();

    String getDescription();

    boolean isAsync();

    void execute(IngestDocumentBridge ingestDocumentBridge, BiConsumer<IngestDocumentBridge, Exception> handler);

    static ProcessorBridge wrap(final Processor delegate) {
        if (delegate instanceof InverseWrapped inverseWrapped) {
            return inverseWrapped.delegate;
        }
        return new Wrapped(delegate);
    }

    class Wrapped extends StableBridgeAPI.Proxy<Processor> implements ProcessorBridge {
        public Wrapped(final Processor delegate) {
            super(delegate);
        }

        @Override
        public String getType() {
            return unwrap().getType();
        }

        @Override
        public String getTag() {
            return unwrap().getTag();
        }

        @Override
        public String getDescription() {
            return unwrap().getDescription();
        }

        @Override
        public boolean isAsync() {
            return unwrap().isAsync();
        }

        @Override
        public void execute(final IngestDocumentBridge ingestDocumentBridge, final BiConsumer<IngestDocumentBridge, Exception> handler) {
            delegate.execute(
                StableBridgeAPI.unwrapNullable(ingestDocumentBridge),
                (id, e) -> handler.accept(IngestDocumentBridge.wrap(id), e)
            );
        }
    }

    @Override
    default Processor unwrap() {
        return new InverseWrapped(this);
    }

    class InverseWrapped implements Processor {
        private final ProcessorBridge delegate;

        public InverseWrapped(final ProcessorBridge delegate) {
            this.delegate = delegate;
        }

        @Override
        public String getType() {
            return delegate.getType();
        }

        @Override
        public String getTag() {
            return delegate.getTag();
        }

        @Override
        public String getDescription() {
            return delegate.getDescription();
        }

        @Override
        public void execute(IngestDocument ingestDocument, BiConsumer<IngestDocument, Exception> handler) {
            this.delegate.execute(IngestDocumentBridge.wrap(ingestDocument), (idb, e) -> handler.accept(idb.unwrap(), e));
        }

        @Override
        public boolean isAsync() {
            return delegate.isAsync();
        }
    }

    class Parameters extends StableBridgeAPI.Proxy<Processor.Parameters> {

        public Parameters(
            final EnvironmentBridge environmentBridge,
            final ScriptServiceBridge scriptServiceBridge,
            final ThreadPoolBridge threadPoolBridge
        ) {
            this(
                new Processor.Parameters(
                    environmentBridge.unwrap(),
                    scriptServiceBridge.unwrap(),
                    null,
                    threadPoolBridge.unwrap().getThreadContext(),
                    threadPoolBridge.unwrap()::relativeTimeInMillis,
                    (delay, command) -> threadPoolBridge.unwrap()
                        .schedule(command, TimeValue.timeValueMillis(delay), threadPoolBridge.unwrap().generic()),
                    null,
                    null,
                    threadPoolBridge.unwrap().generic()::execute,
                    IngestService.createGrokThreadWatchdog(environmentBridge.unwrap(), threadPoolBridge.unwrap())
                )
            );
        }

        private Parameters(final Processor.Parameters delegate) {
            super(delegate);
        }

        @Override
        public Processor.Parameters unwrap() {
            return this.delegate;
        }
    }

    interface Factory extends StableBridgeAPI<Processor.Factory> {
        ProcessorBridge create(
            Map<String, ProcessorBridge.Factory> registry,
            String processorTag,
            String description,
            Map<String, Object> config
        ) throws Exception;

        static Factory wrap(final Processor.Factory delegate) {
            return new Wrapped(delegate);
        }

        @Override
        default Processor.Factory unwrap() {
            final Factory stableAPIFactory = this;
            return (registry, tag, description, config, projectId) -> stableAPIFactory.create(
                StableBridgeAPI.wrap(registry, Factory::wrap),
                tag,
                description,
                config
            ).unwrap();
        }

        class Wrapped extends StableBridgeAPI.Proxy<Processor.Factory> implements Factory {
            private Wrapped(final Processor.Factory delegate) {
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
                return ProcessorBridge.wrap(
                    this.delegate.create(StableBridgeAPI.unwrap(registry), processorTag, description, config, ProjectId.DEFAULT)
                );
            }

            @Override
            public Processor.Factory unwrap() {
                return this.delegate;
            }
        }
    }

}
