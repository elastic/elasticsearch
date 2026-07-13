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
import org.elasticsearch.ingest.Pipeline;
import org.elasticsearch.logstashbridge.StableBridgeAPI;
import org.elasticsearch.logstashbridge.script.ScriptServiceBridge;

import java.util.Map;
import java.util.function.BiConsumer;

/**
 * A {@link StableBridgeAPI} for {@link Pipeline}
 */
public interface PipelineBridge extends StableBridgeAPI<Pipeline> {

    String getId();

    void execute(IngestDocumentBridge bridgedIngestDocument, BiConsumer<IngestDocumentBridge, Exception> bridgedHandler);

    static PipelineBridge fromInternal(final Pipeline pipeline) {
        return new ProxyInternal(pipeline);
    }

    static PipelineBridge create(
        String id,
        Map<String, Object> config,
        Map<String, ProcessorFactoryBridge> processorFactories,
        ScriptServiceBridge scriptServiceBridge
    ) throws Exception {
        return fromInternal(
            Pipeline.create(
                id,
                config,
                StableBridgeAPI.toInternal(processorFactories),
                StableBridgeAPI.toInternalNullable(scriptServiceBridge),
                ProjectId.DEFAULT
            )
        );
    }

    /**
     * An implementation of {@link PipelineBridge} that proxies calls through to
     * an internal {@link Pipeline}.
     * @see StableBridgeAPI.ProxyInternal
     */
    class ProxyInternal extends StableBridgeAPI.ProxyInternal<Pipeline> implements PipelineBridge {

        ProxyInternal(final Pipeline delegate) {
            super(delegate);
        }

        @Override
        public String getId() {
            return internalDelegate.getId();
        }

        @Override
        public void execute(final IngestDocumentBridge ingestDocumentBridge, final BiConsumer<IngestDocumentBridge, Exception> handler) {
            this.internalDelegate.execute(
                StableBridgeAPI.toInternalNullable(ingestDocumentBridge),
                (ingestDocument, e) -> handler.accept(IngestDocumentBridge.fromInternalNullable(ingestDocument), e)
            );
        }
    }
}
