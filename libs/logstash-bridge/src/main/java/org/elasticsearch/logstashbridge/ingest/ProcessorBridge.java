/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.logstashbridge.ingest;

import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.logstashbridge.StableBridgeAPI;

import java.util.function.BiConsumer;

/**
 * An {@link StableBridgeAPI} for {@link Processor}.
 * As a side-effect of upstream, {@link ProcessorBridge#isAsync()} is expected
 * to have a constant value for any given instance, <em>AND</em>:
 * <ul>
 *   <li>{@code true}: must implement {@link ProcessorBridge#execute(IngestDocumentBridge, BiConsumer)}</li>
 *   <li>{@code false}: must implement {@link ProcessorBridge#execute(IngestDocumentBridge)}</li>
 * </ul>
 */
public interface ProcessorBridge extends StableBridgeAPI<Processor> {

    String getType();

    String getTag();

    String getDescription();

    boolean isAsync();

    default void execute(IngestDocumentBridge ingestDocumentBridge, BiConsumer<IngestDocumentBridge, Exception> handler) {
        toInternal().execute(
            StableBridgeAPI.toInternalNullable(ingestDocumentBridge),
            (id, exception) -> handler.accept(IngestDocumentBridge.fromInternalNullable(id), exception)
        );
    }

    default IngestDocumentBridge execute(IngestDocumentBridge ingestDocumentBridge) throws Exception {
        IngestDocument internalSourceIngestDocument = ingestDocumentBridge.toInternal();
        IngestDocument internalResultIngestDocument = toInternal().execute(internalSourceIngestDocument);

        if (internalResultIngestDocument == internalSourceIngestDocument) {
            return ingestDocumentBridge;
        }
        return IngestDocumentBridge.fromInternalNullable(internalResultIngestDocument);
    }

    static ProcessorBridge fromInternal(final Processor internalProcessor) {
        if (internalProcessor instanceof AbstractExternalProcessorBridge.ProxyExternal externalProxy) {
            return externalProxy.getProcessorBridge();
        }
        return new ProxyInternal(internalProcessor);
    }

    /**
     * An implementation of {@link ProcessorBridge} that proxies to an internal {@link Processor}
     * @see StableBridgeAPI.ProxyInternal
     */
    final class ProxyInternal extends StableBridgeAPI.ProxyInternal<Processor> implements ProcessorBridge {
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
    }
}
