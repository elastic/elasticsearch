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
 * The {@link AbstractExternalProcessorBridge} is an abstract base class for implementing
 * the {@link ProcessorBridge} externally to the Elasticsearch code-base. It takes care of
 * the details of maintaining a singular internal-form implementation of {@link Processor}
 * that proxies calls through the external implementation.
 */
public abstract class AbstractExternalProcessorBridge implements ProcessorBridge {
    private ProxyExternal internalProcessor;

    public Processor toInternal() {
        if (internalProcessor == null) {
            internalProcessor = new ProxyExternal();
        }
        return internalProcessor;
    }

    /**
     * The {@link AbstractExternalProcessorBridge.ProxyExternal} is shaped like an internal-{@link Processor},
     * but proxies calls through to an {@link AbstractExternalProcessorBridge}.
     */
    final class ProxyExternal implements Processor {

        @Override
        public String getType() {
            return AbstractExternalProcessorBridge.this.getType();
        }

        @Override
        public String getTag() {
            return AbstractExternalProcessorBridge.this.getTag();
        }

        @Override
        public String getDescription() {
            return AbstractExternalProcessorBridge.this.getDescription();
        }

        @Override
        public void execute(IngestDocument ingestDocument, BiConsumer<IngestDocument, Exception> handler) {
            AbstractExternalProcessorBridge.this.execute(
                IngestDocumentBridge.fromInternalNullable(ingestDocument),
                (ingestDocumentBridge, exception) -> handler.accept(StableBridgeAPI.toInternalNullable(ingestDocumentBridge), exception)
            );
        }

        @Override
        public boolean isAsync() {
            return AbstractExternalProcessorBridge.this.isAsync();
        }

        AbstractExternalProcessorBridge getProcessorBridge() {
            return AbstractExternalProcessorBridge.this;
        }
    }
}
