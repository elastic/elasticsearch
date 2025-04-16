/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.logstashbridge.ingest;

import org.elasticsearch.core.FixForMultiProject;
import org.elasticsearch.ingest.Pipeline;
import org.elasticsearch.logstashbridge.StableBridgeAPI;
import org.elasticsearch.logstashbridge.script.ScriptServiceBridge;

import java.util.Map;
import java.util.function.BiConsumer;

public class PipelineBridge extends StableBridgeAPI.Proxy<Pipeline> {
    public static PipelineBridge wrap(final Pipeline pipeline) {
        return new PipelineBridge(pipeline);
    }

    @FixForMultiProject(description = "should we pass a non-null project ID here?")
    public static PipelineBridge create(
        String id,
        Map<String, Object> config,
        Map<String, ProcessorBridge.Factory> processorFactories,
        ScriptServiceBridge scriptServiceBridge
    ) throws Exception {
        return wrap(
            Pipeline.create(
                id,
                config,
                StableBridgeAPI.unwrap(processorFactories),
                StableBridgeAPI.unwrapNullable(scriptServiceBridge),
                null
            )
        );
    }

    public PipelineBridge(final Pipeline delegate) {
        super(delegate);
    }

    public String getId() {
        return delegate.getId();
    }

    public void execute(final IngestDocumentBridge ingestDocumentBridge, final BiConsumer<IngestDocumentBridge, Exception> handler) {
        this.delegate.execute(
            StableBridgeAPI.unwrapNullable(ingestDocumentBridge),
            (unwrapped, e) -> handler.accept(IngestDocumentBridge.wrap(unwrapped), e)
        );
    }
}
