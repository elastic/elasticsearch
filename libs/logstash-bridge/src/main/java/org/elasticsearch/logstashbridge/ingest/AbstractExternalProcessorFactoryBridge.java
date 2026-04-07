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
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.logstashbridge.StableBridgeAPI;
import org.elasticsearch.logstashbridge.common.ProjectIdBridge;

import java.util.Map;

/**
 * The {@code ProcessorBridge.Factory.AbstractExternal} is an abstract base class for implementing
 * the {@link ProcessorFactoryBridge} externally to the Elasticsearch code-base. It takes care of
 * the details of maintaining a singular internal-form implementation of {@link Processor.Factory}
 * that proxies calls to the external implementation.
 */
public abstract class AbstractExternalProcessorFactoryBridge implements ProcessorFactoryBridge {
    InternalProxy internalDelegate;

    @Override
    public Processor.Factory toInternal() {
        if (this.internalDelegate == null) {
            internalDelegate = new InternalProxy();
        }
        return this.internalDelegate;
    }

    final class InternalProxy implements Processor.Factory {
        @Override
        public Processor create(
            final Map<String, Processor.Factory> processorFactories,
            final String tag,
            final String description,
            final Map<String, Object> config,
            final ProjectId projectId
        ) throws Exception {
            final Map<String, ProcessorFactoryBridge> bridgedProcessorFactories = StableBridgeAPI.fromInternal(
                processorFactories,
                ProcessorFactoryBridge::fromInternal
            );
            final ProjectIdBridge bridgedProjectId = ProjectIdBridge.fromInternal(projectId);
            final ProcessorBridge bridgedProcessor = AbstractExternalProcessorFactoryBridge.this.create(
                bridgedProcessorFactories,
                tag,
                description,
                config,
                bridgedProjectId
            );
            return bridgedProcessor.toInternal();
        }

        ProcessorFactoryBridge toExternal() {
            return AbstractExternalProcessorFactoryBridge.this;
        }
    }
}
