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
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.logstashbridge.StableBridgeAPI;
import org.elasticsearch.logstashbridge.common.ProjectIdBridge;

import java.util.Map;

/**
 * An {@link StableBridgeAPI} for {@link Processor.Factory}
 */
public interface ProcessorFactoryBridge extends StableBridgeAPI<Processor.Factory> {

    @Deprecated // supply ProjectIdBridge
    default ProcessorBridge create(
        Map<String, ProcessorFactoryBridge> registry,
        String processorTag,
        String description,
        Map<String, Object> config
    ) throws Exception {
        return this.create(registry, processorTag, description, config, ProjectIdBridge.getDefault());
    }

    ProcessorBridge create(
        Map<String, ProcessorFactoryBridge> registry,
        String processorTag,
        String description,
        Map<String, Object> config,
        ProjectIdBridge projectId
    ) throws Exception;

    static ProcessorFactoryBridge fromInternal(final Processor.Factory delegate) {
        if (delegate instanceof AbstractExternalProcessorFactoryBridge.InternalProxy internalProxy) {
            return internalProxy.toExternal();
        }
        return new ProxyInternal(delegate);
    }

    /**
     * An implementation of {@link ProcessorFactoryBridge} that proxies to an internal {@link Processor.Factory}
     *
     * @see StableBridgeAPI.ProxyInternal
     */
    class ProxyInternal extends StableBridgeAPI.ProxyInternal<Processor.Factory> implements ProcessorFactoryBridge {
        protected ProxyInternal(final Processor.Factory delegate) {
            super(delegate);
        }

        @FixForMultiProject(description = "should we pass a non-null project ID here?")
        @Override
        public ProcessorBridge create(
            final Map<String, ProcessorFactoryBridge> registry,
            final String processorTag,
            final String description,
            final Map<String, Object> config,
            final ProjectIdBridge bridgedProjectId
        ) throws Exception {
            final Map<String, Processor.Factory> internalRegistry = StableBridgeAPI.toInternal(registry);
            final Processor.Factory internalFactory = toInternal();
            final ProjectId projectId = bridgedProjectId.toInternal();
            final Processor internalProcessor = internalFactory.create(internalRegistry, processorTag, description, config, projectId);
            return ProcessorBridge.fromInternal(internalProcessor);
        }

        @Override
        public Processor.Factory toInternal() {
            return this.internalDelegate;
        }
    }

}
