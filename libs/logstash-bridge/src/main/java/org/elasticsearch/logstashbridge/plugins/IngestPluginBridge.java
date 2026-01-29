/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.logstashbridge.plugins;

import org.elasticsearch.logstashbridge.StableBridgeAPI;
import org.elasticsearch.logstashbridge.ingest.ProcessorFactoryBridge;
import org.elasticsearch.logstashbridge.ingest.ProcessorParametersBridge;
import org.elasticsearch.plugins.IngestPlugin;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;

/**
 * An external bridge for {@link IngestPlugin}
 */
public interface IngestPluginBridge {
    Map<String, ProcessorFactoryBridge> getProcessors(ProcessorParametersBridge parameters);

    static ProxyInternal fromInternal(final IngestPlugin delegate) {
        return new ProxyInternal(delegate);
    }

    /**
     * An implementation of {@link IngestPluginBridge} that proxies calls to an internal {@link IngestPlugin}
     */
    final class ProxyInternal extends StableBridgeAPI.ProxyInternal<IngestPlugin> implements IngestPluginBridge, Closeable {

        private ProxyInternal(final IngestPlugin delegate) {
            super(delegate);
        }

        public Map<String, ProcessorFactoryBridge> getProcessors(final ProcessorParametersBridge parameters) {
            return StableBridgeAPI.fromInternal(
                this.internalDelegate.getProcessors(parameters.toInternal()),
                ProcessorFactoryBridge::fromInternal
            );
        }

        @Override
        public IngestPlugin toInternal() {
            return this.internalDelegate;
        }

        @Override
        public void close() throws IOException {
            if (this.internalDelegate instanceof Closeable closeableDelegate) {
                closeableDelegate.close();
            }
        }
    }
}
