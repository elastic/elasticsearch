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
import org.elasticsearch.logstashbridge.ingest.ProcessorBridge;
import org.elasticsearch.plugins.IngestPlugin;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;

public interface IngestPluginBridge {
    Map<String, ProcessorBridge.Factory> getProcessors(ProcessorBridge.Parameters parameters);

    static Wrapped wrap(final IngestPlugin delegate) {
        return new Wrapped(delegate);
    }

    class Wrapped extends StableBridgeAPI.Proxy<IngestPlugin> implements IngestPluginBridge, Closeable {

        private Wrapped(final IngestPlugin delegate) {
            super(delegate);
        }

        public Map<String, ProcessorBridge.Factory> getProcessors(final ProcessorBridge.Parameters parameters) {
            return StableBridgeAPI.wrap(this.delegate.getProcessors(parameters.unwrap()), ProcessorBridge.Factory::wrap);
        }

        @Override
        public IngestPlugin unwrap() {
            return this.delegate;
        }

        @Override
        public void close() throws IOException {
            if (this.delegate instanceof Closeable closeableDelegate) {
                closeableDelegate.close();
            }
        }
    }
}
