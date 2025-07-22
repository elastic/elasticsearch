/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.logstashbridge.plugins;

import org.elasticsearch.ingest.common.IngestCommonPlugin;
import org.elasticsearch.logstashbridge.StableBridgeAPI;
import org.elasticsearch.logstashbridge.ingest.ProcessorBridge;

import java.util.Map;

/**
 * An external bridge for {@link IngestCommonPlugin}
 */
public class IngestCommonPluginBridge implements IngestPluginBridge {

    private final IngestCommonPlugin delegate;

    public IngestCommonPluginBridge() {
        delegate = new IngestCommonPlugin();
    }

    @Override
    public Map<String, ProcessorBridge.Factory> getProcessors(final ProcessorBridge.Parameters parameters) {
        return StableBridgeAPI.fromInternal(this.delegate.getProcessors(parameters.toInternal()), ProcessorBridge.Factory::fromInternal);
    }
}
