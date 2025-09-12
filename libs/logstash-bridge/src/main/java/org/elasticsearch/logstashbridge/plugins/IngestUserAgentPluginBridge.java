/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.logstashbridge.plugins;

import org.elasticsearch.ingest.useragent.IngestUserAgentPlugin;
import org.elasticsearch.logstashbridge.StableBridgeAPI;
import org.elasticsearch.logstashbridge.ingest.ProcessorBridge;

import java.util.Map;

/**
 * An external bridge for {@link IngestUserAgentPlugin}
 */
public class IngestUserAgentPluginBridge implements IngestPluginBridge {

    private final IngestUserAgentPlugin delegate;

    public IngestUserAgentPluginBridge() {
        delegate = new IngestUserAgentPlugin();
    }

    public Map<String, ProcessorBridge.Factory> getProcessors(final ProcessorBridge.Parameters parameters) {
        return StableBridgeAPI.fromInternal(this.delegate.getProcessors(parameters.toInternal()), ProcessorBridge.Factory::fromInternal);
    }
}
