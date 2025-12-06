/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.logstashbridge.ingest;

import org.elasticsearch.ingest.PipelineConfiguration;
import org.elasticsearch.logstashbridge.StableBridgeAPI;

import java.util.Map;

/**
 * An implementation of {@link PipelineConfigurationBridge} that proxies calls through
 * to an internal {@link PipelineConfiguration}.
 * @see StableBridgeAPI.ProxyInternal
 */
final class ProxyInternalPipelineConfigurationBridge extends StableBridgeAPI.ProxyInternal<PipelineConfiguration>
    implements
        PipelineConfigurationBridge {
    ProxyInternalPipelineConfigurationBridge(final PipelineConfiguration delegate) {
        super(delegate);
    }

    @Override
    public String getId() {
        return internalDelegate.getId();
    }

    @Override
    public Map<String, Object> getConfig() {
        return internalDelegate.getConfig();
    }

    @Override
    public Map<String, Object> getConfig(final boolean unmodifiable) {
        return internalDelegate.getConfig(unmodifiable);
    }

    @Override
    public int hashCode() {
        return internalDelegate.hashCode();
    }

    @Override
    public String toString() {
        return internalDelegate.toString();
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        } else if (obj instanceof ProxyInternalPipelineConfigurationBridge other) {
            return internalDelegate.equals(other.internalDelegate);
        } else {
            return false;
        }
    }
}
