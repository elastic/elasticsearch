/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.logstashbridge.ingest;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.ingest.PipelineConfiguration;
import org.elasticsearch.logstashbridge.StableBridgeAPI;
import org.elasticsearch.xcontent.XContentType;

import java.util.Map;

public class PipelineConfigurationBridge extends StableBridgeAPI.Proxy<PipelineConfiguration> {
    public PipelineConfigurationBridge(final PipelineConfiguration delegate) {
        super(delegate);
    }

    public PipelineConfigurationBridge(final String pipelineId, final String jsonEncodedConfig) {
        this(new PipelineConfiguration(pipelineId, new BytesArray(jsonEncodedConfig), XContentType.JSON));
    }

    public String getId() {
        return delegate.getId();
    }

    public Map<String, Object> getConfig() {
        return delegate.getConfig();
    }

    public Map<String, Object> getConfig(final boolean unmodifiable) {
        return delegate.getConfig(unmodifiable);
    }

    @Override
    public int hashCode() {
        return delegate.hashCode();
    }

    @Override
    public String toString() {
        return delegate.toString();
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        } else if (obj instanceof PipelineConfigurationBridge other) {
            return delegate.equals(other.delegate);
        } else {
            return false;
        }
    }
}
