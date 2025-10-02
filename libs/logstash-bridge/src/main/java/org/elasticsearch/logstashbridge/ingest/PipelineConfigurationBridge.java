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

/**
 * A {@link StableBridgeAPI} for {@link PipelineConfiguration}
 */
public interface PipelineConfigurationBridge extends StableBridgeAPI<PipelineConfiguration> {

    static PipelineConfigurationBridge create(final String pipelineId, final String jsonEncodedConfig) {
        final PipelineConfiguration internal = new PipelineConfiguration(pipelineId, new BytesArray(jsonEncodedConfig), XContentType.JSON);
        return fromInternal(internal);
    }

    static PipelineConfigurationBridge fromInternal(final PipelineConfiguration internal) {
        return new ProxyInternalPipelineConfigurationBridge(internal);
    }

    String getId();

    Map<String, Object> getConfig();

    Map<String, Object> getConfig(boolean unmodifiable);

    int hashCode();

    String toString();

    boolean equals(Object o);

}
