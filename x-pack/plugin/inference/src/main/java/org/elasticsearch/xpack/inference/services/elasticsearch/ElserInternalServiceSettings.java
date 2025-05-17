/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elasticsearch;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.inference.MinimalServiceSettings;
import org.elasticsearch.xpack.core.ml.inference.assignment.AdaptiveAllocationsSettings;

import java.io.IOException;

import static org.elasticsearch.xpack.core.ml.inference.assignment.AdaptiveAllocationsSettings.DEFAULT_MAX_ALLOCATIONS;
import static org.elasticsearch.xpack.core.ml.inference.assignment.AdaptiveAllocationsSettings.DEFAULT_MIN_ALLOCATIONS;
import static org.elasticsearch.xpack.inference.services.elasticsearch.ElserModels.ELSER_V2_MODEL;
import static org.elasticsearch.xpack.inference.services.elasticsearch.ElserModels.ELSER_V2_MODEL_LINUX_X86;

public class ElserInternalServiceSettings extends ElasticsearchInternalServiceSettings {

    public static final String NAME = "elser_mlnode_service_settings";

    public static MinimalServiceSettings minimalServiceSettings() {
        return MinimalServiceSettings.sparseEmbedding(ElasticsearchInternalService.NAME);
    }

    public static ElserInternalServiceSettings defaultEndpointSettings(boolean useLinuxOptimizedModel, Settings settings) {
        return new ElserInternalServiceSettings(
            null,
            1,
            useLinuxOptimizedModel ? ELSER_V2_MODEL_LINUX_X86 : ELSER_V2_MODEL,
            new AdaptiveAllocationsSettings(Boolean.TRUE, DEFAULT_MIN_ALLOCATIONS.get(settings), DEFAULT_MAX_ALLOCATIONS.get(settings))
        );
    }

    public ElserInternalServiceSettings(ElasticsearchInternalServiceSettings other) {
        super(other);
    }

    private ElserInternalServiceSettings(
        Integer numAllocations,
        int numThreads,
        String modelId,
        AdaptiveAllocationsSettings adaptiveAllocationsSettings
    ) {
        this(new ElasticsearchInternalServiceSettings(numAllocations, numThreads, modelId, adaptiveAllocationsSettings, null));
    }

    public ElserInternalServiceSettings(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String getWriteableName() {
        return ElserInternalServiceSettings.NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.V_8_11_X;
    }
}
