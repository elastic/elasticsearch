/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.voyageai;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.xpack.inference.services.settings.FilteredXContentObject;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.io.IOException;
import java.util.Objects;

public abstract class VoyageAIServiceSettings extends FilteredXContentObject implements ServiceSettings {

    private final VoyageAICommonServiceSettings commonSettings;

    protected VoyageAIServiceSettings(VoyageAICommonServiceSettings commonSettings) {
        this.commonSettings = Objects.requireNonNull(commonSettings);
    }

    protected VoyageAIServiceSettings(StreamInput in) throws IOException {
        this.commonSettings = new VoyageAICommonServiceSettings(in);
    }

    public VoyageAICommonServiceSettings commonSettings() {
        return commonSettings;
    }

    @Override
    public String modelId() {
        return commonSettings.modelId();
    }

    public RateLimitSettings rateLimitSettings() {
        return commonSettings.rateLimitSettings();
    }
}
