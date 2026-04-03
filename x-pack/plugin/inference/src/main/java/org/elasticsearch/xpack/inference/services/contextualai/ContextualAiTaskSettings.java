/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.contextualai;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.inference.TaskSettings;

import static org.elasticsearch.xpack.inference.services.contextualai.ContextualAiUtils.ML_INFERENCE_CONTEXTUAL_AI_ADDED;

public abstract class ContextualAiTaskSettings implements TaskSettings {

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        assert false : "should never be called when supportsVersion is used";
        return ML_INFERENCE_CONTEXTUAL_AI_ADDED;
    }

    @Override
    public boolean supportsVersion(TransportVersion version) {
        return ContextualAiUtils.supportsContextualAi(version);
    }
}
