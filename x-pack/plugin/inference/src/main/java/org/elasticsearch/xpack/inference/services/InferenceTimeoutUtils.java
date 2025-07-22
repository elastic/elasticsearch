/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.inference.InferencePlugin;

public class InferenceTimeoutUtils {
    public static TimeValue resolveInferenceTimeout(@Nullable TimeValue timeout, InputType inputType, ClusterService clusterService) {
        if (timeout == null) {
            if (inputType == InputType.SEARCH || inputType == InputType.INTERNAL_SEARCH) {
                return clusterService.getClusterSettings().get(InferencePlugin.INFERENCE_QUERY_TIMEOUT);
            } else {
                return InferenceAction.Request.DEFAULT_TIMEOUT;
            }
        }
        return timeout;
    }
}
