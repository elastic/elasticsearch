/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.action;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.features.FeatureService;

import static org.elasticsearch.xpack.inference.InferenceFeatures.INFERENCE_CCM_ENABLEMENT_SERVICE;

public class CCMActionUtils {

    public static boolean isClusterUpgradedToSupportEnablementService(ClusterState state, FeatureService featureService) {
        return state.clusterRecovered() && featureService.clusterHasFeature(state, INFERENCE_CCM_ENABLEMENT_SERVICE);
    }

    private CCMActionUtils() {}
}
