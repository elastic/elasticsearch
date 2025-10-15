/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openshiftai;

import org.elasticsearch.TransportVersion;

public final class OpenShiftAiUtils {
    public static final TransportVersion ML_INFERENCE_OPENSHIFT_AI_ADDED = TransportVersion.fromName("ml_inference_openshift_ai_added");

    public static boolean supportsOpenShiftAi(TransportVersion version) {
        return version.supports(ML_INFERENCE_OPENSHIFT_AI_ADDED);
    }

    private OpenShiftAiUtils() {}

}
