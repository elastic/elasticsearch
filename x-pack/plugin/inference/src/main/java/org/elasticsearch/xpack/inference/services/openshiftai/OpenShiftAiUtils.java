/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openshiftai;

import org.elasticsearch.TransportVersion;

/**
 * Utility class for OpenShift AI related version checks.
 */
public final class OpenShiftAiUtils {

    /**
     * TransportVersion indicating when OpenShift AI features were added.
     */
    public static final TransportVersion ML_INFERENCE_OPENSHIFT_AI_ADDED = TransportVersion.fromName("ml_inference_openshift_ai_added");

    /**
     * Checks if the given TransportVersion supports OpenShift AI features.
     *
     * @param version the TransportVersion to check
     * @return true if OpenShift AI features are supported, false otherwise
     */
    public static boolean supportsOpenShiftAi(TransportVersion version) {
        return version.supports(ML_INFERENCE_OPENSHIFT_AI_ADDED);
    }

    /**
     * Private constructor to prevent instantiation.
     */
    private OpenShiftAiUtils() {}

}
