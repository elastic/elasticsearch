/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.mixedbread;

import org.elasticsearch.TransportVersion;

/**
 * Utility class for Mixedbread related version checks.
 */
public final class MixedbreadUtils {

    /**
     * TransportVersion indicating when Mixedbread features were added.
     */
    public static final TransportVersion ML_INFERENCE_MIXEDBREAD_ADDED = TransportVersion.fromName("ml_inference_mixedbread_added");

    /**
     * Checks if the given TransportVersion supports Mixedbread features.
     *
     * @param version the TransportVersion to check
     * @return true if Mixedbread features are supported, false otherwise
     */
    public static boolean supportsMixedbread(TransportVersion version) {
        return version.supports(ML_INFERENCE_MIXEDBREAD_ADDED);
    }

    /**
     * Private constructor to prevent instantiation.
     */
    private MixedbreadUtils() {}

}
