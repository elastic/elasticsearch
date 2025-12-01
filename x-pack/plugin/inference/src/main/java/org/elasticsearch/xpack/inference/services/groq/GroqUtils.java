/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.groq;

import org.elasticsearch.TransportVersion;

/**
 * Utility class for Groq inference services.
 */
public final class GroqUtils {
    public static final String HOST = "api.groq.com";

    public static final String OPENAI_PATH = "openai";
    public static final String VERSION_1_PATH = "v1";
    public static final String CHAT_PATH = "chat";
    public static final String COMPLETIONS_PATH = "completions";

    public static final TransportVersion ML_INFERENCE_GROQ_ADDED = TransportVersion.fromName("ml_inference_groq_added");

    /**
     * Check if the given transport version supports Groq inference services.
     *
     * @param version the transport version to check
     * @return true if the version supports Groq inference services, false otherwise
     */
    public static boolean supportsGroq(TransportVersion version) {
        return version.supports(ML_INFERENCE_GROQ_ADDED);
    }

    /**
     * Private constructor to prevent instantiation of this utility class.
     */
    private GroqUtils() {}

}
