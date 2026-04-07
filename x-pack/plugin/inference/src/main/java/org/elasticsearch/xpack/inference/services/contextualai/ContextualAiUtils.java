/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.contextualai;

import org.elasticsearch.TransportVersion;

/**
 * Shared constants and helpers for the Contextual AI inference integration.
 */
public final class ContextualAiUtils {

    public static final TransportVersion ML_INFERENCE_CONTEXTUAL_AI_ADDED = TransportVersion.fromName("contextual_ai_service");

    public static final TransportVersion ML_INFERENCE_CONTEXTUAL_AI_RETURN_DOCUMENTS_REMOVED = TransportVersion.fromName(
        "contextual_ai_return_documents_removed"
    );

    /**
     * Whether the given transport version supports Contextual AI inference objects on the wire.
     */
    public static boolean supportsContextualAi(TransportVersion version) {
        return version.supports(ML_INFERENCE_CONTEXTUAL_AI_ADDED);
    }

    private ContextualAiUtils() {}
}
