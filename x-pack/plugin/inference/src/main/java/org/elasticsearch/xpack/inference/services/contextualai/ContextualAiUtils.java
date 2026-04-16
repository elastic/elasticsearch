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

    public static final TransportVersion INFERENCE_CONTEXTUAL_AI_ADDED = TransportVersion.fromName("contextual_ai_service");
    public static final TransportVersion INFERENCE_CONTEXTUAL_AI_URL_SERVICE_SETTING_REMOVED = TransportVersion.fromName(
        "contextual_ai_url_service_setting_removed"
    );

    private ContextualAiUtils() {}
}
