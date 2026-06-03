/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.googlevertexai.request;

import org.elasticsearch.TransportVersion;

public final class GoogleVertexAiUtils {

    public static final String GOOGLE_VERTEX_AI_HOST_SUFFIX = "-aiplatform.googleapis.com";

    public static final String GOOGLE_DISCOVERY_ENGINE_HOST = "discoveryengine.googleapis.com";

    public static final String V1 = "v1";

    public static final String PROJECTS = "projects";

    public static final String LOCATIONS = "locations";

    public static final String GLOBAL = "global";

    public static final String RANKING_CONFIGS = "rankingConfigs";

    public static final String DEFAULT_RANKING_CONFIG = "default_ranking_config";

    public static final String PUBLISHERS = "publishers";

    public static final String PUBLISHER_GOOGLE = "google";

    public static final String MODELS = "models";

    public static final String PREDICT = "predict";

    public static final String RANK = "rank";

    public static final String STREAM_GENERATE_CONTENT = "streamGenerateContent";

    public static final String GENERATE_CONTENT = "generateContent";

    public static final String QUERY_PARAM_ALT_SSE = "alt=sse";

    public static final TransportVersion ML_INFERENCE_GOOGLE_MODEL_GARDEN_ADDED = TransportVersion.fromName(
        "ml_inference_google_model_garden_added"
    );

    public static boolean supportsModelGarden(TransportVersion version) {
        return version.supports(ML_INFERENCE_GOOGLE_MODEL_GARDEN_ADDED);
    }

    private GoogleVertexAiUtils() {}

}
