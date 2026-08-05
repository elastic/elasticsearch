/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.googlevertexai.request;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Strings;

public final class GoogleVertexAiUtils {

    public static final String GOOGLE_VERTEX_AI_HOST_SUFFIX = "-aiplatform.googleapis.com";

    /** Host of the Vertex AI global endpoint, which (unlike regional endpoints) carries no {@code <location>-} prefix. */
    public static final String GOOGLE_VERTEX_AI_GLOBAL_HOST = "aiplatform.googleapis.com";

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

    public static final TransportVersion GOOGLE_VERTEX_AI_CONFIGURABLE_MAX_BATCH_SIZE = TransportVersion.fromName(
        "google_vertex_ai_configurable_max_batch_size"
    );

    public static final TransportVersion GOOGLE_VERTEX_AI_OPTIONAL_LOCATION = TransportVersion.fromName(
        "google_vertex_ai_optional_location"
    );

    public static boolean supportsModelGarden(TransportVersion version) {
        return version.supports(ML_INFERENCE_GOOGLE_MODEL_GARDEN_ADDED);
    }

    /** A blank or missing location targets the Vertex AI global endpoint. */
    public static boolean isGlobalLocation(@Nullable String location) {
        return location == null || location.isEmpty();
    }

    /** Resolves the location into the Vertex AI URL path segment: {@code "global"} when none is provided, otherwise the location. */
    public static String resolveLocation(@Nullable String location) {
        return isGlobalLocation(location) ? GLOBAL : location;
    }

    /** Resolves the Vertex AI host: the global host when no location is provided, otherwise the regional host. */
    public static String resolveHost(@Nullable String location) {
        return isGlobalLocation(location) ? GOOGLE_VERTEX_AI_GLOBAL_HOST : Strings.format("%s%s", location, GOOGLE_VERTEX_AI_HOST_SUFFIX);
    }

    private GoogleVertexAiUtils() {}

}
