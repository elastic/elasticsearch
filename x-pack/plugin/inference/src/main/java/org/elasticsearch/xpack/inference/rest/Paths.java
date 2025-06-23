/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.rest;

public final class Paths {

    static final String INFERENCE_ID = "inference_id";
    static final String TASK_TYPE_OR_INFERENCE_ID = "task_type_or_id";
    static final String TASK_TYPE = "task_type";
    static final String INFERENCE_ID_PATH = "_inference/{" + TASK_TYPE_OR_INFERENCE_ID + "}";
    static final String TASK_TYPE_INFERENCE_ID_PATH = "_inference/{" + TASK_TYPE_OR_INFERENCE_ID + "}/{" + INFERENCE_ID + "}";
    static final String INFERENCE_DIAGNOSTICS_PATH = "_inference/.diagnostics";
    static final String TASK_TYPE_INFERENCE_ID_UPDATE_PATH = "_inference/{"
        + TASK_TYPE_OR_INFERENCE_ID
        + "}/{"
        + INFERENCE_ID
        + "}/_update";
    static final String INFERENCE_ID_UPDATE_PATH = "_inference/{" + TASK_TYPE_OR_INFERENCE_ID + "}/_update";
    static final String INFERENCE_SERVICES_PATH = "_inference/_services";
    static final String TASK_TYPE_INFERENCE_SERVICES_PATH = "_inference/_services/{" + TASK_TYPE + "}";

    public static final String STREAM_SUFFIX = "_stream";
    static final String STREAM_INFERENCE_ID_PATH = "_inference/{" + TASK_TYPE_OR_INFERENCE_ID + "}/" + STREAM_SUFFIX;
    static final String STREAM_TASK_TYPE_INFERENCE_ID_PATH = "_inference/{"
        + TASK_TYPE_OR_INFERENCE_ID
        + "}/{"
        + INFERENCE_ID
        + "}/"
        + STREAM_SUFFIX;

    public static final String RETURN_MINIMAL_CONFIG = "return_minimal_config";

    private Paths() {

    }
}
