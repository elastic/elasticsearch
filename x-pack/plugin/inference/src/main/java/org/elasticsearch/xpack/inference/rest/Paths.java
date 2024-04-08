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
    static final String INFERENCE_ID_PATH = "_inference/{" + TASK_TYPE_OR_INFERENCE_ID + "}";
    static final String TASK_TYPE_INFERENCE_ID_PATH = "_inference/{" + TASK_TYPE_OR_INFERENCE_ID + "}/{" + INFERENCE_ID + "}";

    private Paths() {

    }
}
