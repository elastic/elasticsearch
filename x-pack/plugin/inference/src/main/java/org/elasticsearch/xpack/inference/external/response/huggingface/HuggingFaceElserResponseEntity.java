/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.response.huggingface;

import org.elasticsearch.inference.InferenceResults;

public record HuggingFaceElserResponseEntity() implements InferenceResults {
    private static final String NAME = "hugging_face_elser_response_entity";
}
