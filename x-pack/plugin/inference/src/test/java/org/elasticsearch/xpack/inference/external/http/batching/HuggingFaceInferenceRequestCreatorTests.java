/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.batching;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.common.TruncatorTests;
import org.elasticsearch.xpack.inference.external.huggingface.HuggingFaceAccount;
import org.elasticsearch.xpack.inference.external.huggingface.HuggingFaceResponseHandler;
import org.elasticsearch.xpack.inference.external.response.huggingface.HuggingFaceElserResponseEntity;
import org.elasticsearch.xpack.inference.services.huggingface.HuggingFaceModel;

public class HuggingFaceInferenceRequestCreatorTests extends ESTestCase {

    public static HuggingFaceInferenceRequestCreator create(HuggingFaceModel model) {
        return new HuggingFaceInferenceRequestCreator(
            new HuggingFaceAccount(model.getUri(), model.getApiKey()),
            new HuggingFaceResponseHandler("hugging face", HuggingFaceElserResponseEntity::fromResponse),
            TruncatorTests.createTruncator(),
            model.getTokenLimit()
        );
    }
}
