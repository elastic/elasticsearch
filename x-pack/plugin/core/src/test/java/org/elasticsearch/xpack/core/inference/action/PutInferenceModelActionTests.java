/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.utils.MlStringsTests;
import org.junit.Before;

import java.util.Locale;

public class PutInferenceModelActionTests extends ESTestCase {
    public static TaskType TASK_TYPE;
    public static String MODEL_ID;
    public static XContentType X_CONTENT_TYPE;
    public static BytesReference BYTES;

    @Before
    public void setup() throws Exception {
        TASK_TYPE = TaskType.SPARSE_EMBEDDING;
        MODEL_ID = randomAlphaOfLengthBetween(1, 10).toLowerCase(Locale.ROOT);
        X_CONTENT_TYPE = randomFrom(XContentType.values());
        BYTES = new BytesArray(randomAlphaOfLengthBetween(1, 10));
    }

    public void testValidate() {
        // valid model ID
        var request = new PutInferenceModelAction.Request(
            TASK_TYPE,
            MODEL_ID + "_-0",
            BYTES,
            X_CONTENT_TYPE,
            InferenceAction.Request.DEFAULT_TIMEOUT
        );
        ActionRequestValidationException validationException = request.validate();
        assertNull(validationException);

        // invalid model IDs

        var invalidRequest = new PutInferenceModelAction.Request(
            TASK_TYPE,
            "",
            BYTES,
            X_CONTENT_TYPE,
            InferenceAction.Request.DEFAULT_TIMEOUT
        );
        validationException = invalidRequest.validate();
        assertNotNull(validationException);

        var invalidRequest2 = new PutInferenceModelAction.Request(
            TASK_TYPE,
            randomAlphaOfLengthBetween(1, 10) + randomFrom(MlStringsTests.SOME_INVALID_CHARS),
            BYTES,
            X_CONTENT_TYPE,
            InferenceAction.Request.DEFAULT_TIMEOUT
        );
        validationException = invalidRequest2.validate();
        assertNotNull(validationException);

        var invalidRequest3 = new PutInferenceModelAction.Request(
            TASK_TYPE,
            null,
            BYTES,
            X_CONTENT_TYPE,
            InferenceAction.Request.DEFAULT_TIMEOUT
        );
        validationException = invalidRequest3.validate();
        assertNotNull(validationException);
    }
}
