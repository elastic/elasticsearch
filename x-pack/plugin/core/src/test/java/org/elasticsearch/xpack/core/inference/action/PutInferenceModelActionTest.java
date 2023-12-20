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
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.utils.MlStrings;
import org.junit.After;
import org.junit.Before;

public class PutInferenceModelActionTest extends ESTestCase {
    public static String TASK_TYPE = randomAlphaOfLengthBetween(1, 10);
    public static String MODEL_ID = randomAlphaOfLengthBetween(1, 10);
    public static XContentType X_CONTENT_TYPE = randomFrom(XContentType.values());
    public static BytesReference BYTES = new BytesArray(randomAlphaOfLengthBetween(1, 10).getBytes());

    @Before
    public void setUp() throws Exception {
        TASK_TYPE = randomAlphaOfLengthBetween(1, 10);
        MODEL_ID = randomAlphaOfLengthBetween(1, 10);
        X_CONTENT_TYPE = randomFrom(XContentType.values());
        BYTES = new BytesArray(randomAlphaOfLengthBetween(1, 10).getBytes());

    }

    @After
    public void tearDown() throws Exception {}

    public void testValidate() {
        // valid model ID
        var request = new PutInferenceModelAction.Request(TASK_TYPE, MODEL_ID + "_-0", BYTES, X_CONTENT_TYPE);
        ActionRequestValidationException validationException = request.validate();
        assertNull(validationException);

        // invalid model ID
        var invalidRequest = new PutInferenceModelAction.Request(TASK_TYPE, "", BYTES, X_CONTENT_TYPE);
        validationException = invalidRequest.validate();
        assertNotNull(validationException);

        var invalidRequest2 = new PutInferenceModelAction.Request(
            TASK_TYPE,
            randomAlphaOfLengthBetween(1, 10) + randomFrom(MlStrings.someInvalidChars),
            BYTES,
            X_CONTENT_TYPE
        );
        validationException = invalidRequest2.validate();
        assertNotNull(validationException);

        var invalidRequest3 = new PutInferenceModelAction.Request(
            TASK_TYPE,
            randomAlphaOfLengthBetween(1, 10) + randomFrom(MlStrings.someInvalidChars),
            BYTES,
            X_CONTENT_TYPE
        );
        validationException = invalidRequest3.validate();
        assertNotNull(validationException);

        var invalidRequest4 = new PutInferenceModelAction.Request(TASK_TYPE, null, BYTES, X_CONTENT_TYPE);
        validationException = invalidRequest4.validate();
        assertNotNull(validationException);
    }
}
