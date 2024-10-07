/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.action;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.utils.MlStringsTests;
import org.junit.Before;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.inference.ModelConfigurations.PARAMETERS;
import static org.elasticsearch.inference.ModelConfigurations.TASK_SETTINGS;

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
        var request = new PutInferenceModelAction.Request(TASK_TYPE, MODEL_ID + "_-0", BYTES, X_CONTENT_TYPE);
        ActionRequestValidationException validationException = request.validate();
        assertNull(validationException);

        // invalid model IDs

        var invalidRequest = new PutInferenceModelAction.Request(TASK_TYPE, "", BYTES, X_CONTENT_TYPE);
        validationException = invalidRequest.validate();
        assertNotNull(validationException);

        var invalidRequest2 = new PutInferenceModelAction.Request(
            TASK_TYPE,
            randomAlphaOfLengthBetween(1, 10) + randomFrom(MlStringsTests.SOME_INVALID_CHARS),
            BYTES,
            X_CONTENT_TYPE
        );
        validationException = invalidRequest2.validate();
        assertNotNull(validationException);

        var invalidRequest3 = new PutInferenceModelAction.Request(TASK_TYPE, null, BYTES, X_CONTENT_TYPE);
        validationException = invalidRequest3.validate();
        assertNotNull(validationException);
    }

    public void testWithParameters() throws IOException {
        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        Map<String, Object> parametersValues = Map.of("top_n", 1, "top_p", 0.1);
        Map<String, Object> serviceSettingsValues = Map.of("model_id", "embed", "dimensions", 1024);
        builder.map(Map.of(PARAMETERS, parametersValues, "service", "elasticsearch", "service_settings", serviceSettingsValues));
        var request = new PutInferenceModelAction.Request(TASK_TYPE, MODEL_ID, BytesReference.bytes(builder), XContentType.JSON);
        Map<String, Object> map = XContentHelper.convertToMap(request.getContent(), false, request.getContentType()).v2();
        assertEquals(parametersValues, map.get(TASK_SETTINGS));
        assertNull(map.get(PARAMETERS));
        assertEquals("elasticsearch", map.get("service"));
        assertEquals(serviceSettingsValues, map.get("service_settings"));
    }

    public void testWithParametersAndTaskSettings() throws IOException {
        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        Map<String, Object> parametersValues = Map.of("top_n", 1, "top_p", 0.1);
        Map<String, Object> taskSettingsValues = Map.of("top_n", 2, "top_p", 0.2);
        Map<String, Object> serviceSettingsValues = Map.of("model_id", "embed", "dimensions", 1024);
        builder.map(
            Map.of(
                PARAMETERS,
                parametersValues,
                TASK_SETTINGS,
                taskSettingsValues,
                "service",
                "elasticsearch",
                "service_settings",
                serviceSettingsValues
            )
        );
        assertThrows(
            ElasticsearchStatusException.class,
            () -> new PutInferenceModelAction.Request(TASK_TYPE, MODEL_ID, BytesReference.bytes(builder), XContentType.JSON).getContent()
        );

    }

    public void testWithTaskSettings() throws IOException {
        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        Map<String, Object> taskSettingsValues = Map.of("top_n", 2, "top_p", 0.2);
        Map<String, Object> serviceSettingsValues = Map.of("model_id", "embed", "dimensions", 1024);
        builder.map(Map.of(TASK_SETTINGS, taskSettingsValues, "service", "elasticsearch", "service_settings", serviceSettingsValues));
        var request = new PutInferenceModelAction.Request(TASK_TYPE, MODEL_ID, BytesReference.bytes(builder), XContentType.JSON);
        Map<String, Object> map = XContentHelper.convertToMap(request.getContent(), false, request.getContentType()).v2();
        assertEquals(taskSettingsValues, map.get(TASK_SETTINGS));
        assertNull(map.get(PARAMETERS));
        assertEquals("elasticsearch", map.get("service"));
        assertEquals(serviceSettingsValues, map.get("service_settings"));
    }
}
