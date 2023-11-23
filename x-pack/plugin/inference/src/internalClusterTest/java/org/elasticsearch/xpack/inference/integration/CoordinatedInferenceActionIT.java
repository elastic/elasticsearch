/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.integration;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.core.Strings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.action.CoordinatedInferenceAction;
import org.elasticsearch.xpack.inference.InferencePlugin;
import org.elasticsearch.xpack.inference.action.PutInferenceModelAction;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.List;

public class CoordinatedInferenceActionIT extends ESSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(InferencePlugin.class, MockServicePlugin.class);
    }

    public void testInternalInferenceAction() throws IOException {
        String modelId = "inference_service_model";
        createMockInferenceServiceModel(modelId);

        List<String> inputs = List.of("important text", "for inference");
        var request = CoordinatedInferenceAction.Request.forInferenceService(modelId, inputs, null);
        var response = client().execute(CoordinatedInferenceAction.INSTANCE, request).actionGet();
        assertEquals(2, response.getInferenceResults().size());
    }

    private void createMockInferenceServiceModel(String modelId) {
        String body = Strings.format("""
            {
              "service": "%s",
              "service_settings": {
                "model": "my_model",
                "api_key": "abc64"
              },
              "task_settings": {
                "temperature": 3
              }
            }
            """, MockServicePlugin.SERVICE_NAME);
        var request = new PutInferenceModelAction.Request(
            MockServicePlugin.SUPPORTED_TASK_TYPE.toString(),
            modelId,
            new BytesArray(body.getBytes(StandardCharsets.UTF_8)),
            XContentType.JSON
        );

        var response = client().execute(PutInferenceModelAction.INSTANCE, request).actionGet();
        assertEquals(MockServicePlugin.SERVICE_NAME, response.getModel().getService());
    }
}
