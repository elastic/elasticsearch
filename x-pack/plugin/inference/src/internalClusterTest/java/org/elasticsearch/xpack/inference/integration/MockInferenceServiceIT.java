/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.integration;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.SecuritySettingsSourceField;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.inference.results.TextExpansionResults;
import org.elasticsearch.xpack.inference.InferencePlugin;
import org.elasticsearch.xpack.inference.action.GetInferenceModelAction;
import org.elasticsearch.xpack.inference.action.InferenceAction;
import org.elasticsearch.xpack.inference.action.PutInferenceModelAction;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;

public class MockInferenceServiceIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(InferencePlugin.class, TestInferenceServicePlugin.class);
    }

    @Override
    protected Function<Client, Client> getClientWrapper() {
        final Map<String, String> headers = Map.of(
            "Authorization",
            basicAuthHeaderValue("x_pack_rest_user", SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING)
        );
        // we need to wrap node clients because we do not specify a user for nodes and all requests will use the system
        // user. This is ok for internal n2n stuff but the test framework does other things like wiping indices, repositories, etc
        // that the system user cannot do. so we wrap the node client with a user that can do these things since the client() calls
        // return a node client
        return client -> client.filterWithHeader(headers);
    }

    public void testMockService() {
        String modelId = "test-mock";
        Model putModel = putMockService(modelId, TaskType.SPARSE_EMBEDDING);
        Model readModel = getModel(modelId, TaskType.SPARSE_EMBEDDING);
        assertModelsAreEqual(putModel, readModel);

        // The response is randomly generated, the input can be anything
        inferOnMockService(modelId, TaskType.SPARSE_EMBEDDING, randomAlphaOfLength(10));
    }

    private Model putMockService(String modelId, TaskType taskType) {
        String body = """
            {
              "service": "test_service",
              "service_settings": {
                "model": "my_model",
                "api_key": "abc64"
              },
              "task_settings": {
                "temperature": 3
              }
            }
            """;
        var request = new PutInferenceModelAction.Request(
            taskType.toString(),
            modelId,
            new BytesArray(body.getBytes(StandardCharsets.UTF_8)),
            XContentType.JSON
        );

        var response = client().execute(PutInferenceModelAction.INSTANCE, request).actionGet();
        assertEquals("test_service", response.getModel().getService());

        assertThat(response.getModel().getServiceSettings(), instanceOf(TestInferenceServicePlugin.TestServiceSettings.class));
        var serviceSettings = (TestInferenceServicePlugin.TestServiceSettings) response.getModel().getServiceSettings();
        assertEquals("my_model", serviceSettings.model());
        assertEquals("abc64", serviceSettings.apiKey());

        assertThat(response.getModel().getTaskSettings(), instanceOf(TestInferenceServicePlugin.TestTaskSettings.class));
        var taskSettings = (TestInferenceServicePlugin.TestTaskSettings) response.getModel().getTaskSettings();
        assertEquals(3, (int) taskSettings.temperature());

        return response.getModel();
    }

    public Model getModel(String modelId, TaskType taskType) {
        var response = client().execute(GetInferenceModelAction.INSTANCE, new GetInferenceModelAction.Request(modelId, taskType.toString()))
            .actionGet();
        return response.getModel();
    }

    private void inferOnMockService(String modelId, TaskType taskType, String input) {
        var response = client().execute(InferenceAction.INSTANCE, new InferenceAction.Request(taskType, modelId, input, Map.of()))
            .actionGet();
        if (taskType == TaskType.SPARSE_EMBEDDING) {
            assertThat(response.getResult(), instanceOf(TextExpansionResults.class));
            var teResult = (TextExpansionResults) response.getResult();
            assertThat(teResult.getWeightedTokens(), not(empty()));
        } else {
            fail("test with task type [" + taskType + "] are not supported yet");
        }
    }

    private void assertModelsAreEqual(Model model1, Model model2) {
        // The test can't rely on Model::equals as the specific subclass
        // may be different. Model loses information about it's implemented
        // subtype when it is streamed across the wire.
        assertEquals(model1.getModelId(), model2.getModelId());
        assertEquals(model1.getService(), model2.getService());
        assertEquals(model1.getTaskType(), model2.getTaskType());

        // TaskSettings and Service settings are named writables so
        // the actual implementing class type is not lost when streamed \
        assertEquals(model1.getServiceSettings(), model2.getServiceSettings());
        assertEquals(model1.getTaskSettings(), model2.getTaskSettings());
    }
}
