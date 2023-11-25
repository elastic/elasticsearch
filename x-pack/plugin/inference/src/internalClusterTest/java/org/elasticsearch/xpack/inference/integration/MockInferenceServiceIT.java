/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.integration;

import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.SecuritySettingsSourceField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.inference.results.TextExpansionResults;
import org.elasticsearch.xpack.inference.InferencePlugin;
import org.elasticsearch.xpack.inference.action.GetInferenceModelAction;
import org.elasticsearch.xpack.inference.action.InferenceAction;
import org.elasticsearch.xpack.inference.action.PutInferenceModelAction;
import org.elasticsearch.xpack.inference.registry.ModelRegistry;
import org.junit.Before;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.elasticsearch.xpack.inference.services.MapParsingUtils.removeFromMapOrThrowIfNull;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;

public class MockInferenceServiceIT extends ESIntegTestCase {

    private static final TimeValue TIMEOUT = new TimeValue(30, TimeUnit.SECONDS);

    private ModelRegistry modelRegistry;

    @Before
    public void createComponents() {
        modelRegistry = new ModelRegistry(client());
    }

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
        ModelConfigurations putModel = putMockService(modelId, "test_service", TaskType.SPARSE_EMBEDDING);
        ModelConfigurations readModel = getModel(modelId, TaskType.SPARSE_EMBEDDING);
        assertModelsAreEqual(putModel, readModel);

        // The response is randomly generated, the input can be anything
        inferOnMockService(modelId, TaskType.SPARSE_EMBEDDING, List.of(randomAlphaOfLength(10)));
    }

    public void testMockServiceWithMultipleInputs() {
        String modelId = "test-mock-with-multi-inputs";
        ModelConfigurations putModel = putMockService(modelId, "test_service", TaskType.SPARSE_EMBEDDING);
        ModelConfigurations readModel = getModel(modelId, TaskType.SPARSE_EMBEDDING);
        assertModelsAreEqual(putModel, readModel);

        // The response is randomly generated, the input can be anything
        inferOnMockService(
            modelId,
            TaskType.SPARSE_EMBEDDING,
            List.of(randomAlphaOfLength(5), randomAlphaOfLength(10), randomAlphaOfLength(15))
        );
    }

    public void testMockService_DoesNotReturnSecretsInGetResponse() throws IOException {
        String modelId = "test-mock";
        putMockService(modelId, "test_service", TaskType.SPARSE_EMBEDDING);
        ModelConfigurations readModel = getModel(modelId, TaskType.SPARSE_EMBEDDING);

        assertThat(readModel.getServiceSettings(), instanceOf(TestInferenceServicePlugin.TestServiceSettings.class));

        var serviceSettings = (TestInferenceServicePlugin.TestServiceSettings) readModel.getServiceSettings();
        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON).prettyPrint();
        serviceSettings.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is("""
            {
              "model" : "my_model"
            }"""));
    }

    public void testGetUnparsedModelMap_ForTestServiceModel_ReturnsSecretsPopulated() {
        String modelId = "test-unparsed";
        putMockService(modelId, "test_service", TaskType.SPARSE_EMBEDDING);

        var listener = new PlainActionFuture<ModelRegistry.ModelConfigMap>();
        modelRegistry.getUnparsedModelMap(modelId, listener);

        var modelConfig = listener.actionGet(TIMEOUT);
        var secretsMap = removeFromMapOrThrowIfNull(modelConfig.secrets(), ModelSecrets.SECRET_SETTINGS);
        var secrets = TestInferenceServicePlugin.TestSecretSettings.fromMap(secretsMap);
        assertThat(secrets.apiKey(), is("abc64"));
    }

    private ModelConfigurations putMockService(String modelId, String serviceName, TaskType taskType) {
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
            """, serviceName);
        var request = new PutInferenceModelAction.Request(
            taskType.toString(),
            modelId,
            new BytesArray(body.getBytes(StandardCharsets.UTF_8)),
            XContentType.JSON
        );

        var response = client().execute(PutInferenceModelAction.INSTANCE, request).actionGet();
        assertEquals(serviceName, response.getModel().getService());

        assertThat(response.getModel().getServiceSettings(), instanceOf(TestInferenceServicePlugin.TestServiceSettings.class));
        var serviceSettings = (TestInferenceServicePlugin.TestServiceSettings) response.getModel().getServiceSettings();
        assertEquals("my_model", serviceSettings.model());

        assertThat(response.getModel().getTaskSettings(), instanceOf(TestInferenceServicePlugin.TestTaskSettings.class));
        var taskSettings = (TestInferenceServicePlugin.TestTaskSettings) response.getModel().getTaskSettings();
        assertEquals(3, (int) taskSettings.temperature());

        return response.getModel();
    }

    public ModelConfigurations getModel(String modelId, TaskType taskType) {
        var response = client().execute(GetInferenceModelAction.INSTANCE, new GetInferenceModelAction.Request(modelId, taskType.toString()))
            .actionGet();
        return response.getModel();
    }

    private List<? extends InferenceResults> inferOnMockService(String modelId, TaskType taskType, List<String> input) {
        var response = client().execute(InferenceAction.INSTANCE, new InferenceAction.Request(taskType, modelId, input, Map.of()))
            .actionGet();
        if (taskType == TaskType.SPARSE_EMBEDDING) {
            response.getResults().forEach(result -> {
                assertThat(result, instanceOf(TextExpansionResults.class));
                var teResult = (TextExpansionResults) result;
                assertThat(teResult.getWeightedTokens(), not(empty()));
            });

        } else {
            fail("test with task type [" + taskType + "] are not supported yet");
        }

        return response.getResults();
    }

    private void assertModelsAreEqual(ModelConfigurations model1, ModelConfigurations model2) {
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
