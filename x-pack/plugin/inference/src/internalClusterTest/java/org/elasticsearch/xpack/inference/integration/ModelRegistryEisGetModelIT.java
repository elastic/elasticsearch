/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.integration;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.UnparsedModel;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.http.MockResponse;
import org.elasticsearch.xpack.inference.registry.ModelRegistry;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceService;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceMinimalSettings;

import java.util.Arrays;
import java.util.Map;
import java.util.function.BiConsumer;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.containsString;

public class ModelRegistryEisGetModelIT extends ModelRegistryEisBase {
    private final TestCase testCase;

    public ModelRegistryEisGetModelIT(TestCase testCase) {
        super();
        this.testCase = testCase;
    }

    public record TestCase(
        String description,
        BiConsumer<ModelRegistry, ActionListener<UnparsedModel>> registryCall,
        String responseJson,
        @Nullable UnparsedModel expectedResult,
        @Nullable String failureMessage,
        @Nullable RestStatus failureStatus
    ) {}

    private static class TestCaseBuilder {
        private final String description;
        private final BiConsumer<ModelRegistry, ActionListener<UnparsedModel>> registryCall;
        private final String responseJson;
        private UnparsedModel expectedResult;
        private String failureMessage;
        private RestStatus failureStatus;

        TestCaseBuilder(String description, BiConsumer<ModelRegistry, ActionListener<UnparsedModel>> registryCall, String responseJson) {
            this.description = description;
            this.registryCall = registryCall;
            this.responseJson = responseJson;
        }

        public TestCaseBuilder withSuccessfulResult(UnparsedModel expectedResult) {
            this.expectedResult = expectedResult;
            return this;
        }

        public TestCaseBuilder withFailure(String failure, RestStatus status) {
            this.failureMessage = failure;
            this.failureStatus = status;
            return this;
        }

        public TestCase build() {
            return new TestCase(description, registryCall, responseJson, expectedResult, failureMessage, failureStatus);
        }
    }

    @ParametersFactory
    public static Iterable<TestCase[]> parameters() {
        return Arrays.asList(
            new TestCase[][] {
                // getModel calls
                {
                    new TestCaseBuilder(
                        "getModel retrieves eis chat completion preconfigured endpoint",
                        (modelRegistry, listener) -> modelRegistry.getModel(
                            ElasticInferenceServiceMinimalSettings.DEFAULT_CHAT_COMPLETION_ENDPOINT_ID_V1,
                            listener
                        ),
                        """
                            {
                                "models": [
                                    {
                                      "model_name": "rainbow-sprinkles",
                                      "task_types": ["chat"]
                                    }
                                ]
                            }
                            """
                    ).withSuccessfulResult(
                        new UnparsedModel(
                            ElasticInferenceServiceMinimalSettings.DEFAULT_CHAT_COMPLETION_ENDPOINT_ID_V1,
                            TaskType.CHAT_COMPLETION,
                            ElasticInferenceService.NAME,
                            Map.of(
                                ModelConfigurations.SERVICE_SETTINGS,
                                Map.of(ServiceFields.MODEL_ID, ElasticInferenceServiceMinimalSettings.DEFAULT_CHAT_COMPLETION_MODEL_ID_V1)
                            ),
                            Map.of()
                        )
                    ).build() },
                {
                    new TestCaseBuilder(
                        "getModel throws an exception when retrieving eis "
                            + "chat completion preconfigured endpoint and it isn't authorized",
                        (modelRegistry, listener) -> modelRegistry.getModel(
                            ElasticInferenceServiceMinimalSettings.DEFAULT_CHAT_COMPLETION_ENDPOINT_ID_V1,
                            listener
                        ),
                        """
                            {
                                "models": [
                                ]
                            }
                            """
                    ).withFailure(
                        "Unable to retrieve the preconfigured inference endpoint "
                            + "[.rainbow-sprinkles-elastic] from the Elastic Inference Service",
                        RestStatus.BAD_REQUEST
                    ).build() },
                {
                    new TestCaseBuilder(
                        "getModel retrieves eis elser preconfigured endpoint",
                        (modelRegistry, listener) -> modelRegistry.getModel(
                            ElasticInferenceServiceMinimalSettings.DEFAULT_ELSER_ENDPOINT_ID_V2,
                            listener
                        ),
                        """
                            {
                                "models": [
                                    {
                                      "model_name": "elser_model_2",
                                      "task_types": ["embed/text/sparse"]
                                    }
                                ]
                            }
                            """
                    ).withSuccessfulResult(
                        new UnparsedModel(
                            ElasticInferenceServiceMinimalSettings.DEFAULT_ELSER_ENDPOINT_ID_V2,
                            TaskType.SPARSE_EMBEDDING,
                            ElasticInferenceService.NAME,
                            Map.of(
                                ModelConfigurations.SERVICE_SETTINGS,
                                Map.of(ServiceFields.MODEL_ID, ElasticInferenceServiceMinimalSettings.DEFAULT_ELSER_2_MODEL_ID)
                            ),
                            Map.of()
                        )
                    ).build() },
                {
                    new TestCaseBuilder(
                        "getModel throws exception when retrieving eis elser preconfigured endpoint and not authorized",
                        (modelRegistry, listener) -> modelRegistry.getModel(
                            ElasticInferenceServiceMinimalSettings.DEFAULT_ELSER_ENDPOINT_ID_V2,
                            listener
                        ),
                        """
                            {
                                "models": [
                                ]
                            }
                            """
                    ).withFailure(
                        "Unable to retrieve the preconfigured inference endpoint [.elser-2-elastic] from the Elastic Inference Service",
                        RestStatus.BAD_REQUEST
                    ).build() },
                {
                    new TestCaseBuilder(
                        "getModel retrieves eis multilingual embed preconfigured endpoint",
                        (modelRegistry, listener) -> modelRegistry.getModel(
                            ElasticInferenceServiceMinimalSettings.DEFAULT_MULTILINGUAL_EMBED_ENDPOINT_ID,
                            listener
                        ),
                        """
                            {
                                "models": [
                                    {
                                      "model_name": "multilingual-embed-v1",
                                      "task_types": ["embed/text/dense"]
                                    }
                                ]
                            }
                            """
                    ).withSuccessfulResult(
                        new UnparsedModel(
                            ElasticInferenceServiceMinimalSettings.DEFAULT_MULTILINGUAL_EMBED_ENDPOINT_ID,
                            TaskType.TEXT_EMBEDDING,
                            ElasticInferenceService.NAME,
                            Map.of(
                                ModelConfigurations.SERVICE_SETTINGS,
                                Map.of(
                                    ServiceFields.MODEL_ID,
                                    ElasticInferenceServiceMinimalSettings.DEFAULT_MULTILINGUAL_EMBED_MODEL_ID,
                                    ServiceFields.SIMILARITY,
                                    SimilarityMeasure.COSINE.toString(),
                                    ServiceFields.DIMENSIONS,
                                    ElasticInferenceServiceMinimalSettings.DENSE_TEXT_EMBEDDINGS_DIMENSIONS,
                                    ServiceFields.ELEMENT_TYPE,
                                    DenseVectorFieldMapper.ElementType.FLOAT.toString()
                                )
                            ),
                            Map.of()
                        )
                    ).build() },
                {
                    new TestCaseBuilder(
                        "getModel throws exception when retrieving eis multilingual embed preconfigured endpoint and not authorized",
                        (modelRegistry, listener) -> modelRegistry.getModel(
                            ElasticInferenceServiceMinimalSettings.DEFAULT_MULTILINGUAL_EMBED_ENDPOINT_ID,
                            listener
                        ),
                        """
                            {
                                "models": [
                                ]
                            }
                            """
                    ).withFailure(
                        "Unable to retrieve the preconfigured inference endpoint "
                            + "[.multilingual-embed-v1-elastic] from the Elastic Inference Service",
                        RestStatus.BAD_REQUEST
                    ).build() },
                {
                    new TestCaseBuilder(
                        "getModel retrieves eis rerank preconfigured endpoint",
                        (modelRegistry, listener) -> modelRegistry.getModel(
                            ElasticInferenceServiceMinimalSettings.DEFAULT_RERANK_ENDPOINT_ID_V1,
                            listener
                        ),
                        """
                            {
                                "models": [
                                    {
                                      "model_name": "rerank-v1",
                                      "task_types": ["rerank/text/text-similarity"]
                                    }
                                ]
                            }
                            """
                    ).withSuccessfulResult(
                        new UnparsedModel(
                            ElasticInferenceServiceMinimalSettings.DEFAULT_RERANK_ENDPOINT_ID_V1,
                            TaskType.RERANK,
                            ElasticInferenceService.NAME,
                            Map.of(
                                ModelConfigurations.SERVICE_SETTINGS,
                                Map.of(ServiceFields.MODEL_ID, ElasticInferenceServiceMinimalSettings.DEFAULT_RERANK_MODEL_ID_V1)
                            ),
                            Map.of()
                        )
                    ).build() },
                {
                    new TestCaseBuilder(
                        "getModel throws exception when retrieving eis rerank preconfigured endpoint and not authorized",
                        (modelRegistry, listener) -> modelRegistry.getModel(
                            ElasticInferenceServiceMinimalSettings.DEFAULT_RERANK_ENDPOINT_ID_V1,
                            listener
                        ),
                        """
                            {
                                "models": [
                                ]
                            }
                            """
                    ).withFailure(
                        "Unable to retrieve the preconfigured inference endpoint [.rerank-v1-elastic] from the Elastic Inference Service",
                        RestStatus.BAD_REQUEST
                    ).build() },
                // getModelWithSecrets calls
                {
                    new TestCaseBuilder(
                        "getModelWithSecrets retrieves eis chat completion preconfigured endpoint",
                        (modelRegistry, listener) -> modelRegistry.getModelWithSecrets(
                            ElasticInferenceServiceMinimalSettings.DEFAULT_CHAT_COMPLETION_ENDPOINT_ID_V1,
                            listener
                        ),
                        """
                            {
                                "models": [
                                    {
                                      "model_name": "rainbow-sprinkles",
                                      "task_types": ["chat"]
                                    }
                                ]
                            }
                            """
                    ).withSuccessfulResult(
                        new UnparsedModel(
                            ElasticInferenceServiceMinimalSettings.DEFAULT_CHAT_COMPLETION_ENDPOINT_ID_V1,
                            TaskType.CHAT_COMPLETION,
                            ElasticInferenceService.NAME,
                            Map.of(
                                ModelConfigurations.SERVICE_SETTINGS,
                                Map.of(ServiceFields.MODEL_ID, ElasticInferenceServiceMinimalSettings.DEFAULT_CHAT_COMPLETION_MODEL_ID_V1)
                            ),
                            Map.of()
                        )
                    ).build() },
                {
                    new TestCaseBuilder(
                        "getModelWithSecrets throws an exception when retrieving eis "
                            + "chat completion preconfigured endpoint and it isn't authorized",
                        (modelRegistry, listener) -> modelRegistry.getModelWithSecrets(
                            ElasticInferenceServiceMinimalSettings.DEFAULT_CHAT_COMPLETION_ENDPOINT_ID_V1,
                            listener
                        ),
                        """
                            {
                                "models": [
                                ]
                            }
                            """
                    ).withFailure(
                        "Unable to retrieve the preconfigured inference endpoint "
                            + "[.rainbow-sprinkles-elastic] from the Elastic Inference Service",
                        RestStatus.BAD_REQUEST
                    ).build() },
                {
                    new TestCaseBuilder(
                        "getModelWithSecrets retrieves eis elser preconfigured endpoint",
                        (modelRegistry, listener) -> modelRegistry.getModelWithSecrets(
                            ElasticInferenceServiceMinimalSettings.DEFAULT_ELSER_ENDPOINT_ID_V2,
                            listener
                        ),
                        """
                            {
                                "models": [
                                    {
                                      "model_name": "elser_model_2",
                                      "task_types": ["embed/text/sparse"]
                                    }
                                ]
                            }
                            """
                    ).withSuccessfulResult(
                        new UnparsedModel(
                            ElasticInferenceServiceMinimalSettings.DEFAULT_ELSER_ENDPOINT_ID_V2,
                            TaskType.SPARSE_EMBEDDING,
                            ElasticInferenceService.NAME,
                            Map.of(
                                ModelConfigurations.SERVICE_SETTINGS,
                                Map.of(ServiceFields.MODEL_ID, ElasticInferenceServiceMinimalSettings.DEFAULT_ELSER_2_MODEL_ID)
                            ),
                            Map.of()
                        )
                    ).build() },
                {
                    new TestCaseBuilder(
                        "getModelWithSecrets throws exception when retrieving eis elser preconfigured endpoint and not authorized",
                        (modelRegistry, listener) -> modelRegistry.getModelWithSecrets(
                            ElasticInferenceServiceMinimalSettings.DEFAULT_ELSER_ENDPOINT_ID_V2,
                            listener
                        ),
                        """
                            {
                                "models": [
                                ]
                            }
                            """
                    ).withFailure(
                        "Unable to retrieve the preconfigured inference endpoint [.elser-2-elastic] from the Elastic Inference Service",
                        RestStatus.BAD_REQUEST
                    ).build() },
                {
                    new TestCaseBuilder(
                        "getModelWithSecrets retrieves eis multilingual embed preconfigured endpoint",
                        (modelRegistry, listener) -> modelRegistry.getModelWithSecrets(
                            ElasticInferenceServiceMinimalSettings.DEFAULT_MULTILINGUAL_EMBED_ENDPOINT_ID,
                            listener
                        ),
                        """
                            {
                                "models": [
                                    {
                                      "model_name": "multilingual-embed-v1",
                                      "task_types": ["embed/text/dense"]
                                    }
                                ]
                            }
                            """
                    ).withSuccessfulResult(
                        new UnparsedModel(
                            ElasticInferenceServiceMinimalSettings.DEFAULT_MULTILINGUAL_EMBED_ENDPOINT_ID,
                            TaskType.TEXT_EMBEDDING,
                            ElasticInferenceService.NAME,
                            Map.of(
                                ModelConfigurations.SERVICE_SETTINGS,
                                Map.of(
                                    ServiceFields.MODEL_ID,
                                    ElasticInferenceServiceMinimalSettings.DEFAULT_MULTILINGUAL_EMBED_MODEL_ID,
                                    ServiceFields.SIMILARITY,
                                    SimilarityMeasure.COSINE.toString(),
                                    ServiceFields.DIMENSIONS,
                                    ElasticInferenceServiceMinimalSettings.DENSE_TEXT_EMBEDDINGS_DIMENSIONS,
                                    ServiceFields.ELEMENT_TYPE,
                                    DenseVectorFieldMapper.ElementType.FLOAT.toString()
                                )
                            ),
                            Map.of()
                        )
                    ).build() },
                {
                    new TestCaseBuilder(
                        "getModelWithSecrets throws exception when retrieving eis "
                            + "multilingual embed preconfigured endpoint and not authorized",
                        (modelRegistry, listener) -> modelRegistry.getModelWithSecrets(
                            ElasticInferenceServiceMinimalSettings.DEFAULT_MULTILINGUAL_EMBED_ENDPOINT_ID,
                            listener
                        ),
                        """
                            {
                                "models": [
                                ]
                            }
                            """
                    ).withFailure(
                        "Unable to retrieve the preconfigured inference endpoint "
                            + "[.multilingual-embed-v1-elastic] from the Elastic Inference Service",
                        RestStatus.BAD_REQUEST
                    ).build() },
                {
                    new TestCaseBuilder(
                        "getModelWithSecrets retrieves eis rerank preconfigured endpoint",
                        (modelRegistry, listener) -> modelRegistry.getModelWithSecrets(
                            ElasticInferenceServiceMinimalSettings.DEFAULT_RERANK_ENDPOINT_ID_V1,
                            listener
                        ),
                        """
                            {
                                "models": [
                                    {
                                      "model_name": "rerank-v1",
                                      "task_types": ["rerank/text/text-similarity"]
                                    }
                                ]
                            }
                            """
                    ).withSuccessfulResult(
                        new UnparsedModel(
                            ElasticInferenceServiceMinimalSettings.DEFAULT_RERANK_ENDPOINT_ID_V1,
                            TaskType.RERANK,
                            ElasticInferenceService.NAME,
                            Map.of(
                                ModelConfigurations.SERVICE_SETTINGS,
                                Map.of(ServiceFields.MODEL_ID, ElasticInferenceServiceMinimalSettings.DEFAULT_RERANK_MODEL_ID_V1)
                            ),
                            Map.of()
                        )
                    ).build() },
                {
                    new TestCaseBuilder(
                        "getModelWithSecrets throws exception when retrieving eis rerank preconfigured endpoint and not authorized",
                        (modelRegistry, listener) -> modelRegistry.getModelWithSecrets(
                            ElasticInferenceServiceMinimalSettings.DEFAULT_RERANK_ENDPOINT_ID_V1,
                            listener
                        ),
                        """
                            {
                                "models": [
                                ]
                            }
                            """
                    ).withFailure(
                        "Unable to retrieve the preconfigured inference endpoint [.rerank-v1-elastic] from the Elastic Inference Service",
                        RestStatus.BAD_REQUEST
                    ).build() } }
        );
    }

    public void test() {
        webServer.enqueue(new MockResponse().setResponseCode(200).setBody(testCase.responseJson));

        PlainActionFuture<UnparsedModel> listener = new PlainActionFuture<>();
        testCase.registryCall.accept(modelRegistry, listener);

        if (testCase.expectedResult != null) {
            assertSuccessfulTestCase(listener);
        } else {
            assertFailureTestCase(listener);
        }
    }

    private void assertSuccessfulTestCase(PlainActionFuture<UnparsedModel> listener) {
        var model = listener.actionGet(TIMEOUT);
        assertThat(model, is(testCase.expectedResult));
    }

    private void assertFailureTestCase(PlainActionFuture<UnparsedModel> listener) {
        var exception = expectThrows(ElasticsearchStatusException.class, () -> listener.actionGet(TIMEOUT));
        assertThat(exception.getMessage(), containsString(testCase.failureMessage));
        assertThat(exception.status(), is(testCase.failureStatus));
    }
}
