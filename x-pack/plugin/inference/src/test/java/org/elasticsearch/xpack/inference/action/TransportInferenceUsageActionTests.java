/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.InferenceFieldMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.protocol.xpack.XPackUsageRequest;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockUtils;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.core.XPackFeatureUsage;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureResponse;
import org.elasticsearch.xpack.core.inference.InferenceFeatureSetUsage;
import org.elasticsearch.xpack.core.inference.action.GetInferenceModelAction;
import org.elasticsearch.xpack.core.inference.usage.ModelStats;
import org.elasticsearch.xpack.core.inference.usage.SemanticTextStats;
import org.elasticsearch.xpack.core.watcher.support.xcontent.XContentSource;
import org.elasticsearch.xpack.inference.registry.ModelRegistry;
import org.elasticsearch.xpack.inference.services.openai.OpenAiService;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_HIDDEN_SETTING;
import static org.elasticsearch.xpack.inference.InferenceFeatures.EMBEDDING_TASK_TYPE;
import static org.elasticsearch.xpack.inference.Utils.TIMEOUT;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.Is.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransportInferenceUsageActionTests extends ESTestCase {

    private static final String EIS_SERVICE = "eis";
    private static final String SOME_MODEL_ID = "some-model";
    private static final String INDEX_1 = "index_1";
    private static final String INDEX_2 = "index_2";
    private static final String INFERENCE_ENDPOINT_1 = "endpoint-001";
    private static final String INFERENCE_ENDPOINT_2 = "endpoint-002";
    private static final String INFERENCE_ENDPOINT_3 = "endpoint-003";
    private static final String INFERENCE_ENDPOINT_5 = "endpoint-005";
    private static final String DEFAULT_ENDPOINT_1 = ".endpoint-001";
    private static final String DEFAULT_MODEL_ID_1 = ".model-id-001";
    private static final String DEFAULT_MODEL_ID_1_LINUX = ".model-id-001_linux-x86_64";
    private static final String EIS_DEFAULT_MODEL_1_STATS_KEY = "_eis__model-id-001";

    private static final String MODEL_ID_1 = "model-id-001";
    private static final String MODEL_ID_2 = "model-id-002";
    private static final String MODEL_ID_3 = "model-id-003";
    private static final String MODEL_ID_4 = "model-id-004";
    private static final String MODEL_ID_5 = "model-id-005";
    private static final String MODEL_ID_6 = "model-id-006";

    private static final String SEMANTIC_FIELD_1 = "semantic-1";
    private static final String SEMANTIC_FIELD_2 = "semantic-2";
    private static final String SEMANTIC_FIELD_3 = "semantic-3";
    private static final String SEMANTIC_FIELD_4 = "semantic-4";
    private static final String SEMANTIC_FIELD_5 = "semantic-5";

    private static final SemanticTextStats EMPTY_SEMANTIC_TEXT_STATS = new SemanticTextStats();

    private Client client;
    private ModelRegistry modelRegistry;
    private ClusterState clusterState;
    private TransportInferenceUsageAction action;
    private FeatureService featureServiceMock;

    @Before
    public void init() {
        client = mock(Client.class);
        ThreadPool threadPool = new TestThreadPool("test");
        when(client.threadPool()).thenReturn(threadPool);

        modelRegistry = mock(ModelRegistry.class);

        givenClusterState(Map.of());

        TransportService transportService = MockUtils.setupTransportServiceWithThreadpoolExecutor(mock(ThreadPool.class));

        featureServiceMock = mock(FeatureService.class);
        when(featureServiceMock.clusterHasFeature(any(), eq(EMBEDDING_TASK_TYPE))).thenReturn(true);
        action = new TransportInferenceUsageAction(
            transportService,
            mock(ClusterService.class),
            mock(ThreadPool.class),
            mock(ActionFilters.class),
            modelRegistry,
            client,
            featureServiceMock
        );
    }

    @After
    public void close() {
        client.threadPool().shutdown();
    }

    public void testGivenServices_NoInferenceFields() throws Exception {
        var huggingFaceElser = "hugging_face_elser";
        givenInferenceEndpoints(
            new ModelConfigurations("model-001", TaskType.TEXT_EMBEDDING, OpenAiService.NAME, mockServiceSettings(MODEL_ID_1)),
            new ModelConfigurations("model-002", TaskType.TEXT_EMBEDDING, OpenAiService.NAME, mockServiceSettings(MODEL_ID_2)),
            new ModelConfigurations("model-003", TaskType.SPARSE_EMBEDDING, huggingFaceElser, mockServiceSettings(MODEL_ID_3)),
            new ModelConfigurations("model-004", TaskType.TEXT_EMBEDDING, OpenAiService.NAME, mockServiceSettings(MODEL_ID_4)),
            new ModelConfigurations("model-005", TaskType.SPARSE_EMBEDDING, OpenAiService.NAME, mockServiceSettings(MODEL_ID_5)),
            new ModelConfigurations("model-006", TaskType.SPARSE_EMBEDDING, huggingFaceElser, mockServiceSettings(MODEL_ID_6))
        );

        XContentSource response = executeAction();

        assertThat(response.getValue(InferenceFeatureSetUsage.MODELS_FIELD), hasSize(9));
        assertStats(response, 0, new ModelStats(Metadata.ALL, TaskType.CHAT_COMPLETION, 0, EMPTY_SEMANTIC_TEXT_STATS));
        assertStats(response, 1, new ModelStats(Metadata.ALL, TaskType.COMPLETION, 0, EMPTY_SEMANTIC_TEXT_STATS));
        assertStats(response, 2, new ModelStats(Metadata.ALL, TaskType.EMBEDDING, 0, EMPTY_SEMANTIC_TEXT_STATS));
        assertStats(response, 3, new ModelStats(Metadata.ALL, TaskType.RERANK, 0, EMPTY_SEMANTIC_TEXT_STATS));
        assertStats(response, 4, new ModelStats(Metadata.ALL, TaskType.SPARSE_EMBEDDING, 3, EMPTY_SEMANTIC_TEXT_STATS));
        assertStats(response, 5, new ModelStats(Metadata.ALL, TaskType.TEXT_EMBEDDING, 3, EMPTY_SEMANTIC_TEXT_STATS));
        assertStats(response, 6, new ModelStats(huggingFaceElser, TaskType.SPARSE_EMBEDDING, 2, EMPTY_SEMANTIC_TEXT_STATS));
        assertStats(response, 7, new ModelStats(OpenAiService.NAME, TaskType.SPARSE_EMBEDDING, 1, EMPTY_SEMANTIC_TEXT_STATS));
        assertStats(response, 8, new ModelStats(OpenAiService.NAME, TaskType.TEXT_EMBEDDING, 3, EMPTY_SEMANTIC_TEXT_STATS));
    }

    public void testGivenFieldRefersToMissingInferenceEndpoint() throws Exception {
        givenInferenceEndpoints();
        givenInferenceFields(
            Map.of(INDEX_1, List.of(new InferenceFieldMetadata(SEMANTIC_FIELD_1, INFERENCE_ENDPOINT_1, new String[0], null)))
        );

        XContentSource response = executeAction();

        assertThat(response.getValue(InferenceFeatureSetUsage.MODELS_FIELD), hasSize(6));
        assertStats(response, 0, new ModelStats(Metadata.ALL, TaskType.CHAT_COMPLETION, 0, EMPTY_SEMANTIC_TEXT_STATS));
        assertStats(response, 1, new ModelStats(Metadata.ALL, TaskType.COMPLETION, 0, EMPTY_SEMANTIC_TEXT_STATS));
        assertStats(response, 2, new ModelStats(Metadata.ALL, TaskType.EMBEDDING, 0, EMPTY_SEMANTIC_TEXT_STATS));
        assertStats(response, 3, new ModelStats(Metadata.ALL, TaskType.RERANK, 0, EMPTY_SEMANTIC_TEXT_STATS));
        assertStats(response, 4, new ModelStats(Metadata.ALL, TaskType.SPARSE_EMBEDDING, 0, EMPTY_SEMANTIC_TEXT_STATS));
        assertStats(response, 5, new ModelStats(Metadata.ALL, TaskType.TEXT_EMBEDDING, 0, EMPTY_SEMANTIC_TEXT_STATS));
    }

    public void testGivenVariousServicesAndInferenceFields() throws Exception {
        var endpoint4 = "endpoint-004";
        var endpoint6 = "endpoint-006";
        var modelId7 = "model-id-007";
        var semanticField6 = "semantic-6";
        var semanticField7 = "semantic-7";
        var semanticField8 = "semantic-8";
        givenInferenceEndpoints(
            new ModelConfigurations(INFERENCE_ENDPOINT_1, TaskType.TEXT_EMBEDDING, OpenAiService.NAME, mockServiceSettings(MODEL_ID_1)),
            new ModelConfigurations(INFERENCE_ENDPOINT_2, TaskType.TEXT_EMBEDDING, OpenAiService.NAME, mockServiceSettings(MODEL_ID_2)),
            new ModelConfigurations(INFERENCE_ENDPOINT_3, TaskType.SPARSE_EMBEDDING, OpenAiService.NAME, mockServiceSettings(MODEL_ID_3)),
            new ModelConfigurations(endpoint4, TaskType.SPARSE_EMBEDDING, OpenAiService.NAME, mockServiceSettings(MODEL_ID_4)),
            new ModelConfigurations(INFERENCE_ENDPOINT_5, TaskType.TEXT_EMBEDDING, EIS_SERVICE, mockServiceSettings(MODEL_ID_5)),
            new ModelConfigurations(endpoint6, TaskType.TEXT_EMBEDDING, EIS_SERVICE, mockServiceSettings(MODEL_ID_6)),
            new ModelConfigurations("endpoint-007", TaskType.TEXT_EMBEDDING, EIS_SERVICE, mockServiceSettings(modelId7)) // unused
        );

        givenInferenceFields(
            Map.of(
                INDEX_1,
                List.of(
                    new InferenceFieldMetadata(SEMANTIC_FIELD_1, INFERENCE_ENDPOINT_1, new String[0], null),
                    new InferenceFieldMetadata(SEMANTIC_FIELD_2, INFERENCE_ENDPOINT_1, new String[0], null),
                    new InferenceFieldMetadata(SEMANTIC_FIELD_3, INFERENCE_ENDPOINT_2, new String[0], null),
                    new InferenceFieldMetadata(SEMANTIC_FIELD_4, INFERENCE_ENDPOINT_2, new String[0], null),
                    new InferenceFieldMetadata(SEMANTIC_FIELD_5, INFERENCE_ENDPOINT_2, new String[0], null),
                    new InferenceFieldMetadata(semanticField6, INFERENCE_ENDPOINT_3, new String[0], null),
                    new InferenceFieldMetadata(semanticField7, endpoint4, new String[0], null),
                    new InferenceFieldMetadata(semanticField8, INFERENCE_ENDPOINT_5, new String[0], null)
                ),
                INDEX_2,
                List.of(
                    new InferenceFieldMetadata(SEMANTIC_FIELD_1, INFERENCE_ENDPOINT_1, new String[0], null),
                    new InferenceFieldMetadata(SEMANTIC_FIELD_2, INFERENCE_ENDPOINT_3, new String[0], null),
                    new InferenceFieldMetadata(SEMANTIC_FIELD_3, INFERENCE_ENDPOINT_3, new String[0], null),
                    new InferenceFieldMetadata(SEMANTIC_FIELD_4, endpoint4, new String[0], null),
                    new InferenceFieldMetadata(SEMANTIC_FIELD_5, endpoint4, new String[0], null),
                    new InferenceFieldMetadata(semanticField6, INFERENCE_ENDPOINT_5, new String[0], null)
                ),
                "index_3",
                List.of(new InferenceFieldMetadata(SEMANTIC_FIELD_1, endpoint6, new String[0], null))
            )
        );

        XContentSource response = executeAction();

        assertThat(response.getValue(InferenceFeatureSetUsage.MODELS_FIELD), hasSize(9));
        assertStats(response, 0, new ModelStats(Metadata.ALL, TaskType.CHAT_COMPLETION, 0, EMPTY_SEMANTIC_TEXT_STATS));
        assertStats(response, 1, new ModelStats(Metadata.ALL, TaskType.COMPLETION, 0, EMPTY_SEMANTIC_TEXT_STATS));
        assertStats(response, 2, new ModelStats(Metadata.ALL, TaskType.EMBEDDING, 0, EMPTY_SEMANTIC_TEXT_STATS));
        assertStats(response, 3, new ModelStats(Metadata.ALL, TaskType.RERANK, 0, EMPTY_SEMANTIC_TEXT_STATS));
        assertStats(response, 4, new ModelStats(Metadata.ALL, TaskType.SPARSE_EMBEDDING, 2, new SemanticTextStats(6, 2, 2)));
        assertStats(response, 5, new ModelStats(Metadata.ALL, TaskType.TEXT_EMBEDDING, 5, new SemanticTextStats(9, 3, 4)));
        assertStats(response, 6, new ModelStats(EIS_SERVICE, TaskType.TEXT_EMBEDDING, 3, new SemanticTextStats(3, 3, 2)));
        assertStats(response, 7, new ModelStats(OpenAiService.NAME, TaskType.SPARSE_EMBEDDING, 2, new SemanticTextStats(6, 2, 2)));
        assertStats(response, 8, new ModelStats(OpenAiService.NAME, TaskType.TEXT_EMBEDDING, 2, new SemanticTextStats(6, 2, 2)));
    }

    public void testGivenServices_InferenceFieldsReferencingDefaultModels() throws Exception {
        var defaultEndpoint2 = ".endpoint-002";
        var defaultEndpoint4 = ".endpoint-004";
        givenInferenceEndpoints(
            new ModelConfigurations(DEFAULT_ENDPOINT_1, TaskType.TEXT_EMBEDDING, EIS_SERVICE, mockServiceSettings(DEFAULT_MODEL_ID_1)),
            new ModelConfigurations(defaultEndpoint2, TaskType.SPARSE_EMBEDDING, EIS_SERVICE, mockServiceSettings(".model-id-002")),
            new ModelConfigurations(INFERENCE_ENDPOINT_3, TaskType.TEXT_EMBEDDING, OpenAiService.NAME, mockServiceSettings(MODEL_ID_3)),
            new ModelConfigurations(defaultEndpoint4, TaskType.SPARSE_EMBEDDING, OpenAiService.NAME, mockServiceSettings(MODEL_ID_4)),
            new ModelConfigurations(INFERENCE_ENDPOINT_5, TaskType.TEXT_EMBEDDING, EIS_SERVICE, mockServiceSettings(DEFAULT_MODEL_ID_1))
        );

        givenDefaultEndpoints(DEFAULT_ENDPOINT_1, defaultEndpoint2, defaultEndpoint4);

        givenInferenceFields(
            Map.of(
                INDEX_1,
                List.of(
                    new InferenceFieldMetadata(SEMANTIC_FIELD_1, DEFAULT_ENDPOINT_1, new String[0], null),
                    new InferenceFieldMetadata(SEMANTIC_FIELD_2, DEFAULT_ENDPOINT_1, new String[0], null),
                    new InferenceFieldMetadata(SEMANTIC_FIELD_3, defaultEndpoint2, new String[0], null),
                    new InferenceFieldMetadata(SEMANTIC_FIELD_4, defaultEndpoint4, new String[0], null),
                    new InferenceFieldMetadata(SEMANTIC_FIELD_5, INFERENCE_ENDPOINT_5, new String[0], null)
                ),
                INDEX_2,
                List.of(
                    new InferenceFieldMetadata(SEMANTIC_FIELD_1, DEFAULT_ENDPOINT_1, new String[0], null),
                    new InferenceFieldMetadata(SEMANTIC_FIELD_2, defaultEndpoint4, new String[0], null),
                    new InferenceFieldMetadata(SEMANTIC_FIELD_3, defaultEndpoint4, new String[0], null),
                    new InferenceFieldMetadata(SEMANTIC_FIELD_4, INFERENCE_ENDPOINT_5, new String[0], null),
                    new InferenceFieldMetadata(SEMANTIC_FIELD_5, INFERENCE_ENDPOINT_5, new String[0], null)
                )
            )
        );

        XContentSource response = executeAction();

        assertThat(response.getValue(InferenceFeatureSetUsage.MODELS_FIELD), hasSize(13));
        assertStats(response, 0, new ModelStats(Metadata.ALL, TaskType.CHAT_COMPLETION, 0, EMPTY_SEMANTIC_TEXT_STATS));
        assertStats(response, 1, new ModelStats(Metadata.ALL, TaskType.COMPLETION, 0, EMPTY_SEMANTIC_TEXT_STATS));
        assertStats(response, 2, new ModelStats(Metadata.ALL, TaskType.EMBEDDING, 0, EMPTY_SEMANTIC_TEXT_STATS));
        assertStats(response, 3, new ModelStats(Metadata.ALL, TaskType.RERANK, 0, EMPTY_SEMANTIC_TEXT_STATS));
        assertStats(response, 4, new ModelStats(Metadata.ALL, TaskType.SPARSE_EMBEDDING, 2, new SemanticTextStats(4, 2, 2)));
        assertStats(response, 5, new ModelStats(Metadata.ALL, TaskType.TEXT_EMBEDDING, 3, new SemanticTextStats(6, 2, 2)));
        assertStats(response, 6, new ModelStats(EIS_DEFAULT_MODEL_1_STATS_KEY, TaskType.TEXT_EMBEDDING, 2, new SemanticTextStats(6, 2, 2)));
        assertStats(response, 7, new ModelStats("_eis__model-id-002", TaskType.SPARSE_EMBEDDING, 1, new SemanticTextStats(1, 1, 1)));
        assertStats(response, 8, new ModelStats("_openai_model-id-004", TaskType.SPARSE_EMBEDDING, 1, new SemanticTextStats(3, 2, 1)));
        assertStats(response, 9, new ModelStats(EIS_SERVICE, TaskType.SPARSE_EMBEDDING, 1, new SemanticTextStats(1, 1, 1)));
        assertStats(response, 10, new ModelStats(EIS_SERVICE, TaskType.TEXT_EMBEDDING, 2, new SemanticTextStats(6, 2, 2)));
        assertStats(response, 11, new ModelStats(OpenAiService.NAME, TaskType.SPARSE_EMBEDDING, 1, new SemanticTextStats(3, 2, 1)));
        assertStats(response, 12, new ModelStats(OpenAiService.NAME, TaskType.TEXT_EMBEDDING, 1, EMPTY_SEMANTIC_TEXT_STATS));
    }

    /**
     * Regression test demonstrating that after removing the {@code TASK_TYPES_WITH_SEMANTIC_TEXT_SUPPORT} filter,
     * default endpoints with task types beyond {@link TaskType#TEXT_EMBEDDING} and {@link TaskType#SPARSE_EMBEDDING}
     * now produce their own stats entries.
     */
    public void testGivenDefaultEndpointWithNonEmbeddingTaskType_DefaultModelStatsAreReported() throws Exception {
        var defaultRerankModelId = ".model-id-rerank";
        givenInferenceEndpoints(
            new ModelConfigurations(DEFAULT_ENDPOINT_1, TaskType.RERANK, EIS_SERVICE, mockServiceSettings(defaultRerankModelId)),
            new ModelConfigurations(INFERENCE_ENDPOINT_2, TaskType.RERANK, EIS_SERVICE, mockServiceSettings(defaultRerankModelId))
        );

        givenDefaultEndpoints(DEFAULT_ENDPOINT_1);

        XContentSource response = executeAction();

        assertThat(response.getValue(InferenceFeatureSetUsage.MODELS_FIELD), hasSize(8));
        assertStats(response, 0, new ModelStats(Metadata.ALL, TaskType.CHAT_COMPLETION, 0, EMPTY_SEMANTIC_TEXT_STATS));
        assertStats(response, 1, new ModelStats(Metadata.ALL, TaskType.COMPLETION, 0, EMPTY_SEMANTIC_TEXT_STATS));
        assertStats(response, 2, new ModelStats(Metadata.ALL, TaskType.EMBEDDING, 0, EMPTY_SEMANTIC_TEXT_STATS));
        assertStats(response, 3, new ModelStats(Metadata.ALL, TaskType.RERANK, 2, EMPTY_SEMANTIC_TEXT_STATS));
        assertStats(response, 4, new ModelStats(Metadata.ALL, TaskType.SPARSE_EMBEDDING, 0, EMPTY_SEMANTIC_TEXT_STATS));
        assertStats(response, 5, new ModelStats(Metadata.ALL, TaskType.TEXT_EMBEDDING, 0, EMPTY_SEMANTIC_TEXT_STATS));
        assertStats(response, 6, new ModelStats("_eis__model-id-rerank", TaskType.RERANK, 2, EMPTY_SEMANTIC_TEXT_STATS));
        assertStats(response, 7, new ModelStats(EIS_SERVICE, TaskType.RERANK, 2, EMPTY_SEMANTIC_TEXT_STATS));
    }

    public void testGivenDefaultModelWithLinuxSuffix() throws Exception {
        givenInferenceEndpoints(
            new ModelConfigurations(
                DEFAULT_ENDPOINT_1,
                TaskType.TEXT_EMBEDDING,
                EIS_SERVICE,
                mockServiceSettings(DEFAULT_MODEL_ID_1_LINUX)
            ),
            new ModelConfigurations(
                INFERENCE_ENDPOINT_2,
                TaskType.TEXT_EMBEDDING,
                EIS_SERVICE,
                mockServiceSettings(DEFAULT_MODEL_ID_1_LINUX)
            )
        );

        givenDefaultEndpoints(DEFAULT_ENDPOINT_1);

        givenInferenceFields(
            Map.of(
                INDEX_1,
                List.of(
                    new InferenceFieldMetadata(SEMANTIC_FIELD_1, DEFAULT_ENDPOINT_1, new String[0], null),
                    new InferenceFieldMetadata(SEMANTIC_FIELD_2, DEFAULT_ENDPOINT_1, new String[0], null),
                    new InferenceFieldMetadata(SEMANTIC_FIELD_3, INFERENCE_ENDPOINT_2, new String[0], null)
                ),
                INDEX_2,
                List.of(
                    new InferenceFieldMetadata(SEMANTIC_FIELD_1, DEFAULT_ENDPOINT_1, new String[0], null),
                    new InferenceFieldMetadata(SEMANTIC_FIELD_2, DEFAULT_ENDPOINT_1, new String[0], null),
                    new InferenceFieldMetadata(SEMANTIC_FIELD_3, INFERENCE_ENDPOINT_2, new String[0], null)
                )
            )
        );

        XContentSource response = executeAction();

        assertThat(response.getValue(InferenceFeatureSetUsage.MODELS_FIELD), hasSize(8));
        assertStats(response, 0, new ModelStats(Metadata.ALL, TaskType.CHAT_COMPLETION, 0, EMPTY_SEMANTIC_TEXT_STATS));
        assertStats(response, 1, new ModelStats(Metadata.ALL, TaskType.COMPLETION, 0, EMPTY_SEMANTIC_TEXT_STATS));
        assertStats(response, 2, new ModelStats(Metadata.ALL, TaskType.EMBEDDING, 0, EMPTY_SEMANTIC_TEXT_STATS));
        assertStats(response, 3, new ModelStats(Metadata.ALL, TaskType.RERANK, 0, EMPTY_SEMANTIC_TEXT_STATS));
        assertStats(response, 4, new ModelStats(Metadata.ALL, TaskType.SPARSE_EMBEDDING, 0, EMPTY_SEMANTIC_TEXT_STATS));
        assertStats(response, 5, new ModelStats(Metadata.ALL, TaskType.TEXT_EMBEDDING, 2, new SemanticTextStats(6, 2, 2)));
        assertStats(response, 6, new ModelStats(EIS_DEFAULT_MODEL_1_STATS_KEY, TaskType.TEXT_EMBEDDING, 2, new SemanticTextStats(6, 2, 2)));
        assertStats(response, 7, new ModelStats(EIS_SERVICE, TaskType.TEXT_EMBEDDING, 2, new SemanticTextStats(6, 2, 2)));
    }

    public void testGivenSameDefaultModelWithAndWithoutLinuxSuffix() throws Exception {
        givenInferenceEndpoints(
            new ModelConfigurations(
                DEFAULT_ENDPOINT_1,
                TaskType.TEXT_EMBEDDING,
                EIS_SERVICE,
                mockServiceSettings(DEFAULT_MODEL_ID_1_LINUX)
            ),
            new ModelConfigurations(INFERENCE_ENDPOINT_2, TaskType.TEXT_EMBEDDING, EIS_SERVICE, mockServiceSettings(DEFAULT_MODEL_ID_1))
        );

        givenDefaultEndpoints(DEFAULT_ENDPOINT_1);

        givenInferenceFields(
            Map.of(
                INDEX_1,
                List.of(
                    new InferenceFieldMetadata(SEMANTIC_FIELD_1, DEFAULT_ENDPOINT_1, new String[0], null),
                    new InferenceFieldMetadata(SEMANTIC_FIELD_2, DEFAULT_ENDPOINT_1, new String[0], null),
                    new InferenceFieldMetadata(SEMANTIC_FIELD_3, INFERENCE_ENDPOINT_2, new String[0], null)
                ),
                INDEX_2,
                List.of(
                    new InferenceFieldMetadata(SEMANTIC_FIELD_1, DEFAULT_ENDPOINT_1, new String[0], null),
                    new InferenceFieldMetadata(SEMANTIC_FIELD_2, DEFAULT_ENDPOINT_1, new String[0], null),
                    new InferenceFieldMetadata(SEMANTIC_FIELD_3, INFERENCE_ENDPOINT_2, new String[0], null)
                )
            )
        );

        XContentSource response = executeAction();

        assertThat(response.getValue(InferenceFeatureSetUsage.MODELS_FIELD), hasSize(8));
        assertStats(response, 0, new ModelStats(Metadata.ALL, TaskType.CHAT_COMPLETION, 0, EMPTY_SEMANTIC_TEXT_STATS));
        assertStats(response, 1, new ModelStats(Metadata.ALL, TaskType.COMPLETION, 0, EMPTY_SEMANTIC_TEXT_STATS));
        assertStats(response, 2, new ModelStats(Metadata.ALL, TaskType.EMBEDDING, 0, EMPTY_SEMANTIC_TEXT_STATS));
        assertStats(response, 3, new ModelStats(Metadata.ALL, TaskType.RERANK, 0, EMPTY_SEMANTIC_TEXT_STATS));
        assertStats(response, 4, new ModelStats(Metadata.ALL, TaskType.SPARSE_EMBEDDING, 0, EMPTY_SEMANTIC_TEXT_STATS));
        assertStats(response, 5, new ModelStats(Metadata.ALL, TaskType.TEXT_EMBEDDING, 2, new SemanticTextStats(6, 2, 2)));
        assertStats(response, 6, new ModelStats(EIS_DEFAULT_MODEL_1_STATS_KEY, TaskType.TEXT_EMBEDDING, 2, new SemanticTextStats(6, 2, 2)));
        assertStats(response, 7, new ModelStats(EIS_SERVICE, TaskType.TEXT_EMBEDDING, 2, new SemanticTextStats(6, 2, 2)));
    }

    public void testGivenExternalServiceModelIsNull() throws Exception {
        givenInferenceEndpoints(
            new ModelConfigurations(INFERENCE_ENDPOINT_1, TaskType.TEXT_EMBEDDING, OpenAiService.NAME, mockServiceSettings(null))
        );
        givenInferenceFields(Map.of(INDEX_1, List.of(new InferenceFieldMetadata("semantic", INFERENCE_ENDPOINT_1, new String[0], null))));

        XContentSource response = executeAction();

        assertThat(response.getValue(InferenceFeatureSetUsage.MODELS_FIELD), hasSize(7));
        assertStats(response, 0, new ModelStats(Metadata.ALL, TaskType.CHAT_COMPLETION, 0, EMPTY_SEMANTIC_TEXT_STATS));
        assertStats(response, 1, new ModelStats(Metadata.ALL, TaskType.COMPLETION, 0, EMPTY_SEMANTIC_TEXT_STATS));
        assertStats(response, 2, new ModelStats(Metadata.ALL, TaskType.EMBEDDING, 0, EMPTY_SEMANTIC_TEXT_STATS));
        assertStats(response, 3, new ModelStats(Metadata.ALL, TaskType.RERANK, 0, EMPTY_SEMANTIC_TEXT_STATS));
        assertStats(response, 4, new ModelStats(Metadata.ALL, TaskType.SPARSE_EMBEDDING, 0, EMPTY_SEMANTIC_TEXT_STATS));
        assertStats(response, 5, new ModelStats(Metadata.ALL, TaskType.TEXT_EMBEDDING, 1, new SemanticTextStats(1, 1, 1)));
        assertStats(response, 6, new ModelStats(OpenAiService.NAME, TaskType.TEXT_EMBEDDING, 1, new SemanticTextStats(1, 1, 1)));
    }

    public void testGivenDuplicateServices() throws Exception {
        givenInferenceEndpoints(
            new ModelConfigurations(INFERENCE_ENDPOINT_1, TaskType.TEXT_EMBEDDING, OpenAiService.NAME, mockServiceSettings(SOME_MODEL_ID)),
            new ModelConfigurations(INFERENCE_ENDPOINT_2, TaskType.TEXT_EMBEDDING, OpenAiService.NAME, mockServiceSettings(SOME_MODEL_ID))
        );
        givenInferenceFields(
            Map.of(
                INDEX_1,
                List.of(
                    new InferenceFieldMetadata(SEMANTIC_FIELD_1, INFERENCE_ENDPOINT_1, new String[0], null),
                    new InferenceFieldMetadata(SEMANTIC_FIELD_2, INFERENCE_ENDPOINT_2, new String[0], null)
                ),
                INDEX_2,
                List.of(
                    new InferenceFieldMetadata(SEMANTIC_FIELD_1, INFERENCE_ENDPOINT_1, new String[0], null),
                    new InferenceFieldMetadata(SEMANTIC_FIELD_2, INFERENCE_ENDPOINT_2, new String[0], null),
                    new InferenceFieldMetadata(SEMANTIC_FIELD_3, INFERENCE_ENDPOINT_2, new String[0], null)
                )
            )
        );

        XContentSource response = executeAction();

        assertThat(response.getValue(InferenceFeatureSetUsage.MODELS_FIELD), hasSize(7));
        assertStats(response, 0, new ModelStats(Metadata.ALL, TaskType.CHAT_COMPLETION, 0, EMPTY_SEMANTIC_TEXT_STATS));
        assertStats(response, 1, new ModelStats(Metadata.ALL, TaskType.COMPLETION, 0, EMPTY_SEMANTIC_TEXT_STATS));
        assertStats(response, 2, new ModelStats(Metadata.ALL, TaskType.EMBEDDING, 0, EMPTY_SEMANTIC_TEXT_STATS));
        assertStats(response, 3, new ModelStats(Metadata.ALL, TaskType.RERANK, 0, EMPTY_SEMANTIC_TEXT_STATS));
        assertStats(response, 4, new ModelStats(Metadata.ALL, TaskType.SPARSE_EMBEDDING, 0, EMPTY_SEMANTIC_TEXT_STATS));
        assertStats(response, 5, new ModelStats(Metadata.ALL, TaskType.TEXT_EMBEDDING, 2, new SemanticTextStats(5, 2, 2)));
        assertStats(response, 6, new ModelStats(OpenAiService.NAME, TaskType.TEXT_EMBEDDING, 2, new SemanticTextStats(5, 2, 2)));
    }

    public void testShouldExcludeSystemIndexFields() throws Exception {
        givenInferenceEndpoints(
            new ModelConfigurations(INFERENCE_ENDPOINT_1, TaskType.TEXT_EMBEDDING, EIS_SERVICE, mockServiceSettings(SOME_MODEL_ID)),
            new ModelConfigurations(INFERENCE_ENDPOINT_2, TaskType.TEXT_EMBEDDING, OpenAiService.NAME, mockServiceSettings(SOME_MODEL_ID))
        );
        givenInferenceFields(
            Map.of(
                INDEX_1,
                List.of(
                    new InferenceFieldMetadata(SEMANTIC_FIELD_1, INFERENCE_ENDPOINT_1, new String[0], null),
                    new InferenceFieldMetadata(SEMANTIC_FIELD_2, INFERENCE_ENDPOINT_2, new String[0], null)
                ),
                INDEX_2,
                List.of(
                    new InferenceFieldMetadata(SEMANTIC_FIELD_1, INFERENCE_ENDPOINT_1, new String[0], null),
                    new InferenceFieldMetadata(SEMANTIC_FIELD_2, INFERENCE_ENDPOINT_2, new String[0], null),
                    new InferenceFieldMetadata(SEMANTIC_FIELD_3, INFERENCE_ENDPOINT_2, new String[0], null)
                )
            ),
            Set.of(INDEX_2),
            Set.of()
        );

        XContentSource response = executeAction();

        assertThat(response.getValue(InferenceFeatureSetUsage.MODELS_FIELD), hasSize(8));
        assertStats(response, 0, new ModelStats(Metadata.ALL, TaskType.CHAT_COMPLETION, 0, EMPTY_SEMANTIC_TEXT_STATS));
        assertStats(response, 1, new ModelStats(Metadata.ALL, TaskType.COMPLETION, 0, EMPTY_SEMANTIC_TEXT_STATS));
        assertStats(response, 2, new ModelStats(Metadata.ALL, TaskType.EMBEDDING, 0, EMPTY_SEMANTIC_TEXT_STATS));
        assertStats(response, 3, new ModelStats(Metadata.ALL, TaskType.RERANK, 0, EMPTY_SEMANTIC_TEXT_STATS));
        assertStats(response, 4, new ModelStats(Metadata.ALL, TaskType.SPARSE_EMBEDDING, 0, EMPTY_SEMANTIC_TEXT_STATS));
        assertStats(response, 5, new ModelStats(Metadata.ALL, TaskType.TEXT_EMBEDDING, 2, new SemanticTextStats(2, 1, 2)));
        assertStats(response, 6, new ModelStats(EIS_SERVICE, TaskType.TEXT_EMBEDDING, 1, new SemanticTextStats(1, 1, 1)));
        assertStats(response, 7, new ModelStats(OpenAiService.NAME, TaskType.TEXT_EMBEDDING, 1, new SemanticTextStats(1, 1, 1)));
    }

    public void testShouldExcludeHiddenIndexFields() throws Exception {
        givenInferenceEndpoints(
            new ModelConfigurations(INFERENCE_ENDPOINT_1, TaskType.TEXT_EMBEDDING, EIS_SERVICE, mockServiceSettings(SOME_MODEL_ID)),
            new ModelConfigurations(INFERENCE_ENDPOINT_2, TaskType.TEXT_EMBEDDING, OpenAiService.NAME, mockServiceSettings(SOME_MODEL_ID))
        );
        givenInferenceFields(
            Map.of(
                INDEX_1,
                List.of(
                    new InferenceFieldMetadata(SEMANTIC_FIELD_1, INFERENCE_ENDPOINT_1, new String[0], null),
                    new InferenceFieldMetadata(SEMANTIC_FIELD_2, INFERENCE_ENDPOINT_2, new String[0], null)
                ),
                INDEX_2,
                List.of(
                    new InferenceFieldMetadata(SEMANTIC_FIELD_1, INFERENCE_ENDPOINT_1, new String[0], null),
                    new InferenceFieldMetadata(SEMANTIC_FIELD_2, INFERENCE_ENDPOINT_2, new String[0], null),
                    new InferenceFieldMetadata(SEMANTIC_FIELD_3, INFERENCE_ENDPOINT_2, new String[0], null)
                )
            ),
            Set.of(),
            Set.of(INDEX_2)
        );

        XContentSource response = executeAction();

        assertThat(response.getValue(InferenceFeatureSetUsage.MODELS_FIELD), hasSize(8));
        assertStats(response, 0, new ModelStats(Metadata.ALL, TaskType.CHAT_COMPLETION, 0, EMPTY_SEMANTIC_TEXT_STATS));
        assertStats(response, 1, new ModelStats(Metadata.ALL, TaskType.COMPLETION, 0, EMPTY_SEMANTIC_TEXT_STATS));
        assertStats(response, 2, new ModelStats(Metadata.ALL, TaskType.EMBEDDING, 0, EMPTY_SEMANTIC_TEXT_STATS));
        assertStats(response, 3, new ModelStats(Metadata.ALL, TaskType.RERANK, 0, EMPTY_SEMANTIC_TEXT_STATS));
        assertStats(response, 4, new ModelStats(Metadata.ALL, TaskType.SPARSE_EMBEDDING, 0, EMPTY_SEMANTIC_TEXT_STATS));
        assertStats(response, 5, new ModelStats(Metadata.ALL, TaskType.TEXT_EMBEDDING, 2, new SemanticTextStats(2, 1, 2)));
        assertStats(response, 6, new ModelStats(EIS_SERVICE, TaskType.TEXT_EMBEDDING, 1, new SemanticTextStats(1, 1, 1)));
        assertStats(response, 7, new ModelStats(OpenAiService.NAME, TaskType.TEXT_EMBEDDING, 1, new SemanticTextStats(1, 1, 1)));
    }

    public void testFailureReturnsEmptyUsage() {
        doAnswer(invocation -> {
            ActionListener<GetInferenceModelAction.Response> listener = invocation.getArgument(2);
            listener.onFailure(new IllegalArgumentException("invalid field"));
            return Void.TYPE;
        }).when(client).execute(any(GetInferenceModelAction.class), any(), any());

        var future = new PlainActionFuture<XPackUsageFeatureResponse>();
        action.localClusterStateOperation(mock(Task.class), mock(XPackUsageRequest.class), clusterState, future);

        var usage = future.actionGet(TIMEOUT);
        var inferenceUsage = (InferenceFeatureSetUsage) usage.getUsage();
        assertThat(inferenceUsage, is(InferenceFeatureSetUsage.EMPTY));
    }

    /**
     * Regression test for <a href="https://github.com/elastic/search-team/issues/14616">#14616 issue</a>.
     * Prior to the fix, inference fields backed by task types outside {{@link TaskType#TEXT_EMBEDDING}, {@link TaskType#SPARSE_EMBEDDING}}
     * caused a NullPointerException in {@code addSemanticTextStats(Map, ModelStats)}.
     */
    public void testGivenNonTextEmbeddingTaskTypeWithSemanticTextField_SemanticTextStatsArePopulated() throws Exception {

        givenInferenceEndpoints(
            new ModelConfigurations(INFERENCE_ENDPOINT_1, TaskType.EMBEDDING, OpenAiService.NAME, mockServiceSettings(MODEL_ID_1)),
            new ModelConfigurations(INFERENCE_ENDPOINT_2, TaskType.RERANK, OpenAiService.NAME, mockServiceSettings(MODEL_ID_2))
        );

        // This is a contrived scenario, it isn't possible for a semantic text field to reference a rerank endpoint
        givenInferenceFields(
            Map.of(
                INDEX_1,
                List.of(
                    new InferenceFieldMetadata(SEMANTIC_FIELD_1, INFERENCE_ENDPOINT_1, new String[0], null),
                    new InferenceFieldMetadata(SEMANTIC_FIELD_2, INFERENCE_ENDPOINT_1, new String[0], null),
                    new InferenceFieldMetadata(SEMANTIC_FIELD_3, INFERENCE_ENDPOINT_2, new String[0], null)
                )
            )
        );

        XContentSource response = executeAction();

        assertThat(response.getValue(InferenceFeatureSetUsage.MODELS_FIELD), hasSize(8));
        assertStats(response, 0, new ModelStats(Metadata.ALL, TaskType.CHAT_COMPLETION, 0, EMPTY_SEMANTIC_TEXT_STATS));
        assertStats(response, 1, new ModelStats(Metadata.ALL, TaskType.COMPLETION, 0, EMPTY_SEMANTIC_TEXT_STATS));
        assertStats(response, 2, new ModelStats(Metadata.ALL, TaskType.EMBEDDING, 1, new SemanticTextStats(2, 1, 1)));
        assertStats(response, 3, new ModelStats(Metadata.ALL, TaskType.RERANK, 1, new SemanticTextStats(1, 1, 1)));
        assertStats(response, 4, new ModelStats(Metadata.ALL, TaskType.SPARSE_EMBEDDING, 0, EMPTY_SEMANTIC_TEXT_STATS));
        assertStats(response, 5, new ModelStats(Metadata.ALL, TaskType.TEXT_EMBEDDING, 0, EMPTY_SEMANTIC_TEXT_STATS));
        assertStats(response, 6, new ModelStats(OpenAiService.NAME, TaskType.EMBEDDING, 1, new SemanticTextStats(2, 1, 1)));
        assertStats(response, 7, new ModelStats(OpenAiService.NAME, TaskType.RERANK, 1, new SemanticTextStats(1, 1, 1)));
    }

    public void testEmbeddingTaskTypeNotReturned_whenClusterDoesNotSupportEmbedding() throws IOException {
        when(featureServiceMock.clusterHasFeature(any(), eq(EMBEDDING_TASK_TYPE))).thenReturn(false);
        givenInferenceEndpoints();

        XContentSource response = executeAction();

        assertThat(response.getValue(InferenceFeatureSetUsage.MODELS_FIELD), hasSize(5));
        assertStats(response, 0, new ModelStats(Metadata.ALL, TaskType.CHAT_COMPLETION, 0, EMPTY_SEMANTIC_TEXT_STATS));
        assertStats(response, 1, new ModelStats(Metadata.ALL, TaskType.COMPLETION, 0, EMPTY_SEMANTIC_TEXT_STATS));
        assertStats(response, 2, new ModelStats(Metadata.ALL, TaskType.RERANK, 0, EMPTY_SEMANTIC_TEXT_STATS));
        assertStats(response, 3, new ModelStats(Metadata.ALL, TaskType.SPARSE_EMBEDDING, 0, EMPTY_SEMANTIC_TEXT_STATS));
        assertStats(response, 4, new ModelStats(Metadata.ALL, TaskType.TEXT_EMBEDDING, 0, EMPTY_SEMANTIC_TEXT_STATS));
    }

    private void givenClusterState(Map<String, IndexMetadata> indices) {
        clusterState = ClusterState.builder(ClusterState.EMPTY_STATE)
            .metadata(Metadata.builder().put(ProjectMetadata.builder(ProjectId.DEFAULT).indices(indices).build()))
            .build();
    }

    private static ServiceSettings mockServiceSettings(String modelId) {
        ServiceSettings serviceSettings = mock(ServiceSettings.class);
        when(serviceSettings.modelId()).thenReturn(modelId);
        return serviceSettings;
    }

    private void givenInferenceEndpoints(ModelConfigurations... endpoints) {
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            var listener = (ActionListener<GetInferenceModelAction.Response>) invocation.getArguments()[2];
            listener.onResponse(new GetInferenceModelAction.Response(Arrays.asList(endpoints)));
            return Void.TYPE;
        }).when(client).execute(any(GetInferenceModelAction.class), any(), any());
    }

    private void givenInferenceFields(Map<String, List<InferenceFieldMetadata>> inferenceFieldsByIndex) {
        givenInferenceFields(inferenceFieldsByIndex, Set.of(), Set.of());
    }

    private void givenInferenceFields(
        Map<String, List<InferenceFieldMetadata>> inferenceFieldsByIndex,
        Set<String> systemIndices,
        Set<String> hiddenIndices
    ) {
        Map<String, IndexMetadata> indices = new HashMap<>();
        for (Map.Entry<String, List<InferenceFieldMetadata>> entry : inferenceFieldsByIndex.entrySet()) {
            String index = entry.getKey();
            IndexMetadata.Builder indexMetadata = IndexMetadata.builder(index)
                .settings(
                    ESTestCase.settings(IndexVersion.current())
                        .put(INDEX_HIDDEN_SETTING.getKey(), hiddenIndices.contains(index) ? "true" : "false")
                )
                .numberOfShards(randomIntBetween(1, 5))
                .system(systemIndices.contains(index))
                .numberOfReplicas(1);
            entry.getValue().forEach(indexMetadata::putInferenceField);
            indices.put(index, indexMetadata.build());
        }
        givenClusterState(indices);
    }

    private XContentSource executeAction() throws IOException {
        PlainActionFuture<XPackUsageFeatureResponse> future = new PlainActionFuture<>();
        action.localClusterStateOperation(mock(Task.class), mock(XPackUsageRequest.class), clusterState, future);

        BytesStreamOutput out = new BytesStreamOutput();
        future.actionGet(ESTestCase.TEST_REQUEST_TIMEOUT).getUsage().writeTo(out);
        XPackFeatureUsage usage = new InferenceFeatureSetUsage(out.bytes().streamInput());

        assertThat(usage.name(), is(XPackField.INFERENCE));
        assertTrue(usage.enabled());
        assertTrue(usage.available());

        XContentBuilder builder = XContentFactory.jsonBuilder();
        usage.toXContent(builder, ToXContent.EMPTY_PARAMS);
        return new XContentSource(builder);
    }

    private void givenDefaultEndpoints(String... ids) {
        for (String id : ids) {
            when(modelRegistry.containsPreconfiguredInferenceEndpointId(id)).thenReturn(true);
        }
    }

    private static void assertStats(XContentSource source, int index, ModelStats stats) {
        assertThat(source.getValue(InferenceFeatureSetUsage.MODELS_FIELD + "." + index + ".service"), is(stats.service()));
        assertThat(source.getValue(InferenceFeatureSetUsage.MODELS_FIELD + "." + index + ".task_type"), is(stats.taskType().name()));
        assertThat(
            ((Integer) source.getValue(InferenceFeatureSetUsage.MODELS_FIELD + "." + index + ".count")).longValue(),
            equalTo(stats.count())
        );
        if (stats.semanticTextStats().isEmpty()) {
            assertThat(source.getValue(InferenceFeatureSetUsage.MODELS_FIELD + "." + index + ".semantic_text"), is(nullValue()));
        } else {
            assertThat(
                ((Integer) source.getValue(InferenceFeatureSetUsage.MODELS_FIELD + "." + index + ".semantic_text.field_count")).longValue(),
                equalTo(stats.semanticTextStats().getFieldCount())
            );
            assertThat(
                ((Integer) source.getValue(InferenceFeatureSetUsage.MODELS_FIELD + "." + index + ".semantic_text.indices_count"))
                    .longValue(),
                equalTo(stats.semanticTextStats().getIndicesCount())
            );
            assertThat(
                ((Integer) source.getValue(InferenceFeatureSetUsage.MODELS_FIELD + "." + index + ".semantic_text.inference_id_count"))
                    .longValue(),
                equalTo(stats.semanticTextStats().getInferenceIdCount())
            );
        }
    }
}
