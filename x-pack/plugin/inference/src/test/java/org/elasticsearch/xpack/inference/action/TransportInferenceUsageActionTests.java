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
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_HIDDEN_SETTING;
import static org.elasticsearch.xpack.inference.Utils.TIMEOUT;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.Is.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransportInferenceUsageActionTests extends ESTestCase {

    private static final SemanticTextStats EMPTY_SEMANTIC_TEXT_STATS = new SemanticTextStats();

    private Client client;
    private ModelRegistry modelRegistry;
    private ClusterState clusterState;
    private TransportInferenceUsageAction action;

    @Before
    public void init() {
        client = mock(Client.class);
        ThreadPool threadPool = new TestThreadPool("test");
        when(client.threadPool()).thenReturn(threadPool);

        modelRegistry = mock(ModelRegistry.class);

        givenClusterState(Map.of());

        TransportService transportService = MockUtils.setupTransportServiceWithThreadpoolExecutor(mock(ThreadPool.class));

        action = new TransportInferenceUsageAction(
            transportService,
            mock(ClusterService.class),
            mock(ThreadPool.class),
            mock(ActionFilters.class),
            modelRegistry,
            client
        );
    }

    @After
    public void close() {
        client.threadPool().shutdown();
    }

    public void testGivenServices_NoInferenceFields() throws Exception {
        givenInferenceEndpoints(
            new ModelConfigurations("model-001", TaskType.TEXT_EMBEDDING, "openai", mockServiceSettings("model-id-001")),
            new ModelConfigurations("model-002", TaskType.TEXT_EMBEDDING, "openai", mockServiceSettings("model-id-002")),
            new ModelConfigurations("model-003", TaskType.SPARSE_EMBEDDING, "hugging_face_elser", mockServiceSettings("model-id-003")),
            new ModelConfigurations("model-004", TaskType.TEXT_EMBEDDING, "openai", mockServiceSettings("model-id-004")),
            new ModelConfigurations("model-005", TaskType.SPARSE_EMBEDDING, "openai", mockServiceSettings("model-id-005")),
            new ModelConfigurations("model-006", TaskType.SPARSE_EMBEDDING, "hugging_face_elser", mockServiceSettings("model-id-006"))
        );

        XContentSource response = executeAction();

        assertThat(response.getValue("models"), hasSize(8));
        assertStats(response, 0, new ModelStats("_all", TaskType.CHAT_COMPLETION, 0, null));
        assertStats(response, 1, new ModelStats("_all", TaskType.COMPLETION, 0, null));
        assertStats(response, 2, new ModelStats("_all", TaskType.RERANK, 0, null));
        assertStats(response, 3, new ModelStats("_all", TaskType.SPARSE_EMBEDDING, 3, EMPTY_SEMANTIC_TEXT_STATS));
        assertStats(response, 4, new ModelStats("_all", TaskType.TEXT_EMBEDDING, 3, EMPTY_SEMANTIC_TEXT_STATS));
        assertStats(response, 5, new ModelStats("hugging_face_elser", TaskType.SPARSE_EMBEDDING, 2, EMPTY_SEMANTIC_TEXT_STATS));
        assertStats(response, 6, new ModelStats("openai", TaskType.SPARSE_EMBEDDING, 1, EMPTY_SEMANTIC_TEXT_STATS));
        assertStats(response, 7, new ModelStats("openai", TaskType.TEXT_EMBEDDING, 3, EMPTY_SEMANTIC_TEXT_STATS));
    }

    public void testGivenFieldRefersToMissingInferenceEndpoint() throws Exception {
        givenInferenceEndpoints();
        givenInferenceFields(Map.of("index_1", List.of(new InferenceFieldMetadata("semantic-1", "endpoint-001", new String[0], null))));

        XContentSource response = executeAction();

        assertThat(response.getValue("models"), hasSize(5));
        assertStats(response, 0, new ModelStats("_all", TaskType.CHAT_COMPLETION, 0, null));
        assertStats(response, 1, new ModelStats("_all", TaskType.COMPLETION, 0, null));
        assertStats(response, 2, new ModelStats("_all", TaskType.RERANK, 0, null));
        assertStats(response, 3, new ModelStats("_all", TaskType.SPARSE_EMBEDDING, 0, EMPTY_SEMANTIC_TEXT_STATS));
        assertStats(response, 4, new ModelStats("_all", TaskType.TEXT_EMBEDDING, 0, EMPTY_SEMANTIC_TEXT_STATS));
    }

    public void testGivenVariousServicesAndInferenceFields() throws Exception {
        givenInferenceEndpoints(
            new ModelConfigurations("endpoint-001", TaskType.TEXT_EMBEDDING, "openai", mockServiceSettings("model-id-001")),
            new ModelConfigurations("endpoint-002", TaskType.TEXT_EMBEDDING, "openai", mockServiceSettings("model-id-002")),
            new ModelConfigurations("endpoint-003", TaskType.SPARSE_EMBEDDING, "openai", mockServiceSettings("model-id-003")),
            new ModelConfigurations("endpoint-004", TaskType.SPARSE_EMBEDDING, "openai", mockServiceSettings("model-id-004")),
            new ModelConfigurations("endpoint-005", TaskType.TEXT_EMBEDDING, "eis", mockServiceSettings("model-id-005")),
            new ModelConfigurations("endpoint-006", TaskType.TEXT_EMBEDDING, "eis", mockServiceSettings("model-id-006")),
            new ModelConfigurations("endpoint-007", TaskType.TEXT_EMBEDDING, "eis", mockServiceSettings("model-id-007")) // unused
        );

        givenInferenceFields(
            Map.of(
                "index_1",
                List.of(
                    new InferenceFieldMetadata("semantic-1", "endpoint-001", new String[0], null),
                    new InferenceFieldMetadata("semantic-2", "endpoint-001", new String[0], null),
                    new InferenceFieldMetadata("semantic-3", "endpoint-002", new String[0], null),
                    new InferenceFieldMetadata("semantic-4", "endpoint-002", new String[0], null),
                    new InferenceFieldMetadata("semantic-5", "endpoint-002", new String[0], null),
                    new InferenceFieldMetadata("semantic-6", "endpoint-003", new String[0], null),
                    new InferenceFieldMetadata("semantic-7", "endpoint-004", new String[0], null),
                    new InferenceFieldMetadata("semantic-8", "endpoint-005", new String[0], null)
                ),
                "index_2",
                List.of(
                    new InferenceFieldMetadata("semantic-1", "endpoint-001", new String[0], null),
                    new InferenceFieldMetadata("semantic-2", "endpoint-003", new String[0], null),
                    new InferenceFieldMetadata("semantic-3", "endpoint-003", new String[0], null),
                    new InferenceFieldMetadata("semantic-4", "endpoint-004", new String[0], null),
                    new InferenceFieldMetadata("semantic-5", "endpoint-004", new String[0], null),
                    new InferenceFieldMetadata("semantic-6", "endpoint-005", new String[0], null)
                ),
                "index_3",
                List.of(new InferenceFieldMetadata("semantic-1", "endpoint-006", new String[0], null))
            )
        );

        XContentSource response = executeAction();

        assertThat(response.getValue("models"), hasSize(8));
        assertStats(response, 0, new ModelStats("_all", TaskType.CHAT_COMPLETION, 0, null));
        assertStats(response, 1, new ModelStats("_all", TaskType.COMPLETION, 0, null));
        assertStats(response, 2, new ModelStats("_all", TaskType.RERANK, 0, null));
        assertStats(response, 3, new ModelStats("_all", TaskType.SPARSE_EMBEDDING, 2, new SemanticTextStats(6, 2, 2)));
        assertStats(response, 4, new ModelStats("_all", TaskType.TEXT_EMBEDDING, 5, new SemanticTextStats(9, 3, 4)));
        assertStats(response, 5, new ModelStats("eis", TaskType.TEXT_EMBEDDING, 3, new SemanticTextStats(3, 3, 2)));
        assertStats(response, 6, new ModelStats("openai", TaskType.SPARSE_EMBEDDING, 2, new SemanticTextStats(6, 2, 2)));
        assertStats(response, 7, new ModelStats("openai", TaskType.TEXT_EMBEDDING, 2, new SemanticTextStats(6, 2, 2)));
    }

    public void testGivenServices_InferenceFieldsReferencingDefaultModels() throws Exception {
        givenInferenceEndpoints(
            new ModelConfigurations(".endpoint-001", TaskType.TEXT_EMBEDDING, "eis", mockServiceSettings(".model-id-001")),
            new ModelConfigurations(".endpoint-002", TaskType.SPARSE_EMBEDDING, "eis", mockServiceSettings(".model-id-002")),
            new ModelConfigurations("endpoint-003", TaskType.TEXT_EMBEDDING, "openai", mockServiceSettings("model-id-003")),
            new ModelConfigurations(".endpoint-004", TaskType.SPARSE_EMBEDDING, "openai", mockServiceSettings("model-id-004")),
            new ModelConfigurations("endpoint-005", TaskType.TEXT_EMBEDDING, "eis", mockServiceSettings(".model-id-001"))
        );

        givenDefaultEndpoints(".endpoint-001", ".endpoint-002", ".endpoint-004");

        givenInferenceFields(
            Map.of(
                "index_1",
                List.of(
                    new InferenceFieldMetadata("semantic-1", ".endpoint-001", new String[0], null),
                    new InferenceFieldMetadata("semantic-2", ".endpoint-001", new String[0], null),
                    new InferenceFieldMetadata("semantic-3", ".endpoint-002", new String[0], null),
                    new InferenceFieldMetadata("semantic-4", ".endpoint-004", new String[0], null),
                    new InferenceFieldMetadata("semantic-5", "endpoint-005", new String[0], null)
                ),
                "index_2",
                List.of(
                    new InferenceFieldMetadata("semantic-1", ".endpoint-001", new String[0], null),
                    new InferenceFieldMetadata("semantic-2", ".endpoint-004", new String[0], null),
                    new InferenceFieldMetadata("semantic-3", ".endpoint-004", new String[0], null),
                    new InferenceFieldMetadata("semantic-4", "endpoint-005", new String[0], null),
                    new InferenceFieldMetadata("semantic-5", "endpoint-005", new String[0], null)
                )
            )
        );

        XContentSource response = executeAction();

        assertThat(response.getValue("models"), hasSize(12));
        assertStats(response, 0, new ModelStats("_all", TaskType.CHAT_COMPLETION, 0, null));
        assertStats(response, 1, new ModelStats("_all", TaskType.COMPLETION, 0, null));
        assertStats(response, 2, new ModelStats("_all", TaskType.RERANK, 0, null));
        assertStats(response, 3, new ModelStats("_all", TaskType.SPARSE_EMBEDDING, 2, new SemanticTextStats(4, 2, 2)));
        assertStats(response, 4, new ModelStats("_all", TaskType.TEXT_EMBEDDING, 3, new SemanticTextStats(6, 2, 2)));
        assertStats(response, 5, new ModelStats("_eis__model-id-001", TaskType.TEXT_EMBEDDING, 2, new SemanticTextStats(6, 2, 2)));
        assertStats(response, 6, new ModelStats("_eis__model-id-002", TaskType.SPARSE_EMBEDDING, 1, new SemanticTextStats(1, 1, 1)));
        assertStats(response, 7, new ModelStats("_openai_model-id-004", TaskType.SPARSE_EMBEDDING, 1, new SemanticTextStats(3, 2, 1)));
        assertStats(response, 8, new ModelStats("eis", TaskType.SPARSE_EMBEDDING, 1, new SemanticTextStats(1, 1, 1)));
        assertStats(response, 9, new ModelStats("eis", TaskType.TEXT_EMBEDDING, 2, new SemanticTextStats(6, 2, 2)));
        assertStats(response, 10, new ModelStats("openai", TaskType.SPARSE_EMBEDDING, 1, new SemanticTextStats(3, 2, 1)));
        assertStats(response, 11, new ModelStats("openai", TaskType.TEXT_EMBEDDING, 1, EMPTY_SEMANTIC_TEXT_STATS));
    }

    public void testGivenDefaultModelWithLinuxSuffix() throws Exception {
        givenInferenceEndpoints(
            new ModelConfigurations(".endpoint-001", TaskType.TEXT_EMBEDDING, "eis", mockServiceSettings(".model-id-001_linux-x86_64")),
            new ModelConfigurations("endpoint-002", TaskType.TEXT_EMBEDDING, "eis", mockServiceSettings(".model-id-001_linux-x86_64"))
        );

        givenDefaultEndpoints(".endpoint-001");

        givenInferenceFields(
            Map.of(
                "index_1",
                List.of(
                    new InferenceFieldMetadata("semantic-1", ".endpoint-001", new String[0], null),
                    new InferenceFieldMetadata("semantic-2", ".endpoint-001", new String[0], null),
                    new InferenceFieldMetadata("semantic-3", "endpoint-002", new String[0], null)
                ),
                "index_2",
                List.of(
                    new InferenceFieldMetadata("semantic-1", ".endpoint-001", new String[0], null),
                    new InferenceFieldMetadata("semantic-2", ".endpoint-001", new String[0], null),
                    new InferenceFieldMetadata("semantic-3", "endpoint-002", new String[0], null)
                )
            )
        );

        XContentSource response = executeAction();

        assertThat(response.getValue("models"), hasSize(7));
        assertStats(response, 0, new ModelStats("_all", TaskType.CHAT_COMPLETION, 0, null));
        assertStats(response, 1, new ModelStats("_all", TaskType.COMPLETION, 0, null));
        assertStats(response, 2, new ModelStats("_all", TaskType.RERANK, 0, null));
        assertStats(response, 3, new ModelStats("_all", TaskType.SPARSE_EMBEDDING, 0, EMPTY_SEMANTIC_TEXT_STATS));
        assertStats(response, 4, new ModelStats("_all", TaskType.TEXT_EMBEDDING, 2, new SemanticTextStats(6, 2, 2)));
        assertStats(response, 5, new ModelStats("_eis__model-id-001", TaskType.TEXT_EMBEDDING, 2, new SemanticTextStats(6, 2, 2)));
        assertStats(response, 6, new ModelStats("eis", TaskType.TEXT_EMBEDDING, 2, new SemanticTextStats(6, 2, 2)));
    }

    public void testGivenSameDefaultModelWithAndWithoutLinuxSuffix() throws Exception {
        givenInferenceEndpoints(
            new ModelConfigurations(".endpoint-001", TaskType.TEXT_EMBEDDING, "eis", mockServiceSettings(".model-id-001_linux-x86_64")),
            new ModelConfigurations("endpoint-002", TaskType.TEXT_EMBEDDING, "eis", mockServiceSettings(".model-id-001"))
        );

        givenDefaultEndpoints(".endpoint-001");

        givenInferenceFields(
            Map.of(
                "index_1",
                List.of(
                    new InferenceFieldMetadata("semantic-1", ".endpoint-001", new String[0], null),
                    new InferenceFieldMetadata("semantic-2", ".endpoint-001", new String[0], null),
                    new InferenceFieldMetadata("semantic-3", "endpoint-002", new String[0], null)
                ),
                "index_2",
                List.of(
                    new InferenceFieldMetadata("semantic-1", ".endpoint-001", new String[0], null),
                    new InferenceFieldMetadata("semantic-2", ".endpoint-001", new String[0], null),
                    new InferenceFieldMetadata("semantic-3", "endpoint-002", new String[0], null)
                )
            )
        );

        XContentSource response = executeAction();

        assertThat(response.getValue("models"), hasSize(7));
        assertStats(response, 0, new ModelStats("_all", TaskType.CHAT_COMPLETION, 0, null));
        assertStats(response, 1, new ModelStats("_all", TaskType.COMPLETION, 0, null));
        assertStats(response, 2, new ModelStats("_all", TaskType.RERANK, 0, null));
        assertStats(response, 3, new ModelStats("_all", TaskType.SPARSE_EMBEDDING, 0, EMPTY_SEMANTIC_TEXT_STATS));
        assertStats(response, 4, new ModelStats("_all", TaskType.TEXT_EMBEDDING, 2, new SemanticTextStats(6, 2, 2)));
        assertStats(response, 5, new ModelStats("_eis__model-id-001", TaskType.TEXT_EMBEDDING, 2, new SemanticTextStats(6, 2, 2)));
        assertStats(response, 6, new ModelStats("eis", TaskType.TEXT_EMBEDDING, 2, new SemanticTextStats(6, 2, 2)));
    }

    public void testGivenExternalServiceModelIsNull() throws Exception {
        givenInferenceEndpoints(new ModelConfigurations("endpoint-001", TaskType.TEXT_EMBEDDING, "openai", mockServiceSettings(null)));
        givenInferenceFields(Map.of("index_1", List.of(new InferenceFieldMetadata("semantic", "endpoint-001", new String[0], null))));

        XContentSource response = executeAction();

        assertThat(response.getValue("models"), hasSize(6));
        assertStats(response, 0, new ModelStats("_all", TaskType.CHAT_COMPLETION, 0, null));
        assertStats(response, 1, new ModelStats("_all", TaskType.COMPLETION, 0, null));
        assertStats(response, 2, new ModelStats("_all", TaskType.RERANK, 0, null));
        assertStats(response, 3, new ModelStats("_all", TaskType.SPARSE_EMBEDDING, 0, EMPTY_SEMANTIC_TEXT_STATS));
        assertStats(response, 4, new ModelStats("_all", TaskType.TEXT_EMBEDDING, 1, new SemanticTextStats(1, 1, 1)));
        assertStats(response, 5, new ModelStats("openai", TaskType.TEXT_EMBEDDING, 1, new SemanticTextStats(1, 1, 1)));
    }

    public void testGivenDuplicateServices() throws Exception {
        givenInferenceEndpoints(
            new ModelConfigurations("endpoint-001", TaskType.TEXT_EMBEDDING, "openai", mockServiceSettings("some-model")),
            new ModelConfigurations("endpoint-002", TaskType.TEXT_EMBEDDING, "openai", mockServiceSettings("some-model"))
        );
        givenInferenceFields(
            Map.of(
                "index_1",
                List.of(
                    new InferenceFieldMetadata("semantic-1", "endpoint-001", new String[0], null),
                    new InferenceFieldMetadata("semantic-2", "endpoint-002", new String[0], null)
                ),
                "index_2",
                List.of(
                    new InferenceFieldMetadata("semantic-1", "endpoint-001", new String[0], null),
                    new InferenceFieldMetadata("semantic-2", "endpoint-002", new String[0], null),
                    new InferenceFieldMetadata("semantic-3", "endpoint-002", new String[0], null)
                )
            )
        );

        XContentSource response = executeAction();

        assertThat(response.getValue("models"), hasSize(6));
        assertStats(response, 0, new ModelStats("_all", TaskType.CHAT_COMPLETION, 0, null));
        assertStats(response, 1, new ModelStats("_all", TaskType.COMPLETION, 0, null));
        assertStats(response, 2, new ModelStats("_all", TaskType.RERANK, 0, null));
        assertStats(response, 3, new ModelStats("_all", TaskType.SPARSE_EMBEDDING, 0, EMPTY_SEMANTIC_TEXT_STATS));
        assertStats(response, 4, new ModelStats("_all", TaskType.TEXT_EMBEDDING, 2, new SemanticTextStats(5, 2, 2)));
        assertStats(response, 5, new ModelStats("openai", TaskType.TEXT_EMBEDDING, 2, new SemanticTextStats(5, 2, 2)));
    }

    public void testShouldExcludeSystemIndexFields() throws Exception {
        givenInferenceEndpoints(
            new ModelConfigurations("endpoint-001", TaskType.TEXT_EMBEDDING, "eis", mockServiceSettings("some-model")),
            new ModelConfigurations("endpoint-002", TaskType.TEXT_EMBEDDING, "openai", mockServiceSettings("some-model"))
        );
        givenInferenceFields(
            Map.of(
                "index_1",
                List.of(
                    new InferenceFieldMetadata("semantic-1", "endpoint-001", new String[0], null),
                    new InferenceFieldMetadata("semantic-2", "endpoint-002", new String[0], null)
                ),
                "index_2",
                List.of(
                    new InferenceFieldMetadata("semantic-1", "endpoint-001", new String[0], null),
                    new InferenceFieldMetadata("semantic-2", "endpoint-002", new String[0], null),
                    new InferenceFieldMetadata("semantic-3", "endpoint-002", new String[0], null)
                )
            ),
            Set.of("index_2"),
            Set.of()
        );

        XContentSource response = executeAction();

        assertThat(response.getValue("models"), hasSize(7));
        assertStats(response, 0, new ModelStats("_all", TaskType.CHAT_COMPLETION, 0, null));
        assertStats(response, 1, new ModelStats("_all", TaskType.COMPLETION, 0, null));
        assertStats(response, 2, new ModelStats("_all", TaskType.RERANK, 0, null));
        assertStats(response, 3, new ModelStats("_all", TaskType.SPARSE_EMBEDDING, 0, EMPTY_SEMANTIC_TEXT_STATS));
        assertStats(response, 4, new ModelStats("_all", TaskType.TEXT_EMBEDDING, 2, new SemanticTextStats(2, 1, 2)));
        assertStats(response, 5, new ModelStats("eis", TaskType.TEXT_EMBEDDING, 1, new SemanticTextStats(1, 1, 1)));
        assertStats(response, 6, new ModelStats("openai", TaskType.TEXT_EMBEDDING, 1, new SemanticTextStats(1, 1, 1)));
    }

    public void testShouldExcludeHiddenIndexFields() throws Exception {
        givenInferenceEndpoints(
            new ModelConfigurations("endpoint-001", TaskType.TEXT_EMBEDDING, "eis", mockServiceSettings("some-model")),
            new ModelConfigurations("endpoint-002", TaskType.TEXT_EMBEDDING, "openai", mockServiceSettings("some-model"))
        );
        givenInferenceFields(
            Map.of(
                "index_1",
                List.of(
                    new InferenceFieldMetadata("semantic-1", "endpoint-001", new String[0], null),
                    new InferenceFieldMetadata("semantic-2", "endpoint-002", new String[0], null)
                ),
                "index_2",
                List.of(
                    new InferenceFieldMetadata("semantic-1", "endpoint-001", new String[0], null),
                    new InferenceFieldMetadata("semantic-2", "endpoint-002", new String[0], null),
                    new InferenceFieldMetadata("semantic-3", "endpoint-002", new String[0], null)
                )
            ),
            Set.of(),
            Set.of("index_2")
        );

        XContentSource response = executeAction();

        assertThat(response.getValue("models"), hasSize(7));
        assertStats(response, 0, new ModelStats("_all", TaskType.CHAT_COMPLETION, 0, null));
        assertStats(response, 1, new ModelStats("_all", TaskType.COMPLETION, 0, null));
        assertStats(response, 2, new ModelStats("_all", TaskType.RERANK, 0, null));
        assertStats(response, 3, new ModelStats("_all", TaskType.SPARSE_EMBEDDING, 0, EMPTY_SEMANTIC_TEXT_STATS));
        assertStats(response, 4, new ModelStats("_all", TaskType.TEXT_EMBEDDING, 2, new SemanticTextStats(2, 1, 2)));
        assertStats(response, 5, new ModelStats("eis", TaskType.TEXT_EMBEDDING, 1, new SemanticTextStats(1, 1, 1)));
        assertStats(response, 6, new ModelStats("openai", TaskType.TEXT_EMBEDDING, 1, new SemanticTextStats(1, 1, 1)));
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

    private XContentSource executeAction() throws ExecutionException, InterruptedException, IOException {
        PlainActionFuture<XPackUsageFeatureResponse> future = new PlainActionFuture<>();
        action.localClusterStateOperation(mock(Task.class), mock(XPackUsageRequest.class), clusterState, future);

        BytesStreamOutput out = new BytesStreamOutput();
        future.get().getUsage().writeTo(out);
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
        assertThat(source.getValue("models." + index + ".service"), is(stats.service()));
        assertThat(source.getValue("models." + index + ".task_type"), is(stats.taskType().name()));
        assertThat(((Integer) source.getValue("models." + index + ".count")).longValue(), equalTo(stats.count()));
        if (stats.semanticTextStats() == null) {
            assertThat(source.getValue("models." + index + ".semantic_text"), is(nullValue()));
        } else {
            assertThat(
                ((Integer) source.getValue("models." + index + ".semantic_text.field_count")).longValue(),
                equalTo(stats.semanticTextStats().getFieldCount())
            );
            assertThat(
                ((Integer) source.getValue("models." + index + ".semantic_text.indices_count")).longValue(),
                equalTo(stats.semanticTextStats().getIndicesCount())
            );
            assertThat(
                ((Integer) source.getValue("models." + index + ".semantic_text.inference_id_count")).longValue(),
                equalTo(stats.semanticTextStats().getInferenceIdCount())
            );
        }
    }
}
