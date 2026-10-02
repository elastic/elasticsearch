/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.integration;

import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.RemoteClusterClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.inference.DataFormat;
import org.elasticsearch.inference.DataType;
import org.elasticsearch.inference.InferenceString;
import org.elasticsearch.inference.InferenceStringGroup;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.license.LicenseSettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.AbstractMultiClustersTestCase;
import org.elasticsearch.transport.RemoteClusterService;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.inference.action.GetInferenceFieldsInternalAction;
import org.elasticsearch.xpack.core.ml.inference.results.MlDenseEmbeddingResults;
import org.elasticsearch.xpack.core.ml.inference.results.TextExpansionResults;
import org.elasticsearch.xpack.inference.FakeMlPlugin;
import org.elasticsearch.xpack.inference.LocalStateInferencePlugin;
import org.elasticsearch.xpack.inference.mock.TestInferenceServicePlugin;
import org.junit.Before;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.inference.integration.GetInferenceFieldsIT.assertInferenceFieldsMap;
import static org.elasticsearch.xpack.inference.integration.GetInferenceFieldsIT.assertInferenceResultsMap;
import static org.elasticsearch.xpack.inference.integration.GetInferenceFieldsIT.generateDefaultWeightFieldMap;
import static org.elasticsearch.xpack.inference.integration.IntegrationTestUtils.createInferenceEndpoint;
import static org.elasticsearch.xpack.inference.integration.IntegrationTestUtils.generateSemanticTextMapping;
import static org.hamcrest.Matchers.containsString;

public class GetInferenceFieldsCrossClusterIT extends AbstractMultiClustersTestCase {
    private static final String REMOTE_CLUSTER = "cluster_a";
    private static final String INDEX_NAME = "test-index";
    private static final String INFERENCE_FIELD = "test-inference-field";
    private static final String INFERENCE_ID = "test-inference-id";
    private static final Map<String, Object> INFERENCE_ENDPOINT_SERVICE_SETTINGS = Map.of("model", "my_model", "api_key", "my_api_key");

    private static final String EMBEDDING_INDEX_NAME = "test-embedding-index";
    private static final String EMBEDDING_INFERENCE_FIELD = "test-embedding-field";
    private static final String EMBEDDING_INFERENCE_ID = "test-embedding-id";
    private static final Map<String, Object> EMBEDDING_ENDPOINT_SERVICE_SETTINGS = Map.of(
        "model",
        "my_model",
        "dimensions",
        256,
        "similarity",
        "cosine",
        "api_key",
        "my_api_key"
    );

    @Override
    protected List<String> remoteClusterAlias() {
        return List.of(REMOTE_CLUSTER);
    }

    @Override
    protected Map<String, Boolean> skipUnavailableForRemoteClusters() {
        return Map.of(REMOTE_CLUSTER, DEFAULT_SKIP_UNAVAILABLE);
    }

    @Override
    protected Settings nodeSettings() {
        return Settings.builder().put(LicenseSettings.SELF_GENERATED_LICENSE_TYPE.getKey(), "trial").build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins(String clusterAlias) {
        return List.of(LocalStateInferencePlugin.class, TestInferenceServicePlugin.class, FakeMlPlugin.class);
    }

    @Before
    public void configureClusters() throws Exception {
        setupTwoClusters();
    }

    public void testRemoteIndex() {
        Consumer<GetInferenceFieldsInternalAction.Request> assertFailedRequest = r -> {
            IllegalArgumentException e = assertThrows(
                IllegalArgumentException.class,
                () -> client().execute(GetInferenceFieldsInternalAction.INSTANCE, r).actionGet(TEST_REQUEST_TIMEOUT)
            );
            assertThat(e.getMessage(), containsString("GetInferenceFieldsInternalAction does not support remote indices"));
        };

        var concreteIndexRequest = new GetInferenceFieldsInternalAction.Request(
            new String[] { REMOTE_CLUSTER + ":test-index" },
            Map.of(),
            false,
            false,
            "foo"
        );
        assertFailedRequest.accept(concreteIndexRequest);

        var wildcardIndexRequest = new GetInferenceFieldsInternalAction.Request(
            new String[] { REMOTE_CLUSTER + ":*" },
            Map.of(),
            false,
            false,
            "foo"
        );
        assertFailedRequest.accept(wildcardIndexRequest);

        var wildcardClusterAndIndexRequest = new GetInferenceFieldsInternalAction.Request(
            new String[] { "*:*" },
            Map.of(),
            false,
            false,
            "foo"
        );
        assertFailedRequest.accept(wildcardClusterAndIndexRequest);
    }

    public void testRemoteClusterAction() {
        RemoteClusterClient remoteClusterClient = client().getRemoteClusterClient(
            REMOTE_CLUSTER,
            EsExecutors.DIRECT_EXECUTOR_SERVICE,
            RemoteClusterService.DisconnectedStrategy.RECONNECT_IF_DISCONNECTED
        );

        var request = new GetInferenceFieldsInternalAction.Request(
            new String[] { INDEX_NAME },
            generateDefaultWeightFieldMap(Set.of(INFERENCE_FIELD)),
            false,
            false,
            "foo"
        );
        PlainActionFuture<GetInferenceFieldsInternalAction.Response> future = new PlainActionFuture<>();
        remoteClusterClient.execute(GetInferenceFieldsInternalAction.REMOTE_TYPE, request, future);

        var response = future.actionGet(TEST_REQUEST_TIMEOUT);
        assertInferenceFieldsMap(
            response.getInferenceFieldsMap(),
            Map.of(INDEX_NAME, Set.of(new GetInferenceFieldsIT.InferenceFieldWithTestMetadata(INFERENCE_FIELD, INFERENCE_ID, 1.0f)))
        );
        assertInferenceResultsMap(response.getInferenceResultsMap(), Map.of(INFERENCE_ID, TextExpansionResults.class));
    }

    public void testRemoteClusterActionWithEmbeddingTaskType() {
        RemoteClusterClient remoteClusterClient = client().getRemoteClusterClient(
            REMOTE_CLUSTER,
            EsExecutors.DIRECT_EXECUTOR_SERVICE,
            RemoteClusterService.DisconnectedStrategy.RECONNECT_IF_DISCONNECTED
        );

        var request = new GetInferenceFieldsInternalAction.Request(
            new String[] { EMBEDDING_INDEX_NAME },
            generateDefaultWeightFieldMap(Set.of(EMBEDDING_INFERENCE_FIELD)),
            false,
            false,
            "foo"
        );
        PlainActionFuture<GetInferenceFieldsInternalAction.Response> future = new PlainActionFuture<>();
        remoteClusterClient.execute(GetInferenceFieldsInternalAction.REMOTE_TYPE, request, future);

        var response = future.actionGet(TEST_REQUEST_TIMEOUT);
        assertInferenceFieldsMap(
            response.getInferenceFieldsMap(),
            Map.of(
                EMBEDDING_INDEX_NAME,
                Set.of(new GetInferenceFieldsIT.InferenceFieldWithTestMetadata(EMBEDDING_INFERENCE_FIELD, EMBEDDING_INFERENCE_ID, 1.0f))
            )
        );
        assertInferenceResultsMap(response.getInferenceResultsMap(), Map.of(EMBEDDING_INFERENCE_ID, MlDenseEmbeddingResults.class));
    }

    public void testRemoteClusterActionWithImageEmbeddingTaskType() {
        RemoteClusterClient remoteClusterClient = client().getRemoteClusterClient(
            REMOTE_CLUSTER,
            EsExecutors.DIRECT_EXECUTOR_SERVICE,
            RemoteClusterService.DisconnectedStrategy.RECONNECT_IF_DISCONNECTED
        );

        var imageInput = new InferenceStringGroup(
            new InferenceString(DataType.IMAGE, DataFormat.BASE64, "data:image/jpeg;base64,aGVsbG8=")
        );
        var request = new GetInferenceFieldsInternalAction.Request(
            new String[] { EMBEDDING_INDEX_NAME },
            generateDefaultWeightFieldMap(Set.of(EMBEDDING_INFERENCE_FIELD)),
            false,
            false,
            imageInput,
            null
        );
        PlainActionFuture<GetInferenceFieldsInternalAction.Response> future = new PlainActionFuture<>();
        remoteClusterClient.execute(GetInferenceFieldsInternalAction.REMOTE_TYPE, request, future);

        var response = future.actionGet(TEST_REQUEST_TIMEOUT);
        assertInferenceFieldsMap(
            response.getInferenceFieldsMap(),
            Map.of(
                EMBEDDING_INDEX_NAME,
                Set.of(new GetInferenceFieldsIT.InferenceFieldWithTestMetadata(EMBEDDING_INFERENCE_FIELD, EMBEDDING_INFERENCE_ID, 1.0f))
            )
        );
        assertInferenceResultsMap(response.getInferenceResultsMap(), Map.of(EMBEDDING_INFERENCE_ID, MlDenseEmbeddingResults.class));
    }

    private void setupTwoClusters() throws IOException {
        setupCluster(LOCAL_CLUSTER);
        setupCluster(REMOTE_CLUSTER);
    }

    private void setupCluster(String clusterAlias) throws IOException {
        final Client client = client(clusterAlias);

        createInferenceEndpoint(client, TaskType.SPARSE_EMBEDDING, INFERENCE_ID, INFERENCE_ENDPOINT_SERVICE_SETTINGS);

        int dataNodeCount = cluster(clusterAlias).numDataNodes();
        XContentBuilder mappings = generateSemanticTextMapping(Map.of(INFERENCE_FIELD, INFERENCE_ID));
        Settings indexSettings = indexSettings(randomIntBetween(1, dataNodeCount), 0).build();
        assertAcked(client.admin().indices().prepareCreate(INDEX_NAME).setSettings(indexSettings).setMapping(mappings));

        createInferenceEndpoint(client, TaskType.EMBEDDING, EMBEDDING_INFERENCE_ID, EMBEDDING_ENDPOINT_SERVICE_SETTINGS);

        XContentBuilder embeddingMappings = generateSemanticTextMapping(Map.of(EMBEDDING_INFERENCE_FIELD, EMBEDDING_INFERENCE_ID));
        assertAcked(
            client.admin()
                .indices()
                .prepareCreate(EMBEDDING_INDEX_NAME)
                .setSettings(indexSettings(randomIntBetween(1, dataNodeCount), 0).build())
                .setMapping(embeddingMappings)
        );
    }
}
