/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.license.LicenseSettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.action.PutInferenceModelAction;
import org.elasticsearch.xpack.inference.FakeMlPlugin;
import org.elasticsearch.xpack.inference.LocalStateInferencePlugin;
import org.elasticsearch.xpack.inference.mock.TestInferenceServicePlugin;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

/**
 * Cross-cluster coverage for ES|QL search against {@code semantic_text} fields and for the inference
 * commands. The multi-cluster csv-spec suite cannot cover either: it hardcodes
 * {@code supportsSemanticTextInference()} to false, and installs the inference test service only in the
 * {@code #newToOld} BWC pairing.
 * <p>
 * The paths unique to ES|QL, and therefore untested elsewhere, are what this suite aims at:
 * {@code QueryBuilderResolver} building {@link org.elasticsearch.action.ResolvedIndices} that span
 * remotes, serialization of the intercepted inference query builder inside the physical plan sent to
 * remote data nodes, and the data-node rewrite resolving inference results against the remote's own
 * cluster alias.
 * <p>
 * Each cluster gets its own inference endpoint id, so a query that resolves results per cluster alias
 * succeeds while one that assumed the coordinator's endpoint would not. The sparse test service emits the
 * same five {@code feature_N} tokens for any input, so every semantic query matches every document;
 * assertions are therefore about which documents come back, not about relevance ordering.
 */
public class CrossClusterSemanticTextIT extends AbstractCrossClusterTestCase {

    private static final String INDEX = "semantic-docs";
    private static final String LOCAL_INFERENCE_ID = "test-sparse-local";
    private static final String REMOTE_INFERENCE_ID = "test-sparse-remote";
    private static final String LOCAL_DENSE_INFERENCE_ID = "test-dense-local";
    private static final String REMOTE_DENSE_INFERENCE_ID = "test-dense-remote";
    private static final String RERANK_INFERENCE_ID = "test-rerank";
    private static final String COMPLETION_INFERENCE_ID = "test-completion";

    @Override
    protected List<String> remoteClusterAlias() {
        return List.of(REMOTE_CLUSTER_1);
    }

    @Override
    protected Map<String, Boolean> skipUnavailableForRemoteClusters() {
        return Map.of(REMOTE_CLUSTER_1, false);
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins(String clusterAlias) {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins(clusterAlias));
        // Both adjustments below are artifacts of the in-JVM test cluster hand-picking plugins. Neither
        // corresponds to anything a real deployment has to do.
        //
        // LocalStateCompositeXPackPlugin is test-only, and only one may be loaded: each registers
        // node.attr.xpack.installed, so a second one fails node startup. LocalStateInferencePlugin is a
        // superset of the async plugin the base class installs - same composite x-pack state, plus inference.
        plugins.remove(EsqlAsyncActionIT.LocalStateEsqlAsync.class);
        plugins.add(LocalStateInferencePlugin.class);
        plugins.add(TestInferenceServicePlugin.class);
        // FakeMlPlugin registers the InferenceResults named writeables that the intercepted query builder
        // carries in the physical plan. Production gets them from MachineLearning#getNamedWriteables, which
        // is not gated on xpack.ml.enabled and ships in the default distribution; this cluster just does not
        // load ML. The _search side needs the same shim, for the same reason - see
        // AbstractSemanticCrossClusterSearchTestCase - since the interceptor serializes the same results into
        // the shard search request.
        plugins.add(FakeMlPlugin.class);
        return plugins;
    }

    @Override
    protected Settings nodeSettings() {
        return Settings.builder().put(super.nodeSettings()).put(LicenseSettings.SELF_GENERATED_LICENSE_TYPE.getKey(), "trial").build();
    }

    public void testMatchOnSemanticTextAcrossClusters() {
        setupSemanticIndices();

        try (EsqlQueryResponse resp = runQuery("FROM " + INDEX + "," + remoteIndex() + " | WHERE body : \"quick\" | KEEP id", false)) {
            assertThat(idsFrom(resp), equalTo(Set.of("local-1", "local-2", "remote-1", "remote-2")));
        }
    }

    public void testMatchOnSemanticTextRemoteOnly() {
        setupSemanticIndices();

        try (EsqlQueryResponse resp = runQuery("FROM " + remoteIndex() + " | WHERE body : \"quick\" | KEEP id", false)) {
            assertThat(idsFrom(resp), equalTo(Set.of("remote-1", "remote-2")));
        }
    }

    public void testMatchOnSemanticTextWithPushedDownFilter() {
        setupSemanticIndices();

        // The intercepted query builder and the pushable term query travel to the data nodes in the same plan.
        String query = "FROM " + INDEX + "," + remoteIndex() + " | WHERE body : \"quick\" AND host == \"host-1\" | KEEP id";
        try (EsqlQueryResponse resp = runQuery(query, false)) {
            assertThat(idsFrom(resp), equalTo(Set.of("local-1", "remote-1")));
        }
    }

    public void testScoreIsPopulatedAcrossClusters() {
        setupSemanticIndices();

        String query = "FROM " + INDEX + "," + remoteIndex() + " METADATA _score | WHERE body : \"quick\" | KEEP id, _score";
        try (EsqlQueryResponse resp = runQuery(query, false)) {
            List<List<Object>> values = getValuesList(resp);
            assertThat(values, hasSize(4));
            for (List<Object> row : values) {
                assertThat("no score for " + row.get(0), (Double) row.get(1), greaterThan(0.0));
            }
        }
    }

    public void testExecutionInfoForSemanticQuery() {
        setupSemanticIndices();

        try (EsqlQueryResponse resp = runQuery("FROM " + INDEX + "," + remoteIndex() + " | WHERE body : \"quick\" | KEEP id", true)) {
            EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
            assertNotNull(executionInfo);
            assertThat(executionInfo.isCrossClusterSearch(), is(true));
            assertThat(executionInfo.clusterAliases(), equalTo(Set.of(LOCAL_CLUSTER, REMOTE_CLUSTER_1)));
            for (String alias : executionInfo.clusterAliases()) {
                assertThat(executionInfo.getCluster(alias).getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SUCCESSFUL));
            }
        }
    }

    public void testKnnOnSemanticTextAcrossClusters() {
        setupSemanticIndices();

        // A literal query vector means the interceptor skips inference, so this covers the other half of the
        // semantic path: rewriting the field to a nested knn over the chunk embeddings on each data node.
        String query = "FROM " + INDEX + "," + remoteIndex() + " | WHERE knn(body_dense, [1, 2, 3]) | KEEP id";
        try (EsqlQueryResponse resp = runQuery(query, false)) {
            assertThat(idsFrom(resp), equalTo(Set.of("local-1", "local-2", "remote-1", "remote-2")));
        }
    }

    public void testMatchOnDenseSemanticTextAcrossClusters() {
        setupSemanticIndices();

        // The dense counterpart of the sparse MATCH tests: each cluster embeds the query text with its own
        // text_embedding endpoint, so this is the path that resolves dense inference results per cluster alias.
        String query = "FROM " + INDEX + "," + remoteIndex() + " | WHERE body_dense : \"quick\" | KEEP id";
        try (EsqlQueryResponse resp = runQuery(query, false)) {
            assertThat(idsFrom(resp), equalTo(Set.of("local-1", "local-2", "remote-1", "remote-2")));
        }
    }

    public void testKnnWithTextEmbeddingAcrossClusters() {
        setupSemanticIndices();

        // TEXT_EMBEDDING folds to a dense_vector literal on the coordinator using the coordinator's endpoint,
        // which then has to survive serialization into the plan sent to the remote data nodes.
        String query = "FROM "
            + INDEX
            + ","
            + remoteIndex()
            + " | WHERE knn(body_dense, TEXT_EMBEDDING(\"quick\", \""
            + LOCAL_DENSE_INFERENCE_ID
            + "\")) | KEEP id";
        try (EsqlQueryResponse resp = runQuery(query, false)) {
            assertThat(idsFrom(resp), equalTo(Set.of("local-1", "local-2", "remote-1", "remote-2")));
        }
    }

    public void testCompletionOverRemoteDataUsesCoordinatorEndpoint() {
        setupSemanticIndices();
        createInferenceEndpoint(client(LOCAL_CLUSTER), COMPLETION_INFERENCE_ID, TaskType.COMPLETION, "completion_test_service");

        String query = "FROM "
            + remoteIndex()
            + " | WHERE body : \"quick\""
            + " | COMPLETION summary = body WITH { \"inference_id\": \""
            + COMPLETION_INFERENCE_ID
            + "\" } | KEEP id, summary";
        try (EsqlQueryResponse resp = runQuery(query, false)) {
            List<List<Object>> values = getValuesList(resp);
            assertThat(values, hasSize(2));
            for (List<Object> row : values) {
                assertNotNull("no completion for " + row.get(0), row.get(1));
            }
        }
    }

    public void testFuseOverSemanticMatchesAcrossClusters() {
        setupSemanticIndices();

        // FUSE keys on _id and _index; _index is cluster-qualified in CCS, so documents from the two
        // clusters stay distinct through the fusion.
        String query = "FROM "
            + INDEX
            + ","
            + remoteIndex()
            + " METADATA _id, _index, _score"
            + " | FORK ( WHERE body : \"quick\" | SORT _score DESC | LIMIT 10 )"
            + "        ( WHERE body : \"lazy\" | SORT _score DESC | LIMIT 10 )"
            + " | FUSE | KEEP id";
        try (EsqlQueryResponse resp = runQuery(query, false)) {
            assertThat(idsFrom(resp), equalTo(Set.of("local-1", "local-2", "remote-1", "remote-2")));
        }
    }

    public void testRerankOverRemoteDataUsesCoordinatorEndpoint() {
        setupSemanticIndices();
        // Deliberately created on the coordinator only: RERANK is ExecutesOn.Coordinator.
        createInferenceEndpoint(client(LOCAL_CLUSTER), RERANK_INFERENCE_ID, TaskType.RERANK, "test_reranking_service");

        try (EsqlQueryResponse resp = runQuery(rerankQuery(), false)) {
            assertThat(idsFrom(resp), equalTo(Set.of("remote-1", "remote-2")));
        }
    }

    public void testRerankFailsWhenEndpointOnlyExistsOnRemote() {
        setupSemanticIndices();
        createInferenceEndpoint(client(REMOTE_CLUSTER_1), RERANK_INFERENCE_ID, TaskType.RERANK, "test_reranking_service");

        Exception e = expectThrows(Exception.class, () -> runQuery(rerankQuery(), false).close());
        assertThat(e.getMessage(), containsString("Inference endpoint not found [" + RERANK_INFERENCE_ID + "]"));
    }

    private String rerankQuery() {
        return "FROM "
            + remoteIndex()
            + " METADATA _score | WHERE body : \"quick\""
            + " | RERANK \"quick\" ON body WITH { \"inference_id\": \""
            + RERANK_INFERENCE_ID
            + "\" } | KEEP id";
    }

    private String remoteIndex() {
        return REMOTE_CLUSTER_1 + ":" + INDEX;
    }

    private static Set<String> idsFrom(EsqlQueryResponse resp) {
        return getValuesList(resp).stream().map(row -> (String) row.get(0)).collect(Collectors.toSet());
    }

    private void setupSemanticIndices() {
        setupSemanticIndex(LOCAL_CLUSTER, "local", LOCAL_INFERENCE_ID, LOCAL_DENSE_INFERENCE_ID);
        setupSemanticIndex(REMOTE_CLUSTER_1, "remote", REMOTE_INFERENCE_ID, REMOTE_DENSE_INFERENCE_ID);
    }

    private void setupSemanticIndex(String clusterAlias, String idPrefix, String sparseInferenceId, String denseInferenceId) {
        Client client = client(clusterAlias);
        createInferenceEndpoint(client, sparseInferenceId, TaskType.SPARSE_EMBEDDING, "test_service");
        createInferenceEndpoint(
            client,
            denseInferenceId,
            TaskType.TEXT_EMBEDDING,
            "text_embedding_test_service",
            ",\n    \"dimensions\": 3,\n    \"similarity\": \"cosine\",\n    \"element_type\": \"float\""
        );

        assertAcked(
            client.admin()
                .indices()
                .prepareCreate(INDEX)
                .setSettings(Settings.builder().put("index.number_of_shards", randomIntBetween(1, 3)))
                .setMapping(
                    "id",
                    "type=keyword",
                    "host",
                    "type=keyword",
                    "body",
                    "type=semantic_text,inference_id=" + sparseInferenceId,
                    "body_dense",
                    "type=semantic_text,inference_id=" + denseInferenceId
                )
        );

        client.prepareBulk()
            .add(
                new IndexRequest(INDEX).id(idPrefix + "-1")
                    .source("id", idPrefix + "-1", "host", "host-1", "body", "the quick brown fox", "body_dense", "the quick brown fox")
            )
            .add(
                new IndexRequest(INDEX).id(idPrefix + "-2")
                    .source("id", idPrefix + "-2", "host", "host-2", "body", "a lazy dog sleeps", "body_dense", "a lazy dog sleeps")
            )
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();
    }

    private void createInferenceEndpoint(Client client, String inferenceId, TaskType taskType, String service) {
        createInferenceEndpoint(client, inferenceId, taskType, service, "");
    }

    private void createInferenceEndpoint(
        Client client,
        String inferenceId,
        TaskType taskType,
        String service,
        String extraServiceSettings
    ) {
        String config = String.format(Locale.ROOT, """
            {
              "service": "%s",
              "service_settings": {
                "model_id": "%s",
                "api_key": "test-key"%s
              }
            }
            """, service, inferenceId, extraServiceSettings);

        PutInferenceModelAction.Request request = new PutInferenceModelAction.Request(
            taskType,
            inferenceId,
            new BytesArray(config),
            XContentType.JSON,
            TEST_REQUEST_TIMEOUT
        );
        client.execute(PutInferenceModelAction.INSTANCE, request).actionGet(TEST_REQUEST_TIMEOUT);
    }
}
