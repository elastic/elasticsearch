/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.search.ccs;

import org.elasticsearch.action.search.OpenPointInTimeRequest;
import org.elasticsearch.action.search.OpenPointInTimeResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.TransportOpenPointInTimeAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.license.LicenseSettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.builder.PointInTimeBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.AbstractMultiClustersTestCase;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.action.PutInferenceModelAction;
import org.elasticsearch.xpack.core.ml.inference.MlInferenceNamedXContentProvider;
import org.elasticsearch.xpack.inference.LocalStateInferencePlugin;
import org.elasticsearch.xpack.inference.mock.TestDenseInferenceServiceExtension;
import org.elasticsearch.xpack.inference.mock.TestInferenceServicePlugin;
import org.elasticsearch.xpack.inference.mock.TestSparseInferenceServiceExtension;
import org.elasticsearch.xpack.inference.queries.SemanticQueryBuilder;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;

public class SemanticCrossClusterSearchIT extends AbstractMultiClustersTestCase {
    private static final String REMOTE_CLUSTER = "cluster_a";
    private static final String INFERENCE_FIELD = "inference_field";

    private static final Map<String, Object> TEXT_EMBEDDING_SERVICE_SETTINGS_1 = Map.of(
        "model",
        "my_model",
        "dimensions",
        256,
        "similarity",
        "cosine",
        "api_key",
        "my_api_key"
    );

    private static final Map<String, Object> TEXT_EMBEDDING_SERVICE_SETTINGS_2 = Map.of(
        "model",
        "my_model",
        "dimensions",
        384,
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
        return Map.of(REMOTE_CLUSTER, randomBoolean());
    }

    @Override
    protected boolean reuseClusters() {
        return false;
    }

    @Override
    protected Settings nodeSettings() {
        return Settings.builder().put(LicenseSettings.SELF_GENERATED_LICENSE_TYPE.getKey(), "trial").build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins(String clusterAlias) {
        return List.of(LocalStateInferencePlugin.class, TestInferenceServicePlugin.class, FakeMlPlugin.class);
    }

    public void testSemanticCrossClusterSearch() throws Exception {
        Map<String, Object> testClusterInfo = setupTwoClusters();
        String localIndex = (String) testClusterInfo.get("local.index");
        String remoteIndex = (String) testClusterInfo.get("remote.index");

        SemanticQueryBuilder queryBuilder = new SemanticQueryBuilder(INFERENCE_FIELD, "foo");
        SearchRequest searchRequest = new SearchRequest(localIndex, REMOTE_CLUSTER + ":" + remoteIndex);
        searchRequest.source(new SearchSourceBuilder().query(queryBuilder).size(10));
        searchRequest.setCcsMinimizeRoundtrips(true);

        assertResponse(client(LOCAL_CLUSTER).search(searchRequest), response -> {
            assertNotNull(response);
            assertEquals(10, response.getHits().getHits().length);
        });
    }

    public void testMatchCrossClusterSearch() throws Exception {
        Map<String, Object> testClusterInfo = setupTwoClusters();
        String localIndex = (String) testClusterInfo.get("local.index");
        String remoteIndex = (String) testClusterInfo.get("remote.index");

        MatchQueryBuilder queryBuilder = new MatchQueryBuilder(INFERENCE_FIELD, "foo");
        SearchRequest searchRequest = new SearchRequest(localIndex, REMOTE_CLUSTER + ":" + remoteIndex);
        searchRequest.source(new SearchSourceBuilder().query(queryBuilder).size(10));
        searchRequest.setCcsMinimizeRoundtrips(true);

        assertResponse(client(LOCAL_CLUSTER).search(searchRequest), response -> {
            assertNotNull(response);
            assertEquals(0, response.getFailedShards());
            assertEquals(10, response.getHits().getHits().length);
        });
    }

    public void testSemanticCrossClusterSearchWithPIT() throws Exception {
        Map<String, Object> testClusterInfo = setupTwoClusters();
        String localIndex = (String) testClusterInfo.get("local.index");
        String remoteIndex = (String) testClusterInfo.get("remote.index");

        BytesReference pitId = openPointInTime(
            new String[] { localIndex, REMOTE_CLUSTER + ":" + remoteIndex },
            TimeValue.timeValueMinutes(2)
        );

        SemanticQueryBuilder queryBuilder = new SemanticQueryBuilder(INFERENCE_FIELD, "foo");
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.source(new SearchSourceBuilder().query(queryBuilder).pointInTimeBuilder(new PointInTimeBuilder(pitId)).size(10));

        IllegalArgumentException e = assertThrows(
            IllegalArgumentException.class,
            () -> client(LOCAL_CLUSTER).search(searchRequest).actionGet(TEST_REQUEST_TIMEOUT)
        );
        assertThat(e.getMessage(), containsString("semantic query supports CCS only when ccs_minimize_roundtrips=true"));
    }

    private Map<String, Object> setupTwoClusters(String[] localIndices, String[] remoteIndices) throws IOException {
        final String localInferenceId = "local_inference_id";
        final String remoteInferenceId = "remote_inference_id";
        createInferenceEndpoint(client(LOCAL_CLUSTER), TaskType.TEXT_EMBEDDING, localInferenceId, TEXT_EMBEDDING_SERVICE_SETTINGS_1);
        createInferenceEndpoint(client(REMOTE_CLUSTER), TaskType.TEXT_EMBEDDING, remoteInferenceId, TEXT_EMBEDDING_SERVICE_SETTINGS_2);

        int numShardsLocal = randomIntBetween(2, 10);
        Settings localSettings = indexSettings(numShardsLocal, randomIntBetween(0, 1)).build();
        for (String localIndex : localIndices) {
            assertAcked(
                client(LOCAL_CLUSTER).admin()
                    .indices()
                    .prepareCreate(localIndex)
                    .setSettings(localSettings)
                    .setMapping(INFERENCE_FIELD, "type=semantic_text,inference_id=" + localInferenceId)
            );
            indexDocs(client(LOCAL_CLUSTER), localIndex);
        }

        int numShardsRemote = randomIntBetween(2, 10);
        final InternalTestCluster remoteCluster = cluster(REMOTE_CLUSTER);
        remoteCluster.ensureAtLeastNumDataNodes(randomIntBetween(1, 3));
        for (String remoteIndex : remoteIndices) {
            assertAcked(
                client(REMOTE_CLUSTER).admin()
                    .indices()
                    .prepareCreate(remoteIndex)
                    .setSettings(indexSettings(numShardsRemote, randomIntBetween(0, 1)))
                    .setMapping(INFERENCE_FIELD, "type=semantic_text,inference_id=" + remoteInferenceId)
            );
            assertFalse(
                client(REMOTE_CLUSTER).admin()
                    .cluster()
                    .prepareHealth(TEST_REQUEST_TIMEOUT, remoteIndex)
                    .setWaitForYellowStatus()
                    .setTimeout(TimeValue.timeValueSeconds(10))
                    .get()
                    .isTimedOut()
            );
            indexDocs(client(REMOTE_CLUSTER), remoteIndex);
        }

        String skipUnavailableKey = Strings.format("cluster.remote.%s.skip_unavailable", REMOTE_CLUSTER);
        Setting<?> skipUnavailableSetting = cluster(REMOTE_CLUSTER).clusterService().getClusterSettings().get(skipUnavailableKey);
        boolean skipUnavailable = (boolean) cluster(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY).clusterService()
            .getClusterSettings()
            .get(skipUnavailableSetting);

        Map<String, Object> clusterInfo = new HashMap<>();
        clusterInfo.put("local.num_shards", numShardsLocal);
        clusterInfo.put("remote.num_shards", numShardsRemote);
        clusterInfo.put("remote.skip_unavailable", skipUnavailable);
        return clusterInfo;
    }

    private Map<String, Object> setupTwoClusters() throws IOException {
        var clusterInfo = setupTwoClusters(new String[] { "demo" }, new String[] { "prod" });
        clusterInfo.put("local.index", "demo");
        clusterInfo.put("remote.index", "prod");
        return clusterInfo;
    }

    private void createInferenceEndpoint(Client client, TaskType taskType, String inferenceId, Map<String, Object> serviceSettings)
        throws IOException {
        final String service = switch (taskType) {
            case TEXT_EMBEDDING -> TestDenseInferenceServiceExtension.TestInferenceService.NAME;
            case SPARSE_EMBEDDING -> TestSparseInferenceServiceExtension.TestInferenceService.NAME;
            default -> throw new IllegalArgumentException("Unhandled task type [" + taskType + "]");
        };

        final BytesReference content;
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            builder.startObject();
            builder.field("service", service);
            builder.field("service_settings", serviceSettings);
            builder.endObject();

            content = BytesReference.bytes(builder);
        }

        PutInferenceModelAction.Request request = new PutInferenceModelAction.Request(
            taskType,
            inferenceId,
            content,
            XContentType.JSON,
            TEST_REQUEST_TIMEOUT
        );
        var responseFuture = client.execute(PutInferenceModelAction.INSTANCE, request);
        assertThat(responseFuture.actionGet(TEST_REQUEST_TIMEOUT).getModel().getInferenceEntityId(), equalTo(inferenceId));
    }

    private int indexDocs(Client client, String index) {
        int numDocs = between(5, 10);
        for (int i = 0; i < numDocs; i++) {
            client.prepareIndex(index).setSource(INFERENCE_FIELD, randomAlphaOfLength(10)).get();
        }
        client.admin().indices().prepareRefresh(index).get();
        return numDocs;
    }

    private BytesReference openPointInTime(String[] indices, TimeValue keepAlive) {
        OpenPointInTimeRequest request = new OpenPointInTimeRequest(indices).keepAlive(keepAlive);
        final OpenPointInTimeResponse response = client().execute(TransportOpenPointInTimeAction.TYPE, request).actionGet();
        return response.getPointInTimeId();
    }

    public static class FakeMlPlugin extends Plugin {
        @Override
        public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
            return new MlInferenceNamedXContentProvider().getNamedWriteables();
        }
    }
}
