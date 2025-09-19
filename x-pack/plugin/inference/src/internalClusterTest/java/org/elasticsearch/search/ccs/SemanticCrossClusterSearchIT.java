/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.search.ccs;

import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.search.OpenPointInTimeRequest;
import org.elasticsearch.action.search.OpenPointInTimeResponse;
import org.elasticsearch.action.search.TransportOpenPointInTimeAction;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.MinimalServiceSettings;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.license.LicenseSettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.AbstractMultiClustersTestCase;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.action.PutInferenceModelAction;
import org.elasticsearch.xpack.core.ml.inference.MlInferenceNamedXContentProvider;
import org.elasticsearch.xpack.core.ml.vectors.TextEmbeddingQueryVectorBuilder;
import org.elasticsearch.xpack.inference.LocalStateInferencePlugin;
import org.elasticsearch.xpack.inference.mock.TestDenseInferenceServiceExtension;
import org.elasticsearch.xpack.inference.mock.TestInferenceServicePlugin;
import org.elasticsearch.xpack.inference.mock.TestSparseInferenceServiceExtension;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.is;

public class SemanticCrossClusterSearchIT extends AbstractMultiClustersTestCase {
    private static final String REMOTE_CLUSTER = "cluster_a";

    @Override
    protected List<String> remoteClusterAlias() {
        return List.of(REMOTE_CLUSTER);
    }

    @Override
    protected Map<String, Boolean> skipUnavailableForRemoteClusters() {
        return Map.of(REMOTE_CLUSTER, DEFAULT_SKIP_UNAVAILABLE);
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

    private void setupTwoClusters(TestIndexInfo localIndexInfo, TestIndexInfo remoteIndexInfo) throws IOException {
        setupCluster(LOCAL_CLUSTER, localIndexInfo);
        setupCluster(REMOTE_CLUSTER, remoteIndexInfo);
    }

    private void setupCluster(String clusterAlias, TestIndexInfo indexInfo) throws IOException {
        final Client client = client(clusterAlias);
        final String indexName = indexInfo.name();

        for (var entry : indexInfo.inferenceEndpoints().entrySet()) {
            String inferenceId = entry.getKey();
            MinimalServiceSettings minimalServiceSettings = entry.getValue();

            Map<String, Object> serviceSettings = new HashMap<>();
            serviceSettings.put("model", randomAlphaOfLength(5));
            serviceSettings.put("api_key", randomAlphaOfLength(5));
            if (minimalServiceSettings.taskType() == TaskType.TEXT_EMBEDDING) {
                serviceSettings.put("dimensions", minimalServiceSettings.dimensions());
                serviceSettings.put("similarity", minimalServiceSettings.similarity());
                serviceSettings.put("element_type", minimalServiceSettings.elementType());
            }

            createInferenceEndpoint(client, minimalServiceSettings.taskType(), inferenceId, serviceSettings);
        }

        InternalTestCluster cluster = cluster(clusterAlias);
        cluster.ensureAtLeastNumDataNodes(randomIntBetween(1, 3));

        Settings indexSettings = indexSettings(randomIntBetween(2, 5), randomIntBetween(0, 1)).build();
        assertAcked(client.admin().indices().prepareCreate(indexName).setSettings(indexSettings).setMapping(indexInfo.mappings()));
        assertFalse(
            client.admin()
                .cluster()
                .prepareHealth(TEST_REQUEST_TIMEOUT, indexName)
                .setWaitForYellowStatus()
                .setTimeout(TimeValue.timeValueSeconds(10))
                .get()
                .isTimedOut()
        );

        for (var entry : indexInfo.docs().entrySet()) {
            String docId = entry.getKey();
            Map<String, Object> doc = entry.getValue();

            DocWriteResponse response = client.prepareIndex(indexName).setId(docId).setSource(doc).execute().actionGet();
            assertThat(response.getResult(), equalTo(DocWriteResponse.Result.CREATED));
        }
        BroadcastResponse refreshResponse = client.admin().indices().prepareRefresh(indexName).execute().actionGet();
        assertThat(refreshResponse.getStatus(), is(RestStatus.OK));
    }

    private static void createInferenceEndpoint(Client client, TaskType taskType, String inferenceId, Map<String, Object> serviceSettings)
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

    private BytesReference openPointInTime(String[] indices, TimeValue keepAlive) {
        OpenPointInTimeRequest request = new OpenPointInTimeRequest(indices).keepAlive(keepAlive);
        final OpenPointInTimeResponse response = client().execute(TransportOpenPointInTimeAction.TYPE, request).actionGet();
        return response.getPointInTimeId();
    }

    public static class FakeMlPlugin extends Plugin implements SearchPlugin {
        @Override
        public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
            return new MlInferenceNamedXContentProvider().getNamedWriteables();
        }

        @Override
        public List<QueryVectorBuilderSpec<?>> getQueryVectorBuilders() {
            return List.of(
                new QueryVectorBuilderSpec<>(
                    TextEmbeddingQueryVectorBuilder.NAME,
                    TextEmbeddingQueryVectorBuilder::new,
                    TextEmbeddingQueryVectorBuilder.PARSER
                )
            );
        }
    }

    private record TestIndexInfo(
        String name,
        Map<String, MinimalServiceSettings> inferenceEndpoints,
        Map<String, Object> mappings,
        Map<String, Map<String, Object>> docs
    ) {
        @Override
        public Map<String, Object> mappings() {
            return Map.of("properties", mappings);
        }
    }
}
