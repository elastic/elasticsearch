/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequestBuilder;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesResponse;
import org.elasticsearch.action.admin.indices.alias.TransportIndicesAliasesAction;
import org.elasticsearch.action.admin.indices.rollover.RolloverAction;
import org.elasticsearch.action.admin.indices.rollover.RolloverRequest;
import org.elasticsearch.action.admin.indices.rollover.RolloverResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.ElasticsearchClient;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.indices.TestIndexNameExpressionResolver;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class MlAnomaliesIndexUpdateTests extends ESTestCase {

    public void testIsAnomaliesWriteAlias() {
        assertTrue(MlAnomaliesIndexUpdate.isAnomaliesWriteAlias(AnomalyDetectorsIndex.resultsWriteAlias("foo")));
        assertFalse(MlAnomaliesIndexUpdate.isAnomaliesWriteAlias(AnomalyDetectorsIndex.jobResultsAliasedName("foo")));
        assertFalse(MlAnomaliesIndexUpdate.isAnomaliesWriteAlias("some-index"));
    }

    public void testIsAnomaliesAlias() {
        assertTrue(MlAnomaliesIndexUpdate.isAnomaliesReadAlias(AnomalyDetectorsIndex.jobResultsAliasedName("foo")));
        assertFalse(MlAnomaliesIndexUpdate.isAnomaliesReadAlias(AnomalyDetectorsIndex.resultsWriteAlias("foo")));
        assertFalse(MlAnomaliesIndexUpdate.isAnomaliesReadAlias("some-index"));
    }

    public void testIsAbleToRun_IndicesDoNotExist() {
        RoutingTable.Builder routingTable = RoutingTable.builder();
        var updater = new MlAnomaliesIndexUpdate(TestIndexNameExpressionResolver.newInstance(), mock(Client.class));

        ClusterState.Builder csBuilder = ClusterState.builder(new ClusterName("_name"));
        csBuilder.routingTable(routingTable.build());
        assertTrue(updater.isAbleToRun(csBuilder.build()));
    }

    public void testIsAbleToRun_IndicesHaveNoRouting() {
        IndexMetadata.Builder indexMetadata = IndexMetadata.builder(".ml-anomalies-shared");
        indexMetadata.settings(
            Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(IndexMetadata.SETTING_INDEX_UUID, "_uuid")
        );

        Metadata.Builder metadata = Metadata.builder();
        metadata.put(indexMetadata);
        ClusterState.Builder csBuilder = ClusterState.builder(new ClusterName("_name"));
        csBuilder.routingTable(RoutingTable.builder().build()); // no routing to index
        csBuilder.metadata(metadata);

        var updater = new MlAnomaliesIndexUpdate(TestIndexNameExpressionResolver.newInstance(), mock(Client.class));

        assertFalse(updater.isAbleToRun(csBuilder.build()));
    }

    public void testBuildIndexAliasesRequest() {
        var anomaliesIndex = ".ml-anomalies-sharedindex";
        var jobs = List.of("job1", "job2");
        IndexMetadata.Builder indexMetadata = createSharedResultsIndex(anomaliesIndex, IndexVersion.current(), jobs);
        Metadata.Builder metadata = Metadata.builder();
        metadata.put(indexMetadata);
        ClusterState.Builder csBuilder = ClusterState.builder(new ClusterName("_name"));
        csBuilder.metadata(metadata);

        var updater = new MlAnomaliesIndexUpdate(
            TestIndexNameExpressionResolver.newInstance(),
            new OriginSettingClient(mock(Client.class), "doesn't matter")
        );

        IndicesAliasesRequestBuilder aliasRequestBuilder = new IndicesAliasesRequestBuilder(mock(ElasticsearchClient.class));

        var newIndex = anomaliesIndex + "-000001";
        var request = updater.addIndexAliasesRequests(aliasRequestBuilder, anomaliesIndex, newIndex, csBuilder.build());
        var actions = request.request().getAliasActions();
        assertThat(actions, hasSize(6));

        // The order in which the alias actions are created
        // is not preserved so look for the item in the list
        for (var job : jobs) {
            var expected = new AliasActionMatcher(
                AnomalyDetectorsIndex.resultsWriteAlias(job),
                newIndex,
                IndicesAliasesRequest.AliasActions.Type.ADD
            );
            assertThat(actions.stream().filter(expected::matches).count(), equalTo(1L));

            expected = new AliasActionMatcher(
                AnomalyDetectorsIndex.resultsWriteAlias(job),
                anomaliesIndex,
                IndicesAliasesRequest.AliasActions.Type.REMOVE
            );
            assertThat(actions.stream().filter(expected::matches).count(), equalTo(1L));

            expected = new AliasActionMatcher(
                AnomalyDetectorsIndex.jobResultsAliasedName(job),
                newIndex,
                IndicesAliasesRequest.AliasActions.Type.ADD
            );
            assertThat(actions.stream().filter(expected::matches).count(), equalTo(1L));
        }
    }

    public void testRunUpdate_UpToDateIndices() {
        String indexName = ".ml-anomalies-sharedindex";
        var jobs = List.of("job1", "job2");
        IndexMetadata.Builder indexMetadata = createSharedResultsIndex(indexName, IndexVersion.current(), jobs);

        Metadata.Builder metadata = Metadata.builder();
        metadata.put(indexMetadata);
        ClusterState.Builder csBuilder = ClusterState.builder(new ClusterName("_name"));
        csBuilder.metadata(metadata);

        var client = mock(Client.class);
        var updater = new MlAnomaliesIndexUpdate(TestIndexNameExpressionResolver.newInstance(), client);
        updater.runUpdate(csBuilder.build());
        // everything up to date so no action for the client
        verify(client).settings();
        verify(client).threadPool();
        verifyNoMoreInteractions(client);
    }

    public void testRunUpdate_LegacyIndex() {
        String indexName = ".ml-anomalies-sharedindex";
        var jobs = List.of("job1", "job2");
        IndexMetadata.Builder indexMetadata = createSharedResultsIndex(indexName, IndexVersions.V_7_17_0, jobs);

        Metadata.Builder metadata = Metadata.builder();
        metadata.put(indexMetadata);
        ClusterState.Builder csBuilder = ClusterState.builder(new ClusterName("_name"));
        csBuilder.metadata(metadata);

        var client = mockClientWithRolloverAndAlias(indexName);
        var updater = new MlAnomaliesIndexUpdate(TestIndexNameExpressionResolver.newInstance(), client);

        updater.runUpdate(csBuilder.build());
        verify(client).settings();
        verify(client, times(7)).threadPool();
        verify(client, times(2)).execute(same(TransportIndicesAliasesAction.TYPE), any(), any());  // create rollover alias and update
        verify(client).execute(same(RolloverAction.INSTANCE), any(), any());  // index rolled over
        verifyNoMoreInteractions(client);
    }

    private record AliasActionMatcher(String aliasName, String index, IndicesAliasesRequest.AliasActions.Type actionType) {
        boolean matches(IndicesAliasesRequest.AliasActions aliasAction) {
            return aliasAction.actionType() == actionType
                && aliasAction.aliases()[0].equals(aliasName)
                && aliasAction.indices()[0].equals(index);
        }
    }

    private IndexMetadata.Builder createSharedResultsIndex(String indexName, IndexVersion indexVersion, List<String> jobs) {
        IndexMetadata.Builder indexMetadata = IndexMetadata.builder(indexName);
        indexMetadata.settings(
            Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, indexVersion)
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(IndexMetadata.SETTING_INDEX_UUID, "_uuid")
        );

        for (var jobId : jobs) {
            indexMetadata.putAlias(AliasMetadata.builder(AnomalyDetectorsIndex.jobResultsAliasedName(jobId)).isHidden(true).build());
            indexMetadata.putAlias(
                AliasMetadata.builder(AnomalyDetectorsIndex.resultsWriteAlias(jobId)).writeIndex(true).isHidden(true).build()
            );
        }

        return indexMetadata;
    }

    @SuppressWarnings("unchecked")
    static Client mockClientWithRolloverAndAlias(String indexName) {
        var client = mock(Client.class);

        var aliasRequestCount = new AtomicInteger(0);

        doAnswer(invocationOnMock -> {
            ActionListener<RolloverResponse> actionListener = (ActionListener<RolloverResponse>) invocationOnMock.getArguments()[2];
            actionListener.onResponse(new RolloverResponse(indexName, indexName + "-new", Map.of(), false, true, true, true, true));
            return null;
        }).when(client).execute(same(RolloverAction.INSTANCE), any(RolloverRequest.class), any(ActionListener.class));

        doAnswer(invocationOnMock -> {
            ActionListener<IndicesAliasesResponse> actionListener = (ActionListener<IndicesAliasesResponse>) invocationOnMock
                .getArguments()[2];
            var request = (IndicesAliasesRequest) invocationOnMock.getArguments()[1];
            // Check the rollover alias is create and deleted
            if (aliasRequestCount.getAndIncrement() == 0) {
                var addAliasAction = new AliasActionMatcher(
                    indexName + ".rollover_alias",
                    indexName,
                    IndicesAliasesRequest.AliasActions.Type.ADD
                );
                assertEquals(1L, request.getAliasActions().stream().filter(addAliasAction::matches).count());
            } else {
                var removeAliasAction = new AliasActionMatcher(
                    indexName + ".rollover_alias",
                    indexName + "-new",
                    IndicesAliasesRequest.AliasActions.Type.REMOVE
                );
                assertEquals(1L, request.getAliasActions().stream().filter(removeAliasAction::matches).count());
            }

            actionListener.onResponse(IndicesAliasesResponse.ACKNOWLEDGED_NO_ERRORS);

            return null;
        }).when(client).execute(same(TransportIndicesAliasesAction.TYPE), any(IndicesAliasesRequest.class), any(ActionListener.class));

        var threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
        when(client.threadPool()).thenReturn(threadPool);

        return client;
    }
}
