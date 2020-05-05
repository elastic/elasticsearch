/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.datafeed;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedUpdate;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.ml.datafeed.persistence.DatafeedConfigProvider;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DatafeedConfigAutoUpdaterTests extends ESTestCase {

    private DatafeedConfigProvider provider;
    private List<DatafeedConfig.Builder> datafeeds = new ArrayList<>();
    private IndexNameExpressionResolver indexNameExpressionResolver = new IndexNameExpressionResolver();

    @Before
    public void setup() {
        provider = mock(DatafeedConfigProvider.class);
        doAnswer(call -> {
            @SuppressWarnings("unchecked")
            ActionListener<List<DatafeedConfig.Builder>> handler = (ActionListener<List<DatafeedConfig.Builder>>) call.getArguments()[2];
            handler.onResponse(datafeeds);
            return null;
        }).when(provider).expandDatafeedConfigs(any(), anyBoolean(), any());
        doAnswer(call -> {
            @SuppressWarnings("unchecked")
            ActionListener<DatafeedConfig> handler = (ActionListener<DatafeedConfig>) call.getArguments()[4];
            handler.onResponse(mock(DatafeedConfig.class));
            return null;
        }).when(provider).updateDatefeedConfig(any(), any(), any(), any(), any());
    }

    public void testWithSuccessfulUpdates() {
        String datafeedWithRewrite1 = "datafeed-with-rewrite-1";
        String datafeedWithRewrite2 = "datafeed-with-rewrite-2";
        String datafeedWithoutRewrite = "datafeed-without-rewrite";
        withDatafeed(datafeedWithoutRewrite, false);
        withDatafeed(datafeedWithRewrite1, true);
        withDatafeed(datafeedWithRewrite2, true);

        DatafeedConfigAutoUpdater updater = new DatafeedConfigAutoUpdater(provider, indexNameExpressionResolver);
        updater.runUpdate();

        verify(provider, times(1)).updateDatefeedConfig(eq(datafeedWithRewrite1),
            any(DatafeedUpdate.class),
            eq(Collections.emptyMap()),
            any(),
            any());
        verify(provider, times(1)).updateDatefeedConfig(eq(datafeedWithRewrite2),
            any(DatafeedUpdate.class),
            eq(Collections.emptyMap()),
            any(),
            any());
        verify(provider, times(0)).updateDatefeedConfig(eq(datafeedWithoutRewrite),
            any(DatafeedUpdate.class),
            eq(Collections.emptyMap()),
            any(),
            any());
    }

    public void testWithUpdateFailures() {
        String datafeedWithRewrite1 = "datafeed-with-rewrite-1";
        String datafeedWithRewriteFailure = "datafeed-with-rewrite-failure";
        String datafeedWithoutRewrite = "datafeed-without-rewrite";
        withDatafeed(datafeedWithoutRewrite, false);
        withDatafeed(datafeedWithRewrite1, true);
        withDatafeed(datafeedWithRewriteFailure, true);

        doAnswer(call -> {
            @SuppressWarnings("unchecked")
            ActionListener<DatafeedConfig> handler = (ActionListener<DatafeedConfig>) call.getArguments()[4];
            handler.onFailure(new ElasticsearchException("something wrong happened"));
            return null;
        }).when(provider).updateDatefeedConfig(eq(datafeedWithRewriteFailure), any(), any(), any(), any());

        DatafeedConfigAutoUpdater updater = new DatafeedConfigAutoUpdater(provider, indexNameExpressionResolver);
        ElasticsearchException ex = expectThrows(ElasticsearchException.class, updater::runUpdate);
        assertThat(ex.getMessage(), equalTo("some datafeeds failed being upgraded."));
        assertThat(ex.getSuppressed().length, equalTo(1));
        assertThat(ex.getSuppressed()[0].getMessage(), equalTo("Failed to update datafeed " + datafeedWithRewriteFailure));

        verify(provider, times(1)).updateDatefeedConfig(eq(datafeedWithRewrite1),
            any(DatafeedUpdate.class),
            eq(Collections.emptyMap()),
            any(),
            any());
        verify(provider, times(1)).updateDatefeedConfig(eq(datafeedWithRewriteFailure),
            any(DatafeedUpdate.class),
            eq(Collections.emptyMap()),
            any(),
            any());
        verify(provider, times(0)).updateDatefeedConfig(eq(datafeedWithoutRewrite),
            any(DatafeedUpdate.class),
            eq(Collections.emptyMap()),
            any(),
            any());
    }

    public void testWithNoUpdates() {
        String datafeedWithoutRewrite1 = "datafeed-without-rewrite-1";
        String datafeedWithoutRewrite2 = "datafeed-without-rewrite-2";
        withDatafeed(datafeedWithoutRewrite1, false);
        withDatafeed(datafeedWithoutRewrite2, false);

        DatafeedConfigAutoUpdater updater = new DatafeedConfigAutoUpdater(provider, indexNameExpressionResolver);
        updater.runUpdate();

        verify(provider, times(0)).updateDatefeedConfig(any(),
            any(DatafeedUpdate.class),
            eq(Collections.emptyMap()),
            any(),
            any());
    }

    public void testIsAbleToRun() {
        Metadata.Builder metadata = Metadata.builder();
        RoutingTable.Builder routingTable = RoutingTable.builder();
        IndexMetadata.Builder indexMetadata = IndexMetadata.builder(AnomalyDetectorsIndex.configIndexName());
        indexMetadata.settings(Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
        );
        metadata.put(indexMetadata);
        Index index = new Index(AnomalyDetectorsIndex.configIndexName(), "_uuid");
        ShardId shardId = new ShardId(index, 0);
        ShardRouting shardRouting = ShardRouting.newUnassigned(shardId, true, RecoverySource.EmptyStoreRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, ""));
        shardRouting = shardRouting.initialize("node_id", null, 0L);
        shardRouting = shardRouting.moveToStarted();
        routingTable.add(IndexRoutingTable.builder(index)
            .addIndexShard(new IndexShardRoutingTable.Builder(shardId).addShard(shardRouting).build()));

        ClusterState.Builder csBuilder = ClusterState.builder(new ClusterName("_name"));
        csBuilder.routingTable(routingTable.build());
        csBuilder.metadata(metadata);

        DatafeedConfigAutoUpdater updater = new DatafeedConfigAutoUpdater(provider, indexNameExpressionResolver);
        assertThat(updater.isAbleToRun(csBuilder.build()), is(true));

        metadata = new Metadata.Builder(csBuilder.build().metadata());
        routingTable = new RoutingTable.Builder(csBuilder.build().routingTable());
        if (randomBoolean()) {
            routingTable.remove(AnomalyDetectorsIndex.configIndexName());
        } else {
            index = new Index(AnomalyDetectorsIndex.configIndexName(), "_uuid");
            shardId = new ShardId(index, 0);
            shardRouting = ShardRouting.newUnassigned(shardId, true, RecoverySource.EmptyStoreRecoverySource.INSTANCE,
                new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, ""));
            shardRouting = shardRouting.initialize("node_id", null, 0L);
            routingTable.add(IndexRoutingTable.builder(index)
                .addIndexShard(new IndexShardRoutingTable.Builder(shardId).addShard(shardRouting).build()));
        }

        csBuilder.routingTable(routingTable.build());
        csBuilder.metadata(metadata);
        assertThat(updater.isAbleToRun(csBuilder.build()), is(false));

        csBuilder.metadata(Metadata.EMPTY_METADATA);
        assertThat(updater.isAbleToRun(csBuilder.build()), is(true));
    }

    private void withDatafeed(String datafeedId, boolean aggsRewritten) {
        DatafeedConfig.Builder builder = mock(DatafeedConfig.Builder.class);
        DatafeedConfig config = mock(DatafeedConfig.class);
        when(config.getId()).thenReturn(datafeedId);
        when(config.aggsRewritten()).thenReturn(aggsRewritten);
        when(builder.build()).thenReturn(config);
        datafeeds.add(builder);
    }

}
