/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.index.engine;

import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.allocation.command.AllocateStalePrimaryAllocationCommand;
import org.elasticsearch.cluster.routing.allocation.command.CancelAllocationCommand;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.IndexLongFieldRange;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.protocol.xpack.frozen.FreezeRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.core.frozen.action.FreezeIndexAction;
import org.elasticsearch.xpack.frozen.FrozenIndices;
import org.joda.time.Instant;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_ROUTING_EXCLUDE_GROUP_SETTING;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;

@ESIntegTestCase.ClusterScope(numDataNodes = 0)
public class FrozenIndexIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(FrozenIndices.class, LocalStateCompositeXPackPlugin.class);
    }

    @Override
    protected boolean addMockInternalEngine() {
        return false;
    }

    public void testTimestampRangeRecalculatedOnStalePrimaryAllocation() throws IOException {
        final List<String> nodeNames = internalCluster().startNodes(2);

        createIndex("index", Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                .build());

        final IndexResponse indexResponse = client().prepareIndex("index")
                .setSource(DataStream.TimestampField.FIXED_TIMESTAMP_FIELD, "2010-01-06T02:03:04.567Z").get();

        ensureGreen("index");

        assertThat(client().admin().indices().prepareFlush("index").get().getSuccessfulShards(), equalTo(2));
        assertThat(client().admin().indices().prepareRefresh("index").get().getSuccessfulShards(), equalTo(2));

        final String excludeSetting = INDEX_ROUTING_EXCLUDE_GROUP_SETTING.getConcreteSettingForNamespace("_name").getKey();
        assertAcked(client().admin().indices().prepareUpdateSettings("index").setSettings(
                Settings.builder().put(excludeSetting, nodeNames.get(0))));
        assertAcked(client().admin().cluster().prepareReroute().add(new CancelAllocationCommand("index", 0, nodeNames.get(0), true)));
        assertThat(client().admin().cluster().prepareHealth("index").get().getUnassignedShards(), equalTo(1));

        assertThat(client().prepareDelete("index", indexResponse.getId()).get().status(), equalTo(RestStatus.OK));

        assertAcked(client().execute(FreezeIndexAction.INSTANCE,
                new FreezeRequest("index").waitForActiveShards(ActiveShardCount.ONE)).actionGet());

        assertThat(client().admin().cluster().prepareState().get().getState().metadata().index("index").getTimestampMillisRange(),
                sameInstance(IndexLongFieldRange.EMPTY));

        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(nodeNames.get(1)));
        assertThat(client().admin().cluster().prepareHealth("index").get().getUnassignedShards(), equalTo(2));
        assertAcked(client().admin().indices().prepareUpdateSettings("index")
                .setSettings(Settings.builder().putNull(excludeSetting)));
        assertThat(client().admin().cluster().prepareHealth("index").get().getUnassignedShards(), equalTo(2));

        assertAcked(client().admin().cluster().prepareReroute().add(
                new AllocateStalePrimaryAllocationCommand("index", 0, nodeNames.get(0), true)));

        ensureYellowAndNoInitializingShards("index");

        final IndexLongFieldRange timestampFieldRange
                = client().admin().cluster().prepareState().get().getState().metadata().index("index").getTimestampMillisRange();
        assertThat(timestampFieldRange, not(sameInstance(IndexLongFieldRange.UNKNOWN)));
        assertThat(timestampFieldRange, not(sameInstance(IndexLongFieldRange.EMPTY)));
        assertTrue(timestampFieldRange.isComplete());
        assertThat(timestampFieldRange.getMin(), equalTo(Instant.parse("2010-01-06T02:03:04.567Z").getMillis()));
        assertThat(timestampFieldRange.getMax(), equalTo(Instant.parse("2010-01-06T02:03:04.567Z").getMillis()));
    }

}
