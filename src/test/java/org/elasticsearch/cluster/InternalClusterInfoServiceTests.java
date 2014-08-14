/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.cluster;

import com.google.common.collect.ImmutableSet;
import org.elasticsearch.action.admin.cluster.node.stats.TransportNodesStatsAction;
import org.elasticsearch.action.admin.indices.stats.*;
import org.elasticsearch.action.admin.indices.stats.CommonStatsFlags.Flag;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.service.IndexShard;
import org.elasticsearch.index.store.StoreStats;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.node.service.NodeService;
import org.elasticsearch.node.settings.NodeSettingsService;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;

/**
 * Test for InternalClsuterInfoService's behavior. A bit specific and navel
 * gaze-y because of all the mocks but gets the job done. Checks that jobs only
 * run on nodes we expect them to run on and that the shard size storage works
 * as expected.
 */
public class InternalClusterInfoServiceTests extends ElasticsearchTestCase {
    private ImmutableSettings settings;
    private NodeSettingsService nodeSettingsService;
    private TransportNodesStatsAction nodesStatsAction;
    private TransportIndicesStatsAction indicesStatsAction;
    private ClusterService clusterService;
    private ThreadPool threadPool;
    private InternalClusterInfoService service;
    private TransportService transportService;
    private NodeService nodeService;
    private IndicesService indicesService;

    @Test
    public void onlyMasterSchedulesJobs() {
        verify(threadPool, never()).schedule(any(TimeValue.class), anyString(), any(Runnable.class));

        service.onMaster();
        verify(threadPool, times(1)).schedule(any(TimeValue.class), anyString(), any(Runnable.class));

        service.offMaster();
        verify(threadPool, times(1)).schedule(any(TimeValue.class), anyString(), any(Runnable.class));

        service.onMaster();
        verify(threadPool, times(2)).schedule(any(TimeValue.class), anyString(), any(Runnable.class));
    }

    @Test
    public void onlyMasterReschedulesJobs() {
        // Snag the reschedule job
        service.onMaster();
        ArgumentCaptor<Runnable> rescheduleJob = ArgumentCaptor.forClass(Runnable.class);
        verify(threadPool, times(1)).schedule(any(TimeValue.class), anyString(), rescheduleJob.capture());

        // Now snag the update job
        rescheduleJob.getValue().run();
        ArgumentCaptor<Runnable> updateJob = ArgumentCaptor.forClass(Runnable.class);
        verify(threadPool.executor(anyString())).execute(updateJob.capture());

        // Now execute the update job and make sure it reschedules if the node is still a master
        boolean demaster = getRandom().nextBoolean();
        if (demaster) {
            service.offMaster();
        }
        updateJob.getValue().run();
        verify(threadPool, demaster ? times(1) : times(2)).schedule(any(TimeValue.class), anyString(), rescheduleJob.capture());
    }

    @Test
    public void indicesStatsCalculateCorrectly() {
        List<ShardStats> shards = new ArrayList<>();
        shards.add(stats("1", 0, true, "1mb"));
        shards.add(stats("1", 0, false, "1.2mb"));
        shards.add(stats("1", 0, false, "1.5mb"));
        shards.add(stats("1", 1, true, "2mb"));
        shards.add(stats("1", 1, false, "2.1mb"));
        shards.add(stats("1", 1, false, "1.9mb"));
        shards.add(stats("2", 0, true, "100gb"));

        IndicesStatsResponse response = mock(IndicesStatsResponse.class);
        when(response.getShards()).thenReturn(shards.toArray(new ShardStats[shards.size()]));

        // Trick the service into getting an empty metadata
        service.clusterChanged(mock(ClusterChangedEvent.class, RETURNS_DEEP_STUBS));
        service.updateIndicesStats(response);
        ClusterInfo info = service.getClusterInfo();

        assertEquals((Long)ByteSizeValue.parseBytesSizeValue("1mb").bytes(), info.getShardSizes().get("[1][0][p]"));
        // The shard size is actually the last shard size with the same id.  Maybe it should be the max?
        assertEquals((Long)ByteSizeValue.parseBytesSizeValue("1.5mb").bytes(), info.getShardSizes().get("[1][0][r]"));
        assertEquals(6, info.getShardSizeBinToShard().get(0).size());
        assertEquals(averageShardSize("1mb", "1.2mb", "1.5mb", "2mb", "2.1mb", "1.9mb"), info.getIndexToAverageShardSize().get("1"));

        assertEquals((Long)ByteSizeValue.parseBytesSizeValue("100gb").bytes(), info.getShardSizes().get("[2][0][p]"));
        assertNull(info.getShardSizes().get("[2][0][r]"));
        assertEquals(1, info.getShardSizeBinToShard().get(11).size());
        assertEquals((Long)ByteSizeValue.parseBytesSizeValue("100gb").bytes(), info.getIndexToAverageShardSize().get("2"));

    }

    private ShardStats stats(String index, int shardId, boolean primary, String size) {
        IndexShard indexShard = mock(IndexShard.class);
        ShardRouting routingEntry = mock(ShardRouting.class);
        when(indexShard.routingEntry()).thenReturn(routingEntry);
        when(routingEntry.index()).thenReturn(index);
        when(routingEntry.shardId()).thenReturn(new ShardId(index, shardId));
        when(routingEntry.primary()).thenReturn(primary);
        StoreStats stats = new StoreStats(ByteSizeValue.parseBytesSizeValue(size).bytes(), 0);
        when(indexShard.storeStats()).thenReturn(stats);
        return new ShardStats(indexShard, new CommonStatsFlags(Flag.Store));
    }

    private Long averageShardSize(String... sizes) {
        long total = 0;
        for (String size: sizes) {
            total += ByteSizeValue.parseBytesSizeValue(size).bytes();
        }
        return total / sizes.length;
    }
    @Before
    public void setup() {
        ClusterName clusterName = new ClusterName("test");
        settings = (ImmutableSettings) ImmutableSettings.builder().build();
        nodeSettingsService = new NodeSettingsService(settings);
        clusterService = mock(ClusterService.class, RETURNS_DEEP_STUBS);
        threadPool = mock(ThreadPool.class, RETURNS_DEEP_STUBS);
        transportService = mock(TransportService.class);
        nodeService = mock(NodeService.class);
        indicesService = mock(IndicesService.class);

        // Make sure there are enough data nodes to run the job. Can't mock the
        // map because it is final.
        ImmutableOpenMap.Builder<String, DiscoveryNode> dataNodes = ImmutableOpenMap.builder();
        when(clusterService.state().getNodes().getDataNodes()).thenReturn(dataNodes.build());

        // Setup actions - these can't be mocked due to final methods.
        ActionFilters actionFilters = new ActionFilters(ImmutableSet.<ActionFilter> of());
        nodesStatsAction = new TransportNodesStatsAction(settings, clusterName, threadPool, clusterService, transportService, nodeService,
                actionFilters);
        indicesStatsAction = new TransportIndicesStatsAction(settings, threadPool, clusterService, transportService, indicesService,
                actionFilters);

        service = new InternalClusterInfoService(settings, nodeSettingsService, nodesStatsAction,
                indicesStatsAction, clusterService, threadPool);
    }
}
