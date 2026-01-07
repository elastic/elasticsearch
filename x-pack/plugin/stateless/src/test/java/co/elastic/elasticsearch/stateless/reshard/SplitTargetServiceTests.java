/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless.reshard;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Map;
import java.util.Set;

import static org.mockito.Mockito.mock;

public class SplitTargetServiceTests extends ESTestCase {
    public void testAcceptHandoff() {
        var threadPool = mock(ThreadPool.class);
        var clusterService = mock(ClusterService.class);
        var reshardIndexService = mock(ReshardIndexService.class);
        var sts = new SplitTargetService(Settings.EMPTY, new NoOpClient(threadPool), clusterService, reshardIndexService);

        var shardId = new ShardId("index", "1", 0);
        var sourceNode = new DiscoveryNode(
            "node1",
            "node_1",
            new TransportAddress(TransportAddress.META_ADDRESS, 10000),
            Map.of(),
            Set.of(),
            null
        );
        var targetNode = new DiscoveryNode(
            "node2",
            "node_2",
            new TransportAddress(TransportAddress.META_ADDRESS, 10001),
            Map.of(),
            Set.of(),
            null
        );
        var indexShard = mock(IndexShard.class);
        var split = new SplitTargetService.Split(shardId, sourceNode, targetNode, 2, 2);
        sts.initializeSplitInCloneState(indexShard, split);

        // Any requests to handoff that do not exactly match stored split information should be rejected.
        var newShardId = new ShardId("foo", "1", 0);
        var request = new TransportReshardSplitAction.Request(newShardId, sourceNode, targetNode, 2, 2);
        assertThrows(IllegalStateException.class, () -> sts.acceptHandoff(indexShard, request, ActionListener.noop()));

        var newSourceNode = new DiscoveryNode(
            "node10",
            "node_1",
            new TransportAddress(TransportAddress.META_ADDRESS, 10000),
            Map.of(),
            Set.of(),
            null
        );
        var request2 = new TransportReshardSplitAction.Request(new ShardId("foo", "1", 0), newSourceNode, targetNode, 2, 2);
        assertThrows(IllegalStateException.class, () -> sts.acceptHandoff(indexShard, request2, ActionListener.noop()));

        var newTargetNode = new DiscoveryNode(
            "node2",
            "node_22",
            new TransportAddress(TransportAddress.META_ADDRESS, 10001),
            Map.of(),
            Set.of(),
            null
        );
        var request3 = new TransportReshardSplitAction.Request(new ShardId("foo", "1", 0), sourceNode, newTargetNode, 2, 2);
        assertThrows(IllegalStateException.class, () -> sts.acceptHandoff(indexShard, request3, ActionListener.noop()));

        var request4 = new TransportReshardSplitAction.Request(new ShardId("foo", "1", 0), sourceNode, targetNode, 100, 2);
        assertThrows(IllegalStateException.class, () -> sts.acceptHandoff(indexShard, request4, ActionListener.noop()));

        var request5 = new TransportReshardSplitAction.Request(new ShardId("foo", "1", 0), sourceNode, targetNode, 2, 100);
        assertThrows(IllegalStateException.class, () -> sts.acceptHandoff(indexShard, request5, ActionListener.noop()));
    }
}
