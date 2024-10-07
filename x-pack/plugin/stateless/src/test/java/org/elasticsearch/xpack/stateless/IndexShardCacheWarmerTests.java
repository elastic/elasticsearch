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

package co.elastic.elasticsearch.stateless;

import co.elastic.elasticsearch.stateless.cache.SharedBlobCacheWarmingService;
import co.elastic.elasticsearch.stateless.objectstore.ObjectStoreService;

import org.apache.logging.log4j.Level;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.core.Strings;
import org.elasticsearch.index.shard.IndexShardTestCase;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.test.MockLog;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class IndexShardCacheWarmerTests extends IndexShardTestCase {

    public void testPreWarmIndexShardCacheWhenShardIsClosedBeforePrewarmingStarted() throws Exception {

        var taskQueue = new DeterministicTaskQueue();
        var indexShard = newShard(true);

        var sharedBlobCacheWarmingService = mock(SharedBlobCacheWarmingService.class);
        var indexShardCacheWarmer = new IndexShardCacheWarmer(
            mock(ObjectStoreService.class),
            sharedBlobCacheWarmingService,
            taskQueue.getThreadPool()
        );

        // Prepare shard routing `role` to correspond to indexing shard setup in Serverless
        var copyShardRoutingWithIndexOnlyRole = new TestShardRouting.Builder(
            indexShard.shardId(),
            indexShard.routingEntry().currentNodeId(),
            indexShard.routingEntry().primary(),
            indexShard.routingEntry().state()
        ).withAllocationId(indexShard.routingEntry().allocationId())
            .withRecoverySource(RecoverySource.PeerRecoverySource.INSTANCE)
            .withRole(ShardRouting.Role.INDEX_ONLY)
            .build();

        updateRoutingEntry(indexShard, copyShardRoutingWithIndexOnlyRole);

        indexShard.markAsRecovering(
            "simulated",
            new RecoveryState(
                indexShard.routingEntry(),
                DiscoveryNodeUtils.builder("index-node-target").build(),
                DiscoveryNodeUtils.builder("index-node-source").build()
            )
        );

        indexShardCacheWarmer.preWarmIndexShardCache(indexShard);

        var store = indexShard.store();
        // Created shard without any store level operations has refCount = 1
        assertThat(store.refCount(), equalTo(1));
        // Decrement explicitly ref count to simulate shard/store closing
        // IndexShard#close is not used here since it would require opening an engine beforehand (to attach it to IndexShard `store`)
        assertThat(store.decRef(), is(true));

        taskQueue.runAllTasks();
        verify(sharedBlobCacheWarmingService, never()).warmCacheForShardRecovery(any(), any(), any(), any());
    }

    public void testLogErrorIfPrewarmingFailed() throws Exception {
        var taskQueue = new DeterministicTaskQueue();
        var indexShard = newShard(true);

        var objectStoreService = mock(ObjectStoreService.class);
        // simulate any blob store/bob container runtime error
        when(objectStoreService.blobStore()).thenThrow(new RuntimeException("simulated"));

        var indexShardCacheWarmer = new IndexShardCacheWarmer(objectStoreService, null, taskQueue.getThreadPool());

        // Prepare shard routing `role` to correspond to indexing shard setup in Serverless
        var copyShardRoutingWithIndexOnlyRole = new TestShardRouting.Builder(
            indexShard.shardId(),
            indexShard.routingEntry().currentNodeId(),
            indexShard.routingEntry().primary(),
            indexShard.routingEntry().state()
        ).withAllocationId(indexShard.routingEntry().allocationId())
            .withRecoverySource(RecoverySource.PeerRecoverySource.INSTANCE)
            .withRole(ShardRouting.Role.INDEX_ONLY)
            .build();

        updateRoutingEntry(indexShard, copyShardRoutingWithIndexOnlyRole);

        indexShard.markAsRecovering(
            "simulated",
            new RecoveryState(
                indexShard.routingEntry(),
                DiscoveryNodeUtils.builder("index-node-target").build(),
                DiscoveryNodeUtils.builder("index-node-source").build()
            )
        );

        try (var mockLog = MockLog.capture(IndexShardCacheWarmer.class)) {
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "expected warn log about failed pre warming index shard cache",
                    IndexShardCacheWarmer.class.getName(),
                    Level.INFO,
                    Strings.format("%s early indexing cache prewarming failed", indexShard.shardId())
                )
            );
            indexShardCacheWarmer.preWarmIndexShardCache(indexShard);
            taskQueue.runAllTasks();
            mockLog.assertAllExpectationsMatched();
        } finally {
            closeShards(indexShard);
        }
    }
}
