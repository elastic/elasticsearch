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

package org.elasticsearch.snapshots;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.RepositoryShardId;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.hamcrest.Matchers.is;

public class SnapshotsServiceTests extends ESTestCase {

    public void testNoopShardStateUpdates() throws Exception {
        final String repoName = "test-repo";
        final long repoStateId = randomNonNegativeLong();
        final Snapshot snapshot = snapshot(repoName, "snapshot-1");
        final SnapshotsInProgress.Entry snapshotNoShards = SnapshotsInProgress.startedEntry(snapshot, randomBoolean(), randomBoolean(),
                Collections.emptyList(), Collections.emptyList(), 1L, repoStateId,
                ImmutableOpenMap.of(), Collections.emptyMap(), Version.CURRENT);

        final String indexName1 = "index-1";
        final ShardId shardId1 = new ShardId(index(indexName1), 0);
        {
            final ClusterState state = stateWithSnapshots(snapshotNoShards);
            final SnapshotsService.ShardSnapshotUpdate shardCompletion =
                    new SnapshotsService.ShardSnapshotUpdate(snapshot, shardId1, successfulShardStatus(uuid()));
            assertIsNoop(state, shardCompletion);
        }

        final SnapshotsInProgress.Entry snapshotInProgress =
                snapshotEntry(snapshot, Collections.singletonList(indexId(indexName1)), shardsMap(shardId1, initShardStatus(uuid())));

        {
            final ClusterState state = stateWithSnapshots(snapshotInProgress);
            final SnapshotsService.ShardSnapshotUpdate shardCompletion = new SnapshotsService.ShardSnapshotUpdate(
                    snapshot("other-repo", snapshot.getSnapshotId().getName()), shardId1, successfulShardStatus(uuid()));
            assertIsNoop(state, shardCompletion);
        }
    }

    public void testUpdateSnapshotToSuccess() throws Exception {
        final String repoName = "test-repo";
        final Snapshot sn1 = snapshot(repoName, "snapshot-1");
        final String indexName1 = "index-1";
        final String dataNodeId = uuid();
        final IndexId indexId1 = indexId(indexName1);
        final ShardId shardId1 = new ShardId(index(indexName1), 0);
        final SnapshotsInProgress.Entry snapshotSingleShard =
                snapshotEntry(sn1, Collections.singletonList(indexId1), shardsMap(shardId1, initShardStatus(dataNodeId)));

        assertThat(snapshotSingleShard.state(), is(SnapshotsInProgress.State.STARTED));

        final SnapshotsService.ShardSnapshotUpdate completeShard = successUpdate(sn1, shardId1, dataNodeId);
        final ClusterState updatedClusterState = applyUpdates(stateWithSnapshots(snapshotSingleShard), completeShard);
        final SnapshotsInProgress snapshotsInProgress = updatedClusterState.custom(SnapshotsInProgress.TYPE);
        final SnapshotsInProgress.Entry updatedSnapshot1 = snapshotsInProgress.entries().get(0);
        assertThat(updatedSnapshot1.state(), is(SnapshotsInProgress.State.SUCCESS));
        assertIsNoop(updatedClusterState, completeShard);
    }

    public void testUpdateSnapshotMultipleShards() throws Exception {
        final String repoName = "test-repo";
        final Snapshot sn1 = snapshot(repoName, "snapshot-1");
        final String indexName1 = "index-1";
        final String dataNodeId = uuid();
        final IndexId indexId1 = indexId(indexName1);
        final Index routingIndex1 = index(indexName1);
        final ShardId shardId1 = new ShardId(routingIndex1, 0);
        final ShardId shardId2 = new ShardId(routingIndex1, 1);
        final SnapshotsInProgress.ShardSnapshotStatus shardInitStatus = initShardStatus(dataNodeId);
        final SnapshotsInProgress.Entry snapshotSingleShard = snapshotEntry(sn1, Collections.singletonList(indexId1),
                ImmutableOpenMap.builder(shardsMap(shardId1, shardInitStatus)).fPut(shardId2, shardInitStatus).build());

        assertThat(snapshotSingleShard.state(), is(SnapshotsInProgress.State.STARTED));

        final SnapshotsService.ShardSnapshotUpdate completeShard = successUpdate(sn1, shardId1, dataNodeId);
        final ClusterState updatedClusterState = applyUpdates(stateWithSnapshots(snapshotSingleShard), completeShard);
        final SnapshotsInProgress snapshotsInProgress = updatedClusterState.custom(SnapshotsInProgress.TYPE);
        final SnapshotsInProgress.Entry updatedSnapshot1 = snapshotsInProgress.entries().get(0);
        assertThat(updatedSnapshot1.state(), is(SnapshotsInProgress.State.STARTED));
        assertIsNoop(updatedClusterState, completeShard);
    }

    public void testUpdateCloneToSuccess() throws Exception {
        final String repoName = "test-repo";
        final Snapshot sourceSnapshot = snapshot(repoName, "source-snapshot");
        final Snapshot targetSnapshot = snapshot(repoName, "target-snapshot");
        final String indexName1 = "index-1";
        final String dataNodeId = uuid();
        final IndexId indexId1 = indexId(indexName1);
        final RepositoryShardId shardId1 = new RepositoryShardId(indexId1, 0);
        final SnapshotsInProgress.Entry snapshotSingleShard =
                cloneEntry(targetSnapshot, sourceSnapshot.getSnapshotId(), clonesMap(shardId1, initShardStatus(dataNodeId)));

        assertThat(snapshotSingleShard.state(), is(SnapshotsInProgress.State.STARTED));

        final SnapshotsService.ShardSnapshotUpdate completeShard = successUpdate(targetSnapshot, shardId1, dataNodeId);
        final ClusterState updatedClusterState = applyUpdates(stateWithSnapshots(snapshotSingleShard), completeShard);
        final SnapshotsInProgress snapshotsInProgress = updatedClusterState.custom(SnapshotsInProgress.TYPE);
        final SnapshotsInProgress.Entry updatedSnapshot1 = snapshotsInProgress.entries().get(0);
        assertThat(updatedSnapshot1.state(), is(SnapshotsInProgress.State.SUCCESS));
        assertIsNoop(updatedClusterState, completeShard);
    }

    public void testUpdateCloneMultipleShards() throws Exception {
        final String repoName = "test-repo";
        final Snapshot sourceSnapshot = snapshot(repoName, "source-snapshot");
        final Snapshot targetSnapshot = snapshot(repoName, "target-snapshot");
        final String indexName1 = "index-1";
        final String dataNodeId = uuid();
        final IndexId indexId1 = indexId(indexName1);
        final RepositoryShardId shardId1 = new RepositoryShardId(indexId1, 0);
        final RepositoryShardId shardId2 = new RepositoryShardId(indexId1, 1);
        final SnapshotsInProgress.ShardSnapshotStatus shardInitStatus = initShardStatus(dataNodeId);
        final SnapshotsInProgress.Entry snapshotSingleShard = cloneEntry(targetSnapshot, sourceSnapshot.getSnapshotId(),
                ImmutableOpenMap.builder(clonesMap(shardId1, shardInitStatus)).fPut(shardId2, shardInitStatus).build());

        assertThat(snapshotSingleShard.state(), is(SnapshotsInProgress.State.STARTED));

        final SnapshotsService.ShardSnapshotUpdate completeShard = successUpdate(targetSnapshot, shardId1, dataNodeId);
        final ClusterState updatedClusterState = applyUpdates(stateWithSnapshots(snapshotSingleShard), completeShard);
        final SnapshotsInProgress snapshotsInProgress = updatedClusterState.custom(SnapshotsInProgress.TYPE);
        final SnapshotsInProgress.Entry updatedSnapshot1 = snapshotsInProgress.entries().get(0);
        assertThat(updatedSnapshot1.state(), is(SnapshotsInProgress.State.STARTED));
        assertIsNoop(updatedClusterState, completeShard);
    }

    private static ImmutableOpenMap<ShardId, SnapshotsInProgress.ShardSnapshotStatus> shardsMap(
            ShardId shardId, SnapshotsInProgress.ShardSnapshotStatus shardStatus) {
        return ImmutableOpenMap.<ShardId, SnapshotsInProgress.ShardSnapshotStatus>builder().fPut(shardId, shardStatus).build();
    }

    private static ImmutableOpenMap<RepositoryShardId, SnapshotsInProgress.ShardSnapshotStatus> clonesMap(
            RepositoryShardId shardId, SnapshotsInProgress.ShardSnapshotStatus shardStatus) {
        return ImmutableOpenMap.<RepositoryShardId, SnapshotsInProgress.ShardSnapshotStatus>builder().fPut(shardId, shardStatus).build();
    }

    private static SnapshotsService.ShardSnapshotUpdate successUpdate(Snapshot snapshot, ShardId shardId, String nodeId) {
        return new SnapshotsService.ShardSnapshotUpdate(snapshot, shardId, successfulShardStatus(nodeId));
    }

    private static SnapshotsService.ShardSnapshotUpdate successUpdate(Snapshot snapshot, RepositoryShardId shardId, String nodeId) {
        return new SnapshotsService.ShardSnapshotUpdate(snapshot, shardId, successfulShardStatus(nodeId));
    }

    private static ClusterState stateWithSnapshots(SnapshotsInProgress.Entry... entries) {
        return ClusterState.builder(ClusterState.EMPTY_STATE).version(randomNonNegativeLong())
                .putCustom(SnapshotsInProgress.TYPE, SnapshotsInProgress.of(Arrays.asList(entries))).build();
    }

    private static void assertIsNoop(ClusterState state, SnapshotsService.ShardSnapshotUpdate shardCompletion) throws Exception {
        assertSame(applyUpdates(state, shardCompletion), state);
    }

    private static ClusterState applyUpdates(ClusterState state, SnapshotsService.ShardSnapshotUpdate... updates) throws Exception {
        return SnapshotsService.SHARD_STATE_EXECUTOR.execute(state, Arrays.asList(updates)).resultingState;
    }

    private static SnapshotsInProgress.Entry snapshotEntry(Snapshot snapshot, List<IndexId> indexIds,
                                                           ImmutableOpenMap<ShardId, SnapshotsInProgress.ShardSnapshotStatus> shards) {
        return SnapshotsInProgress.startedEntry(snapshot, randomBoolean(), randomBoolean(), indexIds, Collections.emptyList(),
                1L, randomNonNegativeLong(), shards, Collections.emptyMap(), Version.CURRENT);
    }

    private static SnapshotsInProgress.Entry cloneEntry(
            Snapshot snapshot, SnapshotId source, ImmutableOpenMap<RepositoryShardId, SnapshotsInProgress.ShardSnapshotStatus> clones) {
        final List<IndexId> indexIds = StreamSupport.stream(clones.keys().spliterator(), false)
                .map(k -> k.value.index()).distinct().collect(Collectors.toList());
        return SnapshotsInProgress.startClone(snapshot, source, indexIds, 1L, randomNonNegativeLong(), Version.CURRENT).withClones(clones);
    }

    private static SnapshotsInProgress.ShardSnapshotStatus initShardStatus(String nodeId) {
        return new SnapshotsInProgress.ShardSnapshotStatus(nodeId, uuid());
    }

    private static SnapshotsInProgress.ShardSnapshotStatus successfulShardStatus(String nodeId) {
        return new SnapshotsInProgress.ShardSnapshotStatus(nodeId, SnapshotsInProgress.ShardState.SUCCESS, uuid());
    }

    private static Snapshot snapshot(String repoName, String name) {
        return new Snapshot(repoName, new SnapshotId(name, uuid()));
    }

    private static Index index(String name) {
        return new Index(name, uuid());
    }

    private static IndexId indexId(String name) {
        return new IndexId(name, uuid());
    }

    private static String uuid() {
        return UUIDs.randomBase64UUID(random());
    }
}
