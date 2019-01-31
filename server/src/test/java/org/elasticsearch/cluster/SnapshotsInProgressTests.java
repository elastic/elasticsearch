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

import org.elasticsearch.cluster.SnapshotsInProgress.Entry;
import org.elasticsearch.cluster.SnapshotsInProgress.ShardSnapshotStatus;
import org.elasticsearch.cluster.SnapshotsInProgress.State;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Unit tests for the {@link SnapshotsInProgress} class and its inner classes.
 */
public class SnapshotsInProgressTests extends ESTestCase {

    /**
     * Makes sure that the indices being waited on before snapshotting commences
     * are populated with all shards in the relocating or initializing state.
     */
    public void testWaitingIndices() {
        final Snapshot snapshot = new Snapshot("repo", new SnapshotId("snap", randomAlphaOfLength(5)));
        final String idx1Name = "idx1";
        final String idx2Name = "idx2";
        final String idx3Name = "idx3";
        final String idx1UUID = randomAlphaOfLength(5);
        final String idx2UUID = randomAlphaOfLength(5);
        final String idx3UUID = randomAlphaOfLength(5);
        final List<IndexId> indices = Arrays.asList(new IndexId(idx1Name, randomAlphaOfLength(5)),
            new IndexId(idx2Name, randomAlphaOfLength(5)), new IndexId(idx3Name, randomAlphaOfLength(5)));
        ImmutableOpenMap.Builder<ShardId, ShardSnapshotStatus> shards = ImmutableOpenMap.builder();

        // test more than one waiting shard in an index
        shards.put(new ShardId(idx1Name, idx1UUID, 0), new ShardSnapshotStatus(randomAlphaOfLength(2), State.WAITING));
        shards.put(new ShardId(idx1Name, idx1UUID, 1), new ShardSnapshotStatus(randomAlphaOfLength(2), State.WAITING));
        shards.put(new ShardId(idx1Name, idx1UUID, 2), new ShardSnapshotStatus(randomAlphaOfLength(2), randomNonWaitingState(), ""));
        // test exactly one waiting shard in an index
        shards.put(new ShardId(idx2Name, idx2UUID, 0), new ShardSnapshotStatus(randomAlphaOfLength(2), State.WAITING));
        shards.put(new ShardId(idx2Name, idx2UUID, 1), new ShardSnapshotStatus(randomAlphaOfLength(2), randomNonWaitingState(), ""));
        // test no waiting shards in an index
        shards.put(new ShardId(idx3Name, idx3UUID, 0), new ShardSnapshotStatus(randomAlphaOfLength(2), randomNonWaitingState(), ""));
        Entry entry = new Entry(snapshot, randomBoolean(), randomBoolean(), State.INIT,
                                indices, System.currentTimeMillis(), randomLong(), shards.build());

        ImmutableOpenMap<String, List<ShardId>> waitingIndices = entry.waitingIndices();
        assertEquals(2, waitingIndices.get(idx1Name).size());
        assertEquals(1, waitingIndices.get(idx2Name).size());
        assertFalse(waitingIndices.containsKey(idx3Name));
    }

    private State randomNonWaitingState() {
        return randomFrom(Arrays.stream(State.values()).filter(s -> s != State.WAITING).collect(Collectors.toSet()));
    }
}
