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

import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.ClusterState.Custom;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.SnapshotsInProgress.Entry;
import org.elasticsearch.cluster.SnapshotsInProgress.ShardState;
import org.elasticsearch.cluster.SnapshotsInProgress.State;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.test.AbstractDiffableWireSerializationTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class SnapshotsInProgressSerializationTests extends AbstractDiffableWireSerializationTestCase<Custom> {

    @Override
    protected Custom createTestInstance() {
        int numberOfSnapshots = randomInt(10);
        List<Entry> entries = new ArrayList<>();
        for (int i = 0; i < numberOfSnapshots; i++) {
            entries.add(randomSnapshot());
        }
        return new SnapshotsInProgress(entries);
    }

    private Entry randomSnapshot() {
        Snapshot snapshot = new Snapshot(randomAlphaOfLength(10), new SnapshotId(randomAlphaOfLength(10), randomAlphaOfLength(10)));
        boolean includeGlobalState = randomBoolean();
        boolean partial = randomBoolean();
        State state = randomFrom(State.values());
        int numberOfIndices = randomIntBetween(0, 10);
        List<IndexId> indices = new ArrayList<>();
        for (int i = 0; i < numberOfIndices; i++) {
            indices.add(new IndexId(randomAlphaOfLength(10), randomAlphaOfLength(10)));
        }
        long startTime = randomLong();
        long repositoryStateId = randomLong();
        ImmutableOpenMap.Builder<ShardId, SnapshotsInProgress.ShardSnapshotStatus> builder = ImmutableOpenMap.builder();
        final List<Index> esIndices =
            indices.stream().map(i -> new Index(i.getName(), randomAlphaOfLength(10))).collect(Collectors.toList());
        for (Index idx : esIndices) {
            int shardsCount = randomIntBetween(1, 10);
            for (int j = 0; j < shardsCount; j++) {
                ShardId shardId = new ShardId(idx, j);
                String nodeId = randomAlphaOfLength(10);
                ShardState shardState = randomFrom(ShardState.values());
                builder.put(shardId, new SnapshotsInProgress.ShardSnapshotStatus(nodeId, shardState,
                    shardState.failed() ? randomAlphaOfLength(10) : null, "1"));
            }
        }
        ImmutableOpenMap<ShardId, SnapshotsInProgress.ShardSnapshotStatus> shards = builder.build();
        return new Entry(snapshot, includeGlobalState, partial, state, indices, startTime, repositoryStateId, shards,
            SnapshotInfoTests.randomUserMetadata(), randomBoolean());
    }

    @Override
    protected Writeable.Reader<Custom> instanceReader() {
        return SnapshotsInProgress::new;
    }

    @Override
    protected Custom makeTestChanges(Custom testInstance) {
        SnapshotsInProgress snapshots = (SnapshotsInProgress) testInstance;
        List<Entry> entries = new ArrayList<>(snapshots.entries());
        if (randomBoolean() && entries.size() > 1) {
            // remove some elements
            int leaveElements = randomIntBetween(0, entries.size() - 1);
            entries = randomSubsetOf(leaveElements, entries.toArray(new Entry[leaveElements]));
        }
        if (randomBoolean()) {
            // add some elements
            int addElements = randomInt(10);
            for (int i = 0; i < addElements; i++) {
                entries.add(randomSnapshot());
            }
        }
        if (randomBoolean()) {
            // modify some elements
            for (int i = 0; i < entries.size(); i++) {
                if (randomBoolean()) {
                    entries.set(i, new Entry(entries.get(i), randomFrom(State.values()), entries.get(i).shards()));
                }
            }
        }
        return new SnapshotsInProgress(entries);
    }

    @Override
    protected Writeable.Reader<Diff<Custom>> diffReader() {
        return SnapshotsInProgress::readDiffFrom;
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(ClusterModule.getNamedWriteables());
    }

    @Override
    protected Custom mutateInstance(Custom instance) {
        List<Entry> entries = new ArrayList<>(((SnapshotsInProgress) instance).entries());
        boolean addEntry = entries.isEmpty() ? true : randomBoolean();
        if (addEntry) {
            entries.add(randomSnapshot());
        } else {
            entries.remove(randomIntBetween(0, entries.size() - 1));
        }
        return new SnapshotsInProgress(entries);
    }

}
