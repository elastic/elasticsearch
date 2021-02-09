/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SnapshotsInProgressSerializationTests extends AbstractDiffableWireSerializationTestCase<Custom> {

    @Override
    protected Custom createTestInstance() {
        int numberOfSnapshots = randomInt(10);
        List<Entry> entries = new ArrayList<>();
        for (int i = 0; i < numberOfSnapshots; i++) {
            entries.add(randomSnapshot());
        }
        return SnapshotsInProgress.of(entries);
    }

    private Entry randomSnapshot() {
        Snapshot snapshot = new Snapshot(randomAlphaOfLength(10), new SnapshotId(randomAlphaOfLength(10), randomAlphaOfLength(10)));
        boolean includeGlobalState = randomBoolean();
        boolean partial = randomBoolean();
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
        List<String> dataStreams = Arrays.asList(generateRandomStringArray(10, 10, false));
        for (Index idx : esIndices) {
            int shardsCount = randomIntBetween(1, 10);
            for (int j = 0; j < shardsCount; j++) {
                ShardId shardId = new ShardId(idx, j);
                String nodeId = randomAlphaOfLength(10);
                ShardState shardState = randomFrom(ShardState.values());
                builder.put(shardId,
                        shardState == ShardState.QUEUED ? SnapshotsInProgress.ShardSnapshotStatus.UNASSIGNED_QUEUED :
                                new SnapshotsInProgress.ShardSnapshotStatus(nodeId, shardState,
                                        shardState.failed() ? randomAlphaOfLength(10) : null, "1"));
            }
        }
        ImmutableOpenMap<ShardId, SnapshotsInProgress.ShardSnapshotStatus> shards = builder.build();
        return new Entry(snapshot, includeGlobalState, partial, randomState(shards), indices, dataStreams,
                startTime, repositoryStateId, shards, null, SnapshotInfoTests.randomUserMetadata(), VersionUtils.randomVersion(random()));
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
                    final Entry entry = entries.get(i);
                    entries.set(i, mutateEntry(entry));
                }
            }
        }
        return SnapshotsInProgress.of(entries);
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
        if (false || entries.isEmpty()) {
            // add or remove an entry
            boolean addEntry = entries.isEmpty() ? true : randomBoolean();
            if (addEntry) {
                entries.add(randomSnapshot());
            } else {
                entries.remove(randomIntBetween(0, entries.size() - 1));
            }
        } else {
            // mutate an entry
            int index = randomIntBetween(0, entries.size() - 1);
            Entry entry = entries.get(index);
            entries.set(index, mutateEntry(entry));
        }
        return SnapshotsInProgress.of(entries);
    }

    private Entry mutateEntry(Entry entry) {
        switch (randomInt(7)) {
            case 0:
                boolean includeGlobalState = entry.includeGlobalState() == false;
                return new Entry(entry.snapshot(), includeGlobalState, entry.partial(), entry.state(), entry.indices(), entry.dataStreams(),
                    entry.startTime(), entry.repositoryStateId(), entry.shards(), entry.failure(), entry.userMetadata(), entry.version());
            case 1:
                boolean partial = entry.partial() == false;
                return new Entry(entry.snapshot(), entry.includeGlobalState(), partial, entry.state(), entry.indices(), entry.dataStreams(),
                    entry.startTime(), entry.repositoryStateId(), entry.shards(), entry.failure(), entry.userMetadata(), entry.version());
            case 2:
                List<String> dataStreams = Stream.concat(
                    entry.dataStreams().stream(),
                    Stream.of(randomAlphaOfLength(10)))
                    .collect(Collectors.toList());
                return new Entry(entry.snapshot(), entry.includeGlobalState(), entry.partial(), entry.state(), entry.indices(),
                    dataStreams, entry.startTime(), entry.repositoryStateId(), entry.shards(), entry.failure(), entry.userMetadata(),
                    entry.version());
            case 3:
                long startTime = randomValueOtherThan(entry.startTime(), ESTestCase::randomLong);
                return new Entry(entry.snapshot(), entry.includeGlobalState(), entry.partial(), entry.state(), entry.indices(),
                    entry.dataStreams(), startTime, entry.repositoryStateId(), entry.shards(), entry.failure(), entry.userMetadata(),
                    entry.version());
            case 4:
                long repositoryStateId = randomValueOtherThan(entry.startTime(), ESTestCase::randomLong);
                return new Entry(entry.snapshot(), entry.includeGlobalState(), entry.partial(), entry.state(), entry.indices(),
                    entry.dataStreams(), entry.startTime(), repositoryStateId, entry.shards(), entry.failure(), entry.userMetadata(),
                    entry.version());
            case 5:
                String failure = randomValueOtherThan(entry.failure(), () -> randomAlphaOfLengthBetween(2, 10));
                return new Entry(entry.snapshot(), entry.includeGlobalState(), entry.partial(), entry.state(), entry.indices(),
                    entry.dataStreams(), entry.startTime(), entry.repositoryStateId(), entry.shards(), failure, entry.userMetadata(),
                    entry.version());
            case 6:
                List<IndexId> indices = entry.indices();
                ImmutableOpenMap<ShardId, SnapshotsInProgress.ShardSnapshotStatus> shards = entry.shards();
                IndexId indexId = new IndexId(randomAlphaOfLength(10), randomAlphaOfLength(10));
                indices.add(indexId);
                ImmutableOpenMap.Builder<ShardId, SnapshotsInProgress.ShardSnapshotStatus> builder = ImmutableOpenMap.builder(shards);
                Index index = new Index(indexId.getName(), randomAlphaOfLength(10));
                int shardsCount = randomIntBetween(1, 10);
                for (int j = 0; j < shardsCount; j++) {
                    ShardId shardId = new ShardId(index, j);
                    String nodeId = randomAlphaOfLength(10);
                    ShardState shardState = randomFrom(ShardState.values());
                    builder.put(shardId,
                        shardState == ShardState.QUEUED ? SnapshotsInProgress.ShardSnapshotStatus.UNASSIGNED_QUEUED :
                            new SnapshotsInProgress.ShardSnapshotStatus(nodeId, shardState,
                                shardState.failed() ? randomAlphaOfLength(10) : null, "1"));
                }
                shards = builder.build();
                return new Entry(entry.snapshot(), entry.includeGlobalState(), entry.partial(), randomState(shards), indices,
                    entry.dataStreams(), entry.startTime(), entry.repositoryStateId(), shards, entry.failure(), entry.userMetadata(),
                    entry.version());
            case 7:
                Map<String, Object> userMetadata = entry.userMetadata() != null ? new HashMap<>(entry.userMetadata()) : new HashMap<>();
                String key = randomAlphaOfLengthBetween(2, 10);
                if (userMetadata.containsKey(key)) {
                    userMetadata.remove(key);
                } else {
                    userMetadata.put(key, randomAlphaOfLengthBetween(2, 10));
                }
                return new Entry(entry.snapshot(), entry.includeGlobalState(), entry.partial(), entry.state(), entry.indices(),
                    entry.dataStreams(), entry.startTime(), entry.repositoryStateId(), entry.shards(), entry.failure(), userMetadata,
                    entry.version());
            default:
                throw new IllegalArgumentException("invalid randomization case");
        }
    }

    public static State randomState(ImmutableOpenMap<ShardId, SnapshotsInProgress.ShardSnapshotStatus> shards) {
        return SnapshotsInProgress.completed(shards.values())
                ? randomFrom(State.SUCCESS, State.FAILED) : randomFrom(State.STARTED, State.INIT, State.ABORTED);
    }
}
