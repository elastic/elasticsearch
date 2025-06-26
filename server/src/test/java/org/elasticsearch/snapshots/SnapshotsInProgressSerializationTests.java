/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.snapshots;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterState.Custom;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.SnapshotsInProgress.Entry;
import org.elasticsearch.cluster.SnapshotsInProgress.ShardState;
import org.elasticsearch.cluster.SnapshotsInProgress.State;
import org.elasticsearch.cluster.metadata.NodesShutdownMetadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.cluster.version.CompatibilityVersions;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.RepositoryOperation.ProjectRepo;
import org.elasticsearch.repositories.ShardGeneration;
import org.elasticsearch.repositories.ShardSnapshotResult;
import org.elasticsearch.test.AbstractChunkedSerializingTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.SimpleDiffableWireSerializationTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.test.index.IndexVersionUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;

public class SnapshotsInProgressSerializationTests extends SimpleDiffableWireSerializationTestCase<Custom> {

    public static final ClusterState CLUSTER_STATE_FOR_NODE_SHUTDOWNS = ClusterState.builder(ClusterName.DEFAULT)
        .putCompatibilityVersions("local", new CompatibilityVersions(TransportVersion.current(), Map.of()))
        .build();

    @Override
    protected Custom createTestInstance() {
        return createTestInstance(() -> randomSnapshot(randomProjectIdOrDefault()));
    }

    private Custom createTestInstance(Supplier<Entry> randomEntrySupplier) {
        int numberOfSnapshots = randomInt(20);
        SnapshotsInProgress snapshotsInProgress = SnapshotsInProgress.EMPTY;
        for (int i = 0; i < numberOfSnapshots; i++) {
            snapshotsInProgress = snapshotsInProgress.withAddedEntry(randomEntrySupplier.get());
        }

        final var nodeIdsForRemoval = randomList(3, ESTestCase::randomUUID);
        if (nodeIdsForRemoval.isEmpty() == false) {
            snapshotsInProgress = snapshotsInProgress.withUpdatedNodeIdsForRemoval(
                getClusterStateWithNodeShutdownMetadata(nodeIdsForRemoval)
            );
        }

        return snapshotsInProgress;
    }

    public void testSerializationBwc() throws IOException {
        final var oldVersion = TransportVersionUtils.getPreviousVersion(TransportVersions.PROJECT_ID_IN_SNAPSHOT);
        final BytesStreamOutput out = new BytesStreamOutput();
        out.setTransportVersion(oldVersion);
        final Custom original = createTestInstance(() -> randomSnapshot(ProjectId.DEFAULT));
        original.writeTo(out);

        final var in = out.bytes().streamInput();
        in.setTransportVersion(oldVersion);
        final SnapshotsInProgress fromStream = new SnapshotsInProgress(in);
        assertThat(fromStream, equalTo(original));
    }

    public void testDiffSerializationBwc() throws IOException {
        final var oldVersion = TransportVersionUtils.getPreviousVersion(TransportVersions.PROJECT_ID_IN_SNAPSHOT);
        final BytesStreamOutput out = new BytesStreamOutput();
        out.setTransportVersion(oldVersion);

        final Custom before = createTestInstance(() -> randomSnapshot(ProjectId.DEFAULT));
        final Custom after = makeTestChanges(before, () -> randomSnapshot(ProjectId.DEFAULT));
        final Diff<Custom> diff = after.diff(before);
        diff.writeTo(out);

        final var in = out.bytes().streamInput();
        in.setTransportVersion(oldVersion);
        final NamedDiff<Custom> diffFromStream = SnapshotsInProgress.readDiffFrom(in);

        assertThat(diffFromStream.apply(before), equalTo(after));
    }

    private ClusterState getClusterStateWithNodeShutdownMetadata(List<String> nodeIdsForRemoval) {
        return CLUSTER_STATE_FOR_NODE_SHUTDOWNS.copyAndUpdateMetadata(
            mdb -> mdb.putCustom(
                NodesShutdownMetadata.TYPE,
                new NodesShutdownMetadata(
                    nodeIdsForRemoval.stream()
                        .collect(
                            Collectors.toMap(
                                Function.identity(),
                                nodeId -> SingleNodeShutdownMetadata.builder()
                                    .setType(SingleNodeShutdownMetadata.Type.REMOVE)
                                    .setNodeId(nodeId)
                                    .setNodeEphemeralId(nodeId)
                                    .setStartedAtMillis(randomNonNegativeLong())
                                    .setReason(getTestName())
                                    .build()
                            )
                        )
                )
            )
        );
    }

    private Entry randomSnapshot() {
        return randomSnapshot(randomProjectIdOrDefault());
    }

    private Entry randomSnapshot(ProjectId projectId) {
        Snapshot snapshot = new Snapshot(
            projectId,
            "repo-" + randomInt(5),
            new SnapshotId(randomAlphaOfLength(10), randomAlphaOfLength(10))
        );
        boolean includeGlobalState = randomBoolean();
        boolean partial = randomBoolean();
        int numberOfIndices = randomIntBetween(0, 10);
        Map<String, IndexId> indices = new HashMap<>();
        for (int i = 0; i < numberOfIndices; i++) {
            final String name = randomAlphaOfLength(10);
            indices.put(name, new IndexId(name, randomAlphaOfLength(10)));
        }
        long startTime = randomLong();
        long repositoryStateId = randomLong();
        Map<ShardId, SnapshotsInProgress.ShardSnapshotStatus> shards = new HashMap<>();
        final List<Index> esIndices = indices.keySet().stream().map(i -> new Index(i, randomAlphaOfLength(10))).toList();
        List<String> dataStreams = Arrays.asList(generateRandomStringArray(10, 10, false));
        for (Index idx : esIndices) {
            int shardsCount = randomIntBetween(1, 10);
            for (int j = 0; j < shardsCount; j++) {
                shards.put(new ShardId(idx, j), randomShardSnapshotStatus(randomAlphaOfLength(10)));
            }
        }
        List<SnapshotFeatureInfo> featureStates = randomList(5, SnapshotFeatureInfoTests::randomSnapshotFeatureInfo);
        return Entry.snapshot(
            snapshot,
            includeGlobalState,
            partial,
            randomState(shards),
            indices,
            dataStreams,
            featureStates,
            startTime,
            repositoryStateId,
            shards,
            null,
            SnapshotInfoTestUtils.randomUserMetadata(),
            IndexVersionUtils.randomVersion()
        );
    }

    private SnapshotsInProgress.ShardSnapshotStatus randomShardSnapshotStatus(String nodeId) {
        ShardState shardState = randomFrom(ShardState.values());
        if (shardState == ShardState.QUEUED) {
            return SnapshotsInProgress.ShardSnapshotStatus.UNASSIGNED_QUEUED;
        } else if (shardState == ShardState.SUCCESS) {
            final ShardSnapshotResult shardSnapshotResult = new ShardSnapshotResult(new ShardGeneration(1L), ByteSizeValue.ofBytes(1L), 1);
            return SnapshotsInProgress.ShardSnapshotStatus.success(nodeId, shardSnapshotResult);
        } else {
            final String reason = shardState.failed() ? randomAlphaOfLength(10) : null;
            return new SnapshotsInProgress.ShardSnapshotStatus(nodeId, shardState, new ShardGeneration(1L), reason);
        }
    }

    @Override
    protected Writeable.Reader<Custom> instanceReader() {
        return SnapshotsInProgress::new;
    }

    @Override
    protected Custom makeTestChanges(Custom testInstance) {
        return makeTestChanges(testInstance, () -> randomSnapshot(randomProjectIdOrDefault()));
    }

    protected Custom makeTestChanges(Custom testInstance, Supplier<Entry> randomEntrySupplier) {
        final SnapshotsInProgress snapshots = (SnapshotsInProgress) testInstance;
        SnapshotsInProgress updatedInstance = SnapshotsInProgress.EMPTY;
        if (randomBoolean() && snapshots.count() > 1) {
            // remove some elements
            int leaveElements = randomIntBetween(0, snapshots.count() - 1);
            for (List<Entry> entriesForRepo : snapshots.entriesByRepo()) {
                for (Entry entry : entriesForRepo) {
                    if (updatedInstance.count() == leaveElements) {
                        break;
                    }
                    if (randomBoolean()) {
                        updatedInstance = updatedInstance.withAddedEntry(entry);
                    }
                }
            }
        }
        if (randomBoolean()) {
            // add some elements
            int addElements = randomInt(10);
            for (int i = 0; i < addElements; i++) {
                updatedInstance = updatedInstance.withAddedEntry(randomEntrySupplier.get());
            }
        }
        if (randomBoolean()) {
            // modify some elements
            for (List<Entry> perRepoEntries : updatedInstance.entriesByRepo()) {
                List<Entry> entries = new ArrayList<>(perRepoEntries);
                for (int i = 0; i < entries.size(); i++) {
                    if (randomBoolean()) {
                        final Entry entry = entries.get(i);
                        entries.set(i, mutateEntryWithLegalChange(entry));
                    }
                }
                if (randomBoolean()) {
                    entries = shuffledList(entries);
                }
                final Entry firstEntry = perRepoEntries.get(0);
                updatedInstance = updatedInstance.createCopyWithUpdatedEntriesForRepo(
                    firstEntry.projectId(),
                    firstEntry.repository(),
                    entries
                );
            }
        }
        return updatedInstance;
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
        final SnapshotsInProgress snapshotsInProgress = (SnapshotsInProgress) instance;
        if (randomBoolean()) {
            if (snapshotsInProgress.isEmpty()) {
                // add an entry
                return snapshotsInProgress.withAddedEntry(randomSnapshot());
            } else {
                // mutate or remove an entry
                final var repo = randomFrom(
                    snapshotsInProgress.asStream()
                        .map(entry -> new ProjectRepo(entry.projectId(), entry.repository()))
                        .collect(Collectors.toSet())
                );
                final List<Entry> forRepo = snapshotsInProgress.forRepo(repo.projectId(), repo.name());
                int index = randomIntBetween(0, forRepo.size() - 1);
                Entry entry = forRepo.get(index);
                final List<Entry> updatedEntries = new ArrayList<>(forRepo);
                if (randomBoolean()) {
                    updatedEntries.set(index, mutateEntry(entry));
                } else {
                    updatedEntries.remove(index);
                }
                return snapshotsInProgress.createCopyWithUpdatedEntriesForRepo(repo.projectId(), repo.name(), updatedEntries);
            }
        } else {
            return snapshotsInProgress.withUpdatedNodeIdsForRemoval(
                getClusterStateWithNodeShutdownMetadata(randomList(1, 3, ESTestCase::randomUUID))
            );
        }
    }

    private Entry mutateEntry(Entry entry) {
        switch (randomInt(5)) {
            case 0 -> {
                boolean includeGlobalState = entry.includeGlobalState() == false;
                return Entry.snapshot(
                    entry.snapshot(),
                    includeGlobalState,
                    entry.partial(),
                    entry.state(),
                    entry.indices(),
                    entry.dataStreams(),
                    entry.featureStates(),
                    entry.repositoryStateId(),
                    entry.startTime(),
                    entry.shards(),
                    entry.failure(),
                    entry.userMetadata(),
                    entry.version()
                );
            }
            case 1 -> {
                boolean partial = entry.partial() == false;
                return Entry.snapshot(
                    entry.snapshot(),
                    entry.includeGlobalState(),
                    partial,
                    entry.state(),
                    entry.indices(),
                    entry.dataStreams(),
                    entry.featureStates(),
                    entry.startTime(),
                    entry.repositoryStateId(),
                    entry.shards(),
                    entry.failure(),
                    entry.userMetadata(),
                    entry.version()
                );
            }
            case 2 -> {
                long startTime = randomValueOtherThan(entry.startTime(), ESTestCase::randomLong);
                return Entry.snapshot(
                    entry.snapshot(),
                    entry.includeGlobalState(),
                    entry.partial(),
                    entry.state(),
                    entry.indices(),
                    entry.dataStreams(),
                    entry.featureStates(),
                    startTime,
                    entry.repositoryStateId(),
                    entry.shards(),
                    entry.failure(),
                    entry.userMetadata(),
                    entry.version()
                );
            }
            case 3 -> {
                Map<String, Object> userMetadata = entry.userMetadata() != null ? new HashMap<>(entry.userMetadata()) : new HashMap<>();
                String key = randomAlphaOfLengthBetween(2, 10);
                if (userMetadata.containsKey(key)) {
                    userMetadata.remove(key);
                } else {
                    userMetadata.put(key, randomAlphaOfLengthBetween(2, 10));
                }
                return Entry.snapshot(
                    entry.snapshot(),
                    entry.includeGlobalState(),
                    entry.partial(),
                    entry.state(),
                    entry.indices(),
                    entry.dataStreams(),
                    entry.featureStates(),
                    entry.startTime(),
                    entry.repositoryStateId(),
                    entry.shards(),
                    entry.failure(),
                    userMetadata,
                    entry.version()
                );
            }
            case 4 -> {
                List<SnapshotFeatureInfo> featureStates = randomList(
                    1,
                    5,
                    () -> randomValueOtherThanMany(entry.featureStates()::contains, SnapshotFeatureInfoTests::randomSnapshotFeatureInfo)
                );
                return Entry.snapshot(
                    entry.snapshot(),
                    entry.includeGlobalState(),
                    entry.partial(),
                    entry.state(),
                    entry.indices(),
                    entry.dataStreams(),
                    featureStates,
                    entry.startTime(),
                    entry.repositoryStateId(),
                    entry.shards(),
                    entry.failure(),
                    entry.userMetadata(),
                    entry.version()
                );
            }
            case 5 -> {
                return mutateEntryWithLegalChange(entry);
            }
            default -> throw new IllegalArgumentException("invalid randomization case");
        }
    }

    // mutates an entry with a change that could occur as part of a cluster state update and is thus diffable
    private Entry mutateEntryWithLegalChange(Entry entry) {
        switch (randomInt(3)) {
            case 0 -> {
                List<String> dataStreams = Stream.concat(entry.dataStreams().stream(), Stream.of(randomAlphaOfLength(10))).toList();
                return Entry.snapshot(
                    entry.snapshot(),
                    entry.includeGlobalState(),
                    entry.partial(),
                    entry.state(),
                    entry.indices(),
                    dataStreams,
                    entry.featureStates(),
                    entry.startTime(),
                    entry.repositoryStateId(),
                    entry.shards(),
                    entry.failure(),
                    entry.userMetadata(),
                    entry.version()
                );
            }
            case 1 -> {
                long repositoryStateId = randomValueOtherThan(entry.repositoryStateId(), ESTestCase::randomLong);
                return Entry.snapshot(
                    entry.snapshot(),
                    entry.includeGlobalState(),
                    entry.partial(),
                    entry.state(),
                    entry.indices(),
                    entry.dataStreams(),
                    entry.featureStates(),
                    entry.startTime(),
                    repositoryStateId,
                    entry.shards(),
                    entry.failure(),
                    entry.userMetadata(),
                    entry.version()
                );
            }
            case 2 -> {
                String failure = randomValueOtherThan(entry.failure(), () -> randomAlphaOfLengthBetween(2, 10));
                return Entry.snapshot(
                    entry.snapshot(),
                    entry.includeGlobalState(),
                    entry.partial(),
                    entry.state(),
                    entry.indices(),
                    entry.dataStreams(),
                    entry.featureStates(),
                    entry.startTime(),
                    entry.repositoryStateId(),
                    entry.shards(),
                    failure,
                    entry.userMetadata(),
                    entry.version()
                );
            }
            case 3 -> {
                Map<String, IndexId> indices = new HashMap<>(entry.indices());
                IndexId indexId = new IndexId(randomAlphaOfLength(10), randomAlphaOfLength(10));
                indices.put(indexId.getName(), indexId);
                Map<ShardId, SnapshotsInProgress.ShardSnapshotStatus> shards = new HashMap<>(entry.shards());
                Index index = new Index(indexId.getName(), randomAlphaOfLength(10));
                int shardsCount = randomIntBetween(1, 10);
                for (int j = 0; j < shardsCount; j++) {
                    shards.put(new ShardId(index, j), randomShardSnapshotStatus(randomAlphaOfLength(10)));
                }
                return Entry.snapshot(
                    entry.snapshot(),
                    entry.includeGlobalState(),
                    entry.partial(),
                    randomState(shards),
                    indices,
                    entry.dataStreams(),
                    entry.featureStates(),
                    entry.startTime(),
                    entry.repositoryStateId(),
                    shards,
                    entry.failure(),
                    entry.userMetadata(),
                    entry.version()
                );
            }
            default -> throw new IllegalArgumentException("invalid randomization case");
        }
    }

    public void testXContent() throws IOException {
        final IndexId indexId = new IndexId("index", "uuid");
        final ProjectId projectId = ProjectId.fromId("some-project");
        SnapshotsInProgress sip = SnapshotsInProgress.EMPTY.withAddedEntry(
            Entry.snapshot(
                new Snapshot(projectId, "repo", new SnapshotId("name", "uuid")),
                true,
                true,
                State.SUCCESS,
                Collections.singletonMap(indexId.getName(), indexId),
                Collections.emptyList(),
                Collections.emptyList(),
                1234567,
                0,
                Map.of(
                    new ShardId("index", "uuid", 0),
                    SnapshotsInProgress.ShardSnapshotStatus.success(
                        "nodeId",
                        new ShardSnapshotResult(new ShardGeneration("shardgen"), ByteSizeValue.ofBytes(1L), 1)
                    ),
                    new ShardId("index", "uuid", 1),
                    new SnapshotsInProgress.ShardSnapshotStatus(
                        "nodeId",
                        ShardState.FAILED,
                        new ShardGeneration("fail-gen"),
                        "failure-reason"
                    )
                ),
                null,
                null,
                IndexVersion.current()
            )
        )
            .withUpdatedNodeIdsForRemoval(
                CLUSTER_STATE_FOR_NODE_SHUTDOWNS.copyAndUpdateMetadata(
                    b -> b.putCustom(
                        NodesShutdownMetadata.TYPE,
                        new NodesShutdownMetadata(
                            Map.of(
                                "node-id",
                                SingleNodeShutdownMetadata.builder()
                                    .setNodeId("node-id")
                                    .setNodeEphemeralId("node-id")
                                    .setType(SingleNodeShutdownMetadata.Type.REMOVE)
                                    .setStartedAtMillis(randomNonNegativeLong())
                                    .setReason("test")
                                    .build()
                            )
                        )
                    )
                )
            );

        AbstractChunkedSerializingTestCase.assertChunkCount(sip, instance -> Math.toIntExact(instance.asStream().count() + 5));
        final var json = Strings.toString(sip, false, true);
        assertThat(
            json,
            anyOf(
                equalTo(XContentHelper.stripWhitespace("""
                    {
                      "snapshots": [
                        {
                          "project_id": "some-project",
                          "repository": "repo",
                          "snapshot": "name",
                          "uuid": "uuid",
                          "include_global_state": true,
                          "partial": true,
                          "state": "SUCCESS",
                          "indices": [ { "name": "index", "id": "uuid" } ],
                          "start_time": "1970-01-01T00:20:34.567Z",
                          "start_time_millis": 1234567,
                          "repository_state_id": 0,
                          "shards": [
                            {
                              "index": {
                                "index_name": "index",
                                "index_uuid": "uuid"
                              },
                              "shard": 0,
                              "state": "SUCCESS",
                              "generation": "shardgen",
                              "node": "nodeId",
                              "result": {
                                "generation": "shardgen",
                                "size": "1b",
                                "size_in_bytes": 1,
                                "segments": 1
                              }
                            },
                            {
                              "index": {
                                "index_name": "index",
                                "index_uuid": "uuid"
                              },
                              "shard": 1,
                              "state": "FAILED",
                              "generation": "fail-gen",
                              "node": "nodeId",
                              "reason": "failure-reason"
                            }
                          ],
                          "feature_states": [],
                          "data_streams": []
                        }
                      ],
                      "node_ids_for_removal":["node-id"]
                    }""")),
                // or the shards might be in the other order:
                equalTo(XContentHelper.stripWhitespace("""
                    {
                      "snapshots": [
                        {
                          "project_id": "some-project",
                          "repository": "repo",
                          "snapshot": "name",
                          "uuid": "uuid",
                          "include_global_state": true,
                          "partial": true,
                          "state": "SUCCESS",
                          "indices": [ { "name": "index", "id": "uuid" } ],
                          "start_time": "1970-01-01T00:20:34.567Z",
                          "start_time_millis": 1234567,
                          "repository_state_id": 0,
                          "shards": [
                            {
                              "index": {
                                "index_name": "index",
                                "index_uuid": "uuid"
                              },
                              "shard": 1,
                              "state": "FAILED",
                              "generation": "fail-gen",
                              "node": "nodeId",
                              "reason": "failure-reason"
                            },
                            {
                              "index": {
                                "index_name": "index",
                                "index_uuid": "uuid"
                              },
                              "shard": 0,
                              "state": "SUCCESS",
                              "generation": "shardgen",
                              "node": "nodeId",
                              "result": {
                                "generation": "shardgen",
                                "size": "1b",
                                "size_in_bytes": 1,
                                "segments": 1
                              }
                            }
                          ],
                          "feature_states": [],
                          "data_streams": []
                        }
                      ],
                      "node_ids_for_removal":["node-id"]
                    }"""))
            )
        );
    }

    public static State randomState(Map<ShardId, SnapshotsInProgress.ShardSnapshotStatus> shards) {
        if (SnapshotsInProgress.completed(shards.values())) {
            return randomFrom(State.SUCCESS, State.FAILED);
        }
        if (shards.values()
            .stream()
            .map(SnapshotsInProgress.ShardSnapshotStatus::state)
            .allMatch(st -> st.completed() || st == ShardState.ABORTED)) {
            return State.ABORTED;
        }
        return State.STARTED;
    }
}
