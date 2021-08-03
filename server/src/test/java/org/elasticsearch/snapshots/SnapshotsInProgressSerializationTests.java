/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.snapshots;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.ClusterState.Custom;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.SnapshotsInProgress.Entry;
import org.elasticsearch.cluster.SnapshotsInProgress.ShardState;
import org.elasticsearch.cluster.SnapshotsInProgress.State;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.ShardSnapshotResult;
import org.elasticsearch.test.AbstractDiffableWireSerializationTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;

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
        Map<String, IndexId> indices = new HashMap<>();
        for (int i = 0; i < numberOfIndices; i++) {
            final String name = randomAlphaOfLength(10);
            indices.put(name, new IndexId(name, randomAlphaOfLength(10)));
        }
        long startTime = randomLong();
        long repositoryStateId = randomLong();
        ImmutableOpenMap.Builder<ShardId, SnapshotsInProgress.ShardSnapshotStatus> builder = ImmutableOpenMap.builder();
        final List<Index> esIndices = indices.keySet()
            .stream()
            .map(i -> new Index(i, randomAlphaOfLength(10)))
            .collect(Collectors.toList());
        List<String> dataStreams = Arrays.asList(generateRandomStringArray(10, 10, false));
        for (Index idx : esIndices) {
            int shardsCount = randomIntBetween(1, 10);
            for (int j = 0; j < shardsCount; j++) {
                builder.put(new ShardId(idx, j), randomShardSnapshotStatus(randomAlphaOfLength(10)));
            }
        }
        List<SnapshotFeatureInfo> featureStates = randomList(5, SnapshotFeatureInfoTests::randomSnapshotFeatureInfo);
        ImmutableOpenMap<ShardId, SnapshotsInProgress.ShardSnapshotStatus> shards = builder.build();
        return new Entry(
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
            VersionUtils.randomVersion(random())
        );
    }

    private SnapshotsInProgress.ShardSnapshotStatus randomShardSnapshotStatus(String nodeId) {
        ShardState shardState = randomFrom(ShardState.values());
        if (shardState == ShardState.QUEUED) {
            return SnapshotsInProgress.ShardSnapshotStatus.UNASSIGNED_QUEUED;
        } else if (shardState == ShardState.SUCCESS) {
            final ShardSnapshotResult shardSnapshotResult = new ShardSnapshotResult("1", new ByteSizeValue(1L), 1);
            return SnapshotsInProgress.ShardSnapshotStatus.success(nodeId, shardSnapshotResult);
        } else {
            final String reason = shardState.failed() ? randomAlphaOfLength(10) : null;
            return new SnapshotsInProgress.ShardSnapshotStatus(nodeId, shardState, reason, "1");
        }
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
        switch (randomInt(8)) {
            case 0:
                boolean includeGlobalState = entry.includeGlobalState() == false;
                return new Entry(
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
            case 1:
                boolean partial = entry.partial() == false;
                return new Entry(
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
            case 2:
                List<String> dataStreams = Stream.concat(entry.dataStreams().stream(), Stream.of(randomAlphaOfLength(10)))
                    .collect(Collectors.toList());
                return new Entry(
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
            case 3:
                long startTime = randomValueOtherThan(entry.startTime(), ESTestCase::randomLong);
                return new Entry(
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
            case 4:
                long repositoryStateId = randomValueOtherThan(entry.startTime(), ESTestCase::randomLong);
                return new Entry(
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
            case 5:
                String failure = randomValueOtherThan(entry.failure(), () -> randomAlphaOfLengthBetween(2, 10));
                return new Entry(
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
            case 6:
                Map<String, IndexId> indices = new HashMap<>(entry.indices());
                ImmutableOpenMap<ShardId, SnapshotsInProgress.ShardSnapshotStatus> shards = entry.shards();
                IndexId indexId = new IndexId(randomAlphaOfLength(10), randomAlphaOfLength(10));
                indices.put(indexId.getName(), indexId);
                ImmutableOpenMap.Builder<ShardId, SnapshotsInProgress.ShardSnapshotStatus> builder = ImmutableOpenMap.builder(shards);
                Index index = new Index(indexId.getName(), randomAlphaOfLength(10));
                int shardsCount = randomIntBetween(1, 10);
                for (int j = 0; j < shardsCount; j++) {
                    builder.put(new ShardId(index, j), randomShardSnapshotStatus(randomAlphaOfLength(10)));
                }
                shards = builder.build();
                return new Entry(
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
            case 7:
                Map<String, Object> userMetadata = entry.userMetadata() != null ? new HashMap<>(entry.userMetadata()) : new HashMap<>();
                String key = randomAlphaOfLengthBetween(2, 10);
                if (userMetadata.containsKey(key)) {
                    userMetadata.remove(key);
                } else {
                    userMetadata.put(key, randomAlphaOfLengthBetween(2, 10));
                }
                return new Entry(
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
            case 8:
                List<SnapshotFeatureInfo> featureStates = randomList(
                    1,
                    5,
                    () -> randomValueOtherThanMany(entry.featureStates()::contains, SnapshotFeatureInfoTests::randomSnapshotFeatureInfo)
                );
                return new Entry(
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
            default:
                throw new IllegalArgumentException("invalid randomization case");
        }
    }

    public void testXContent() throws IOException {
        final IndexId indexId = new IndexId("index", "uuid");
        SnapshotsInProgress sip = SnapshotsInProgress.of(
            Collections.singletonList(
                new Entry(
                    new Snapshot("repo", new SnapshotId("name", "uuid")),
                    true,
                    true,
                    State.SUCCESS,
                    Collections.singletonMap(indexId.getName(), indexId),
                    Collections.emptyList(),
                    Collections.emptyList(),
                    1234567,
                    0,
                    ImmutableOpenMap.<ShardId, SnapshotsInProgress.ShardSnapshotStatus>builder()
                        .fPut(
                            new ShardId("index", "uuid", 0),
                            SnapshotsInProgress.ShardSnapshotStatus.success(
                                "nodeId",
                                new ShardSnapshotResult("shardgen", new ByteSizeValue(1L), 1)
                            )
                        )
                        .fPut(
                            new ShardId("index", "uuid", 1),
                            new SnapshotsInProgress.ShardSnapshotStatus("nodeId", ShardState.FAILED, "failure-reason", "fail-gen")
                        )
                        .build(),
                    null,
                    null,
                    Version.CURRENT
                )
            )
        );

        try (XContentBuilder builder = jsonBuilder()) {
            builder.humanReadable(true);
            builder.startObject();
            sip.toXContent(builder, ToXContent.EMPTY_PARAMS);
            builder.endObject();
            String json = Strings.toString(builder);
            assertThat(
                json,
                anyOf(
                    equalTo(
                        "{\"snapshots\":[{\"repository\":\"repo\",\"snapshot\":\"name\",\"uuid\":\"uuid\","
                            + "\"include_global_state\":true,\"partial\":true,\"state\":\"SUCCESS\","
                            + "\"indices\":[{\"name\":\"index\",\"id\":\"uuid\"}],\"start_time\":\"1970-01-01T00:20:34.567Z\","
                            + "\"start_time_millis\":1234567,\"repository_state_id\":0,\"shards\":["
                            + "{\"index\":{\"index_name\":\"index\",\"index_uuid\":\"uuid\"},\"shard\":0,\"state\":\"SUCCESS\","
                            + "\"generation\":\"shardgen\",\"node\":\"nodeId\","
                            + "\"result\":{\"generation\":\"shardgen\",\"size\":\"1b\",\"size_in_bytes\":1,\"segments\":1}},"
                            + "{\"index\":{\"index_name\":\"index\",\"index_uuid\":\"uuid\"},\"shard\":1,\"state\":\"FAILED\","
                            + "\"generation\":\"fail-gen\",\"node\":\"nodeId\",\"reason\":\"failure-reason\"}"
                            + "],\"feature_states\":[],\"data_streams\":[]}]}"
                    ), // or the shards might be in the other order:
                    equalTo(
                        "{\"snapshots\":[{\"repository\":\"repo\",\"snapshot\":\"name\",\"uuid\":\"uuid\","
                            + "\"include_global_state\":true,\"partial\":true,\"state\":\"SUCCESS\","
                            + "\"indices\":[{\"name\":\"index\",\"id\":\"uuid\"}],\"start_time\":\"1970-01-01T00:20:34.567Z\","
                            + "\"start_time_millis\":1234567,\"repository_state_id\":0,\"shards\":["
                            + "{\"index\":{\"index_name\":\"index\",\"index_uuid\":\"uuid\"},\"shard\":1,\"state\":\"FAILED\","
                            + "\"generation\":\"fail-gen\",\"node\":\"nodeId\",\"reason\":\"failure-reason\"},"
                            + "{\"index\":{\"index_name\":\"index\",\"index_uuid\":\"uuid\"},\"shard\":0,\"state\":\"SUCCESS\","
                            + "\"generation\":\"shardgen\",\"node\":\"nodeId\","
                            + "\"result\":{\"generation\":\"shardgen\",\"size\":\"1b\",\"size_in_bytes\":1,\"segments\":1}}"
                            + "],\"feature_states\":[],\"data_streams\":[]}]}"
                    )
                )
            );
        }
    }

    public static State randomState(ImmutableOpenMap<ShardId, SnapshotsInProgress.ShardSnapshotStatus> shards) {
        return SnapshotsInProgress.completed(shards.values())
            ? randomFrom(State.SUCCESS, State.FAILED)
            : randomFrom(State.STARTED, State.INIT, State.ABORTED);
    }
}
