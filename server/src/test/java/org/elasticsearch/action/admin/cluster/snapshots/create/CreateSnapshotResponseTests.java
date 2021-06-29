/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.snapshots.create;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotFeatureInfo;
import org.elasticsearch.snapshots.SnapshotFeatureInfoTests;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.SnapshotInfoTestUtils;
import org.elasticsearch.snapshots.SnapshotShardFailure;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.function.Predicate;

public class CreateSnapshotResponseTests extends AbstractXContentTestCase<CreateSnapshotResponse> {

    @Override
    protected CreateSnapshotResponse doParseInstance(XContentParser parser) throws IOException {
        return CreateSnapshotResponse.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    @Override
    protected CreateSnapshotResponse createTestInstance() {
        SnapshotId snapshotId = new SnapshotId("test", UUID.randomUUID().toString());
        List<String> indices = new ArrayList<>();
        indices.add("test0");
        indices.add("test1");

        List<String> dataStreams = new ArrayList<>();
        dataStreams.add("test0");
        dataStreams.add("test1");

        List<SnapshotFeatureInfo> featureStates = randomList(5, SnapshotFeatureInfoTests::randomSnapshotFeatureInfo);

        String reason = "reason";
        long startTime = System.currentTimeMillis();
        long endTime = startTime + 10000;
        int totalShards = randomIntBetween(1, 3);
        int successfulShards = randomIntBetween(0, totalShards);
        List<SnapshotShardFailure> shardFailures = new ArrayList<>();

        for (int count = successfulShards; count < totalShards; ++count) {
            shardFailures.add(
                new SnapshotShardFailure("node-id", new ShardId("index-" + count, UUID.randomUUID().toString(), randomInt()), "reason")
            );
        }

        boolean globalState = randomBoolean();

        return new CreateSnapshotResponse(
            new SnapshotInfo(
                new Snapshot("test-repo", snapshotId),
                indices,
                dataStreams,
                featureStates,
                reason,
                endTime,
                totalShards,
                shardFailures,
                globalState,
                SnapshotInfoTestUtils.randomUserMetadata(),
                startTime,
                Collections.emptyMap()
            )
        );
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        // Don't inject random fields into the custom snapshot metadata, because the metadata map is equality-checked after doing a
        // round-trip through xContent serialization/deserialization. Even though the rest of the object ignores unknown fields,
        // `metadata` doesn't ignore unknown fields (it just includes them in the parsed object, because the keys are arbitrary), so any
        // new fields added to the metadata before it gets deserialized that weren't in the serialized version will cause the equality
        // check to fail.
        return field -> field.startsWith("snapshot.metadata");
    }
}
