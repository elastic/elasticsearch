/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.snapshots.get;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotFeatureInfo;
import org.elasticsearch.snapshots.SnapshotFeatureInfoTests;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.SnapshotInfoTestUtils;
import org.elasticsearch.snapshots.SnapshotShardFailure;
import org.elasticsearch.test.AbstractChunkedSerializingTestCase;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class GetSnapshotsResponseTests extends ESTestCase {
    // We can not subclass AbstractSerializingTestCase because it
    // can only be used for instances with equals and hashCode
    // GetSnapshotResponse does not override equals and hashCode.
    // It does not override equals and hashCode, because it
    // contains ElasticsearchException, which does not override equals and hashCode.

    private GetSnapshotsResponse copyInstance(GetSnapshotsResponse instance) throws IOException {
        return copyInstance(
            instance,
            new NamedWriteableRegistry(Collections.emptyList()),
            StreamOutput::writeWriteable,
            GetSnapshotsResponse::new,
            TransportVersion.current()
        );

    }

    private void assertEqualInstances(GetSnapshotsResponse expectedInstance, GetSnapshotsResponse newInstance) {
        assertEquals(expectedInstance.getSnapshots(), newInstance.getSnapshots());
        assertEquals(expectedInstance.next(), newInstance.next());
    }

    private List<SnapshotInfo> createSnapshotInfos(String repoName) {
        ArrayList<SnapshotInfo> snapshots = new ArrayList<>();
        final int targetSize = between(5, 10);
        for (int i = 0; i < targetSize; ++i) {
            SnapshotId snapshotId = new SnapshotId("snapshot " + i, UUIDs.base64UUID());
            String reason = randomBoolean() ? null : "reason";
            ShardId shardId = new ShardId("index", UUIDs.base64UUID(), 2);
            List<SnapshotShardFailure> shardFailures = Collections.singletonList(new SnapshotShardFailure("node-id", shardId, "reason"));
            List<SnapshotFeatureInfo> featureInfos = randomList(5, SnapshotFeatureInfoTests::randomSnapshotFeatureInfo);
            snapshots.add(
                new SnapshotInfo(
                    new Snapshot(repoName, snapshotId),
                    Arrays.asList("index1", "index2"),
                    Collections.singletonList("ds"),
                    featureInfos,
                    reason,
                    System.currentTimeMillis(),
                    randomIntBetween(2, 3),
                    shardFailures,
                    randomBoolean(),
                    SnapshotInfoTestUtils.randomUserMetadata(),
                    System.currentTimeMillis(),
                    SnapshotInfoTestUtils.randomIndexSnapshotDetails()
                )
            );
        }
        return snapshots;
    }

    private GetSnapshotsResponse createTestInstance() {
        Set<String> repositories = new HashSet<>();
        List<SnapshotInfo> responses = new ArrayList<>();

        for (int i = 0; i < randomIntBetween(0, 5); i++) {
            String repository = randomValueOtherThanMany(repositories::contains, () -> randomAlphaOfLength(10));
            repositories.add(repository);
            responses.addAll(createSnapshotInfos(repository));
        }

        for (int i = 0; i < randomIntBetween(0, 5); i++) {
            String repository = randomValueOtherThanMany(repositories::contains, () -> randomAlphaOfLength(10));
            repositories.add(repository);
        }

        return new GetSnapshotsResponse(
            responses,
            randomBoolean()
                ? Base64.getUrlEncoder()
                    .encodeToString(
                        (randomAlphaOfLengthBetween(1, 5) + "," + randomAlphaOfLengthBetween(1, 5) + "," + randomAlphaOfLengthBetween(1, 5))
                            .getBytes(StandardCharsets.UTF_8)
                    )
                : null,
            randomIntBetween(responses.size(), responses.size() + 100),
            randomIntBetween(0, 100)
        );
    }

    public void testSerialization() throws IOException {
        GetSnapshotsResponse testInstance = createTestInstance();
        GetSnapshotsResponse deserializedInstance = copyInstance(testInstance);
        assertEqualInstances(testInstance, deserializedInstance);
    }

    public void testChunking() {
        AbstractChunkedSerializingTestCase.assertChunkCount(createTestInstance(), response -> response.getSnapshots().size() + 2);
    }

}
