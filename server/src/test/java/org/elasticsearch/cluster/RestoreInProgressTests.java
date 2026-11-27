/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster;

import org.apache.logging.log4j.Level;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.test.AbstractChunkedSerializingTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class RestoreInProgressTests extends ESTestCase {
    public void testChunking() throws IOException {
        final var ripBuilder = new RestoreInProgress.Builder();
        final var entryCount = between(0, 5);
        for (int i = 0; i < entryCount; i++) {
            ripBuilder.add(
                new RestoreInProgress.Entry(
                    "uuid-" + i,
                    new Snapshot(randomAlphaOfLength(10), new SnapshotId(randomAlphaOfLength(10), randomAlphaOfLength(10))),
                    randomFrom(RestoreInProgress.State.values()),
                    randomBoolean(),
                    List.of(),
                    Map.of()
                )
            );
        }

        AbstractChunkedSerializingTestCase.assertChunkCount(ripBuilder.build(), ignored -> entryCount + 2);
    }

    /**
     * The cluster state API shows restores through {@code toXContentChunked}. The flag is internal, so a restore renders identically
     * whether or not its caller asked for its shards to be reported.
     */
    public void testReportShardRestoringIsNotExposedInXContent() throws IOException {
        final RestorePair restore = randomRestorePair();

        assertThat(toJson(restore.reporting()), equalTo(toJson(restore.notReporting())));
    }

    /**
     * A shard's classified failure reason survives a round trip to a node that classifies failures, and an older node that does not
     * classify them receives the failure without one, keeping the rest of the status.
     */
    public void testShardFailureReasonSerialization() throws IOException {
        final ShardId shardId = new ShardId(randomAlphaOfLength(8), randomUUID(), 0);
        final RestoreInProgress.ShardRestoreStatus failed = RestoreInProgress.ShardRestoreStatus.failed(
            randomBoolean() ? null : randomAlphaOfLength(6),
            randomFrom(RestoreInProgress.ShardRestoreFailureReason.values()),
            randomBoolean() ? null : randomAlphaOfLength(20)
        );
        final RestoreInProgress restore = restoreInProgress(
            new RestoreInProgress.Entry(
                randomUUID(),
                new Snapshot(randomAlphaOfLength(10), new SnapshotId(randomAlphaOfLength(10), randomAlphaOfLength(10))),
                RestoreInProgress.State.FAILURE,
                randomBoolean(),
                List.of(shardId.getIndexName()),
                Map.of(shardId, failed),
                randomBoolean()
            )
        );
        final NamedWriteableRegistry registry = new NamedWriteableRegistry(List.of());

        final RestoreInProgress current = copyWriteable(restore, registry, RestoreInProgress::new, TransportVersion.current());
        assertThat(onlyShardStatus(current), equalTo(failed));

        final RestoreInProgress older = copyWriteable(
            restore,
            registry,
            RestoreInProgress::new,
            TransportVersionUtils.randomVersionNotSupporting(RestoreInProgress.RESTORE_SHARD_FAILURE_REASON)
        );
        assertThat(
            onlyShardStatus(older),
            equalTo(new RestoreInProgress.ShardRestoreStatus(failed.nodeId(), RestoreInProgress.State.FAILURE, failed.reason()))
        );
    }

    /**
     * Each failure reason has its own wire id, and {@link RestoreInProgress.ShardRestoreFailureReason#fromId} finds it, so the wire format
     * does not depend on the order of the constants.
     */
    public void testShardFailureReasonIdsAreUnique() {
        final RestoreInProgress.ShardRestoreFailureReason[] reasons = RestoreInProgress.ShardRestoreFailureReason.values();
        assertThat(
            Arrays.stream(reasons).map(RestoreInProgress.ShardRestoreFailureReason::id).distinct().count(),
            equalTo((long) reasons.length)
        );
        for (RestoreInProgress.ShardRestoreFailureReason reason : reasons) {
            assertThat(RestoreInProgress.ShardRestoreFailureReason.fromId(reason.id()), equalTo(reason));
        }
    }

    /**
     * A failure reason that a newer node added, whose id this node does not know, is read as an unclassified failure rather than failing
     * to read the cluster state, so adding a reason needs no new transport version.
     */
    public void testUnknownShardFailureReasonIsUnclassified() throws IOException {
        final byte unknownId = (byte) RestoreInProgress.ShardRestoreFailureReason.values().length;
        assertThat(RestoreInProgress.ShardRestoreFailureReason.fromId(unknownId), nullValue());

        final String nodeId = randomBoolean() ? null : randomAlphaOfLength(6);
        final String reason = randomBoolean() ? null : randomAlphaOfLength(20);
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setTransportVersion(TransportVersion.current());
            // the status as a newer node writes it, with a reason id that this node does not know
            out.writeOptionalString(nodeId);
            out.writeByte(RestoreInProgress.State.FAILURE.value());
            out.writeOptionalString(reason);
            out.writeBoolean(true);
            out.writeByte(unknownId);
            try (StreamInput in = out.bytes().streamInput()) {
                in.setTransportVersion(TransportVersion.current());
                assertThat(
                    RestoreInProgress.ShardRestoreStatus.readFrom(in),
                    equalTo(new RestoreInProgress.ShardRestoreStatus(nodeId, RestoreInProgress.State.FAILURE, reason))
                );
            }
        }
    }

    /**
     * A failure reason on a status that is not a failure is a bug. It trips an assertion in tests, and is logged as an error so that it
     * is visible in production, where assertions are disabled.
     */
    public void testFailureReasonOnNonFailureStateLogsError() {
        MockLog.assertThatLogger(
            () -> expectThrows(
                AssertionError.class,
                () -> new RestoreInProgress.ShardRestoreStatus(
                    "node",
                    randomFrom(RestoreInProgress.State.INIT, RestoreInProgress.State.STARTED, RestoreInProgress.State.SUCCESS),
                    null,
                    randomFrom(RestoreInProgress.ShardRestoreFailureReason.values())
                )
            ),
            RestoreInProgress.class,
            new MockLog.SeenEventExpectation(
                "failure reason on a non-failure state",
                RestoreInProgress.class.getCanonicalName(),
                Level.ERROR,
                "shard restore status on node [node] has failure reason [*] but state [*]"
            )
        );
    }

    private static RestoreInProgress.ShardRestoreStatus onlyShardStatus(RestoreInProgress restoreInProgress) {
        return restoreInProgress.iterator().next().shards().values().iterator().next();
    }

    private record RestorePair(RestoreInProgress reporting, RestoreInProgress notReporting) {}

    /**
     * Two restores that are identical except that the first asks for its shards to be reported.
     */
    private static RestorePair randomRestorePair() {
        final String uuid = randomUUID();
        final Snapshot snapshot = new Snapshot(randomAlphaOfLength(10), new SnapshotId(randomAlphaOfLength(10), randomAlphaOfLength(10)));
        final RestoreInProgress.State state = randomFrom(RestoreInProgress.State.values());
        final boolean quiet = randomBoolean();
        final List<String> indices = List.of(randomAlphaOfLength(8));
        final var shards = Map.of(
            new ShardId(indices.get(0), randomUUID(), 0),
            new RestoreInProgress.ShardRestoreStatus(randomAlphaOfLength(6))
        );
        return new RestorePair(
            restoreInProgress(new RestoreInProgress.Entry(uuid, snapshot, state, quiet, indices, shards, true)),
            restoreInProgress(new RestoreInProgress.Entry(uuid, snapshot, state, quiet, indices, shards, false))
        );
    }

    private static String toJson(RestoreInProgress restoreInProgress) throws IOException {
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            builder.startObject();
            ChunkedToXContent.wrapAsToXContent(restoreInProgress).toXContent(builder, ToXContent.EMPTY_PARAMS);
            builder.endObject();
            return Strings.toString(builder);
        }
    }

    private static RestoreInProgress restoreInProgress(RestoreInProgress.Entry entry) {
        return new RestoreInProgress.Builder().add(entry).build();
    }

}
