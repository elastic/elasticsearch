/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.test.AbstractChunkedSerializingTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

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
     *
     * <p>This test is temporary. It pins the behaviour while the flag is only held in memory by the master, and it should be revisited
     * when the flag becomes part of the wire format.
     */
    public void testReportShardRestoringIsNotExposedInXContent() throws IOException {
        final RestorePair restore = randomRestorePair();

        assertThat(toJson(restore.reporting()), equalTo(toJson(restore.notReporting())));
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
