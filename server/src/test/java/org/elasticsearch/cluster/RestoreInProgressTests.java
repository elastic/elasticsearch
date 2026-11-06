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
import org.elasticsearch.index.Index;
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
import java.util.Set;
import java.util.stream.Collectors;

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

    /**
     * A rename keeps the index UUID: the entry's index list and shard keys carry the new name and the shard statuses are unchanged,
     * while a restore that does not include the index is left as it is.
     */
    public void testWithRenamedIndex() {
        final Index previous = new Index("before", randomAlphaOfLength(10));
        final Index current = new Index("after", previous.getUUID());
        final Index other = new Index("other", randomAlphaOfLength(10));
        final var status = new RestoreInProgress.ShardRestoreStatus(randomAlphaOfLength(5), RestoreInProgress.State.STARTED);
        final var withIndex = new RestoreInProgress.Entry(
            "uuid-1",
            randomSnapshot(),
            RestoreInProgress.State.STARTED,
            randomBoolean(),
            List.of("before", "other"),
            Map.of(new ShardId(previous, 0), status, new ShardId(previous, 1), status, new ShardId(other, 0), status),
            randomBoolean()
        );
        final var withoutIndex = new RestoreInProgress.Entry(
            "uuid-2",
            randomSnapshot(),
            RestoreInProgress.State.STARTED,
            randomBoolean(),
            List.of("other"),
            Map.of(new ShardId(other, 0), status)
        );
        final RestoreInProgress restores = new RestoreInProgress.Builder().add(withIndex).add(withoutIndex).build();

        final RestoreInProgress renamed = restores.withRenamedIndex(previous, current);
        assertNotSame(restores, renamed);
        final RestoreInProgress.Entry renamedEntry = renamed.get("uuid-1");
        assertThat(renamedEntry.indices(), equalTo(List.of("after", "other")));
        assertThat(
            renamedEntry.shards().keySet().stream().map(ShardId::getIndexName).collect(Collectors.toSet()),
            equalTo(Set.of("after", "other"))
        );
        assertThat(renamedEntry.shards().size(), equalTo(3));
        assertSame(status, renamedEntry.shards().get(new ShardId(current, 1)));
        assertThat(renamedEntry.reportShardRestoring(), equalTo(withIndex.reportShardRestoring()));
        assertSame(withoutIndex, renamed.get("uuid-2"));

        final Index absent = new Index("absent", randomAlphaOfLength(10));
        assertSame(restores, restores.withRenamedIndex(absent, new Index("absent-renamed", absent.getUUID())));
    }

    private static Snapshot randomSnapshot() {
        return new Snapshot(randomAlphaOfLength(10), new SnapshotId(randomAlphaOfLength(10), randomAlphaOfLength(10)));
    }
}
