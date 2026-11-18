/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.snapshots.blobstore;

import org.apache.lucene.util.Version;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot.FileInfo;
import org.elasticsearch.index.store.StoreFileMetadata;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class BlobStoreIndexShardSnapshotsTests extends ESTestCase {

    public void testToFromXContent() throws IOException {
        final FileInfo shared = fileInfo("__0", "_0.cfs");
        final FileInfo onlyFirst = fileInfo("__1", "_0.si");
        final BlobStoreIndexShardSnapshots snapshots = BlobStoreIndexShardSnapshots.EMPTY.withAddedSnapshot(
            new SnapshotFiles("snapshot_1", List.of(shared, onlyFirst), "state-1")
        ).withAddedSnapshot(new SnapshotFiles("snapshot_2", List.of(shared), null));

        final BlobStoreIndexShardSnapshots parsed;
        try (XContentBuilder builder = XContentFactory.contentBuilder(XContentType.SMILE)) {
            builder.startObject();
            snapshots.toXContent(builder, ToXContent.EMPTY_PARAMS);
            builder.endObject();
            try (XContentParser parser = createParser(builder)) {
                parsed = BlobStoreIndexShardSnapshots.fromXContent(parser);
            }
        }

        assertThat(parsed.snapshots().size(), equalTo(2));
        assertSnapshotEquals(snapshots.snapshots().get(0), parsed.snapshots().get(0));
        assertSnapshotEquals(snapshots.snapshots().get(1), parsed.snapshots().get(1));
        assertTrue(parsed.findNameFile("__0").isSame(shared));
        assertTrue(parsed.findNameFile("__1").isSame(onlyFirst));
    }

    public void testDuplicateSnapshotName() {
        final SnapshotFiles snapshotFiles = new SnapshotFiles("snapshot_1", List.of(fileInfo("__0", "_0.cfs")), null);
        final BlobStoreIndexShardSnapshots snapshots = BlobStoreIndexShardSnapshots.EMPTY.withAddedSnapshot(snapshotFiles)
            .withAddedSnapshot(snapshotFiles);

        final IllegalStateException e = expectThrows(IllegalStateException.class, () -> {
            try (XContentBuilder builder = XContentFactory.contentBuilder(XContentType.SMILE)) {
                builder.startObject();
                snapshots.toXContent(builder, ToXContent.EMPTY_PARAMS);
            }
        });
        assertThat(e.getMessage(), containsString("snapshot_1"));
    }

    private static void assertSnapshotEquals(SnapshotFiles expected, SnapshotFiles actual) {
        assertThat(actual.snapshot(), equalTo(expected.snapshot()));
        assertThat(actual.shardStateIdentifier(), equalTo(expected.shardStateIdentifier()));
        assertTrue(actual.isSame(expected));
    }

    private static FileInfo fileInfo(String name, String physicalName) {
        return new FileInfo(
            name,
            new StoreFileMetadata(physicalName, 100L, "checksum", Version.LATEST.toString()),
            ByteSizeValue.ofBytes(100)
        );
    }
}
