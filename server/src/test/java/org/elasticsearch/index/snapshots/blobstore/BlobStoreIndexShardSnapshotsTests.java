/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.index.snapshots.blobstore;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.ByteBuffersIndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Version;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.lucene.store.ByteArrayIndexInput;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot.FileInfo;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreFileMetadata;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.smile.SmileXContent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.index.store.StoreFileMetadata.UNAVAILABLE_WRITER_UUID;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class BlobStoreIndexShardSnapshotsTests extends ESTestCase {

    public void testEmptyRetainsNothing() {
        assertThat(BlobStoreIndexShardSnapshots.EMPTY.retainedBlobBytes(), equalTo(0L));
    }

    /**
     * Snapshots share files and some files are inlined in the metadata. The total counts each shared blob once and skips the inlined files,
     * after additions, a clone, and after deletions that drop files only some snapshots referenced.
     */
    public void testRetainedBlobBytesCountsEachSharedBlobOnce() throws IOException {
        final List<FileInfo> pool = new ArrayList<>();
        for (int i = 0; i < between(5, 30); i++) {
            pool.add(randomBoolean() ? blobFile("__" + i) : inlinedFile("__" + i));
        }

        final Map<String, List<FileInfo>> filesBySnapshot = new HashMap<>();
        final List<SnapshotId> snapshotIds = new ArrayList<>();
        var history = BlobStoreIndexShardSnapshots.EMPTY;
        for (int i = 0; i < between(1, 10); i++) {
            final var snapshotId = new SnapshotId("snap-" + i, randomUUID());
            final List<FileInfo> files = randomSubsetOf(between(0, pool.size()), pool);
            history = history.withAddedSnapshot(new SnapshotFiles(snapshotId.getName(), files, null));
            filesBySnapshot.put(snapshotId.getName(), files);
            snapshotIds.add(snapshotId);
            assertThat(history.retainedBlobBytes(), equalTo(expectedRetainedBlobBytes(filesBySnapshot, snapshotIds)));
        }

        final var source = randomFrom(snapshotIds);
        history = history.withClone(source.getName(), "clone");
        filesBySnapshot.put("clone", filesBySnapshot.get(source.getName()));
        snapshotIds.add(new SnapshotId("clone", randomUUID()));
        assertThat(history.retainedBlobBytes(), equalTo(expectedRetainedBlobBytes(filesBySnapshot, snapshotIds)));

        final List<SnapshotId> survivors = randomSubsetOf(between(0, snapshotIds.size()), snapshotIds);
        history = history.withRetainedSnapshots(new HashSet<>(survivors));
        assertThat(history.retainedBlobBytes(), equalTo(expectedRetainedBlobBytes(filesBySnapshot, survivors)));
    }

    public void testOverflowIsReportedNotWrapped() {
        final var huge = BlobStoreIndexShardSnapshots.EMPTY.withAddedSnapshot(
            new SnapshotFiles("snap", List.of(blobFile("__a", Long.MAX_VALUE), blobFile("__b", 1)), null)
        );
        expectThrows(ArithmeticException.class, huge::retainedBlobBytes);
    }

    public void testResolveFileReferencesInEitherFieldOrder() throws IOException {
        final var shared = blobFile("__shared", 100);
        final var firstOnly = blobFile("__first", 200);
        for (boolean filesFirst : List.of(true, false)) {
            final var parsed = parseShardHistory(List.of(shared, firstOnly), filesFirst, shared.name());
            assertEquals(List.of("first", "second", "empty"), parsed.snapshots().stream().map(SnapshotFiles::snapshot).toList());

            final var first = parsed.snapshots().get(0);
            final var second = parsed.snapshots().get(1);
            assertEquals("first-state", first.shardStateIdentifier());
            assertEquals("second-state", second.shardStateIdentifier());
            assertEquals(List.of(firstOnly.name(), shared.name()), first.indexFiles().stream().map(FileInfo::name).toList());
            assertSame(parsed.findNameFile(shared.name()), first.indexFiles().get(1));
            assertSame(first.indexFiles().get(1), second.indexFiles().getFirst());
            assertTrue(parsed.snapshots().get(2).indexFiles().isEmpty());

            final var retained = parsed.withRetainedSnapshots(Set.of(new SnapshotId("second", "snapshot-uuid")));
            assertNull(retained.findNameFile(firstOnly.name()));
            assertSame(parsed.findNameFile(shared.name()), retained.findNameFile(shared.name()));
        }
    }

    public void testRejectMissingFileReferencesInEitherFieldOrder() throws IOException {
        final var firstOnly = blobFile("__first", 200);
        for (boolean filesFirst : List.of(true, false)) {
            final var error = expectThrows(AssertionError.class, () -> parseShardHistory(List.of(firstOnly), filesFirst, "__missing"));
            assertThat(error.getMessage(), containsString("shard index inconsistent at file [__missing]"));
        }
    }

    private BlobStoreIndexShardSnapshots parseShardHistory(List<FileInfo> files, boolean filesFirst, String sharedFile) throws IOException {
        try (var builder = XContentFactory.smileBuilder()) {
            builder.startObject();
            if (filesFirst) {
                writeFileTable(builder, files);
            }
            builder.startObject("snapshots");
            builder.startObject("first").field("shard_state_id", "first-state").array("files", "__first", sharedFile).endObject();
            builder.startObject("second").array("files", sharedFile).field("shard_state_id", "second-state").endObject();
            builder.startObject("empty").startArray("files").endArray().endObject();
            builder.startObject("no-files").endObject();
            builder.endObject();
            if (filesFirst == false) {
                writeFileTable(builder, files);
            }
            builder.endObject();
            try (var parser = createParser(SmileXContent.smileXContent, BytesReference.toBytes(BytesReference.bytes(builder)))) {
                return BlobStoreIndexShardSnapshots.fromXContent(parser);
            }
        }
    }

    private static void writeFileTable(XContentBuilder builder, List<FileInfo> files) throws IOException {
        builder.startArray("files");
        for (FileInfo file : files) {
            FileInfo.toXContent(file, builder, ToXContent.EMPTY_PARAMS);
        }
        builder.endArray();
    }

    private static long expectedRetainedBlobBytes(Map<String, List<FileInfo>> filesBySnapshot, List<SnapshotId> retained) {
        final Set<String> seen = new HashSet<>();
        long total = 0;
        for (SnapshotId snapshotId : retained) {
            for (FileInfo file : filesBySnapshot.get(snapshotId.getName())) {
                if (file.metadata().hashEqualsContents() == false && seen.add(file.name())) {
                    total += file.length();
                }
            }
        }
        return total;
    }

    private static FileInfo blobFile(String name) {
        return blobFile(name, between(1, 1_000_000));
    }

    private static FileInfo blobFile(String name, long length) {
        final var metadata = new StoreFileMetadata(
            name + ".cfs",
            length,
            randomAlphaOfLength(8),
            Version.LATEST.toString(),
            null,
            UNAVAILABLE_WRITER_UUID
        );
        return new FileInfo(name, metadata, null);
    }

    /** A file whose whole contents are in the metadata, so there is no blob. It needs a valid Lucene footer for the checksum to verify. */
    private static FileInfo inlinedFile(String name) throws IOException {
        final var buffer = new ByteBuffersDataOutput();
        try (var out = new ByteBuffersIndexOutput(buffer, "inlined", name)) {
            CodecUtil.writeHeader(out, "inlined", 0);
            out.writeBytes(randomByteArrayOfLength(between(1, 100)), 0, 1);
            CodecUtil.writeFooter(out);
        }
        final byte[] contents = buffer.toArrayCopy();
        final String checksum = Store.digestToString(CodecUtil.retrieveChecksum(new ByteArrayIndexInput(name, contents)));
        final var metadata = new StoreFileMetadata(
            name + ".si",
            contents.length,
            checksum,
            Version.LATEST.toString(),
            new BytesRef(contents),
            UNAVAILABLE_WRITER_UUID
        );
        return new FileInfo(name, metadata, null);
    }
}
