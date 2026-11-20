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
import org.elasticsearch.xcontent.FilterXContentParserWrapper;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.smile.SmileXContent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;

import static org.elasticsearch.index.store.StoreFileMetadata.UNAVAILABLE_WRITER_UUID;
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

    public void testRetentionCanSkipFileNamesForDeletedSnapshots() throws IOException {
        final int fileCount = 256;
        final int snapshotCount = 50;
        final var files = IntStream.range(0, fileCount).mapToObj(i -> blobFile("__blob-" + i, 1L)).toList();
        var shardSnapshots = BlobStoreIndexShardSnapshots.EMPTY;
        for (int i = 0; i < snapshotCount; i++) {
            shardSnapshots = shardSnapshots.withAddedSnapshot(new SnapshotFiles("snapshot-" + i, files, null));
        }
        final var retained = new SnapshotId("snapshot-0", "retained-uuid");

        final BytesReference metadata;
        try (var builder = XContentFactory.smileBuilder()) {
            builder.startObject();
            shardSnapshots.toXContent(builder, ToXContent.EMPTY_PARAMS);
            builder.endObject();
            metadata = BytesReference.bytes(builder);
        }

        final byte[] bytes = BytesReference.toBytes(metadata);
        try (var parser = new BlobNameCountingParser(createParser(SmileXContent.smileXContent, bytes))) {
            final var parsed = BlobStoreIndexShardSnapshots.fromXContent(parser);
            assertEquals(snapshotCount, parsed.snapshots().size());
            assertEquals(fileCount * snapshotCount, parsed.snapshots().stream().mapToInt(snapshot -> snapshot.indexFiles().size()).sum());
            assertEquals(fileCount * (1 + snapshotCount), parser.blobNameObjects.size());
            assertEquals(fileCount, new HashSet<>(parser.blobNameObjects).size());
        }
        try (var parser = new BlobNameCountingParser(createParser(SmileXContent.smileXContent, bytes))) {
            final var remaining = BlobStoreIndexShardSnapshots.fromXContent(parser, Set.of(retained.getName()));
            assertEquals(1, remaining.snapshots().size());
            assertEquals(retained.getName(), remaining.snapshots().getFirst().snapshot());
            assertEquals(fileCount, remaining.snapshots().getFirst().indexFiles().size());
            assertEquals(fileCount * 2, parser.blobNameObjects.size());
        }
    }

    private static class BlobNameCountingParser extends FilterXContentParserWrapper {
        private final Set<String> blobNameObjects = Collections.newSetFromMap(new IdentityHashMap<>());

        BlobNameCountingParser(XContentParser parser) {
            super(parser);
        }

        @Override
        public String text() throws IOException {
            final String text = super.text();
            if (text.startsWith("__blob-")) {
                blobNameObjects.add(text);
            }
            return text;
        }
    }
}
