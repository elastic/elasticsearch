/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.snapshots.IndexShardSnapshotFailedException;
import org.elasticsearch.index.snapshots.IndexShardSnapshotStatus;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreFileMetadata;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.test.DummyShardLock;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.IndexSettingsModule;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.containsString;

public class LocalPrimarySnapshotShardContextTests extends ESTestCase {

    /**
     * Verifies the failure contract of {@link LocalPrimarySnapshotShardContext#assertFileContentsMatchHash}: genuine
     * verification failures (hash mismatch, corruption) are reported as {@link IndexShardSnapshotFailedException} so that they
     * fail the shard snapshot, and never as an {@link AssertionError}, which would escape the snapshot thread pool worker
     * without completing the shard snapshot and leave the snapshot in progress forever (see #154655). Transient I/O failures
     * do not indicate a verification failure and are ignored.
     */
    public void testAssertFileContentsMatchHash() throws IOException {
        final AtomicReference<Supplier<IOException>> openInputFailure = new AtomicReference<>();
        try (Directory directory = new FilterDirectory(new NIOFSDirectory(createTempDir())) {
            @Override
            public IndexInput openInput(String name, IOContext context) throws IOException {
                final var failure = openInputFailure.get();
                if (failure != null) {
                    throw failure.get();
                }
                return super.openInput(name, context);
            }
        }) {
            try (IndexWriter writer = new IndexWriter(directory, newIndexWriterConfig().setCodec(TestUtil.getDefaultCodec()))) {
                for (int i = 0; i < randomIntBetween(1, 10); i++) {
                    final var doc = new Document();
                    doc.add(new StringField("id", "" + i, Field.Store.YES));
                    writer.addDocument(doc);
                }
                writer.commit();
            }

            final IndexSettings indexSettings = IndexSettingsModule.newIndexSettings(
                "index",
                Settings.builder().put(IndexMetadata.SETTING_INDEX_UUID, "_uuid").build()
            );
            final var shardId = new ShardId(indexSettings.getIndex(), 0);
            try (Store store = new Store(shardId, indexSettings, directory, new DummyShardLock(shardId))) {
                final IndexCommit indexCommit = Lucene.getIndexCommit(Lucene.readSegmentInfos(store.directory()), store.directory());
                final var context = new LocalPrimarySnapshotShardContext(
                    store,
                    null,
                    new SnapshotId("snapshot", "_na_"),
                    new IndexId(indexSettings.getIndex().getName(), indexSettings.getUUID()),
                    new SnapshotIndexCommit(new Engine.IndexCommitRef(indexCommit, () -> {})),
                    null,
                    IndexShardSnapshotStatus.newInitializing(null, randomNonNegativeLong()),
                    IndexVersion.current(),
                    randomMillisUpToYear9999(),
                    new PlainActionFuture<>()
                );

                // The segments file is one of the files whose full contents are stored as the metadata hash and verified
                final StoreFileMetadata segmentsMetadata = store.getMetadata(indexCommit).get(indexCommit.getSegmentsFileName());
                assertTrue(segmentsMetadata.hashEqualsContents());

                // Matching contents verify successfully
                assertTrue(context.assertFileContentsMatchHash(fileInfo(segmentsMetadata)));

                // A transient I/O failure is not a verification failure and must not throw
                openInputFailure.set(() -> new IOException("simulated transient failure"));
                assertTrue(context.assertFileContentsMatchHash(fileInfo(segmentsMetadata)));
                openInputFailure.set(null);

                // A hash mismatch fails the shard snapshot
                final BytesRef hash = segmentsMetadata.hash();
                final byte[] tamperedHash = ArrayUtil.copyOfSubArray(hash.bytes, hash.offset, hash.offset + hash.length);
                tamperedHash[randomIntBetween(0, tamperedHash.length - 1)]++;
                final var tamperedMetadata = new StoreFileMetadata(
                    segmentsMetadata.name(),
                    segmentsMetadata.length(),
                    segmentsMetadata.checksum(),
                    segmentsMetadata.writtenBy(),
                    new BytesRef(tamperedHash),
                    segmentsMetadata.writerUuid()
                );
                final var mismatchException = expectThrows(
                    IndexShardSnapshotFailedException.class,
                    () -> context.assertFileContentsMatchHash(fileInfo(tamperedMetadata))
                );
                assertThat(mismatchException.getMessage(), containsString("differ from the hash"));

                // A corruption exception fails the shard snapshot and marks the store corrupted
                assertFalse(store.isMarkedCorrupted());
                openInputFailure.set(() -> new CorruptIndexException("simulated corruption", "_test_resource"));
                final var corruptionException = expectThrows(
                    IndexShardSnapshotFailedException.class,
                    () -> context.assertFileContentsMatchHash(fileInfo(segmentsMetadata))
                );
                assertThat(corruptionException.getMessage(), containsString("corruption"));
                openInputFailure.set(null);
                assertTrue(store.isMarkedCorrupted());
            }
        }
    }

    private static BlobStoreIndexShardSnapshot.FileInfo fileInfo(StoreFileMetadata metadata) {
        return new BlobStoreIndexShardSnapshot.FileInfo("__" + metadata.name(), metadata, null);
    }
}
