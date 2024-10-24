/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.index.store;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexFormatTooNewException;
import org.apache.lucene.index.IndexFormatTooOldException;
import org.apache.lucene.index.IndexNotFoundException;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.KeepOnlyLastCommitDeletionPolicy;
import org.apache.lucene.index.NoDeletionPolicy;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.SnapshotDeletionPolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.FilterIndexInput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.store.BaseDirectoryWrapper;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Version;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.io.stream.OutputStreamStreamOutput;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.store.FilterIndexOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.env.ShardLock;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.seqno.ReplicationTracker;
import org.elasticsearch.index.seqno.RetentionLease;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.store.TransportNodesListShardStoreMetadata;
import org.elasticsearch.test.DummyShardLock;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.IndexSettingsModule;
import org.hamcrest.Matchers;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongUnaryOperator;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableMap;
import static org.elasticsearch.test.TransportVersionUtils.randomCompatibleVersion;
import static org.elasticsearch.test.TransportVersionUtils.randomVersion;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.sameInstance;

public class StoreTests extends ESTestCase {

    private static final IndexSettings INDEX_SETTINGS = IndexSettingsModule.newIndexSettings(
        "index",
        Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()).build()
    );
    private static final Version MIN_SUPPORTED_LUCENE_VERSION = IndexVersions.MINIMUM_COMPATIBLE.luceneVersion();

    public void testRefCount() {
        final ShardId shardId = new ShardId("index", "_na_", 1);
        IndexSettings indexSettings = INDEX_SETTINGS;
        Store store = new Store(shardId, indexSettings, StoreTests.newDirectory(random()), new DummyShardLock(shardId));
        int incs = randomIntBetween(1, 100);
        for (int i = 0; i < incs; i++) {
            if (randomBoolean()) {
                store.incRef();
            } else {
                assertTrue(store.tryIncRef());
            }
            store.ensureOpen();
        }

        for (int i = 0; i < incs; i++) {
            store.decRef();
            store.ensureOpen();
        }

        store.incRef();
        store.close();
        for (int i = 0; i < incs; i++) {
            if (randomBoolean()) {
                store.incRef();
            } else {
                assertTrue(store.tryIncRef());
            }
            store.ensureOpen();
        }

        for (int i = 0; i < incs; i++) {
            store.decRef();
            store.ensureOpen();
        }

        store.decRef();
        assertThat(store.refCount(), Matchers.equalTo(0));
        assertFalse(store.tryIncRef());
        expectThrows(IllegalStateException.class, store::incRef);
        expectThrows(IllegalStateException.class, store::ensureOpen);
    }

    public void testNewChecksums() throws IOException {

        class CorruptibleInput extends FilterIndexInput {

            private final long corruptionPosition;

            CorruptibleInput(IndexInput in, long corruptionPosition) {
                super(in.toString(), in);
                this.corruptionPosition = corruptionPosition;
            }

            private static byte maybeCorruptionMask(boolean doCorrupt) {
                return (byte) ((doCorrupt ? 1 : 0) << between(0, Byte.SIZE - 1));
            }

            @Override
            public byte readByte() throws IOException {
                return (byte) (super.readByte() ^ maybeCorruptionMask(in.getFilePointer() == corruptionPosition));
            }

            @Override
            public void readBytes(byte[] b, int offset, int len) throws IOException {
                final var startPointer = in.getFilePointer();
                super.readBytes(b, offset, len);
                if (startPointer <= corruptionPosition && corruptionPosition < in.getFilePointer()) {
                    b[Math.toIntExact(offset + corruptionPosition - startPointer)] ^= maybeCorruptionMask(true);
                }
            }
        }

        class CorruptibleDirectory extends FilterDirectory {
            String corruptFileName;
            int openCount;
            long corruptionPosition;

            CorruptibleDirectory() {
                super(StoreTests.newDirectory(random()));
            }

            @Override
            public IndexInput openInput(String name, IOContext context) throws IOException {
                if (name.equals(corruptFileName)) {
                    // the files whose checksums are validated are opened once by Lucene and then once again by ES to copy their contents
                    // into memory, and we want to make sure that we detect a corruption in ES so we make sure that Lucene sees the correct
                    // contents
                    assert openCount < 2;
                    openCount += 1;
                    if (openCount == 2) {
                        return new CorruptibleInput(super.openInput(name, context), corruptionPosition);
                    }
                }
                return super.openInput(name, context);
            }
        }

        final ShardId shardId = new ShardId("index", "_na_", 1);
        final CorruptibleDirectory directory = new CorruptibleDirectory();

        Store store = new Store(shardId, INDEX_SETTINGS, directory, new DummyShardLock(shardId));
        // set default codec - all segments need checksums
        IndexWriter writer = new IndexWriter(
            store.directory(),
            newIndexWriterConfig(random(), new MockAnalyzer(random())).setCodec(TestUtil.getDefaultCodec())
        );
        int docs = 1 + random().nextInt(100);

        for (int i = 0; i < docs; i++) {
            Document doc = new Document();
            doc.add(new TextField("id", "" + i, random().nextBoolean() ? Field.Store.YES : Field.Store.NO));
            doc.add(
                new TextField(
                    "body",
                    TestUtil.randomRealisticUnicodeString(random()),
                    random().nextBoolean() ? Field.Store.YES : Field.Store.NO
                )
            );
            doc.add(new SortedDocValuesField("dv", new BytesRef(TestUtil.randomRealisticUnicodeString(random()))));
            writer.addDocument(doc);
        }
        if (random().nextBoolean()) {
            for (int i = 0; i < docs; i++) {
                if (random().nextBoolean()) {
                    Document doc = new Document();
                    doc.add(new TextField("id", "" + i, random().nextBoolean() ? Field.Store.YES : Field.Store.NO));
                    doc.add(
                        new TextField(
                            "body",
                            TestUtil.randomRealisticUnicodeString(random()),
                            random().nextBoolean() ? Field.Store.YES : Field.Store.NO
                        )
                    );
                    writer.updateDocument(new Term("id", "" + i), doc);
                }
            }
        }
        if (random().nextBoolean()) {
            DirectoryReader.open(writer).close(); // flush
        }
        Store.MetadataSnapshot metadata;
        // check before we committed
        try {
            store.getMetadata(null);
            fail("no index present - expected exception");
        } catch (IndexNotFoundException ex) {
            // expected
        }
        writer.commit();
        writer.close();

        final IndexCommit indexCommit;
        try (var reader = DirectoryReader.open(store.directory())) {
            indexCommit = reader.getIndexCommit();
        }

        metadata = store.getMetadata(randomBoolean() ? indexCommit : null);
        assertThat(metadata.fileMetadataMap().isEmpty(), is(false));
        for (StoreFileMetadata meta : metadata) {
            try (IndexInput input = store.directory().openInput(meta.name(), IOContext.READONCE)) {
                String checksum = Store.digestToString(CodecUtil.retrieveChecksum(input));
                assertThat("File: " + meta.name() + " has a different checksum", meta.checksum(), equalTo(checksum));
                assertThat(meta.writtenBy(), equalTo(Version.LATEST.toString()));
                if (Store.MetadataSnapshot.isReadAsHash(meta.name())) {
                    assertThat(meta.hash().length, allOf(greaterThan(CodecUtil.footerLength()), equalTo(Math.toIntExact(meta.length()))));
                } else {
                    assertThat(meta.hash().length, equalTo(0));
                }
            }
        }
        assertConsistent(store, metadata);

        TestUtil.checkIndex(store.directory());

        final var metaToCorrupt = randomFrom(
            metadata.fileMetadataMap().values().stream().filter(meta -> Store.MetadataSnapshot.isReadAsHash(meta.name())).toList()
        );
        directory.corruptFileName = metaToCorrupt.name();
        directory.openCount = 0;
        directory.corruptionPosition = randomLongBetween(0, metaToCorrupt.length() - 1);
        expectThrows(CorruptIndexException.class, () -> store.getMetadata(randomBoolean() ? indexCommit : null));

        assertDeleteContent(store, store.directory());
        IOUtils.close(store);
    }

    public void testCheckIntegrity() throws IOException {
        Directory dir = newDirectory();
        long luceneFileLength = 0;

        try (IndexOutput output = dir.createOutput("lucene_checksum.bin", IOContext.DEFAULT)) {
            int iters = scaledRandomIntBetween(10, 100);
            for (int i = 0; i < iters; i++) {
                BytesRef bytesRef = new BytesRef(TestUtil.randomRealisticUnicodeString(random(), 10, 1024));
                output.writeBytes(bytesRef.bytes, bytesRef.offset, bytesRef.length);
                luceneFileLength += bytesRef.length;
            }
            CodecUtil.writeFooter(output);
            luceneFileLength += CodecUtil.footerLength();

        }

        try (IndexInput indexInput = dir.openInput("lucene_checksum.bin", IOContext.DEFAULT)) {
            assertEquals(luceneFileLength, indexInput.length());
        }

        dir.close();

    }

    public void testVerifyingIndexInput() throws IOException {
        Directory dir = newDirectory();
        IndexOutput output = dir.createOutput("foo.bar", IOContext.DEFAULT);
        int iters = scaledRandomIntBetween(10, 100);
        for (int i = 0; i < iters; i++) {
            BytesRef bytesRef = new BytesRef(TestUtil.randomRealisticUnicodeString(random(), 10, 1024));
            output.writeBytes(bytesRef.bytes, bytesRef.offset, bytesRef.length);
        }
        CodecUtil.writeFooter(output);
        output.close();

        // Check file
        IndexInput indexInput = dir.openInput("foo.bar", IOContext.DEFAULT);
        long checksum = CodecUtil.retrieveChecksum(indexInput);
        indexInput.seek(0);
        IndexInput verifyingIndexInput = new Store.VerifyingIndexInput(dir.openInput("foo.bar", IOContext.DEFAULT));
        readIndexInputFullyWithRandomSeeks(verifyingIndexInput);
        Store.verify(verifyingIndexInput);
        assertThat(checksum, equalTo(((ChecksumIndexInput) verifyingIndexInput).getChecksum()));
        IOUtils.close(indexInput, verifyingIndexInput);

        // Corrupt file and check again
        corruptFile(dir, "foo.bar", "foo1.bar");
        verifyingIndexInput = new Store.VerifyingIndexInput(dir.openInput("foo1.bar", IOContext.DEFAULT));
        readIndexInputFullyWithRandomSeeks(verifyingIndexInput);
        try {
            Store.verify(verifyingIndexInput);
            fail("should be a corrupted index");
        } catch (CorruptIndexException | IndexFormatTooOldException | IndexFormatTooNewException ex) {
            // ok
        }
        IOUtils.close(verifyingIndexInput);
        IOUtils.close(dir);
    }

    private void readIndexInputFullyWithRandomSeeks(IndexInput indexInput) throws IOException {
        BytesRef ref = new BytesRef(scaledRandomIntBetween(1, 1024));
        long pos = 0;
        while (pos < indexInput.length()) {
            assertEquals(pos, indexInput.getFilePointer());
            int op = random().nextInt(5);
            if (op == 0) {
                int shift = 100 - randomIntBetween(0, 200);
                pos = Math.min(indexInput.length() - 1, Math.max(0, pos + shift));
                indexInput.seek(pos);
            } else if (op == 1) {
                indexInput.readByte();
                pos++;
            } else {
                int min = (int) Math.min(indexInput.length() - pos, ref.bytes.length);
                indexInput.readBytes(ref.bytes, ref.offset, min);
                pos += min;
            }
        }
    }

    private void corruptFile(Directory dir, String fileIn, String fileOut) throws IOException {
        IndexInput input = dir.openInput(fileIn, IOContext.READONCE);
        IndexOutput output = dir.createOutput(fileOut, IOContext.DEFAULT);
        long len = input.length();
        byte[] b = new byte[1024];
        long broken = randomInt((int) len - 1);
        long pos = 0;
        while (pos < len) {
            int min = (int) Math.min(input.length() - pos, b.length);
            input.readBytes(b, 0, min);
            if (broken >= pos && broken < pos + min) {
                // Flip one byte
                int flipPos = (int) (broken - pos);
                b[flipPos] = (byte) (b[flipPos] ^ 42);
            }
            output.writeBytes(b, min);
            pos += min;
        }
        IOUtils.close(input, output);

    }

    public void assertDeleteContent(Store store, Directory dir) throws IOException {
        deleteContent(store.directory());
        assertThat(Arrays.toString(store.directory().listAll()), store.directory().listAll().length, equalTo(0));
        assertThat(store.stats(0L, LongUnaryOperator.identity()).sizeInBytes(), equalTo(0L));
        assertThat(dir.listAll().length, equalTo(0));
    }

    public static void assertConsistent(Store store, Store.MetadataSnapshot metadata) throws IOException {
        for (String file : store.directory().listAll()) {
            if (IndexWriter.WRITE_LOCK_NAME.equals(file) == false && file.startsWith("extra") == false) {
                assertTrue(
                    file + " is not in the map: " + metadata.fileMetadataMap().size() + " vs. " + store.directory().listAll().length,
                    metadata.fileMetadataMap().containsKey(file)
                );
            } else {
                assertFalse(
                    file + " is not in the map: " + metadata.fileMetadataMap().size() + " vs. " + store.directory().listAll().length,
                    metadata.fileMetadataMap().containsKey(file)
                );
            }
        }
    }

    public void testRecoveryDiff() throws IOException, InterruptedException {
        int numDocs = 2 + random().nextInt(100);
        List<Document> docs = new ArrayList<>();
        for (int i = 0; i < numDocs; i++) {
            Document doc = new Document();
            final Field.Store stringFieldStored = random().nextBoolean() ? Field.Store.YES : Field.Store.NO;
            doc.add(new StringField("id", "" + i, stringFieldStored));
            final String textFieldContent = TestUtil.randomRealisticUnicodeString(random());
            final Field.Store textFieldStored = random().nextBoolean() ? Field.Store.YES : Field.Store.NO;
            doc.add(new TextField("body", textFieldContent, textFieldStored));
            final String docValueFieldContent = TestUtil.randomRealisticUnicodeString(random());
            doc.add(new BinaryDocValuesField("dv", new BytesRef(docValueFieldContent)));
            docs.add(doc);
            logger.info(
                "--> doc [{}] id=[{}] (store={}) body=[{}] (store={}) dv=[{}]",
                i,
                i,
                stringFieldStored,
                textFieldContent,
                textFieldStored,
                docValueFieldContent
            );
        }
        long seed = random().nextLong();
        Store.MetadataSnapshot first;
        {
            Random random = new Random(seed);
            IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random)).setCodec(TestUtil.getDefaultCodec());
            iwc.setMergePolicy(NoMergePolicy.INSTANCE);
            iwc.setUseCompoundFile(random.nextBoolean());
            final ShardId shardId = new ShardId("index", "_na_", 1);
            Store store = new Store(shardId, INDEX_SETTINGS, StoreTests.newDirectory(random()), new DummyShardLock(shardId));
            IndexWriter writer = new IndexWriter(store.directory(), iwc);
            final boolean lotsOfSegments = rarely(random);
            for (Document d : docs) {
                writer.addDocument(d);
                if (lotsOfSegments && random.nextBoolean() || rarely(random)) {
                    logger.info("--> commit after doc {}", d.getField("id").stringValue());
                    writer.commit();
                }
            }
            writer.commit();
            writer.close();
            first = store.getMetadata(null);
            assertDeleteContent(store, store.directory());
            store.close();
        }
        long time = new Date().getTime();
        while (time == new Date().getTime()) {
            Thread.sleep(10); // bump the time
        }
        Store.MetadataSnapshot second;
        Store store;
        {
            Random random = new Random(seed);
            IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random)).setCodec(TestUtil.getDefaultCodec());
            iwc.setMergePolicy(NoMergePolicy.INSTANCE);
            iwc.setUseCompoundFile(random.nextBoolean());
            final ShardId shardId = new ShardId("index", "_na_", 1);
            store = new Store(shardId, INDEX_SETTINGS, StoreTests.newDirectory(random()), new DummyShardLock(shardId));
            IndexWriter writer = new IndexWriter(store.directory(), iwc);
            final boolean lotsOfSegments = rarely(random);
            for (Document d : docs) {
                writer.addDocument(d);
                if (lotsOfSegments && random.nextBoolean() || rarely(random)) {
                    writer.commit();
                }
            }
            writer.commit();
            writer.close();
            second = store.getMetadata(null);
        }
        Store.RecoveryDiff diff = first.recoveryDiff(second);
        assertThat(first.size(), equalTo(second.size()));
        for (StoreFileMetadata md : first) {
            assertThat(second.get(md.name()), notNullValue());
            // si files are different - containing timestamps etc
            assertThat(second.get(md.name()).isSame(md), equalTo(false));
        }
        assertThat(diff.different.size(), equalTo(first.size()));
        assertThat(diff.identical.size(), equalTo(0)); // in lucene 5 nothing is identical - we use random ids in file headers
        assertThat(diff.missing, empty());

        // check the self diff
        Store.RecoveryDiff selfDiff = first.recoveryDiff(first);
        assertThat(selfDiff.identical.size(), equalTo(first.size()));
        assertThat(selfDiff.different, empty());
        assertThat(selfDiff.missing, empty());

        // delete a doc
        final String deleteId = Integer.toString(random().nextInt(numDocs));
        Store.MetadataSnapshot metadata;
        {
            Random random = new Random(seed);
            IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random)).setCodec(TestUtil.getDefaultCodec());
            iwc.setMergePolicy(NoMergePolicy.INSTANCE);
            iwc.setUseCompoundFile(random.nextBoolean());
            iwc.setOpenMode(IndexWriterConfig.OpenMode.APPEND);
            IndexWriter writer = new IndexWriter(store.directory(), iwc);
            logger.info("--> delete doc {}", deleteId);
            writer.deleteDocuments(new Term("id", deleteId));
            writer.commit();
            writer.close();
            metadata = store.getMetadata(null);
        }
        StoreFileMetadata delFile = null;
        for (StoreFileMetadata md : metadata) {
            if (md.name().endsWith(".liv")) {
                delFile = md;
                logger.info("--> delFile=[{}]", delFile);
                break;
            }
        }
        Store.RecoveryDiff afterDeleteDiff = metadata.recoveryDiff(second);
        if (delFile != null) {
            assertThat(afterDeleteDiff.toString(), afterDeleteDiff.identical.size(), equalTo(metadata.size() - 2)); // segments_N + del file
            assertThat(afterDeleteDiff.toString(), afterDeleteDiff.different.size(), equalTo(0));
            assertThat(afterDeleteDiff.toString(), afterDeleteDiff.missing.size(), equalTo(2));
        } else {
            // an entire segment must be missing (single doc segment got dropped)
            assertThat(afterDeleteDiff.toString(), afterDeleteDiff.identical.size(), greaterThan(0));
            assertThat(afterDeleteDiff.toString(), afterDeleteDiff.different.size(), equalTo(0));
            assertThat(afterDeleteDiff.toString(), afterDeleteDiff.missing.size(), equalTo(1)); // the commit file is different
        }

        // check the self diff
        selfDiff = metadata.recoveryDiff(metadata);
        assertThat(selfDiff.identical.size(), equalTo(metadata.size()));
        assertThat(selfDiff.different, empty());
        assertThat(selfDiff.missing, empty());

        // add a new commit
        {
            Random random = new Random(seed);
            IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random)).setCodec(TestUtil.getDefaultCodec());
            iwc.setMergePolicy(NoMergePolicy.INSTANCE);
            iwc.setUseCompoundFile(true); // force CFS - easier to test here since we know it will add 3 files
            iwc.setOpenMode(IndexWriterConfig.OpenMode.APPEND);
            IndexWriter writer = new IndexWriter(store.directory(), iwc);
            logger.info("--> add new empty doc");
            writer.addDocument(new Document());
            writer.close();
        }

        Store.MetadataSnapshot newCommitMetadata = store.getMetadata(null);
        Store.RecoveryDiff newCommitDiff = newCommitMetadata.recoveryDiff(metadata);
        if (delFile != null) {
            assertThat(newCommitDiff.toString(), newCommitDiff.identical.size(), equalTo(newCommitMetadata.size() - 4)); // segments_N, cfs,
                                                                                                                         // cfe, si for the
                                                                                                                         // new segment
            assertThat(newCommitDiff.toString(), newCommitDiff.different.size(), equalTo(0)); // the del file must be different
            assertThat(newCommitDiff.toString(), newCommitDiff.missing.size(), equalTo(4)); // segments_N,cfs, cfe, si for the new segment
            assertTrue(newCommitDiff.toString(), newCommitDiff.identical.stream().anyMatch(m -> m.name().endsWith(".liv")));
        } else {
            assertThat(newCommitDiff.toString(), newCommitDiff.identical.size(), equalTo(newCommitMetadata.size() - 4)); // segments_N, cfs,
                                                                                                                         // cfe, si for the
                                                                                                                         // new segment
            assertThat(newCommitDiff.toString(), newCommitDiff.different.size(), equalTo(0));
            assertThat(newCommitDiff.toString(), newCommitDiff.missing.size(), equalTo(4)); // an entire segment must be missing (single doc
                                                                                            // segment got dropped) plus the commit is
                                                                                            // different
        }

        // update doc values
        Store.MetadataSnapshot dvUpdateSnapshot;
        final String updateId = randomValueOtherThan(deleteId, () -> Integer.toString(random().nextInt(numDocs)));
        {
            Random random = new Random(seed);
            IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random)).setCodec(TestUtil.getDefaultCodec());
            iwc.setMergePolicy(NoMergePolicy.INSTANCE);
            iwc.setUseCompoundFile(random.nextBoolean());
            iwc.setOpenMode(IndexWriterConfig.OpenMode.APPEND);
            try (IndexWriter writer = new IndexWriter(store.directory(), iwc)) {
                final String newDocValue = TestUtil.randomRealisticUnicodeString(random());
                logger.info("--> update doc [{}] with dv=[{}]", updateId, newDocValue);
                writer.updateBinaryDocValue(new Term("id", updateId), "dv", new BytesRef(newDocValue));
                writer.commit();
            }
            dvUpdateSnapshot = store.getMetadata(null);
        }
        logger.info("--> source: {}", dvUpdateSnapshot.fileMetadataMap());
        logger.info("--> target: {}", newCommitMetadata.fileMetadataMap());
        Store.RecoveryDiff dvUpdateDiff = dvUpdateSnapshot.recoveryDiff(newCommitMetadata);
        final int delFileCount;
        if (delFile == null || dvUpdateDiff.different.isEmpty()) {
            // liv file either doesn't exist or belongs to a different segment from the one that we just updated
            delFileCount = 0;
            assertThat(dvUpdateDiff.toString(), dvUpdateDiff.different, empty());
        } else {
            // liv file is generational and belongs to the updated segment
            delFileCount = 1;
            assertThat(dvUpdateDiff.toString(), dvUpdateDiff.different.size(), equalTo(1));
            assertThat(dvUpdateDiff.toString(), dvUpdateDiff.different.get(0).name(), endsWith(".liv"));
        }

        assertThat(dvUpdateDiff.toString(), dvUpdateDiff.identical.size(), equalTo(dvUpdateSnapshot.size() - 4 - delFileCount));
        assertThat(dvUpdateDiff.toString(), dvUpdateDiff.different.size(), equalTo(delFileCount));
        assertThat(dvUpdateDiff.toString(), dvUpdateDiff.missing.size(), equalTo(4)); // segments_N, fnm, dvd, dvm for the updated segment

        deleteContent(store.directory());
        IOUtils.close(store);
    }

    public void testCleanupFromSnapshot() throws IOException {
        final ShardId shardId = new ShardId("index", "_na_", 1);
        Store store = new Store(shardId, INDEX_SETTINGS, StoreTests.newDirectory(random()), new DummyShardLock(shardId));
        // this time random codec....
        IndexWriterConfig indexWriterConfig = newIndexWriterConfig(random(), new MockAnalyzer(random())).setCodec(
            TestUtil.getDefaultCodec()
        );
        // we keep all commits and that allows us clean based on multiple snapshots
        indexWriterConfig.setIndexDeletionPolicy(NoDeletionPolicy.INSTANCE);
        IndexWriter writer = new IndexWriter(store.directory(), indexWriterConfig);
        int docs = 1 + random().nextInt(100);
        int numCommits = 0;
        for (int i = 0; i < docs; i++) {
            if (i > 0 && randomIntBetween(0, 10) == 0) {
                writer.commit();
                numCommits++;
            }
            Document doc = new Document();
            doc.add(new TextField("id", "" + i, random().nextBoolean() ? Field.Store.YES : Field.Store.NO));
            doc.add(
                new TextField(
                    "body",
                    TestUtil.randomRealisticUnicodeString(random()),
                    random().nextBoolean() ? Field.Store.YES : Field.Store.NO
                )
            );
            doc.add(new SortedDocValuesField("dv", new BytesRef(TestUtil.randomRealisticUnicodeString(random()))));
            writer.addDocument(doc);

        }
        if (numCommits < 1) {
            writer.commit();
            Document doc = new Document();
            doc.add(new TextField("id", "" + docs++, random().nextBoolean() ? Field.Store.YES : Field.Store.NO));
            doc.add(
                new TextField(
                    "body",
                    TestUtil.randomRealisticUnicodeString(random()),
                    random().nextBoolean() ? Field.Store.YES : Field.Store.NO
                )
            );
            doc.add(new SortedDocValuesField("dv", new BytesRef(TestUtil.randomRealisticUnicodeString(random()))));
            writer.addDocument(doc);
        }

        Store.MetadataSnapshot firstMeta = store.getMetadata(null);

        if (random().nextBoolean()) {
            for (int i = 0; i < docs; i++) {
                if (random().nextBoolean()) {
                    Document doc = new Document();
                    doc.add(new TextField("id", "" + i, random().nextBoolean() ? Field.Store.YES : Field.Store.NO));
                    doc.add(
                        new TextField(
                            "body",
                            TestUtil.randomRealisticUnicodeString(random()),
                            random().nextBoolean() ? Field.Store.YES : Field.Store.NO
                        )
                    );
                    writer.updateDocument(new Term("id", "" + i), doc);
                }
            }
        }
        writer.commit();
        writer.close();

        Store.MetadataSnapshot secondMeta = store.getMetadata(null);

        if (randomBoolean()) {
            store.cleanupAndVerify("test", firstMeta);
            String[] strings = store.directory().listAll();
            int numNotFound = 0;
            for (String file : strings) {
                if (file.startsWith("extra")) {
                    continue;
                }
                assertTrue(firstMeta.contains(file) || file.equals("write.lock"));
                if (secondMeta.contains(file) == false) {
                    numNotFound++;
                }

            }
            assertTrue("at least one file must not be in here since we have two commits?", numNotFound > 0);
        } else {
            store.cleanupAndVerify("test", secondMeta);
            String[] strings = store.directory().listAll();
            int numNotFound = 0;
            for (String file : strings) {
                if (file.startsWith("extra")) {
                    continue;
                }
                assertTrue(file, secondMeta.contains(file) || file.equals("write.lock"));
                if (firstMeta.contains(file) == false) {
                    numNotFound++;
                }

            }
            assertTrue("at least one file must not be in here since we have two commits?", numNotFound > 0);
        }

        deleteContent(store.directory());
        IOUtils.close(store);
    }

    public void testOnCloseCallback() throws IOException {
        final ShardId shardId = new ShardId(
            new Index(randomRealisticUnicodeOfCodepointLengthBetween(1, 10), "_na_"),
            randomIntBetween(0, 100)
        );
        final AtomicInteger count = new AtomicInteger(0);
        final ShardLock lock = new DummyShardLock(shardId);

        Store store = new Store(shardId, INDEX_SETTINGS, StoreTests.newDirectory(random()), lock, theLock -> {
            assertEquals(shardId, theLock.getShardId());
            assertEquals(lock, theLock);
            count.incrementAndGet();
        }, false);
        assertEquals(count.get(), 0);

        final int iters = randomIntBetween(1, 10);
        for (int i = 0; i < iters; i++) {
            store.close();
        }

        assertEquals(count.get(), 1);
    }

    public void testStoreStats() throws IOException {
        final ShardId shardId = new ShardId("index", "_na_", 1);
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
            .put(Store.INDEX_STORE_STATS_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.timeValueMinutes(0))
            .build();
        Store store = new Store(
            shardId,
            IndexSettingsModule.newIndexSettings("index", settings),
            StoreTests.newDirectory(random()),
            new DummyShardLock(shardId)
        );
        long initialStoreSize = 0;
        for (String extraFiles : store.directory().listAll()) {
            assertTrue("expected extraFS file but got: " + extraFiles, extraFiles.startsWith("extra"));
            initialStoreSize += store.directory().fileLength(extraFiles);
        }
        final long localStoreSizeDelta = randomLongBetween(-initialStoreSize, initialStoreSize);
        final long reservedBytes = randomBoolean() ? StoreStats.UNKNOWN_RESERVED_BYTES : randomLongBetween(0L, Integer.MAX_VALUE);
        StoreStats stats = store.stats(reservedBytes, size -> size + localStoreSizeDelta);
        assertEquals(initialStoreSize, stats.totalDataSetSizeInBytes());
        assertEquals(initialStoreSize + localStoreSizeDelta, stats.sizeInBytes());
        assertEquals(reservedBytes, stats.reservedSizeInBytes());

        stats.add(null);
        assertEquals(initialStoreSize, stats.totalDataSetSizeInBytes());
        assertEquals(initialStoreSize + localStoreSizeDelta, stats.sizeInBytes());
        assertEquals(reservedBytes, stats.reservedSizeInBytes());

        final long otherStatsDataSetBytes = randomLongBetween(0L, Integer.MAX_VALUE);
        final long otherStatsLocalBytes = randomLongBetween(0L, Integer.MAX_VALUE);
        final long otherStatsReservedBytes = randomBoolean() ? StoreStats.UNKNOWN_RESERVED_BYTES : randomLongBetween(0L, Integer.MAX_VALUE);
        stats.add(new StoreStats(otherStatsLocalBytes, otherStatsDataSetBytes, otherStatsReservedBytes));
        assertEquals(initialStoreSize + otherStatsDataSetBytes, stats.totalDataSetSizeInBytes());
        assertEquals(initialStoreSize + otherStatsLocalBytes + localStoreSizeDelta, stats.sizeInBytes());
        assertEquals(Math.max(reservedBytes, 0L) + Math.max(otherStatsReservedBytes, 0L), stats.reservedSizeInBytes());

        Directory dir = store.directory();
        final long length;
        try (IndexOutput output = dir.createOutput("foo.bar", IOContext.DEFAULT)) {
            int iters = scaledRandomIntBetween(10, 100);
            for (int i = 0; i < iters; i++) {
                BytesRef bytesRef = new BytesRef(TestUtil.randomRealisticUnicodeString(random(), 10, 1024));
                output.writeBytes(bytesRef.bytes, bytesRef.offset, bytesRef.length);
            }
            length = output.getFilePointer();
        }

        assertTrue(numNonExtraFiles(store) > 0);
        stats = store.stats(0L, size -> size + localStoreSizeDelta);
        assertEquals(initialStoreSize + length, stats.totalDataSetSizeInBytes());
        assertEquals(initialStoreSize + localStoreSizeDelta + length, stats.sizeInBytes());

        deleteContent(store.directory());
        IOUtils.close(store);
    }

    public void testStoreSizes() throws IOException {
        // directory that returns total written bytes as the data set size
        final var directory = new ByteSizeDirectory(StoreTests.newDirectory(random())) {

            final AtomicLong dataSetBytes = new AtomicLong(estimateSizeInBytes(getDelegate()));

            @Override
            public long estimateSizeInBytes() throws IOException {
                return estimateSizeInBytes(getDelegate());
            }

            @Override
            public long estimateDataSetSizeInBytes() {
                return dataSetBytes.get();
            }

            @Override
            public IndexOutput createOutput(String name, IOContext context) throws IOException {
                return wrap(super.createOutput(name, context));
            }

            @Override
            public IndexOutput createTempOutput(String prefix, String suffix, IOContext context) throws IOException {
                return wrap(super.createTempOutput(prefix, suffix, context));
            }

            private IndexOutput wrap(IndexOutput output) {
                return new FilterIndexOutput("wrapper", output) {
                    @Override
                    public void writeByte(byte b) throws IOException {
                        super.writeByte(b);
                        dataSetBytes.incrementAndGet();
                    }

                    @Override
                    public void writeBytes(byte[] b, int offset, int length) throws IOException {
                        super.writeBytes(b, offset, length);
                        dataSetBytes.addAndGet(length);
                    }
                };
            }
        };

        final ShardId shardId = new ShardId("index", "_na_", 1);
        final Store store = new Store(
            shardId,
            IndexSettingsModule.newIndexSettings(
                "index",
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
                    .put(Store.INDEX_STORE_STATS_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.timeValueMinutes(0))
                    .build()
            ),
            directory,
            new DummyShardLock(shardId)
        );
        long initialStoreSize = 0L;
        for (String extraFiles : store.directory().listAll()) {
            assertTrue("expected extraFS file but got: " + extraFiles, extraFiles.startsWith("extra"));
            initialStoreSize += store.directory().fileLength(extraFiles);
        }

        StoreStats stats = store.stats(0L, LongUnaryOperator.identity());
        assertThat(stats.sizeInBytes(), equalTo(initialStoreSize));
        assertThat(stats.totalDataSetSizeInBytes(), equalTo(initialStoreSize));

        long additionalStoreSize = 0L;

        int iters = randomIntBetween(1, 10);
        for (int i = 0; i < iters; i++) {
            try (IndexOutput output = directory.createOutput(i + ".bar", IOContext.DEFAULT)) {
                BytesRef bytesRef = new BytesRef(TestUtil.randomRealisticUnicodeString(random(), 10, 1024));
                output.writeBytes(bytesRef.bytes, bytesRef.offset, bytesRef.length);
                additionalStoreSize += output.getFilePointer();
            }
        }

        stats = store.stats(0L, LongUnaryOperator.identity());
        assertThat(stats.sizeInBytes(), equalTo(initialStoreSize + additionalStoreSize));
        assertThat(stats.totalDataSetSizeInBytes(), equalTo(initialStoreSize + additionalStoreSize));

        long deletionsStoreSize = 0L;

        var randomFiles = randomSubsetOf(Arrays.asList(directory.listAll()));
        for (String randomFile : randomFiles) {
            try {
                long length = directory.fileLength(randomFile);
                directory.deleteFile(randomFile);
                deletionsStoreSize += length;
            } catch (NoSuchFileException | FileNotFoundException e) {
                // ignore
            }
        }

        stats = store.stats(0L, LongUnaryOperator.identity());
        assertThat(stats.sizeInBytes(), equalTo(initialStoreSize + additionalStoreSize - deletionsStoreSize));
        assertThat(stats.totalDataSetSizeInBytes(), equalTo(initialStoreSize + additionalStoreSize));

        deleteContent(store.directory());
        IOUtils.close(store);
    }

    public static void deleteContent(Directory directory) throws IOException {
        final String[] files = directory.listAll();
        final List<IOException> exceptions = new ArrayList<>();
        for (String file : files) {
            try {
                directory.deleteFile(file);
            } catch (NoSuchFileException | FileNotFoundException e) {
                // ignore
            } catch (IOException e) {
                exceptions.add(e);
            }
        }
        ExceptionsHelper.rethrowAndSuppress(exceptions);
    }

    public int numNonExtraFiles(Store store) throws IOException {
        int numNonExtra = 0;
        for (String file : store.directory().listAll()) {
            if (file.startsWith("extra") == false) {
                numNonExtra++;
            }
        }
        return numNonExtra;
    }

    public void testMetadataSnapshotStreaming() throws Exception {
        Store.MetadataSnapshot outMetadataSnapshot = createMetadataSnapshot();
        TransportVersion targetVersion = randomVersion(random());

        ByteArrayOutputStream outBuffer = new ByteArrayOutputStream();
        OutputStreamStreamOutput out = new OutputStreamStreamOutput(outBuffer);
        out.setTransportVersion(targetVersion);
        outMetadataSnapshot.writeTo(out);

        ByteArrayInputStream inBuffer = new ByteArrayInputStream(outBuffer.toByteArray());
        InputStreamStreamInput in = new InputStreamStreamInput(inBuffer);
        in.setTransportVersion(targetVersion);
        Store.MetadataSnapshot inMetadataSnapshot = Store.MetadataSnapshot.readFrom(in);
        Map<String, StoreFileMetadata> origEntries = new HashMap<>();
        origEntries.putAll(outMetadataSnapshot.fileMetadataMap());
        for (Map.Entry<String, StoreFileMetadata> entry : inMetadataSnapshot.fileMetadataMap().entrySet()) {
            assertThat(entry.getValue().name(), equalTo(origEntries.remove(entry.getKey()).name()));
        }
        assertThat(origEntries.size(), equalTo(0));
        assertThat(inMetadataSnapshot.commitUserData(), equalTo(outMetadataSnapshot.commitUserData()));
    }

    public void testEmptyMetadataSnapshotStreaming() throws Exception {
        var outMetadataSnapshot = randomBoolean() ? Store.MetadataSnapshot.EMPTY : new Store.MetadataSnapshot(emptyMap(), emptyMap(), 0L);
        var targetVersion = randomCompatibleVersion(random());

        var outBuffer = new ByteArrayOutputStream();
        var out = new OutputStreamStreamOutput(outBuffer);
        out.setTransportVersion(targetVersion);
        outMetadataSnapshot.writeTo(out);

        var inBuffer = new ByteArrayInputStream(outBuffer.toByteArray());
        var in = new InputStreamStreamInput(inBuffer);
        in.setTransportVersion(targetVersion);
        assertThat(Store.MetadataSnapshot.readFrom(in), sameInstance(Store.MetadataSnapshot.EMPTY));
    }

    protected Store.MetadataSnapshot createMetadataSnapshot() {
        StoreFileMetadata storeFileMetadata1 = new StoreFileMetadata("segments", 1, "666", MIN_SUPPORTED_LUCENE_VERSION.toString());
        StoreFileMetadata storeFileMetadata2 = new StoreFileMetadata("no_segments", 1, "666", MIN_SUPPORTED_LUCENE_VERSION.toString());
        Map<String, StoreFileMetadata> storeFileMetadataMap = new HashMap<>();
        storeFileMetadataMap.put(storeFileMetadata1.name(), storeFileMetadata1);
        storeFileMetadataMap.put(storeFileMetadata2.name(), storeFileMetadata2);
        return new Store.MetadataSnapshot(unmodifiableMap(storeFileMetadataMap), Map.of("userdata_1", "test", "userdata_2", "test"), 0);
    }

    public void testUserDataRead() throws IOException {
        final ShardId shardId = new ShardId("index", "_na_", 1);
        Store store = new Store(shardId, INDEX_SETTINGS, StoreTests.newDirectory(random()), new DummyShardLock(shardId));
        IndexWriterConfig config = newIndexWriterConfig(random(), new MockAnalyzer(random())).setCodec(TestUtil.getDefaultCodec());
        SnapshotDeletionPolicy deletionPolicy = new SnapshotDeletionPolicy(new KeepOnlyLastCommitDeletionPolicy());
        config.setIndexDeletionPolicy(deletionPolicy);
        IndexWriter writer = new IndexWriter(store.directory(), config);
        Document doc = new Document();
        doc.add(new TextField("id", "1", Field.Store.NO));
        writer.addDocument(doc);
        writer.commit();
        writer.close();
        Store.MetadataSnapshot metadata;
        metadata = store.getMetadata(randomBoolean() ? null : deletionPolicy.snapshot());
        assertFalse(metadata.fileMetadataMap().isEmpty());
        // do not check for correct files, we have enough tests for that above
        TestUtil.checkIndex(store.directory());
        assertDeleteContent(store, store.directory());
        IOUtils.close(store);
    }

    public void testStreamStoreFilesMetadata() throws Exception {
        Store.MetadataSnapshot metadataSnapshot = createMetadataSnapshot();
        int numOfLeases = randomIntBetween(0, 10);
        List<RetentionLease> peerRecoveryRetentionLeases = new ArrayList<>();
        for (int i = 0; i < numOfLeases; i++) {
            peerRecoveryRetentionLeases.add(
                new RetentionLease(
                    ReplicationTracker.getPeerRecoveryRetentionLeaseId(UUIDs.randomBase64UUID()),
                    randomNonNegativeLong(),
                    randomNonNegativeLong(),
                    ReplicationTracker.PEER_RECOVERY_RETENTION_LEASE_SOURCE
                )
            );
        }
        TransportNodesListShardStoreMetadata.StoreFilesMetadata outStoreFileMetadata =
            new TransportNodesListShardStoreMetadata.StoreFilesMetadata(metadataSnapshot, peerRecoveryRetentionLeases);
        ByteArrayOutputStream outBuffer = new ByteArrayOutputStream();
        OutputStreamStreamOutput out = new OutputStreamStreamOutput(outBuffer);
        TransportVersion targetVersion = randomCompatibleVersion(random());
        out.setTransportVersion(targetVersion);
        outStoreFileMetadata.writeTo(out);
        ByteArrayInputStream inBuffer = new ByteArrayInputStream(outBuffer.toByteArray());
        InputStreamStreamInput in = new InputStreamStreamInput(inBuffer);
        in.setTransportVersion(targetVersion);
        var inStoreFileMetadata = TransportNodesListShardStoreMetadata.StoreFilesMetadata.readFrom(in);
        Iterator<StoreFileMetadata> outFiles = outStoreFileMetadata.iterator();
        for (StoreFileMetadata inFile : inStoreFileMetadata) {
            assertThat(inFile.name(), equalTo(outFiles.next().name()));
        }
        assertThat(outStoreFileMetadata.peerRecoveryRetentionLeases(), equalTo(peerRecoveryRetentionLeases));
    }

    public void testStreamEmptyStoreFilesMetadata() throws Exception {
        var outStoreFileMetadata = randomBoolean()
            ? TransportNodesListShardStoreMetadata.StoreFilesMetadata.EMPTY
            : new TransportNodesListShardStoreMetadata.StoreFilesMetadata(Store.MetadataSnapshot.EMPTY, emptyList());
        var outBuffer = new ByteArrayOutputStream();
        var out = new OutputStreamStreamOutput(outBuffer);
        var targetVersion = randomCompatibleVersion(random());
        out.setTransportVersion(targetVersion);
        outStoreFileMetadata.writeTo(out);

        var inBuffer = new ByteArrayInputStream(outBuffer.toByteArray());
        var in = new InputStreamStreamInput(inBuffer);
        in.setTransportVersion(targetVersion);
        assertThat(
            TransportNodesListShardStoreMetadata.StoreFilesMetadata.readFrom(in),
            sameInstance(TransportNodesListShardStoreMetadata.StoreFilesMetadata.EMPTY)
        );
    }

    public void testMarkCorruptedOnTruncatedSegmentsFile() throws IOException {
        IndexWriterConfig iwc = newIndexWriterConfig();
        final ShardId shardId = new ShardId("index", "_na_", 1);
        Store store = new Store(shardId, INDEX_SETTINGS, StoreTests.newDirectory(random()), new DummyShardLock(shardId));
        IndexWriter writer = new IndexWriter(store.directory(), iwc);

        int numDocs = 1 + random().nextInt(10);
        List<Document> docs = new ArrayList<>();
        for (int i = 0; i < numDocs; i++) {
            Document doc = new Document();
            doc.add(new StringField("id", "" + i, random().nextBoolean() ? Field.Store.YES : Field.Store.NO));
            doc.add(
                new TextField(
                    "body",
                    TestUtil.randomRealisticUnicodeString(random()),
                    random().nextBoolean() ? Field.Store.YES : Field.Store.NO
                )
            );
            doc.add(new SortedDocValuesField("dv", new BytesRef(TestUtil.randomRealisticUnicodeString(random()))));
            docs.add(doc);
        }
        for (Document d : docs) {
            writer.addDocument(d);
        }
        writer.commit();
        writer.close();
        SegmentInfos segmentCommitInfos = store.readLastCommittedSegmentsInfo();
        store.directory().deleteFile(segmentCommitInfos.getSegmentsFileName());
        try (IndexOutput out = store.directory().createOutput(segmentCommitInfos.getSegmentsFileName(), IOContext.DEFAULT)) {
            // empty file
        }

        try {
            if (randomBoolean()) {
                store.getMetadata(null);
            } else {
                store.readLastCommittedSegmentsInfo();
            }
            fail("corrupted segments_N file");
        } catch (CorruptIndexException ex) {
            // expected
        }
        assertTrue(store.isMarkedCorrupted());
        // we have to remove the index since it's corrupted and might fail the MocKDirWrapper checkindex call
        Lucene.cleanLuceneIndex(store.directory());
        store.close();
    }

    public void testCanOpenIndex() throws IOException {
        final ShardId shardId = new ShardId("index", "_na_", 1);
        IndexWriterConfig iwc = newIndexWriterConfig();
        Path tempDir = createTempDir();
        final BaseDirectoryWrapper dir = newFSDirectory(tempDir);
        assertFalse(StoreUtils.canOpenIndex(logger, tempDir, shardId, (id, l, d) -> new DummyShardLock(id)));
        IndexWriter writer = new IndexWriter(dir, iwc);
        Document doc = new Document();
        doc.add(new StringField("id", "1", random().nextBoolean() ? Field.Store.YES : Field.Store.NO));
        writer.addDocument(doc);
        writer.commit();
        writer.close();
        assertTrue(StoreUtils.canOpenIndex(logger, tempDir, shardId, (id, l, d) -> new DummyShardLock(id)));
        Store store = new Store(shardId, INDEX_SETTINGS, dir, new DummyShardLock(shardId));
        store.markStoreCorrupted(new CorruptIndexException("foo", "bar"));
        assertFalse(StoreUtils.canOpenIndex(logger, tempDir, shardId, (id, l, d) -> new DummyShardLock(id)));
        store.close();
    }

    public void testDeserializeCorruptionException() throws IOException {
        final ShardId shardId = new ShardId("index", "_na_", 1);
        final Directory dir = new ByteBuffersDirectory(); // I use ram dir to prevent that virusscanner being a PITA
        Store store = new Store(shardId, INDEX_SETTINGS, dir, new DummyShardLock(shardId));
        CorruptIndexException ex = new CorruptIndexException("foo", "bar");
        store.markStoreCorrupted(ex);
        try {
            store.failIfCorrupted();
            fail("should be corrupted");
        } catch (CorruptIndexException e) {
            assertEquals(ex.getMessage(), e.getMessage());
            assertEquals(ex.toString(), e.toString());
            assertArrayEquals(ex.getStackTrace(), e.getStackTrace());
        }

        store.removeCorruptionMarker();
        assertFalse(store.isMarkedCorrupted());
        FileNotFoundException ioe = new FileNotFoundException("foobar");
        store.markStoreCorrupted(ioe);
        try {
            store.failIfCorrupted();
            fail("should be corrupted");
        } catch (CorruptIndexException e) {
            assertEquals("foobar (resource=preexisting_corruption)", e.getMessage());
            assertArrayEquals(ioe.getStackTrace(), e.getCause().getStackTrace());
        }
        store.close();
    }

    public void testCorruptionMarkerVersionCheck() throws IOException {
        final ShardId shardId = new ShardId("index", "_na_", 1);
        final Directory dir = new ByteBuffersDirectory(); // I use ram dir to prevent that virusscanner being a PITA

        try (Store store = new Store(shardId, INDEX_SETTINGS, dir, new DummyShardLock(shardId))) {
            final String corruptionMarkerName = Store.CORRUPTED_MARKER_NAME_PREFIX + UUIDs.randomBase64UUID();
            try (IndexOutput output = dir.createOutput(corruptionMarkerName, IOContext.DEFAULT)) {
                CodecUtil.writeHeader(output, Store.CODEC, Store.CORRUPTED_MARKER_CODEC_VERSION + randomFrom(1, 2, -1, -2, -3));
                // we only need the header to trigger the exception
            }
            final IOException ioException = expectThrows(IOException.class, store::failIfCorrupted);
            assertThat(ioException, anyOf(instanceOf(IndexFormatTooOldException.class), instanceOf(IndexFormatTooNewException.class)));
            assertThat(ioException.getMessage(), containsString(corruptionMarkerName));
        }
    }

    public void testHistoryUUIDCanBeForced() throws IOException {
        final ShardId shardId = new ShardId("index", "_na_", 1);
        try (Store store = new Store(shardId, INDEX_SETTINGS, StoreTests.newDirectory(random()), new DummyShardLock(shardId))) {

            store.createEmpty();

            SegmentInfos segmentInfos = Lucene.readSegmentInfos(store.directory());
            assertThat(segmentInfos.getUserData(), hasKey(Engine.HISTORY_UUID_KEY));
            final String oldHistoryUUID = segmentInfos.getUserData().get(Engine.HISTORY_UUID_KEY);

            store.bootstrapNewHistory();

            segmentInfos = Lucene.readSegmentInfos(store.directory());
            assertThat(segmentInfos.getUserData(), hasKey(Engine.HISTORY_UUID_KEY));
            assertThat(segmentInfos.getUserData().get(Engine.HISTORY_UUID_KEY), not(equalTo(oldHistoryUUID)));
        }
    }

    public void testGetPendingFiles() throws IOException {
        final ShardId shardId = new ShardId("index", "_na_", 1);
        final String testfile = "testfile";
        try (Store store = new Store(shardId, INDEX_SETTINGS, new NIOFSDirectory(createTempDir()), new DummyShardLock(shardId))) {
            store.directory().createOutput(testfile, IOContext.DEFAULT).close();
            try (IndexInput input = store.directory().openInput(testfile, IOContext.DEFAULT)) {
                store.directory().deleteFile(testfile);
                assertEquals(FilterDirectory.unwrap(store.directory()).getPendingDeletions(), store.directory().getPendingDeletions());
            }
        }
    }

    public void testVersionIsIncludedInBootstrapCommit() throws IOException {
        final ShardId shardId = new ShardId("index", "_na_", 1);
        try (Store store = new Store(shardId, INDEX_SETTINGS, StoreTests.newDirectory(random()), new DummyShardLock(shardId))) {

            store.createEmpty();

            SegmentInfos segmentInfos = Lucene.readSegmentInfos(store.directory());
            assertThat(segmentInfos.getUserData(), hasKey(Engine.ES_VERSION));
            assertThat(segmentInfos.getUserData().get(Engine.ES_VERSION), is(equalTo(IndexVersion.current().toString())));
        }
    }

    public void testReadMetadataSnapshotReturnsEmptyWhenIndexIsCorrupted() throws IOException {
        var shardId = new ShardId("index", "_na_", 1);
        var dir = createTempDir();
        try (Store store = new Store(shardId, INDEX_SETTINGS, new NIOFSDirectory(dir), new DummyShardLock(shardId))) {
            store.createEmpty();
            store.markStoreCorrupted(new IOException("test exception"));
            var metadata = Store.readMetadataSnapshot(dir, shardId, (id, l, d) -> new DummyShardLock(id), logger);
            assertThat(metadata, equalTo(Store.MetadataSnapshot.EMPTY));
        }
    }
}
