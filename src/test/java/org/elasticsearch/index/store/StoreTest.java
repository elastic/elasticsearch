/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.index.store;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.*;
import org.apache.lucene.store.*;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.TestUtil;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.distributor.Distributor;
import org.elasticsearch.index.store.distributor.LeastUsedDistributor;
import org.elasticsearch.index.store.distributor.RandomWeightedDistributor;
import org.elasticsearch.test.ElasticsearchLuceneTestCase;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.*;

public class StoreTest extends ElasticsearchLuceneTestCase {

    @Test
    public void testVerifyingIndexOutput() throws IOException {
        Directory dir = newDirectory();
        IndexOutput output = dir.createOutput("foo.bar", IOContext.DEFAULT);
        int iters = scaledRandomIntBetween(10, 100);
        for (int i = 0; i < iters; i++) {
            BytesRef bytesRef = new BytesRef(TestUtil.randomRealisticUnicodeString(random(), 10, 1024));
            output.writeBytes(bytesRef.bytes, bytesRef.offset, bytesRef.length);
        }
        CodecUtil.writeFooter(output);
        output.close();
        IndexInput indexInput = dir.openInput("foo.bar", IOContext.DEFAULT);
        String checksum = Store.digestToString(CodecUtil.retrieveChecksum(indexInput));
        indexInput.seek(0);
        BytesRef ref = new BytesRef(scaledRandomIntBetween(1, 1024));
        long length = indexInput.length();
        IndexOutput verifyingOutput = new Store.VerifyingIndexOutput(new StoreFileMetaData("foo1.bar", length, checksum, TEST_VERSION_CURRENT), dir.createOutput("foo1.bar", IOContext.DEFAULT));
        while (length > 0) {
            if (random().nextInt(10) == 0) {
                verifyingOutput.writeByte(indexInput.readByte());
                length--;
            } else {
                int min = (int) Math.min(length, ref.bytes.length);
                indexInput.readBytes(ref.bytes, ref.offset, min);
                verifyingOutput.writeBytes(ref.bytes, ref.offset, min);
                length -= min;
            }
        }
        Store.verify(verifyingOutput);
        verifyingOutput.writeByte((byte) 0x0);
        try {
            Store.verify(verifyingOutput);
            fail("should be a corrupted index");
        } catch (CorruptIndexException ex) {
            // ok
        }
        IOUtils.close(indexInput, verifyingOutput, dir);
    }

    @Test
    public void testVerifyingIndexOutputWithBogusInput() throws IOException {
        Directory dir = newDirectory();
        int length = scaledRandomIntBetween(10, 1024);
        IndexOutput verifyingOutput = new Store.VerifyingIndexOutput(new StoreFileMetaData("foo1.bar", length, "", TEST_VERSION_CURRENT), dir.createOutput("foo1.bar", IOContext.DEFAULT));
        try {
            while (length > 0) {
                verifyingOutput.writeByte((byte) random().nextInt());
                length--;
            }
            fail("should be a corrupted index");
        } catch (CorruptIndexException ex) {
            // ok
        }
        IOUtils.close(verifyingOutput, dir);
    }

    @Test
    public void testWriteLegacyChecksums() throws IOException {
        final ShardId shardId = new ShardId(new Index("index"), 1);
        DirectoryService directoryService = new LuceneManagedDirectoryService();
        Store store = new Store(shardId, ImmutableSettings.EMPTY, null, null, directoryService, randomDistributor(directoryService));
        // set default codec - all segments need checksums
        IndexWriter writer = new IndexWriter(store.directory(), newIndexWriterConfig(random(), TEST_VERSION_CURRENT, new MockAnalyzer(random())).setCodec(actualDefaultCodec()));
        int docs = 1 + random().nextInt(100);

        for (int i = 0; i < docs; i++) {
            Document doc = new Document();
            doc.add(new TextField("id", "" + i, random().nextBoolean() ? Field.Store.YES : Field.Store.NO));
            doc.add(new TextField("body", TestUtil.randomRealisticUnicodeString(random()), random().nextBoolean() ? Field.Store.YES : Field.Store.NO));
            doc.add(new SortedDocValuesField("dv", new BytesRef(TestUtil.randomRealisticUnicodeString(random()))));
            writer.addDocument(doc);
        }
        if (random().nextBoolean()) {
            for (int i = 0; i < docs; i++) {
                if (random().nextBoolean()) {
                    Document doc = new Document();
                    doc.add(new TextField("id", "" + i, random().nextBoolean() ? Field.Store.YES : Field.Store.NO));
                    doc.add(new TextField("body", TestUtil.randomRealisticUnicodeString(random()), random().nextBoolean() ? Field.Store.YES : Field.Store.NO));
                    writer.updateDocument(new Term("id", "" + i), doc);
                }
            }
        }
        if (random().nextBoolean()) {
            DirectoryReader.open(writer, random().nextBoolean()).close(); // flush
        }
        // check before we committed
        Store.MetadataSnapshot metadata = store.getMetadata();
        assertThat(metadata.asMap().isEmpty(), is(true));   // nothing committed

        writer.close();
        Store.LegacyChecksums checksums = new Store.LegacyChecksums();
        Map<String, StoreFileMetaData> legacyMeta = new HashMap<>();
        for (String file : store.directory().listAll()) {
            if (file.equals("write.lock") || file.equals(IndexFileNames.SEGMENTS_GEN)) {
                continue;
            }
            try (IndexInput input = store.directory().openInput(file, IOContext.READONCE)) {
                String checksum = Store.digestToString(CodecUtil.retrieveChecksum(input));
                StoreFileMetaData storeFileMetaData = new StoreFileMetaData(file, store.directory().fileLength(file), checksum, null);
                legacyMeta.put(file, storeFileMetaData);
                checksums.add(storeFileMetaData);

            }

        }
        checksums.write(store);

        metadata = store.getMetadata();
        Map<String, StoreFileMetaData> stringStoreFileMetaDataMap = metadata.asMap();
        assertThat(legacyMeta.size(), equalTo(stringStoreFileMetaDataMap.size()));
        for (StoreFileMetaData meta : legacyMeta.values()) {
            assertTrue(stringStoreFileMetaDataMap.containsKey(meta.name()));
            assertTrue(stringStoreFileMetaDataMap.get(meta.name()).isSame(meta));
        }
        assertDeleteContent(store, directoryService);
        IOUtils.close(store);

    }

    @Test
    public void testNewChecksums() throws IOException {
        final ShardId shardId = new ShardId(new Index("index"), 1);
        DirectoryService directoryService = new LuceneManagedDirectoryService();
        Store store = new Store(shardId, ImmutableSettings.EMPTY, null, null, directoryService, randomDistributor(directoryService));
        // set default codec - all segments need checksums
        IndexWriter writer = new IndexWriter(store.directory(), newIndexWriterConfig(random(), TEST_VERSION_CURRENT, new MockAnalyzer(random())).setCodec(actualDefaultCodec()));
        int docs = 1 + random().nextInt(100);

        for (int i = 0; i < docs; i++) {
            Document doc = new Document();
            doc.add(new TextField("id", "" + i, random().nextBoolean() ? Field.Store.YES : Field.Store.NO));
            doc.add(new TextField("body", TestUtil.randomRealisticUnicodeString(random()), random().nextBoolean() ? Field.Store.YES : Field.Store.NO));
            doc.add(new SortedDocValuesField("dv", new BytesRef(TestUtil.randomRealisticUnicodeString(random()))));
            writer.addDocument(doc);
        }
        if (random().nextBoolean()) {
            for (int i = 0; i < docs; i++) {
                if (random().nextBoolean()) {
                    Document doc = new Document();
                    doc.add(new TextField("id", "" + i, random().nextBoolean() ? Field.Store.YES : Field.Store.NO));
                    doc.add(new TextField("body", TestUtil.randomRealisticUnicodeString(random()), random().nextBoolean() ? Field.Store.YES : Field.Store.NO));
                    writer.updateDocument(new Term("id", "" + i), doc);
                }
            }
        }
        if (random().nextBoolean()) {
            DirectoryReader.open(writer, random().nextBoolean()).close(); // flush
        }
        // check before we committed
        Store.MetadataSnapshot metadata = store.getMetadata();
        assertThat(metadata.asMap().isEmpty(), is(true)); // nothing committed

        writer.commit();
        writer.close();
        metadata = store.getMetadata();
        assertThat(metadata.asMap().isEmpty(), is(false));
        for (StoreFileMetaData meta : metadata) {
            try (IndexInput input = store.directory().openInput(meta.name(), IOContext.DEFAULT)) {
                String checksum = Store.digestToString(CodecUtil.retrieveChecksum(input));
                assertThat("File: " + meta.name() + " has a different checksum", meta.checksum(), equalTo(checksum));
                assertThat(meta.hasLegacyChecksum(), equalTo(false));
                assertThat(meta.writtenBy(), equalTo(TEST_VERSION_CURRENT));
            }
        }
        assertConsistent(store, metadata);

        TestUtil.checkIndex(store.directory());
        assertDeleteContent(store, directoryService);
        IOUtils.close(store);
    }

    @Test
    public void testMixedChecksums() throws IOException {
        final ShardId shardId = new ShardId(new Index("index"), 1);
        DirectoryService directoryService = new LuceneManagedDirectoryService();
        Store store = new Store(shardId, ImmutableSettings.EMPTY, null, null, directoryService, randomDistributor(directoryService));
        // this time random codec....
        IndexWriter writer = new IndexWriter(store.directory(), newIndexWriterConfig(random(), TEST_VERSION_CURRENT, new MockAnalyzer(random())).setCodec(actualDefaultCodec()));
        int docs = 1 + random().nextInt(100);

        for (int i = 0; i < docs; i++) {
            Document doc = new Document();
            doc.add(new TextField("id", "" + i, random().nextBoolean() ? Field.Store.YES : Field.Store.NO));
            doc.add(new TextField("body", TestUtil.randomRealisticUnicodeString(random()), random().nextBoolean() ? Field.Store.YES : Field.Store.NO));
            doc.add(new SortedDocValuesField("dv", new BytesRef(TestUtil.randomRealisticUnicodeString(random()))));
            writer.addDocument(doc);
        }
        if (random().nextBoolean()) {
            for (int i = 0; i < docs; i++) {
                if (random().nextBoolean()) {
                    Document doc = new Document();
                    doc.add(new TextField("id", "" + i, random().nextBoolean() ? Field.Store.YES : Field.Store.NO));
                    doc.add(new TextField("body", TestUtil.randomRealisticUnicodeString(random()), random().nextBoolean() ? Field.Store.YES : Field.Store.NO));
                    writer.updateDocument(new Term("id", "" + i), doc);
                }
            }
        }
        if (random().nextBoolean()) {
            DirectoryReader.open(writer, random().nextBoolean()).close(); // flush
        }
        // check before we committed
        Store.MetadataSnapshot metadata = store.getMetadata();
        assertThat(metadata.asMap().isEmpty(), is(true)); // nothing committed
        writer.commit();
        writer.close();
        Store.LegacyChecksums checksums = new Store.LegacyChecksums();
        metadata = store.getMetadata();
        assertThat(metadata.asMap().isEmpty(), is(false));
        for (StoreFileMetaData meta : metadata) {
            try (IndexInput input = store.directory().openInput(meta.name(), IOContext.DEFAULT)) {
                if (meta.checksum() == null) {
                    String checksum = null;
                    try {
                        CodecUtil.retrieveChecksum(input);
                        fail("expected a corrupt index - posting format has not checksums");
                    } catch (CorruptIndexException ex) {
                        try (ChecksumIndexInput checksumIndexInput = store.directory().openChecksumInput(meta.name(), IOContext.DEFAULT)) {
                            checksumIndexInput.seek(meta.length());
                            checksum = Store.digestToString(checksumIndexInput.getChecksum());
                        }
                        // fine - it's a postings format without checksums
                        checksums.add(new StoreFileMetaData(meta.name(), meta.length(), checksum, null));
                    }
                } else {
                    String checksum = Store.digestToString(CodecUtil.retrieveChecksum(input));
                    assertThat("File: " + meta.name() + " has a different checksum", meta.checksum(), equalTo(checksum));
                    assertThat(meta.hasLegacyChecksum(), equalTo(false));
                    assertThat(meta.writtenBy(), equalTo(TEST_VERSION_CURRENT));
                }
            }
        }
        assertConsistent(store, metadata);
        checksums.write(store);
        metadata = store.getMetadata();
        assertThat(metadata.asMap().isEmpty(), is(false));
        for (StoreFileMetaData meta : metadata) {
            assertThat("file: " + meta.name() + " has a null checksum", meta.checksum(), not(nullValue()));
            if (meta.hasLegacyChecksum()) {
                try (ChecksumIndexInput checksumIndexInput = store.directory().openChecksumInput(meta.name(), IOContext.DEFAULT)) {
                    checksumIndexInput.seek(meta.length());
                    assertThat(meta.checksum(), equalTo(Store.digestToString(checksumIndexInput.getChecksum())));
                }
            } else {
                try (IndexInput input = store.directory().openInput(meta.name(), IOContext.DEFAULT)) {
                    String checksum = Store.digestToString(CodecUtil.retrieveChecksum(input));
                    assertThat("File: " + meta.name() + " has a different checksum", meta.checksum(), equalTo(checksum));
                    assertThat(meta.hasLegacyChecksum(), equalTo(false));
                    assertThat(meta.writtenBy(), equalTo(TEST_VERSION_CURRENT));
                }
            }
        }
        assertConsistent(store, metadata);
        TestUtil.checkIndex(store.directory());
        assertDeleteContent(store, directoryService);
        IOUtils.close(store);
    }

    @Test
    public void testRenameFile() throws IOException {
        final ShardId shardId = new ShardId(new Index("index"), 1);
        DirectoryService directoryService = new LuceneManagedDirectoryService(false);
        Store store = new Store(shardId, ImmutableSettings.EMPTY, null, null, directoryService, randomDistributor(directoryService));
        {
            IndexOutput output = store.directory().createOutput("foo.bar", IOContext.DEFAULT);
            int iters = scaledRandomIntBetween(10, 100);
            for (int i = 0; i < iters; i++) {
                BytesRef bytesRef = new BytesRef(TestUtil.randomRealisticUnicodeString(random(), 10, 1024));
                output.writeBytes(bytesRef.bytes, bytesRef.offset, bytesRef.length);
            }
            CodecUtil.writeFooter(output);
            output.close();
        }
        store.renameFile("foo.bar", "bar.foo");
        assertThat(store.directory().listAll().length, is(1));
        final long lastChecksum;
        try (IndexInput input = store.directory().openInput("bar.foo", IOContext.DEFAULT)) {
            lastChecksum = CodecUtil.checksumEntireFile(input);
        }

        try {
            store.directory().openInput("foo.bar", IOContext.DEFAULT);
            fail("file was renamed");
        } catch (FileNotFoundException | NoSuchFileException ex) {
            // expected
        }
        {
            IndexOutput output = store.directory().createOutput("foo.bar", IOContext.DEFAULT);
            int iters = scaledRandomIntBetween(10, 100);
            for (int i = 0; i < iters; i++) {
                BytesRef bytesRef = new BytesRef(TestUtil.randomRealisticUnicodeString(random(), 10, 1024));
                output.writeBytes(bytesRef.bytes, bytesRef.offset, bytesRef.length);
            }
            CodecUtil.writeFooter(output);
            output.close();
        }
        try {
            store.renameFile("foo.bar", "bar.foo");
            fail("targe file already exists");
        } catch (IOException ex) {
            // expected
        }

        try (IndexInput input = store.directory().openInput("bar.foo", IOContext.DEFAULT)) {
            assertThat(lastChecksum, equalTo(CodecUtil.checksumEntireFile(input)));
        }
        assertThat(store.directory().listAll().length, is(2));
        assertDeleteContent(store, directoryService);
        IOUtils.close(store);
    }

    public void assertDeleteContent(Store store,DirectoryService service) throws IOException {
        store.deleteContent();
        assertThat(Arrays.toString(store.directory().listAll()), store.directory().listAll().length, equalTo(0));
        assertThat(store.stats().sizeInBytes(), equalTo(0l));
        for (Directory dir : service.build()) {
            assertThat(dir.listAll().length, equalTo(0));
        }
    }

    private  final class LuceneManagedDirectoryService implements DirectoryService {
        private final Directory[] dirs;

        public LuceneManagedDirectoryService() {
            this(true);
        }
        public LuceneManagedDirectoryService(boolean preventDoubleWrite) {
            this.dirs = new Directory[1 + random().nextInt(5)];
            for (int i = 0; i < dirs.length; i++) {
                dirs[i]  = newDirectory();
                if (dirs[i] instanceof MockDirectoryWrapper) {
                    ((MockDirectoryWrapper)dirs[i]).setPreventDoubleWrite(preventDoubleWrite);
                }
            }
        }
        @Override
        public Directory[] build() throws IOException {
            return dirs;
        }

        @Override
        public long throttleTimeInNanos() {
            return random().nextInt(1000);
        }

        @Override
        public void renameFile(Directory dir, String from, String to) throws IOException {
            dir.copy(dir, from, to, IOContext.DEFAULT);
            dir.deleteFile(from);
        }

        @Override
        public void fullDelete(Directory dir) throws IOException {
            for (String file : dir.listAll()) {
                dir.deleteFile(file);
            }
        }
    }

    public static void assertConsistent(Store store, Store.MetadataSnapshot metadata) throws IOException {
        for (String file : store.directory().listAll()) {
            if (!"write.lock".equals(file) && !IndexFileNames.SEGMENTS_GEN.equals(file) && !Store.isChecksum(file)) {
                assertTrue(file + " is not in the map: " + metadata.asMap().size() + " vs. " + store.directory().listAll().length, metadata.asMap().containsKey(file));
            } else {
                assertFalse(file + " is not in the map: " + metadata.asMap().size() + " vs. " + store.directory().listAll().length, metadata.asMap().containsKey(file));
            }
        }
    }

    private Distributor randomDistributor(DirectoryService service) throws IOException {
        return random().nextBoolean() ? new LeastUsedDistributor(service) : new RandomWeightedDistributor(service);
    }



}
