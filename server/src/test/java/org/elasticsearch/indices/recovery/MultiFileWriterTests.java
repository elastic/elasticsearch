/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indices.recovery;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.ByteBuffersIndexOutput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.tests.util.TestUtil;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardTestCase;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreFileMetadata;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Collections;

import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

public class MultiFileWriterTests extends IndexShardTestCase {

    private IndexShard indexShard;
    private Directory directory;
    private Directory directorySpy;
    private Store store;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        indexShard = newShard(true);
        directory = newMockFSDirectory(indexShard.shardPath().resolveIndex());
        directorySpy = spy(directory);
        store = createStore(indexShard.shardId(), indexShard.indexSettings(), directorySpy);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        directory.close();
        closeShards(indexShard);
    }

    public void testWritesFile() throws IOException {
        var fileWriter = createMultiFileWriter(true);
        var file = createFile("file");

        fileWriter.writeFile(file.metadata, 10, new ByteArrayInputStream(file.bytes));

        verify(directorySpy).createOutput("temp_file", IOContext.DEFAULT);
        verify(directorySpy).sync(Collections.singleton("temp_file"));
    }

    public void testFailsToWriteFileWithIncorrectChecksum() throws IOException {
        var fileWriter = createMultiFileWriter(true);
        var file = createFile("file");

        expectThrows(
            CorruptIndexException.class,
            () -> fileWriter.writeFile(withWrongChecksum(file.metadata), 10, new ByteArrayInputStream(file.bytes))
        );

        verify(directorySpy).createOutput("temp_file", IOContext.DEFAULT);
        verify(directorySpy).deleteFile("temp_file");
        verify(directorySpy, never()).sync(anyCollection());
    }

    public void testWritesFileWithIncorrectChecksumWithoutVerification() throws IOException {
        var fileWriter = createMultiFileWriter(false);
        var file = createFile("file");

        fileWriter.writeFile(withWrongChecksum(file.metadata), 10, new ByteArrayInputStream(file.bytes));

        verify(directorySpy).createOutput("temp_file", IOContext.DEFAULT);
        verify(directorySpy).sync(Collections.singleton("temp_file"));
    }

    private MultiFileWriter createMultiFileWriter(boolean verifyOutput) {
        return new MultiFileWriter(store, mock(RecoveryState.Index.class), "temp_", logger, verifyOutput);
    }

    private record FileAndMetadata(byte[] bytes, StoreFileMetadata metadata) {}

    private static FileAndMetadata createFile(String name) throws IOException {
        var buffer = new ByteBuffersDataOutput();
        var output = new ByteBuffersIndexOutput(buffer, "test", name);
        output.writeString(TestUtil.randomRealisticUnicodeString(random(), 10, 1024));
        CodecUtil.writeBEInt(output, CodecUtil.FOOTER_MAGIC);
        CodecUtil.writeBEInt(output, 0);
        String checksum = Store.digestToString(output.getChecksum());
        CodecUtil.writeBELong(output, output.getChecksum());
        output.close();

        return new FileAndMetadata(
            buffer.toArrayCopy(),
            new StoreFileMetadata(name, buffer.size(), checksum, IndexVersion.current().luceneVersion().toString())
        );
    }

    private static StoreFileMetadata withWrongChecksum(StoreFileMetadata metadata) {
        var newChecksum = randomValueOtherThan(metadata.checksum(), () -> randomAlphaOfLength(6));
        return new StoreFileMetadata(metadata.name(), metadata.length(), newChecksum, metadata.writtenBy());
    }
}
