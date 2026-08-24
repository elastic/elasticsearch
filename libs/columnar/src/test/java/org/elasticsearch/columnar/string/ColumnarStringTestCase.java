/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.string;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.columnar.FormatVersion;
import org.elasticsearch.columnar.substrate.ChunkCodec;
import org.elasticsearch.columnar.substrate.ColumnarCodecUtil;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.elasticsearch.columnar.ColumnarTestUtils.randomValidBlockSize;

/**
 * Base class for tests that need a string column on disk. It owns the whole lifecycle — the data and meta files,
 * their headers and footers, the metadata round trip, and closing the directory and input afterwards — so a test
 * describes a column as an array of per-document values and asserts against a reader over it.
 *
 * <p>The files are fixtures rather than real segments — this class writes and reads them both — so they carry
 * their own codec names. Nothing here has to track the names the format itself uses.
 */
public abstract class ColumnarStringTestCase extends ESTestCase {

    private static final String DATA_FILE = "str.cnd";
    private static final String META_FILE = "str.cnm";
    private static final String DATA_CODEC = "ColumnarStringTestData";
    private static final String META_CODEC = "ColumnarStringTestMeta";

    /** What a test does with the column it asked for. */
    protected interface ColumnCheck {
        void check(StringColumnMetadata metadata, StringColumnReader reader) throws IOException;
    }

    /**
     * Writes {@code docValues} as a single-valued string column — a {@code null} entry being a document with no
     * value — reads the metadata back through the header and footer checks, and runs {@code check} over a reader
     * on it.
     *
     * <p>The block size, the chunk codec and the bytes a chunk holds are all random, so repeated runs land the
     * values on different block boundaries and different chunk boundaries, and read them back both verbatim
     * and compressed. A test that needs particular boundaries fixes them with
     * {@link #withColumn(BytesRef[], int, ChunkCodec, int, ColumnCheck)}.
     */
    protected void withColumn(final BytesRef[] docValues, final ColumnCheck check) throws IOException {
        withColumn(docValues, randomValidBlockSize(), randomChunkCodec(), randomTargetChunkBytes(), check);
    }

    /** As {@link #withColumn(BytesRef[], ColumnCheck)}, with the block size fixed. */
    protected void withColumn(final BytesRef[] docValues, final int blockSize, final ColumnCheck check) throws IOException {
        withColumn(docValues, blockSize, randomChunkCodec(), randomTargetChunkBytes(), check);
    }

    /** As {@link #withColumn(BytesRef[], ColumnCheck)}, with every layout choice fixed. */
    protected void withColumn(
        final BytesRef[] docValues,
        final int blockSize,
        final ChunkCodec chunkCodec,
        final int targetChunkBytes,
        final ColumnCheck check
    ) throws IOException {
        withColumn(docValues, blockSize, chunkCodec, targetChunkBytes, DictionaryPolicy.NONE, check);
    }

    /**
     * As {@link #withColumn(BytesRef[], ColumnCheck)}, under a dictionary policy. The default is
     * {@link DictionaryPolicy#NONE}, so a test asks for a dictionary rather than happening to get one.
     */
    protected void withColumn(
        final BytesRef[] docValues,
        final int blockSize,
        final ChunkCodec chunkCodec,
        final int targetChunkBytes,
        final DictionaryPolicy policy,
        final ColumnCheck check
    ) throws IOException {
        final byte[] segmentId = new byte[16];
        random().nextBytes(segmentId);
        try (Directory dir = newDirectory()) {
            final StringColumnMetadata metadata = writeColumn(dir, segmentId, docValues, blockSize, chunkCodec, targetChunkBytes, policy);
            try (IndexInput data = openData(dir, segmentId)) {
                check.check(metadata, new StringColumnReader(metadata, data));
            }
        }
    }

    /** Verbatim or compressed; a value must read back the same either way. */
    protected static ChunkCodec randomChunkCodec() {
        return randomFrom(ChunkCodec.IDENTITY, ChunkCodec.ZSTD);
    }

    /**
     * Bytes a chunk holds before it closes. The small sizes matter more than the realistic one: they put a
     * chunk boundary every few blocks, which is where a value that straddles two of them would be lost.
     */
    protected static int randomTargetChunkBytes() {
        return randomFrom(64, 512, 4096, 64 * 1024);
    }

    /** The number of documents in {@code docValues} that have a value. */
    protected static int numDocsWithField(final BytesRef[] docValues) {
        int numDocsWithField = 0;
        for (BytesRef value : docValues) {
            if (value != null) {
                numDocsWithField++;
            }
        }
        return numDocsWithField;
    }

    private static StringColumnMetadata writeColumn(
        final Directory dir,
        final byte[] segmentId,
        final BytesRef[] docValues,
        final int blockSize,
        final ChunkCodec chunkCodec,
        final int targetChunkBytes,
        final DictionaryPolicy policy
    ) throws IOException {
        final int numDocsWithField = numDocsWithField(docValues);
        final StringColumnMetadata written;
        try (IndexOutput out = dir.createOutput(DATA_FILE, IOContext.DEFAULT)) {
            ColumnarCodecUtil.writeHeader(out, DATA_CODEC, FormatVersion.CURRENT, segmentId, "");
            written = StringColumnWriter.write(
                docValues.length,
                numDocsWithField,
                numDocsWithField,
                () -> cursor(docValues),
                blockSize,
                chunkCodec,
                targetChunkBytes,
                policy,
                dir,
                IOContext.DEFAULT,
                out
            );
            ColumnarCodecUtil.writeFooter(out);
        }
        try (IndexOutput meta = dir.createOutput(META_FILE, IOContext.DEFAULT)) {
            ColumnarCodecUtil.writeHeader(meta, META_CODEC, FormatVersion.CURRENT, segmentId, "");
            written.writeTo(meta);
            ColumnarCodecUtil.writeFooter(meta);
        }
        try (ChecksumIndexInput in = dir.openChecksumInput(META_FILE)) {
            final FormatVersion version = ColumnarCodecUtil.checkHeader(in, META_CODEC, segmentId, "");
            final StringColumnMetadata read = StringColumnMetadata.readFrom(in, docValues.length, version);
            ColumnarCodecUtil.checkFooter(in);
            return read;
        }
    }

    private static IndexInput openData(final Directory dir, final byte[] segmentId) throws IOException {
        final IndexInput data = dir.openInput(DATA_FILE, IOContext.DEFAULT);
        boolean success = false;
        try {
            CodecUtil.checksumEntireFile(data);
            ColumnarCodecUtil.checkHeader(data, DATA_CODEC, segmentId, "");
            success = true;
            return data;
        } finally {
            if (success == false) {
                IOUtils.closeWhileHandlingException(data);
            }
        }
    }

    /** A fresh single-valued cursor over {@code docValues}; {@code advance} is unsupported, as the writer never calls it. */
    protected static StringColumnValues cursor(final BytesRef[] docValues) {
        return new StringColumnValues() {
            private int doc = -1;

            @Override
            public int valueCount() {
                return 1;
            }

            @Override
            public BytesRef nextValue() {
                return docValues[doc];
            }

            @Override
            public int docID() {
                return doc;
            }

            @Override
            public int nextDoc() {
                for (doc = doc + 1; doc < docValues.length; doc++) {
                    if (docValues[doc] != null) {
                        return doc;
                    }
                }
                return doc = DocIdSetIterator.NO_MORE_DOCS;
            }

            @Override
            public int advance(int target) {
                throw new UnsupportedOperationException();
            }

            @Override
            public long cost() {
                return docValues.length;
            }
        };
    }
}
