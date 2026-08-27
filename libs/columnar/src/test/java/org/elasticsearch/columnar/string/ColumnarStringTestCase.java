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
 * describes a column as an array of per-document slots and asserts against a reader over it.
 *
 * <p>A column is described as {@code BytesRef[][]}: a {@code null} row is a document with no value at all, and a
 * {@code null} element within a row is a null slot. A row must hold at least one non-null slot, matching the
 * mapper's rule that a document with no non-null value writes no binary field — the shape the codec therefore
 * never sees.
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
     * {@link #withColumn(BytesRef[][], int, ChunkCodec, int, ColumnCheck)}.
     */
    protected void withColumn(final BytesRef[] docValues, final ColumnCheck check) throws IOException {
        withColumn(singleValued(docValues), check);
    }

    /** As {@link #withColumn(BytesRef[], ColumnCheck)}, with the block size fixed. */
    protected void withColumn(final BytesRef[] docValues, final int blockSize, final ColumnCheck check) throws IOException {
        withColumn(singleValued(docValues), blockSize, check);
    }

    /** As {@link #withColumn(BytesRef[], ColumnCheck)}, with every layout choice fixed. */
    protected void withColumn(
        final BytesRef[] docValues,
        final int blockSize,
        final ChunkCodec chunkCodec,
        final int targetChunkBytes,
        final ColumnCheck check
    ) throws IOException {
        withColumn(singleValued(docValues), blockSize, chunkCodec, targetChunkBytes, check);
    }

    /** As {@link #withColumn(BytesRef[], ColumnCheck)}, over a column whose documents may hold several slots. */
    protected void withColumn(final BytesRef[][] docSlots, final ColumnCheck check) throws IOException {
        withColumn(docSlots, randomValidBlockSize(), randomChunkCodec(), randomTargetChunkBytes(), check);
    }

    /** As {@link #withColumn(BytesRef[][], ColumnCheck)}, with the block size fixed. */
    protected void withColumn(final BytesRef[][] docSlots, final int blockSize, final ColumnCheck check) throws IOException {
        withColumn(docSlots, blockSize, randomChunkCodec(), randomTargetChunkBytes(), check);
    }

    /** As {@link #withColumn(BytesRef[][], ColumnCheck)}, with every layout choice fixed. */
    protected void withColumn(
        final BytesRef[][] docSlots,
        final int blockSize,
        final ChunkCodec chunkCodec,
        final int targetChunkBytes,
        final ColumnCheck check
    ) throws IOException {
        final byte[] segmentId = new byte[16];
        random().nextBytes(segmentId);
        try (Directory dir = newDirectory()) {
            final StringColumnMetadata metadata = writeColumn(dir, segmentId, docSlots, blockSize, chunkCodec, targetChunkBytes);
            try (IndexInput data = openData(dir, segmentId)) {
                check.check(metadata, new StringColumnReader(metadata, data));
            }
        }
    }

    /**
     * Random per-document slots. {@code sparse} leaves some documents without a value at all; {@code nulls}
     * puts null slots among the values, always leaving a document at least one non-null slot so the result
     * is a column the mapper could really have produced.
     */
    protected static BytesRef[][] randomDocSlots(final int maxDoc, final int maxSlots, final boolean sparse, final boolean nulls) {
        final BytesRef[][] docSlots = new BytesRef[maxDoc][];
        for (int doc = 0; doc < maxDoc; doc++) {
            if (sparse && randomBoolean()) {
                continue;
            }
            final BytesRef[] slots = new BytesRef[between(1, maxSlots)];
            int nonNull = 0;
            for (int slot = 0; slot < slots.length; slot++) {
                if (nulls && randomBoolean()) {
                    continue;
                }
                slots[slot] = new BytesRef(randomAlphaOfLengthBetween(0, 40));
                nonNull++;
            }
            if (nonNull == 0) {
                slots[between(0, slots.length - 1)] = new BytesRef(randomAlphaOfLengthBetween(0, 40));
            }
            docSlots[doc] = slots;
        }
        return docSlots;
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

    /** One slot per document, so a {@code BytesRef[]} column reads as the degenerate {@code BytesRef[][]} one. */
    protected static BytesRef[][] singleValued(final BytesRef[] docValues) {
        final BytesRef[][] docSlots = new BytesRef[docValues.length][];
        for (int doc = 0; doc < docValues.length; doc++) {
            docSlots[doc] = docValues[doc] == null ? null : new BytesRef[] { docValues[doc] };
        }
        return docSlots;
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

    /** The number of documents in {@code docSlots} that have at least one slot. */
    protected static int numDocsWithField(final BytesRef[][] docSlots) {
        int numDocsWithField = 0;
        for (BytesRef[] slots : docSlots) {
            if (slots != null) {
                numDocsWithField++;
            }
        }
        return numDocsWithField;
    }

    /** The total number of slots across every document, null slots included. */
    protected static long numValues(final BytesRef[][] docSlots) {
        long numValues = 0;
        for (BytesRef[] slots : docSlots) {
            if (slots != null) {
                numValues += slots.length;
            }
        }
        return numValues;
    }

    /** The total number of null slots across every document. */
    protected static long numNullSlots(final BytesRef[][] docSlots) {
        long numNullSlots = 0;
        for (BytesRef[] slots : docSlots) {
            if (slots != null) {
                for (BytesRef slot : slots) {
                    if (slot == null) {
                        numNullSlots++;
                    }
                }
            }
        }
        return numNullSlots;
    }

    private static StringColumnMetadata writeColumn(
        final Directory dir,
        final byte[] segmentId,
        final BytesRef[][] docSlots,
        final int blockSize,
        final ChunkCodec chunkCodec,
        final int targetChunkBytes
    ) throws IOException {
        final StringColumnMetadata written;
        try (IndexOutput out = dir.createOutput(DATA_FILE, IOContext.DEFAULT)) {
            ColumnarCodecUtil.writeHeader(out, DATA_CODEC, FormatVersion.CURRENT, segmentId, "");
            written = StringColumnWriter.write(
                docSlots.length,
                numDocsWithField(docSlots),
                numValues(docSlots),
                numNullSlots(docSlots),
                () -> cursor(docSlots),
                blockSize,
                chunkCodec,
                targetChunkBytes,
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
            final StringColumnMetadata read = StringColumnMetadata.readFrom(in, docSlots.length, version);
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

    /** A fresh cursor over {@code docSlots}; {@code advance} is unsupported, as the writer never calls it. */
    private static StringColumnValues cursor(final BytesRef[][] docSlots) {
        return new StringColumnValues() {
            private int doc = -1;
            private int upto;

            @Override
            public int valueCount() {
                return docSlots[doc].length;
            }

            @Override
            public int nullCount() {
                int nulls = 0;
                for (BytesRef slot : docSlots[doc]) {
                    if (slot == null) {
                        nulls++;
                    }
                }
                return nulls;
            }

            @Override
            public BytesRef nextValue() {
                return docSlots[doc][upto++];
            }

            @Override
            public int docID() {
                return doc;
            }

            @Override
            public int nextDoc() {
                for (doc = doc + 1; doc < docSlots.length; doc++) {
                    if (docSlots[doc] != null) {
                        upto = 0;
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
                return docSlots.length;
            }
        };
    }
}
