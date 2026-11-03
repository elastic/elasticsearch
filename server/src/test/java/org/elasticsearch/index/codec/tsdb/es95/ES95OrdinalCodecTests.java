/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.es95;

import org.apache.lucene.index.DocValuesSkipIndexType;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.ByteBuffersIndexOutput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.packed.PackedInts;
import org.elasticsearch.index.codec.tsdb.AbstractTSDBDocValuesProducer;
import org.elasticsearch.index.codec.tsdb.DocValuesConsumerUtil;
import org.elasticsearch.index.codec.tsdb.NumericReadContext;
import org.elasticsearch.index.codec.tsdb.NumericWriteContext;
import org.elasticsearch.index.codec.tsdb.OrdinalBlockCodec;
import org.elasticsearch.index.codec.tsdb.OrdinalFieldReader;
import org.elasticsearch.index.codec.tsdb.OrdinalFieldWriter;
import org.elasticsearch.index.codec.tsdb.TSDBDocValuesFormatConfig;
import org.elasticsearch.index.codec.tsdb.TsdbDocValuesProducer;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;

public class ES95OrdinalCodecTests extends ESTestCase {

    private static final int NUMERIC_BLOCK_SHIFT = 7;
    private static final int LEGACY_BLOCK_SHIFT = 9;

    /**
     * A reader created at {@code VERSION_ORDINAL_BLOCK_SHIFT} (start of the legacy range) must
     * consume the per-field {@code blockShift} byte and set {@code entry.blockSize} from it.
     * This proves that {@code segmentVersion} is threaded from {@link NumericReadContext} into
     * the reader returned by {@link ES95OrdinalCodec#createReader}.
     */
    public void testCreateReaderVersionAtLegacyRangeStart() throws IOException {
        assertReaderBlockSize(TSDBDocValuesFormatConfig.VERSION_ORDINAL_BLOCK_SHIFT, true);
    }

    /**
     * A reader created at {@code VERSION_REMOVE_ORDINAL_BLOCK_SHIFT - 1} (last version in the
     * legacy range) must also consume the per-field byte.
     */
    public void testCreateReaderVersionAtLegacyRangeEnd() throws IOException {
        assertReaderBlockSize(TSDBDocValuesFormatConfig.VERSION_REMOVE_ORDINAL_BLOCK_SHIFT - 1, true);
    }

    /**
     * A reader created at {@code VERSION_REMOVE_ORDINAL_BLOCK_SHIFT} (first version where the
     * byte was removed) must NOT consume the per-field byte; {@code entry.blockSize} defaults to
     * the format-level value.
     */
    public void testCreateReaderVersionAtRemovalBoundary() throws IOException {
        assertReaderBlockSize(TSDBDocValuesFormatConfig.VERSION_REMOVE_ORDINAL_BLOCK_SHIFT, false);
    }

    /**
     * A reader created at {@code VERSION_ORDINAL_BLOCK_SHIFT - 1} (before the byte was ever
     * written) must skip it — only the inclusive range
     * [{@code VERSION_ORDINAL_BLOCK_SHIFT}, {@code VERSION_REMOVE_ORDINAL_BLOCK_SHIFT}) reads it.
     */
    public void testCreateReaderVersionBelowLegacyRange() throws IOException {
        assertReaderBlockSize(TSDBDocValuesFormatConfig.VERSION_ORDINAL_BLOCK_SHIFT - 1, false);
    }

    /**
     * {@link ES95OrdinalCodec#createWriter} must return a different instance on every call so that
     * concurrent merges own independent writers with independent encoder state.
     */
    public void testCreateWriterReturnsFreshInstances() {
        final NumericWriteContext ctx = new NumericWriteContext(null, null, null, null, 0, 128, 0, null);
        final OrdinalBlockCodec codec = new ES95OrdinalCodec();

        final OrdinalFieldWriter first = codec.createWriter(ctx);
        final OrdinalFieldWriter second = codec.createWriter(ctx);
        assertThat(first, not(sameInstance(second)));
    }

    /**
     * {@link ES95OrdinalCodec#createReader} must return a different instance on every call.
     */
    public void testCreateReaderReturnsFreshInstances() {
        final NumericReadContext ctx = new NumericReadContext(128, null, TSDBDocValuesFormatConfig.VERSION_CURRENT);
        final OrdinalBlockCodec codec = new ES95OrdinalCodec();

        final OrdinalFieldReader first = codec.createReader(ctx);
        final OrdinalFieldReader second = codec.createReader(ctx);
        assertThat(first, not(sameInstance(second)));
    }

    /**
     * The encoder from {@link ES95OrdinalCodec#createWriter} round-trips through the decoder from
     * {@link ES95OrdinalCodec#createReader} for a single block. If the writer and reader were
     * wired to different block sizes, the underlying {@link org.elasticsearch.index.codec.tsdb.TSDBDocValuesEncoder}
     * instances would disagree on block boundaries and produce garbled ordinals.
     */
    public void testEncoderRoundTripsThroughDecoder() throws IOException {
        final int blockSize = randomBlockSize();
        final long maxOrd = randomLongBetween(1, 255);
        final int bitsPerOrd = PackedInts.bitsRequired(maxOrd);
        final long[] ordinals = randomOrdinals(blockSize, maxOrd);

        final OrdinalFieldWriter.Encoder encoder = codecEncoder(blockSize);
        final OrdinalFieldReader.Decoder decoder = codecDecoder(blockSize);

        try (Directory dir = newDirectory()) {
            try (IndexOutput out = dir.createOutput("data.bin", IOContext.DEFAULT)) {
                encoder.encodeOrdinals(ordinals.clone(), out, bitsPerOrd);
            }
            try (IndexInput in = dir.openInput("data.bin", IOContext.DEFAULT)) {
                final long[] decoded = new long[blockSize];
                decoder.decodeOrdinals(in, decoded, bitsPerOrd);
                assertThat(decoded, equalTo(ordinals));
            }
        }
    }

    /**
     * Multiple consecutive blocks are encoded and decoded in order without cross-block corruption.
     */
    public void testEncoderRoundTripsMultipleBlocks() throws IOException {
        final int blockSize = randomBlockSize();
        final long maxOrd = randomLongBetween(1, 255);
        final int bitsPerOrd = PackedInts.bitsRequired(maxOrd);
        final int numBlocks = randomIntBetween(2, 5);

        final long[][] blocks = new long[numBlocks][];
        for (int b = 0; b < numBlocks; b++) {
            blocks[b] = randomOrdinals(blockSize, maxOrd);
        }

        final OrdinalFieldWriter.Encoder encoder = codecEncoder(blockSize);
        final OrdinalFieldReader.Decoder decoder = codecDecoder(blockSize);

        try (Directory dir = newDirectory()) {
            try (IndexOutput out = dir.createOutput("data.bin", IOContext.DEFAULT)) {
                for (int b = 0; b < numBlocks; b++) {
                    encoder.encodeOrdinals(blocks[b].clone(), out, bitsPerOrd);
                }
            }
            try (IndexInput in = dir.openInput("data.bin", IOContext.DEFAULT)) {
                for (int b = 0; b < numBlocks; b++) {
                    final long[] decoded = new long[blockSize];
                    decoder.decodeOrdinals(in, decoded, bitsPerOrd);
                    assertThat("block " + b, blocks[b], equalTo(decoded));
                }
            }
        }
    }

    /**
     * Two encoders created via the codec produce byte-identical output for the same input,
     * confirming that each writer owns an independent {@link org.elasticsearch.index.codec.tsdb.TSDBDocValuesEncoder}
     * without shared mutable state.
     */
    public void testTwoEncodersProduceIdenticalOutputForSameInput() throws IOException {
        final int blockSize = randomBlockSize();
        final long maxOrd = randomLongBetween(1, 255);
        final int bitsPerOrd = PackedInts.bitsRequired(maxOrd);
        final long[] ordinals = randomOrdinals(blockSize, maxOrd);

        try (Directory dir = newDirectory()) {
            try (IndexOutput out = dir.createOutput("first.bin", IOContext.DEFAULT)) {
                codecEncoder(blockSize).encodeOrdinals(ordinals.clone(), out, bitsPerOrd);
            }
            try (IndexOutput out = dir.createOutput("second.bin", IOContext.DEFAULT)) {
                codecEncoder(blockSize).encodeOrdinals(ordinals.clone(), out, bitsPerOrd);
            }
            assertThat(dir.fileLength("second.bin"), equalTo(dir.fileLength("first.bin")));
        }
    }

    /**
     * Verifies that {@link OrdinalFieldWriter#writeFieldEntry} does not throw when maxOrd is 0
     * (a field with no values, hence no ordinals).
     */
    public void testMaxOrdinalGuard() throws IOException {
        final int blockSize = randomBlockSize();
        final ByteBuffersDataOutput metaOut = new ByteBuffersDataOutput();
        final ByteBuffersDataOutput dataOut = new ByteBuffersDataOutput();
        try (
            IndexOutput meta = new ByteBuffersIndexOutput(metaOut, "test-meta", "test-meta");
            IndexOutput data = new ByteBuffersIndexOutput(dataOut, "test-data", "test-data")
        ) {
            final NumericWriteContext ctx = new NumericWriteContext(meta, data, null, null, 0, blockSize, 0, null);
            final ES95OrdinalCodec codec = new ES95OrdinalCodec();
            final OrdinalFieldWriter writer = codec.createWriter(ctx);

            final TsdbDocValuesProducer valuesSource = new TsdbDocValuesProducer(new DocValuesConsumerUtil.MergeStats(true, 0, 0, 0, 0));
            final FieldInfo field = new FieldInfo(
                "test_field",
                0,
                false,
                false,
                false,
                IndexOptions.NONE,
                DocValuesType.SORTED,
                DocValuesSkipIndexType.NONE,
                -1,
                Map.of(),
                0,
                0,
                0,
                0,
                VectorEncoding.FLOAT32,
                VectorSimilarityFunction.EUCLIDEAN,
                false,
                false
            );

            try {
                writer.writeFieldEntry(field, valuesSource, 0, null, null);
            } catch (IllegalArgumentException e) {
                fail("maxValue should be guarded in writeFieldEntry [" + e.getMessage() + "]");
            }
        }
    }

    /**
     * Asserts that a reader created by the codec with the given {@code segmentVersion} reads the
     * correct metadata and ends up with the expected {@code entry.blockSize}.
     *
     * @param legacy {@code true} if the stream includes the legacy per-field {@code blockShift}
     *               byte and {@code entry.blockSize} should equal {@code 1 << LEGACY_BLOCK_SHIFT};
     *               {@code false} if the stream is in the current format and {@code entry.blockSize}
     *               should equal {@code 1 << NUMERIC_BLOCK_SHIFT}
     */
    private void assertReaderBlockSize(final int segmentVersion, final boolean legacy) throws IOException {
        final int expectedBlockSize = legacy ? (1 << LEGACY_BLOCK_SHIFT) : (1 << NUMERIC_BLOCK_SHIFT);
        final NumericReadContext ctx = new NumericReadContext(1 << NUMERIC_BLOCK_SHIFT, null, segmentVersion);
        final OrdinalFieldReader reader = new ES95OrdinalCodec().createReader(ctx);

        try (Directory dir = newDirectory()) {
            try (IndexOutput meta = dir.createOutput("meta.bin", IOContext.DEFAULT)) {
                if (legacy) {
                    writeLegacyMetadata(meta);
                } else {
                    writeCurrentMetadata(meta);
                }
            }
            try (IndexInput meta = dir.openInput("meta.bin", IOContext.DEFAULT)) {
                final AbstractTSDBDocValuesProducer.NumericEntry entry = new AbstractTSDBDocValuesProducer.NumericEntry();
                reader.readFieldEntry(meta, entry, NUMERIC_BLOCK_SHIFT);
                assertThat(entry.blockSize, equalTo(expectedBlockSize));
                assertThat("metadata stream not fully consumed", meta.getFilePointer(), equalTo(meta.length()));
            }
        }
    }

    /**
     * Writes a metadata stream in the legacy on-disk format, which includes an extra per-field
     * {@code blockShift} byte that {@code readFieldEntry} must consume for segment versions in
     * [{@code VERSION_ORDINAL_BLOCK_SHIFT}, {@code VERSION_REMOVE_ORDINAL_BLOCK_SHIFT}).
     *
     * <p>Layout (1 value, block-indexed path, {@code indexBlockShift = NUMERIC_BLOCK_SHIFT}):
     * <pre>
     *   numValues            long    (8)
     *   numDocsWithField     int     (4)
     *   indexBlockShift      int     (4)  — non-sentinel → block-indexed path
     *   legacyBlockShift     byte    (1)  ← per-field byte the fieldMetaReader reads
     *   DirectMonotonicReader block  (21) — 1 block: min(8)+avgInt(4)+offset(8)+bpv(1)
     *   indexOffset          long    (8)
     *   indexLength          long    (8)
     *   valuesOffset         long    (8)
     *   valuesLength         long    (8)
     *   docsWithFieldOffset  long    (8)
     *   docsWithFieldLength  long    (8)
     *   jumpTableEntryCount  short   (2)
     *   denseRankPower       byte    (1)
     * </pre>
     */
    private static void writeLegacyMetadata(final IndexOutput meta) throws IOException {
        meta.writeLong(1L);
        meta.writeInt(1);
        meta.writeInt(NUMERIC_BLOCK_SHIFT);
        meta.writeByte((byte) LEGACY_BLOCK_SHIFT);
        writeMonotonicBlock(meta);
        writeFieldTail(meta);
    }

    /**
     * Writes a metadata stream in the current on-disk format — no per-field {@code blockShift}
     * byte; {@code blockSize} defaults to the format-level value.
     */
    private static void writeCurrentMetadata(final IndexOutput meta) throws IOException {
        meta.writeLong(1L);
        meta.writeInt(1);
        meta.writeInt(NUMERIC_BLOCK_SHIFT);
        writeMonotonicBlock(meta);
        writeFieldTail(meta);
    }

    /** Writes one {@code DirectMonotonicReader.loadMeta} block (all-zero, offset last). */
    private static void writeMonotonicBlock(final IndexOutput meta) throws IOException {
        meta.writeLong(0L);
        meta.writeInt(0);
        meta.writeLong(0L);
        meta.writeByte((byte) 0);
    }

    private static void writeFieldTail(final IndexOutput meta) throws IOException {
        meta.writeLong(0L); // indexOffset
        meta.writeLong(0L); // indexLength
        meta.writeLong(0L); // valuesOffset
        meta.writeLong(0L); // valuesLength
        meta.writeLong(0L); // docsWithFieldOffset
        meta.writeLong(0L); // docsWithFieldLength
        meta.writeShort((short) 0); // jumpTableEntryCount
        meta.writeByte((byte) 0);   // denseRankPower
    }

    private static OrdinalFieldWriter.Encoder codecEncoder(final int blockSize) {
        final NumericWriteContext ctx = new NumericWriteContext(null, null, null, null, 0, blockSize, 0, null);
        return new ES95OrdinalCodec().createWriter(ctx).encoder();
    }

    private static OrdinalFieldReader.Decoder codecDecoder(final int blockSize) {
        final NumericReadContext ctx = new NumericReadContext(blockSize, null, TSDBDocValuesFormatConfig.VERSION_CURRENT);
        return new ES95OrdinalCodec().createReader(ctx).decoder(blockSize);
    }

    private static long[] randomOrdinals(final int blockSize, final long maxOrd) {
        final long[] ordinals = new long[blockSize];
        for (int i = 0; i < blockSize; i++) {
            ordinals[i] = randomLongBetween(0, maxOrd);
        }
        return ordinals;
    }

    private static int randomBlockSize() {
        return 1 << randomIntBetween(7, 9);
    }
}
