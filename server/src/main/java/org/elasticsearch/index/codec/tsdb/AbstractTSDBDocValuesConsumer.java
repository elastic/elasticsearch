/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.codecs.compressing.Compressor;
import org.apache.lucene.codecs.lucene90.IndexedDISI;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValuesSkipIndexType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.SortedSetSelector;
import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.store.ByteBuffersDataInput;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.ByteBuffersIndexOutput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.LongsRef;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.compress.LZ4;
import org.apache.lucene.util.packed.DirectMonotonicWriter;
import org.apache.lucene.util.packed.PackedInts;
import org.elasticsearch.core.IOUtils;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.index.codec.tsdb.DocValuesConsumerUtil.compatibleWithOptimizedMerge;

/**
 * Abstract base class for TSDB doc values consumers. Owns the shared wire-format logic
 * for writing numeric, binary, sorted, sorted-numeric, and sorted-set doc values.
 * Concrete subclasses provide the numeric block encoding strategy via
 * {@link #numericBlockWriter(FieldInfo, long[], int)}.
 */
public abstract class AbstractTSDBDocValuesConsumer extends XDocValuesConsumer {

    /** Type tag written to meta for numeric doc values fields. */
    public static final byte NUMERIC = 0;
    /** Type tag written to meta for binary doc values fields. */
    public static final byte BINARY = 1;
    /** Type tag written to meta for sorted doc values fields. */
    public static final byte SORTED = 2;
    /** Type tag written to meta for sorted-set doc values fields. */
    public static final byte SORTED_SET = 3;
    /** Type tag written to meta for sorted-numeric doc values fields. */
    public static final byte SORTED_NUMERIC = 4;

    /** Index block shift sentinel indicating a single ordinal (no index needed). */
    public static final int INDEX_SINGLE_ORDINAL = -1;
    /** Index block shift sentinel indicating ordinal range encoding. */
    public static final int INDEX_ORDINAL_RANGE = -2;

    final Directory dir;
    final IOContext context;
    IndexOutput data, meta;
    final int maxDoc;
    byte[] termsDictBuffer;
    final boolean enableOptimizedMerge;
    final int primarySortFieldNumber;
    protected final int numericBlockSize;
    final SegmentWriteState state;

    private final String metaCodecName;
    final TSDBDocValuesFormatConfig formatConfig;
    final long[] skipIndexJumpLengthPerLevel;

    /**
     * Construct a new consumer that writes doc values in the TSDB wire format.
     *
     * @param state              segment write state
     * @param enableOptimizedMerge whether optimized merge is enabled
     * @param dataCodec          codec name for the data file header
     * @param dataExtension      file extension for the data file
     * @param metaCodec          codec name for the meta file header
     * @param metaExtension      file extension for the meta file
     * @param formatConfig       format-specific configuration for this codec version
     */
    @SuppressWarnings("this-escape")
    protected AbstractTSDBDocValuesConsumer(
        final SegmentWriteState state,
        boolean enableOptimizedMerge,
        final String dataCodec,
        final String dataExtension,
        final String metaCodec,
        final String metaExtension,
        final TSDBDocValuesFormatConfig formatConfig
    ) throws IOException {
        this.state = state;
        this.termsDictBuffer = new byte[1 << 14];
        this.dir = state.directory;
        this.primarySortFieldNumber = AbstractTSDBDocValuesProducer.primarySortFieldNumber(state.segmentInfo, state.fieldInfos);
        this.context = state.context;
        this.numericBlockSize = 1 << formatConfig.numericBlockShift();
        this.metaCodecName = metaCodec;
        this.formatConfig = formatConfig;
        this.skipIndexJumpLengthPerLevel = skipIndexJumpLengths(formatConfig.skipIndexLevelShift(), formatConfig.skipIndexMaxLevel());

        boolean success = false;
        try {
            final String dataName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, dataExtension);
            data = state.directory.createOutput(dataName, state.context);
            CodecUtil.writeIndexHeader(data, dataCodec, formatConfig.versionCurrent(), state.segmentInfo.getId(), state.segmentSuffix);

            final String metaName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, metaExtension);
            meta = state.directory.createOutput(metaName, state.context);
            CodecUtil.writeIndexHeader(meta, metaCodec, formatConfig.versionCurrent(), state.segmentInfo.getId(), state.segmentSuffix);
            meta.writeByte((byte) formatConfig.numericBlockShift());

            maxDoc = state.segmentInfo.maxDoc();
            this.enableOptimizedMerge = enableOptimizedMerge;
            success = true;
        } finally {
            if (success == false) {
                IOUtils.closeWhileHandlingException(this);
            }
        }
    }

    /**
     * Computes how many bytes the reader should skip at each level of the skip index.
     *
     * <p>The skip index is a multi-level tree that lets the reader quickly skip over groups of
     * documents whose values fall outside a target range. Each entry in the tree stores a summary
     * in a fixed-size format:
     * maxDocID(4) + minDocID(4) + maxValue(8) + minValue(8) + docCount(4) + levels(1) = 29 bytes.
     * Higher levels summarize progressively larger groups: level 0 covers one interval, level 1
     * covers {@code 1 << levelShift} intervals, level 2 covers {@code 1 << (2 * levelShift)},
     * and so on.
     *
     * <p>When the reader sees that an entry's {@code maxDocID} is below the target, it skips the
     * rest of that entry and all entries underneath it. This method pre-computes those skip
     * distances so the reader can jump forward by a fixed number of bytes without scanning.
     *
     * @param levelShift how many intervals each level groups together, expressed as a bit shift
     * @param maxLevel   the number of levels in the tree
     * @return array where {@code result[level]} is the number of bytes to skip at that level
     */
    static long[] skipIndexJumpLengths(int levelShift, int maxLevel) {
        final long skipIndexIntervalBytes = 29L;
        final long[] jumpLengths = new long[maxLevel];
        jumpLengths[0] = skipIndexIntervalBytes - 5L;
        for (int level = 1; level < maxLevel; level++) {
            jumpLengths[level] = jumpLengths[level - 1];
            jumpLengths[level] += (1L << (level * levelShift)) * skipIndexIntervalBytes;
            jumpLengths[level] -= (1L << ((level - 1) * levelShift));
        }
        return jumpLengths;
    }

    /**
     * Creates a writer for numeric blocks.
     *
     * <p>Called lazily on the first full block, so the {@code sample} buffer contains real data
     * that subclasses can use for codec selection.
     *
     * @param field     the field being encoded
     * @param sample    the first full block of values, usable as sample data for codec selection
     * @param blockSize the resolved block size for this field
     * @return a writer capable of encoding blocks for this field
     * @see NumericBlockWriter
     */
    protected abstract NumericBlockWriter numericBlockWriter(FieldInfo field, long[] sample, int blockSize);

    /**
     * Returns the encoder used for doc offsets in compressed binary blocks.
     *
     * @return the doc offsets encoder
     */
    protected abstract DocOffsetsCodec.Encoder docOffsetsEncoder();

    @Override
    public void addNumericField(final FieldInfo field, final DocValuesProducer valuesProducer) throws IOException {
        meta.writeInt(field.number);
        meta.writeByte(NUMERIC);
        final DocValuesSource source = DocValuesSource.fromProducer(valuesProducer);
        final DocValuesSource producer = new DocValuesSource(source.mergeStats) {
            @Override
            public SortedNumericDocValues getSortedNumeric(final FieldInfo f) throws IOException {
                return DocValues.singleton(source.getNumeric(f));
            }
        };
        if (field.docValuesSkipIndexType() != DocValuesSkipIndexType.NONE) {
            writeSkipIndex(field, producer);
        }
        writeField(field, producer, -1, null, numericBlockSize);
    }

    private boolean shouldEncodeOrdinalRange(final FieldInfo field, long maxOrd, int numDocsWithValue, long numValues) {
        return maxDoc > 1
            && field.number == primarySortFieldNumber
            && numDocsWithValue == numValues
            && (numDocsWithValue / maxOrd) >= formatConfig.minDocsPerOrdinalForRangeEncoding();
    }

    private long[] writeField(
        final FieldInfo field,
        final DocValuesSource valuesSource,
        long maxOrd,
        final OffsetsAccumulator offsetsAccumulator,
        int blockSize
    ) throws IOException {
        final int blockShift = Integer.numberOfTrailingZeros(blockSize);
        int numDocsWithValue = 0;
        long numValues = 0;

        // NOTE: the merge path and the indexing path use different strategies for computing stats and writing DISI.
        // The merge path has pre-computed stats and uses DISIAccumulator (single-pass); the indexing path computes
        // stats by iterating and writes DISI via IndexedDISI.writeBitSet (separate pass). Unifying these paths
        // would require making stats always available upfront and reconciling the DISI strategies.
        SortedNumericDocValues values;
        if (valuesSource.mergeStats.supported()) {
            numDocsWithValue = valuesSource.mergeStats.sumNumDocsWithField();
            numValues = valuesSource.mergeStats.sumNumValues();
        } else {
            values = valuesSource.getSortedNumeric(field);
            for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
                numDocsWithValue++;
                final int count = values.docValueCount();
                numValues += count;
            }
        }

        meta.writeLong(numValues);
        meta.writeInt(numDocsWithValue);

        DISIAccumulator disiAccumulator = null;
        try {
            if (numValues > 0) {
                assert numDocsWithValue > 0;
                final ByteBuffersDataOutput indexOut = new ByteBuffersDataOutput();
                DirectMonotonicWriter indexWriter = null;

                final long valuesDataOffset = data.getFilePointer();
                if (maxOrd == 1) {
                    meta.writeInt(INDEX_SINGLE_ORDINAL);
                } else if (shouldEncodeOrdinalRange(field, maxOrd, numDocsWithValue, numValues)) {
                    assert offsetsAccumulator == null;
                    meta.writeInt(INDEX_ORDINAL_RANGE);
                    meta.writeVInt(Math.toIntExact(maxOrd));
                    meta.writeByte((byte) formatConfig.ordinalRangeBlockShift());
                    values = valuesSource.getSortedNumeric(field);
                    if (valuesSource.mergeStats.supported() && numDocsWithValue < maxDoc) {
                        disiAccumulator = new DISIAccumulator(dir, context, data, IndexedDISI.DEFAULT_DENSE_RANK_POWER);
                    }
                    DirectMonotonicWriter startDocs = DirectMonotonicWriter.getInstance(
                        meta,
                        data,
                        maxOrd + 1,
                        formatConfig.ordinalRangeBlockShift()
                    );
                    long lastOrd = 0;
                    startDocs.add(0);
                    for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
                        if (disiAccumulator != null) {
                            disiAccumulator.addDocId(doc);
                        }
                        final long nextOrd = values.nextValue();
                        if (nextOrd != lastOrd) {
                            lastOrd = nextOrd;
                            startDocs.add(doc);
                        }
                    }
                    startDocs.add(maxDoc);
                    startDocs.finish();
                } else {
                    indexWriter = DirectMonotonicWriter.getInstance(
                        meta,
                        new ByteBuffersIndexOutput(indexOut, "temp-dv-index", "temp-dv-index"),
                        1L + ((numValues - 1) >>> blockShift),
                        formatConfig.directMonotonicBlockShift()
                    );
                    meta.writeInt(formatConfig.directMonotonicBlockShift());
                    final long[] buffer = new long[blockSize];
                    int bufferSize = 0;
                    NumericBlockWriter blockWriter = null;
                    final boolean useOrdinals = maxOrd >= 0;
                    if (useOrdinals) {
                        blockWriter = numericBlockWriter(field, buffer, blockSize);
                    }
                    values = valuesSource.getSortedNumeric(field);
                    final int bitsPerOrd = useOrdinals ? PackedInts.bitsRequired(maxOrd - 1) : -1;
                    if (valuesSource.mergeStats.supported() && numDocsWithValue < maxDoc) {
                        disiAccumulator = new DISIAccumulator(dir, context, data, IndexedDISI.DEFAULT_DENSE_RANK_POWER);
                    }
                    for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
                        if (disiAccumulator != null) {
                            disiAccumulator.addDocId(doc);
                        }
                        final int count = values.docValueCount();
                        if (offsetsAccumulator != null) {
                            offsetsAccumulator.addDoc(count);
                        }
                        for (int i = 0; i < count; ++i) {
                            buffer[bufferSize++] = values.nextValue();
                            if (bufferSize == blockSize) {
                                indexWriter.add(data.getFilePointer() - valuesDataOffset);
                                if (useOrdinals) {
                                    blockWriter.writeOrdinals(buffer, data, bitsPerOrd);
                                } else {
                                    if (blockWriter == null) {
                                        blockWriter = numericBlockWriter(field, buffer, blockSize);
                                    }
                                    blockWriter.write(buffer, blockSize, data);
                                }
                                bufferSize = 0;
                            }
                        }
                    }
                    if (bufferSize > 0) {
                        indexWriter.add(data.getFilePointer() - valuesDataOffset);
                        Arrays.fill(buffer, bufferSize, blockSize, 0L);
                        if (useOrdinals) {
                            blockWriter.writeOrdinals(buffer, data, bitsPerOrd);
                        } else {
                            if (blockWriter == null) {
                                blockWriter = numericBlockWriter(field, buffer, blockSize);
                            }
                            blockWriter.write(buffer, blockSize, data);
                        }
                    }
                }

                final long valuesDataLength = data.getFilePointer() - valuesDataOffset;
                if (indexWriter != null) {
                    indexWriter.finish();
                }
                final long indexDataOffset = data.getFilePointer();
                data.copyBytes(indexOut.toDataInput(), indexOut.size());
                meta.writeLong(indexDataOffset);
                meta.writeLong(data.getFilePointer() - indexDataOffset);

                meta.writeLong(valuesDataOffset);
                meta.writeLong(valuesDataLength);
            }

            if (numDocsWithValue == 0) {
                meta.writeLong(-2); // docsWithFieldOffset
                meta.writeLong(0L); // docsWithFieldLength
                meta.writeShort((short) -1); // jumpTableEntryCount
                meta.writeByte((byte) -1); // denseRankPower
            } else if (numDocsWithValue == maxDoc) {
                meta.writeLong(-1); // docsWithFieldOffset
                meta.writeLong(0L); // docsWithFieldLength
                meta.writeShort((short) -1); // jumpTableEntryCount
                meta.writeByte((byte) -1); // denseRankPower
            } else {
                long offset = data.getFilePointer();
                meta.writeLong(offset); // docsWithFieldOffset
                final short jumpTableEntryCount;
                if (maxOrd != 1 && disiAccumulator != null) {
                    jumpTableEntryCount = disiAccumulator.build(data);
                } else {
                    values = valuesSource.getSortedNumeric(field);
                    jumpTableEntryCount = IndexedDISI.writeBitSet(values, data, IndexedDISI.DEFAULT_DENSE_RANK_POWER);
                }
                meta.writeLong(data.getFilePointer() - offset); // docsWithFieldLength
                meta.writeShort(jumpTableEntryCount);
                meta.writeByte(IndexedDISI.DEFAULT_DENSE_RANK_POWER);
            }
        } finally {
            IOUtils.close(disiAccumulator);
        }

        return new long[] { numDocsWithValue, numValues };
    }

    @Override
    public void mergeNumericField(final FieldInfo mergeFieldInfo, final MergeState mergeState) throws IOException {
        final DocValuesConsumerUtil.MergeStats mergeStats = compatibleWithOptimizedMerge(enableOptimizedMerge, mergeState, mergeFieldInfo);
        if (mergeStats.supported()) {
            mergeNumericField(mergeStats, mergeFieldInfo, mergeState);
        } else {
            super.mergeNumericField(mergeFieldInfo, mergeState);
        }
    }

    @Override
    public void mergeBinaryField(final FieldInfo mergeFieldInfo, final MergeState mergeState) throws IOException {
        final DocValuesConsumerUtil.MergeStats mergeStats = compatibleWithOptimizedMerge(enableOptimizedMerge, mergeState, mergeFieldInfo);
        if (mergeStats.supported()) {
            mergeBinaryField(mergeStats, mergeFieldInfo, mergeState);
        } else {
            super.mergeBinaryField(mergeFieldInfo, mergeState);
        }
    }

    @Override
    public void addBinaryField(final FieldInfo field, final DocValuesProducer valuesProducer) throws IOException {
        meta.writeInt(field.number);
        meta.writeByte(BINARY);
        meta.writeByte(formatConfig.binaryCompressionMode().code);

        final DocValuesSource source = DocValuesSource.fromProducer(valuesProducer);
        if (source.mergeStats.supported()) {
            final int numDocsWithField = source.mergeStats.sumNumDocsWithField();
            final int minLength = source.mergeStats.minLength();
            final int maxLength = source.mergeStats.maxLength();

            assert numDocsWithField <= maxDoc;

            BinaryDocValues values = source.getBinary(field);
            long start = data.getFilePointer();
            meta.writeLong(start); // dataOffset

            DISIAccumulator disiAccumulator = null;
            BinaryWriter binaryWriter = null;
            try {
                if (numDocsWithField > 0 && numDocsWithField < maxDoc) {
                    disiAccumulator = new DISIAccumulator(dir, context, data, IndexedDISI.DEFAULT_DENSE_RANK_POWER);
                }

                assert maxLength >= minLength;
                if (formatConfig.binaryCompressionMode() == BinaryDVCompressionMode.NO_COMPRESS) {
                    final OffsetsAccumulator offsetsAccumulator = maxLength > minLength
                        ? new OffsetsAccumulator(dir, context, data, numDocsWithField, formatConfig.directMonotonicBlockShift())
                        : null;
                    binaryWriter = new DirectBinaryWriter(offsetsAccumulator, null);
                } else {
                    binaryWriter = new CompressedBinaryBlockWriter(formatConfig.binaryCompressionMode());
                }

                for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
                    BytesRef v = values.binaryValue();
                    binaryWriter.addDoc(v);
                    if (disiAccumulator != null) {
                        disiAccumulator.addDocId(doc);
                    }
                }
                binaryWriter.flushData();
                meta.writeLong(data.getFilePointer() - start); // dataLength

                if (numDocsWithField == 0) {
                    meta.writeLong(-2); // docsWithFieldOffset
                    meta.writeLong(0L); // docsWithFieldLength
                    meta.writeShort((short) -1); // jumpTableEntryCount
                    meta.writeByte((byte) -1); // denseRankPower
                } else if (numDocsWithField == maxDoc) {
                    meta.writeLong(-1); // docsWithFieldOffset
                    meta.writeLong(0L); // docsWithFieldLength
                    meta.writeShort((short) -1); // jumpTableEntryCount
                    meta.writeByte((byte) -1); // denseRankPower
                } else {
                    long offset = data.getFilePointer();
                    meta.writeLong(offset); // docsWithFieldOffset
                    final short jumpTableEntryCount = disiAccumulator.build(data);
                    meta.writeLong(data.getFilePointer() - offset); // docsWithFieldLength
                    meta.writeShort(jumpTableEntryCount);
                    meta.writeByte(IndexedDISI.DEFAULT_DENSE_RANK_POWER);
                }

                meta.writeInt(numDocsWithField);
                meta.writeInt(minLength);
                meta.writeInt(maxLength);

                binaryWriter.writeAddressMetadata(minLength, maxLength, numDocsWithField);
            } finally {
                IOUtils.close(disiAccumulator, binaryWriter);
            }
        } else {
            BinaryWriter binaryWriter = null;
            try {
                if (formatConfig.binaryCompressionMode() == BinaryDVCompressionMode.NO_COMPRESS) {
                    binaryWriter = new DirectBinaryWriter(null, source.getBinary(field));
                } else {
                    binaryWriter = new CompressedBinaryBlockWriter(formatConfig.binaryCompressionMode());
                }

                BinaryDocValues values = source.getBinary(field);
                long start = data.getFilePointer();
                meta.writeLong(start); // dataOffset
                int numDocsWithField = 0;
                int minLength = Integer.MAX_VALUE;
                int maxLength = 0;
                for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
                    numDocsWithField++;
                    BytesRef v = values.binaryValue();
                    int length = v.length;
                    binaryWriter.addDoc(v);
                    minLength = Math.min(length, minLength);
                    maxLength = Math.max(length, maxLength);
                }
                binaryWriter.flushData();

                assert numDocsWithField <= maxDoc;
                meta.writeLong(data.getFilePointer() - start); // dataLength

                if (numDocsWithField == 0) {
                    meta.writeLong(-2); // docsWithFieldOffset
                    meta.writeLong(0L); // docsWithFieldLength
                    meta.writeShort((short) -1); // jumpTableEntryCount
                    meta.writeByte((byte) -1); // denseRankPower
                } else if (numDocsWithField == maxDoc) {
                    meta.writeLong(-1); // docsWithFieldOffset
                    meta.writeLong(0L); // docsWithFieldLength
                    meta.writeShort((short) -1); // jumpTableEntryCount
                    meta.writeByte((byte) -1); // denseRankPower
                } else {
                    long offset = data.getFilePointer();
                    meta.writeLong(offset); // docsWithFieldOffset
                    values = source.getBinary(field);
                    final short jumpTableEntryCount = IndexedDISI.writeBitSet(values, data, IndexedDISI.DEFAULT_DENSE_RANK_POWER);
                    meta.writeLong(data.getFilePointer() - offset); // docsWithFieldLength
                    meta.writeShort(jumpTableEntryCount);
                    meta.writeByte(IndexedDISI.DEFAULT_DENSE_RANK_POWER);
                }

                meta.writeInt(numDocsWithField);
                meta.writeInt(minLength);
                meta.writeInt(maxLength);

                binaryWriter.writeAddressMetadata(minLength, maxLength, numDocsWithField);
            } finally {
                IOUtils.close(binaryWriter);
            }
        }
    }

    private sealed interface BinaryWriter extends Closeable {
        void addDoc(BytesRef v) throws IOException;

        default void flushData() throws IOException {}

        default void writeAddressMetadata(int minLength, int maxLength, int numDocsWithField) throws IOException {}

        @Override
        default void close() throws IOException {}
    }

    private final class DirectBinaryWriter implements BinaryWriter {
        final OffsetsAccumulator offsetsAccumulator;
        final BinaryDocValues values;

        private DirectBinaryWriter(final OffsetsAccumulator offsetsAccumulator, final BinaryDocValues values) {
            this.offsetsAccumulator = offsetsAccumulator;
            this.values = values;
        }

        @Override
        public void addDoc(final BytesRef v) throws IOException {
            data.writeBytes(v.bytes, v.offset, v.length);
            if (offsetsAccumulator != null) {
                offsetsAccumulator.addDoc(v.length);
            }
        }

        @Override
        public void writeAddressMetadata(int minLength, int maxLength, int numDocsWithField) throws IOException {
            if (offsetsAccumulator != null) {
                offsetsAccumulator.build(meta, data);
            } else if (values != null) {
                if (maxLength > minLength) {
                    long addressStart = data.getFilePointer();
                    meta.writeLong(addressStart);
                    meta.writeVInt(formatConfig.directMonotonicBlockShift());

                    final DirectMonotonicWriter writer = DirectMonotonicWriter.getInstance(
                        meta,
                        data,
                        numDocsWithField + 1,
                        formatConfig.directMonotonicBlockShift()
                    );
                    long addr = 0;
                    writer.add(addr);
                    for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
                        addr += values.binaryValue().length;
                        writer.add(addr);
                    }
                    writer.finish();
                    meta.writeLong(data.getFilePointer() - addressStart);
                }
            }
        }

        @Override
        public void close() throws IOException {
            IOUtils.close(offsetsAccumulator);
        }
    }

    private final class CompressedBinaryBlockWriter implements BinaryWriter {
        final Compressor compressor;

        final int[] docOffsets = new int[formatConfig.blockCountThreshold() + 1];

        int uncompressedBlockLength = 0;
        int maxUncompressedBlockLength = 0;
        int numDocsInCurrentBlock = 0;

        byte[] block = BytesRef.EMPTY_BYTES;
        int totalChunks = 0;
        int maxNumDocsInAnyBlock = 0;

        final BlockMetadataAccumulator blockMetaAcc;

        CompressedBinaryBlockWriter(final BinaryDVCompressionMode compressionMode) throws IOException {
            this.compressor = compressionMode.compressionMode().newCompressor();
            long blockAddressesStart = data.getFilePointer();
            this.blockMetaAcc = new BlockMetadataAccumulator(
                state.directory,
                state.context,
                data,
                blockAddressesStart,
                metaCodecName,
                formatConfig.versionCurrent(),
                formatConfig.directMonotonicBlockShift()
            );
        }

        @Override
        public void addDoc(final BytesRef v) throws IOException {
            block = ArrayUtil.grow(block, uncompressedBlockLength + v.length);
            System.arraycopy(v.bytes, v.offset, block, uncompressedBlockLength, v.length);
            uncompressedBlockLength += v.length;

            numDocsInCurrentBlock++;
            docOffsets[numDocsInCurrentBlock] = uncompressedBlockLength;

            if (uncompressedBlockLength >= formatConfig.blockBytesThreshold()
                || numDocsInCurrentBlock >= formatConfig.blockCountThreshold()) {
                flushData();
            }
        }

        @Override
        public void flushData() throws IOException {
            if (numDocsInCurrentBlock == 0) {
                return;
            }

            totalChunks++;
            long thisBlockStartPointer = data.getFilePointer();

            final boolean shouldCompress = formatConfig.enablePerBlockCompression();
            final BinaryDVCompressionMode.BlockHeader header = new BinaryDVCompressionMode.BlockHeader(shouldCompress);
            data.writeByte(header.toByte());

            data.writeVInt(uncompressedBlockLength);

            maxUncompressedBlockLength = Math.max(maxUncompressedBlockLength, uncompressedBlockLength);
            maxNumDocsInAnyBlock = Math.max(maxNumDocsInAnyBlock, numDocsInCurrentBlock);

            docOffsetsEncoder().encode(docOffsets, numDocsInCurrentBlock, data);

            if (shouldCompress) {
                compress(block, uncompressedBlockLength, data);
            } else {
                data.writeBytes(block, 0, uncompressedBlockLength);
            }

            long blockLenBytes = data.getFilePointer() - thisBlockStartPointer;
            blockMetaAcc.addDoc(numDocsInCurrentBlock, blockLenBytes);
            numDocsInCurrentBlock = uncompressedBlockLength = 0;
        }

        void compress(final byte[] data, int uncompressedLength, final DataOutput output) throws IOException {
            ByteBuffer inputBuffer = ByteBuffer.wrap(data, 0, uncompressedLength);
            ByteBuffersDataInput input = new ByteBuffersDataInput(List.of(inputBuffer));
            compressor.compress(input, output);
        }

        @Override
        public void writeAddressMetadata(int minLength, int maxLength, int numDocsWithField) throws IOException {
            if (totalChunks == 0) {
                return;
            }

            long dataAddressesStart = data.getFilePointer();
            meta.writeLong(dataAddressesStart);
            meta.writeVInt(totalChunks);
            meta.writeVInt(maxUncompressedBlockLength);
            meta.writeVInt(maxNumDocsInAnyBlock);
            meta.writeVInt(formatConfig.directMonotonicBlockShift());

            blockMetaAcc.build(meta, data);
        }

        @Override
        public void close() throws IOException {
            IOUtils.close(blockMetaAcc);
        }
    }

    @Override
    public void addSortedField(final FieldInfo field, final DocValuesProducer valuesProducer) throws IOException {
        meta.writeInt(field.number);
        meta.writeByte(SORTED);
        doAddSortedField(field, valuesProducer, false);
    }

    @Override
    public void mergeSortedField(final FieldInfo mergeFieldInfo, final MergeState mergeState) throws IOException {
        final DocValuesConsumerUtil.MergeStats result = compatibleWithOptimizedMerge(enableOptimizedMerge, mergeState, mergeFieldInfo);
        if (result.supported()) {
            mergeSortedField(result, mergeFieldInfo, mergeState);
        } else {
            super.mergeSortedField(mergeFieldInfo, mergeState);
        }
    }

    private void doAddSortedField(final FieldInfo field, final DocValuesProducer valuesProducer, boolean addTypeByte) throws IOException {
        final DocValuesSource producer = new IndexingDocValuesSource(valuesProducer) {
            @Override
            public SortedNumericDocValues getSortedNumeric(final FieldInfo field) throws IOException {
                SortedDocValues sorted = valuesProducer.getSorted(field);
                NumericDocValues sortedOrds = new NumericDocValues() {
                    @Override
                    public long longValue() throws IOException {
                        return sorted.ordValue();
                    }

                    @Override
                    public boolean advanceExact(int target) throws IOException {
                        return sorted.advanceExact(target);
                    }

                    @Override
                    public int docID() {
                        return sorted.docID();
                    }

                    @Override
                    public int nextDoc() throws IOException {
                        return sorted.nextDoc();
                    }

                    @Override
                    public int advance(int target) throws IOException {
                        return sorted.advance(target);
                    }

                    @Override
                    public long cost() {
                        return sorted.cost();
                    }
                };
                return DocValues.singleton(sortedOrds);
            }
        };
        if (field.docValuesSkipIndexType() != DocValuesSkipIndexType.NONE) {
            writeSkipIndex(field, producer);
        }
        if (addTypeByte) {
            meta.writeByte((byte) 0); // multiValued (0 = singleValued)
        }
        SortedDocValues sorted = valuesProducer.getSorted(field);
        int maxOrd = sorted.getValueCount();
        final int blockSize = numericBlockSize;
        writeField(field, producer, maxOrd, null, blockSize);
        addTermsDict(DocValues.singleton(valuesProducer.getSorted(field)));
    }

    private void addTermsDict(final SortedSetDocValues values) throws IOException {
        final long size = values.getValueCount();
        meta.writeVLong(size);

        int blockMask = formatConfig.termsBlockLz4Mask();

        meta.writeInt(formatConfig.directMonotonicBlockShift());
        ByteBuffersDataOutput addressBuffer = new ByteBuffersDataOutput();
        ByteBuffersIndexOutput addressOutput = new ByteBuffersIndexOutput(addressBuffer, "temp", "temp");
        long numBlocks = (size + blockMask) >>> formatConfig.termsBlockLz4Shift();
        DirectMonotonicWriter writer = DirectMonotonicWriter.getInstance(
            meta,
            addressOutput,
            numBlocks,
            formatConfig.directMonotonicBlockShift()
        );

        BytesRefBuilder previous = new BytesRefBuilder();
        long ord = 0;
        long start = data.getFilePointer();
        int maxLength = 0, maxBlockLength = 0;
        TermsEnum iterator = values.termsEnum();

        LZ4.FastCompressionHashTable ht = new LZ4.FastCompressionHashTable();
        ByteArrayDataOutput bufferedOutput = new ByteArrayDataOutput(termsDictBuffer);
        int dictLength = 0;

        for (BytesRef term = iterator.next(); term != null; term = iterator.next()) {
            if ((ord & blockMask) == 0) {
                if (ord != 0) {
                    final int uncompressedLength = compressAndGetTermsDictBlockLength(bufferedOutput, dictLength, ht);
                    maxBlockLength = Math.max(maxBlockLength, uncompressedLength);
                    bufferedOutput.reset(termsDictBuffer);
                }

                writer.add(data.getFilePointer() - start);
                data.writeVInt(term.length);
                data.writeBytes(term.bytes, term.offset, term.length);
                bufferedOutput = maybeGrowBuffer(bufferedOutput, term.length);
                bufferedOutput.writeBytes(term.bytes, term.offset, term.length);
                dictLength = term.length;
            } else {
                final int prefixLength = StringHelper.bytesDifference(previous.get(), term);
                final int suffixLength = term.length - prefixLength;
                assert suffixLength > 0; // terms are unique
                bufferedOutput = maybeGrowBuffer(bufferedOutput, suffixLength + 11);
                bufferedOutput.writeByte((byte) (Math.min(prefixLength, 15) | (Math.min(15, suffixLength - 1) << 4)));
                if (prefixLength >= 15) {
                    bufferedOutput.writeVInt(prefixLength - 15);
                }
                if (suffixLength >= 16) {
                    bufferedOutput.writeVInt(suffixLength - 16);
                }
                bufferedOutput.writeBytes(term.bytes, term.offset + prefixLength, suffixLength);
            }
            maxLength = Math.max(maxLength, term.length);
            previous.copyBytes(term);
            ++ord;
        }
        if (bufferedOutput.getPosition() > dictLength) {
            final int uncompressedLength = compressAndGetTermsDictBlockLength(bufferedOutput, dictLength, ht);
            maxBlockLength = Math.max(maxBlockLength, uncompressedLength);
        }

        writer.finish();
        meta.writeInt(maxLength);
        meta.writeInt(maxBlockLength);
        meta.writeLong(start);
        meta.writeLong(data.getFilePointer() - start);
        start = data.getFilePointer();
        addressBuffer.copyTo(data);
        meta.writeLong(start);
        meta.writeLong(data.getFilePointer() - start);

        writeTermsIndex(values);
    }

    private int compressAndGetTermsDictBlockLength(
        final ByteArrayDataOutput bufferedOutput,
        int dictLength,
        final LZ4.FastCompressionHashTable ht
    ) throws IOException {
        int uncompressedLength = bufferedOutput.getPosition() - dictLength;
        data.writeVInt(uncompressedLength);
        LZ4.compressWithDictionary(termsDictBuffer, 0, dictLength, uncompressedLength, data, ht);
        return uncompressedLength;
    }

    private ByteArrayDataOutput maybeGrowBuffer(ByteArrayDataOutput bufferedOutput, int termLength) {
        int pos = bufferedOutput.getPosition(), originalLength = termsDictBuffer.length;
        if (pos + termLength >= originalLength - 1) {
            termsDictBuffer = ArrayUtil.grow(termsDictBuffer, originalLength + termLength);
            bufferedOutput = new ByteArrayDataOutput(termsDictBuffer, pos, termsDictBuffer.length - pos);
        }
        return bufferedOutput;
    }

    private void writeTermsIndex(final SortedSetDocValues values) throws IOException {
        final long size = values.getValueCount();
        meta.writeInt(formatConfig.termsReverseIndexShift());
        long start = data.getFilePointer();

        long numBlocks = 1L + ((size + formatConfig.termsReverseIndexMask()) >>> formatConfig.termsReverseIndexShift());
        ByteBuffersDataOutput addressBuffer = new ByteBuffersDataOutput();
        DirectMonotonicWriter writer;
        try (ByteBuffersIndexOutput addressOutput = new ByteBuffersIndexOutput(addressBuffer, "temp", "temp")) {
            writer = DirectMonotonicWriter.getInstance(meta, addressOutput, numBlocks, formatConfig.directMonotonicBlockShift());
            TermsEnum iterator = values.termsEnum();
            BytesRefBuilder previous = new BytesRefBuilder();
            long offset = 0;
            long ord = 0;
            for (BytesRef term = iterator.next(); term != null; term = iterator.next()) {
                if ((ord & formatConfig.termsReverseIndexMask()) == 0) {
                    writer.add(offset);
                    final int sortKeyLength;
                    if (ord == 0) {
                        sortKeyLength = 0;
                    } else {
                        sortKeyLength = StringHelper.sortKeyLength(previous.get(), term);
                    }
                    offset += sortKeyLength;
                    data.writeBytes(term.bytes, term.offset, sortKeyLength);
                } else if ((ord & formatConfig.termsReverseIndexMask()) == formatConfig.termsReverseIndexMask()) {
                    previous.copyBytes(term);
                }
                ++ord;
            }
            writer.add(offset);
            writer.finish();
            meta.writeLong(start);
            meta.writeLong(data.getFilePointer() - start);
            start = data.getFilePointer();
            addressBuffer.copyTo(data);
            meta.writeLong(start);
            meta.writeLong(data.getFilePointer() - start);
        }
    }

    @Override
    public void addSortedNumericField(final FieldInfo field, final DocValuesProducer valuesProducer) throws IOException {
        meta.writeInt(field.number);
        meta.writeByte(SORTED_NUMERIC);
        writeSortedNumericField(field, new IndexingDocValuesSource(valuesProducer), -1);
    }

    private void writeSortedNumericField(final FieldInfo field, final DocValuesSource valuesSource, long maxOrd) throws IOException {
        if (field.docValuesSkipIndexType() != DocValuesSkipIndexType.NONE) {
            writeSkipIndex(field, valuesSource);
        }
        if (maxOrd > -1) {
            meta.writeByte((byte) 1); // multiValued (1 = multiValued)
        }

        final int blockSize = numericBlockSize;
        if (valuesSource.mergeStats.supported()) {
            int numDocsWithField = valuesSource.mergeStats.sumNumDocsWithField();
            long numValues = valuesSource.mergeStats.sumNumValues();
            if (numDocsWithField == numValues) {
                writeField(field, valuesSource, maxOrd, null, blockSize);
            } else {
                assert numValues > numDocsWithField;
                try (
                    OffsetsAccumulator accumulator = new OffsetsAccumulator(
                        dir,
                        context,
                        data,
                        numDocsWithField,
                        formatConfig.directMonotonicBlockShift()
                    )
                ) {
                    writeField(field, valuesSource, maxOrd, accumulator, blockSize);
                    accumulator.build(meta, data);
                }
            }
        } else {
            long[] stats = writeField(field, valuesSource, maxOrd, null, blockSize);
            int numDocsWithField = Math.toIntExact(stats[0]);
            long numValues = stats[1];
            assert numValues >= numDocsWithField;

            if (numValues > numDocsWithField) {
                long start = data.getFilePointer();
                meta.writeLong(start);
                meta.writeVInt(formatConfig.directMonotonicBlockShift());

                final DirectMonotonicWriter addressesWriter = DirectMonotonicWriter.getInstance(
                    meta,
                    data,
                    numDocsWithField + 1L,
                    formatConfig.directMonotonicBlockShift()
                );
                long addr = 0;
                addressesWriter.add(addr);
                SortedNumericDocValues values = valuesSource.getSortedNumeric(field);
                for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
                    addr += values.docValueCount();
                    addressesWriter.add(addr);
                }
                addressesWriter.finish();
                meta.writeLong(data.getFilePointer() - start);
            }
        }
    }

    @Override
    public void mergeSortedNumericField(final FieldInfo mergeFieldInfo, final MergeState mergeState) throws IOException {
        final DocValuesConsumerUtil.MergeStats result = compatibleWithOptimizedMerge(enableOptimizedMerge, mergeState, mergeFieldInfo);
        if (result.supported()) {
            mergeSortedNumericField(result, mergeFieldInfo, mergeState);
        } else {
            super.mergeSortedNumericField(mergeFieldInfo, mergeState);
        }
    }

    private static boolean isSingleValued(final FieldInfo field, final DocValuesSource producer) throws IOException {
        if (producer.mergeStats.supported()) {
            return producer.mergeStats.sumNumValues() == producer.mergeStats.sumNumDocsWithField();
        }

        SortedSetDocValues values = producer.getSortedSet(field);
        if (DocValues.unwrapSingleton(values) != null) {
            return true;
        }

        assert values.docID() == -1;
        for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
            int docValueCount = values.docValueCount();
            assert docValueCount > 0;
            if (docValueCount > 1) {
                return false;
            }
        }
        return true;
    }

    @Override
    public void mergeSortedSetField(final FieldInfo mergeFieldInfo, final MergeState mergeState) throws IOException {
        final DocValuesConsumerUtil.MergeStats result = compatibleWithOptimizedMerge(enableOptimizedMerge, mergeState, mergeFieldInfo);
        if (result.supported()) {
            mergeSortedSetField(result, mergeFieldInfo, mergeState);
        } else {
            super.mergeSortedSetField(mergeFieldInfo, mergeState);
        }
    }

    @Override
    public void addSortedSetField(final FieldInfo field, final DocValuesProducer valuesProducer) throws IOException {
        meta.writeInt(field.number);
        meta.writeByte(SORTED_SET);

        if (isSingleValued(field, new IndexingDocValuesSource(valuesProducer))) {
            doAddSortedField(field, new IndexingDocValuesSource(valuesProducer) {
                @Override
                public SortedDocValues getSorted(final FieldInfo field) throws IOException {
                    return SortedSetSelector.wrap(valuesProducer.getSortedSet(field), SortedSetSelector.Type.MIN);
                }
            }, true);
            return;
        }

        SortedSetDocValues values = valuesProducer.getSortedSet(field);
        long maxOrd = values.getValueCount();
        writeSortedNumericField(field, new IndexingDocValuesSource(valuesProducer) {
            @Override
            public SortedNumericDocValues getSortedNumeric(final FieldInfo field) throws IOException {
                SortedSetDocValues values = valuesProducer.getSortedSet(field);
                return new SortedNumericDocValues() {

                    long[] ords = LongsRef.EMPTY_LONGS;
                    int i, docValueCount;

                    @Override
                    public long nextValue() {
                        return ords[i++];
                    }

                    @Override
                    public int docValueCount() {
                        return docValueCount;
                    }

                    @Override
                    public boolean advanceExact(int target) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public int docID() {
                        return values.docID();
                    }

                    @Override
                    public int nextDoc() throws IOException {
                        int doc = values.nextDoc();
                        if (doc != NO_MORE_DOCS) {
                            docValueCount = values.docValueCount();
                            ords = ArrayUtil.grow(ords, docValueCount);
                            for (int j = 0; j < docValueCount; j++) {
                                ords[j] = values.nextOrd();
                            }
                            i = 0;
                        }
                        return doc;
                    }

                    @Override
                    public int advance(int target) throws IOException {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public long cost() {
                        return values.cost();
                    }
                };
            }
        }, maxOrd);

        addTermsDict(valuesProducer.getSortedSet(field));
    }

    @Override
    public void close() throws IOException {
        boolean success = false;
        try {
            if (meta != null) {
                meta.writeInt(-1); // write EOF marker
                CodecUtil.writeFooter(meta); // write checksum
            }
            if (data != null) {
                CodecUtil.writeFooter(data); // write checksum
            }
            success = true;
        } finally {
            if (success) {
                IOUtils.close(data, meta);
            } else {
                IOUtils.closeWhileHandlingException(data, meta);
            }
            meta = data = null;
        }
    }

    private static class SkipAccumulator {
        int minDocID;
        int maxDocID;
        int docCount;
        long minValue;
        long maxValue;

        SkipAccumulator(int docID) {
            minDocID = docID;
            minValue = Long.MAX_VALUE;
            maxValue = Long.MIN_VALUE;
            docCount = 0;
        }

        boolean isDone(int skipIndexIntervalSize, int valueCount, long nextValue, int nextDoc) {
            if (docCount < skipIndexIntervalSize) {
                return false;
            }
            return valueCount > 1 || minValue != maxValue || minValue != nextValue || docCount != nextDoc - minDocID;
        }

        void accumulate(long value) {
            minValue = Math.min(minValue, value);
            maxValue = Math.max(maxValue, value);
        }

        void accumulate(final SkipAccumulator other) {
            assert minDocID <= other.minDocID && maxDocID < other.maxDocID;
            maxDocID = other.maxDocID;
            minValue = Math.min(minValue, other.minValue);
            maxValue = Math.max(maxValue, other.maxValue);
            docCount += other.docCount;
        }

        void nextDoc(int docID) {
            maxDocID = docID;
            ++docCount;
        }

        public static SkipAccumulator merge(final List<SkipAccumulator> list, int index, int length) {
            SkipAccumulator acc = new SkipAccumulator(list.get(index).minDocID);
            for (int i = 0; i < length; i++) {
                acc.accumulate(list.get(index + i));
            }
            return acc;
        }
    }

    private void writeSkipIndex(final FieldInfo field, final DocValuesProducer valuesProducer) throws IOException {
        assert field.docValuesSkipIndexType() != DocValuesSkipIndexType.NONE;
        final long start = data.getFilePointer();
        final SortedNumericDocValues values = valuesProducer.getSortedNumeric(field);
        long globalMaxValue = Long.MIN_VALUE;
        long globalMinValue = Long.MAX_VALUE;
        int globalDocCount = 0;
        int maxDocId = -1;
        final List<SkipAccumulator> accumulators = new ArrayList<>();
        SkipAccumulator accumulator = null;
        final int maxAccumulators = 1 << (formatConfig.skipIndexLevelShift() * (formatConfig.skipIndexMaxLevel() - 1));
        for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
            final long firstValue = values.nextValue();
            if (accumulator != null && accumulator.isDone(formatConfig.skipIndexIntervalSize(), values.docValueCount(), firstValue, doc)) {
                globalMaxValue = Math.max(globalMaxValue, accumulator.maxValue);
                globalMinValue = Math.min(globalMinValue, accumulator.minValue);
                globalDocCount += accumulator.docCount;
                maxDocId = accumulator.maxDocID;
                accumulator = null;
                if (accumulators.size() == maxAccumulators) {
                    writeLevels(accumulators);
                    accumulators.clear();
                }
            }
            if (accumulator == null) {
                accumulator = new SkipAccumulator(doc);
                accumulators.add(accumulator);
            }
            accumulator.nextDoc(doc);
            accumulator.accumulate(firstValue);
            for (int i = 1, end = values.docValueCount(); i < end; ++i) {
                accumulator.accumulate(values.nextValue());
            }
        }

        if (accumulators.isEmpty() == false) {
            globalMaxValue = Math.max(globalMaxValue, accumulator.maxValue);
            globalMinValue = Math.min(globalMinValue, accumulator.minValue);
            globalDocCount += accumulator.docCount;
            maxDocId = accumulator.maxDocID;
            writeLevels(accumulators);
        }
        meta.writeLong(start); // record the start in meta
        meta.writeLong(data.getFilePointer() - start); // record the length
        assert globalDocCount == 0 || globalMaxValue >= globalMinValue;
        meta.writeLong(globalMaxValue);
        meta.writeLong(globalMinValue);
        assert globalDocCount <= maxDocId + 1;
        meta.writeInt(globalDocCount);
        meta.writeInt(maxDocId);
    }

    private void writeLevels(final List<SkipAccumulator> accumulators) throws IOException {
        final List<List<SkipAccumulator>> accumulatorsLevels = new ArrayList<>(formatConfig.skipIndexMaxLevel());
        accumulatorsLevels.add(accumulators);
        for (int i = 0; i < formatConfig.skipIndexMaxLevel() - 1; i++) {
            accumulatorsLevels.add(buildLevel(accumulatorsLevels.get(i)));
        }
        int totalAccumulators = accumulators.size();
        for (int index = 0; index < totalAccumulators; index++) {
            final int levels = getLevels(index, totalAccumulators);
            data.writeByte((byte) levels);
            for (int level = levels - 1; level >= 0; level--) {
                final SkipAccumulator acc = accumulatorsLevels.get(level).get(index >> (formatConfig.skipIndexLevelShift() * level));
                data.writeInt(acc.maxDocID);
                data.writeInt(acc.minDocID);
                data.writeLong(acc.maxValue);
                data.writeLong(acc.minValue);
                data.writeInt(acc.docCount);
            }
        }
    }

    private List<SkipAccumulator> buildLevel(final List<SkipAccumulator> accumulators) {
        final int levelSize = 1 << formatConfig.skipIndexLevelShift();
        final List<SkipAccumulator> collector = new ArrayList<>();
        for (int i = 0; i < accumulators.size() - levelSize + 1; i += levelSize) {
            collector.add(SkipAccumulator.merge(accumulators, i, levelSize));
        }
        return collector;
    }

    private int getLevels(int index, int size) {
        if (Integer.numberOfTrailingZeros(index) >= formatConfig.skipIndexLevelShift()) {
            final int left = size - index;
            for (int level = formatConfig.skipIndexMaxLevel() - 1; level > 0; level--) {
                final int numberIntervals = 1 << (formatConfig.skipIndexLevelShift() * level);
                if (left >= numberIntervals && index % numberIntervals == 0) {
                    return level + 1;
                }
            }
        }
        return 1;
    }
}
