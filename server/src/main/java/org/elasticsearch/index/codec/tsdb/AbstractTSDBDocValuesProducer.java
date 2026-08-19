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
import org.apache.lucene.codecs.compressing.Decompressor;
import org.apache.lucene.codecs.lucene90.IndexedDISI;
import org.apache.lucene.index.BaseTermsEnum;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValuesSkipIndexType;
import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.ImpactsEnum;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.internal.hppc.IntObjectHashMap;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.FileTypeHint;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.LongValues;
import org.apache.lucene.util.compress.LZ4;
import org.apache.lucene.util.packed.DirectMonotonicReader;
import org.apache.lucene.util.packed.PackedInts;
import org.elasticsearch.core.Assertions;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.codec.tsdb.pipeline.PipelineDescriptor;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.blockloader.docvalues.BlockDocValuesReader;
import org.elasticsearch.index.mapper.blockloader.docvalues.CustomBinaryDocValuesReader;
import org.elasticsearch.lucene.queries.BinaryDocValuesContainsTermQuery;
import org.elasticsearch.simdvec.ESVectorUtil;

import java.io.IOException;
import java.util.Arrays;

/**
 * Base class for TSDB doc values producers.
 *
 * <p>Owns the wire-format reading for numeric, binary, sorted, sorted-numeric, and sorted-set
 * doc values. Concrete subclasses construct this class with a {@link NumericBlockCodec} and an
 * {@link OrdinalBlockCodec}; those codecs supply the per-field readers and decoders the wire-format
 * code drives during segment open and value iteration.
 */
public abstract class AbstractTSDBDocValuesProducer extends DocValuesProducer {

    final IntObjectHashMap<NumericEntry> numerics;
    final IntObjectHashMap<BinaryEntry> binaries;
    final IntObjectHashMap<SortedEntry> sorted;
    final IntObjectHashMap<SortedSetEntry> sortedSets;
    final IntObjectHashMap<SortedNumericEntry> sortedNumerics;
    private final IntObjectHashMap<DocValuesSkipperEntry> skippers;
    private final IndexInput data, skip;
    private final int maxDoc;
    final int version;
    private final int primarySortFieldNumber;
    private final boolean merging;
    private final long[] skipIndexJumpLengthPerLevel;
    private static final int DEFAULT_NUMERIC_BLOCK_SHIFT = 7;
    /**
     * matchCost for the numeric range {@link TwoPhaseIterator} returned by {@code tryRangeIterator}.
     * Confirming a doc may decompress a numeric block, but that cost amortizes over the whole block,
     * so per-doc confirmation is cheap; this matches Lucene's own numeric range two-phase convention.
     */
    private static final float RANGE_ITERATOR_MATCH_COST = 2f;
    private final TSDBDocValuesFormatConfig formatConfig;
    private final DocOffsetsCodec.Decoder docOffsetsDecoder;
    private final NumericBlockCodec numericCodec;
    private final OrdinalBlockCodec ordinalCodec;
    private NumericReadContext readContext;

    @SuppressWarnings("this-escape")
    protected AbstractTSDBDocValuesProducer(
        final SegmentReadState state,
        final String dataCodec,
        final String dataExtension,
        final String metaCodec,
        final String metaExtension,
        final String skipCodec,
        final String skipExtension,
        final TSDBDocValuesFormatConfig formatConfig,
        final DocOffsetsCodec.Decoder docOffsetsDecoder,
        final NumericBlockCodec numericCodec,
        final OrdinalBlockCodec ordinalCodec
    ) throws IOException {
        this.docOffsetsDecoder = docOffsetsDecoder;
        this.numericCodec = numericCodec;
        this.ordinalCodec = ordinalCodec;
        this.numerics = new IntObjectHashMap<>();
        this.binaries = new IntObjectHashMap<>();
        this.sorted = new IntObjectHashMap<>();
        this.sortedSets = new IntObjectHashMap<>();
        this.sortedNumerics = new IntObjectHashMap<>();
        this.skippers = new IntObjectHashMap<>();
        this.maxDoc = state.segmentInfo.maxDoc();
        this.primarySortFieldNumber = primarySortFieldNumber(state.segmentInfo, state.fieldInfos);
        this.merging = false;
        this.formatConfig = formatConfig;
        this.skipIndexJumpLengthPerLevel = AbstractTSDBDocValuesConsumer.skipIndexJumpLengths(
            formatConfig.skipIndexLevelShift(),
            formatConfig.skipIndexMaxLevel()
        );

        int version = -1;
        int blockShift = DEFAULT_NUMERIC_BLOCK_SHIFT;
        String metaName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, metaExtension);

        try (ChecksumIndexInput in = state.directory.openChecksumInput(metaName)) {
            Throwable priorE = null;

            try {
                version = CodecUtil.checkIndexHeader(
                    in,
                    metaCodec,
                    TSDBDocValuesFormatConfig.VERSION_START,
                    TSDBDocValuesFormatConfig.VERSION_CURRENT,
                    state.segmentInfo.getId(),
                    state.segmentSuffix
                );
                if (version >= TSDBDocValuesFormatConfig.VERSION_NUMERIC_LARGE_BLOCKS) {
                    blockShift = in.readByte();
                }
                this.readContext = new NumericReadContext(1 << blockShift, formatConfig, version);
                readFields(in, state.fieldInfos, version, blockShift);
                if (version < TSDBDocValuesFormatConfig.VERSION_SKIPPER_MAX_VALUE_COUNT) {
                    inferMaxValueCounts(state.fieldInfos);
                }
            } catch (Throwable exception) {
                priorE = exception;
            } finally {
                CodecUtil.checkFooter(in, priorE);
            }
        }

        String dataName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, dataExtension);
        this.data = state.directory.openInput(dataName, state.context);
        boolean success = false;
        try {
            final int version2 = CodecUtil.checkIndexHeader(
                data,
                dataCodec,
                TSDBDocValuesFormatConfig.VERSION_START,
                TSDBDocValuesFormatConfig.VERSION_CURRENT,
                state.segmentInfo.getId(),
                state.segmentSuffix
            );
            if (version != version2) {
                throw new CorruptIndexException("Format versions mismatch: meta=" + version + ", data=" + version2, data);
            }

            // NOTE: data file is too costly to verify checksum against all the bytes on open,
            // but for now we at least verify proper structure of the checksum footer: which looks
            // for FOOTER_MAGIC + algorithmID. This is cheap and can detect some forms of corruption
            // such as file truncation.
            CodecUtil.retrieveChecksum(data);

            success = true;
            this.version = version;
        } finally {
            if (success == false) {
                IOUtils.closeWhileHandlingException(this.data);
            }
        }

        if (version >= TSDBDocValuesFormatConfig.VERSION_SEPARATE_SKIPLIST) {
            IndexInput tempIn = null;
            try {
                String skipIndexName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, skipExtension);
                tempIn = state.directory.openInput(skipIndexName, state.context.withHints(FileTypeHint.INDEX));
                final int skipVersion = CodecUtil.checkIndexHeader(
                    tempIn,
                    skipCodec,
                    TSDBDocValuesFormatConfig.VERSION_START,
                    TSDBDocValuesFormatConfig.VERSION_CURRENT,
                    state.segmentInfo.getId(),
                    state.segmentSuffix
                );
                if (version != skipVersion) {
                    throw new CorruptIndexException("Format versions mismatch: meta=" + version + ", skipIndex=" + skipVersion, tempIn);
                }
                CodecUtil.retrieveChecksum(tempIn);
            } catch (Throwable t) {
                IOUtils.closeWhileHandlingException(data, tempIn);
                throw t;
            }
            this.skip = tempIn;
        } else {
            this.skip = null;
        }
    }

    protected AbstractTSDBDocValuesProducer(final AbstractTSDBDocValuesProducer original) {
        this.docOffsetsDecoder = original.docOffsetsDecoder;
        this.numericCodec = original.numericCodec;
        this.ordinalCodec = original.ordinalCodec;
        this.readContext = original.readContext;
        this.numerics = original.numerics;
        this.binaries = original.binaries;
        this.sorted = original.sorted;
        this.sortedSets = original.sortedSets;
        this.sortedNumerics = original.sortedNumerics;
        this.skippers = original.skippers;
        this.data = original.data.clone();
        this.skip = original.skip == null ? null : original.skip.clone();
        this.maxDoc = original.maxDoc;
        this.version = original.version;
        this.primarySortFieldNumber = original.primarySortFieldNumber;
        this.merging = true;
        this.skipIndexJumpLengthPerLevel = original.skipIndexJumpLengthPerLevel;
        this.formatConfig = original.formatConfig;
    }

    /**
     * Creates a copy of this producer for merge operations.
     *
     * <p>The returned instance shares the underlying {@link IndexInput} (via clone) but maintains
     * independent read state, allowing concurrent iteration during merges.
     *
     * @return a merge-safe copy of this producer
     */
    protected abstract AbstractTSDBDocValuesProducer createMergeInstance();

    @Override
    public DocValuesProducer getMergeInstance() {
        return createMergeInstance();
    }

    @Override
    public NumericDocValues getNumeric(FieldInfo field) throws IOException {
        NumericEntry entry = numerics.get(field.number);
        return getNumeric(entry, AbstractTSDBDocValuesConsumer.NO_MAX_ORD, field);
    }

    @Override
    public BinaryDocValues getBinary(FieldInfo field) throws IOException {
        BinaryEntry entry = binaries.get(field.number);
        if (entry.docsWithFieldOffset == -2) {
            return DocValues.emptyBinary();
        }

        return switch (entry.compression) {
            case NO_COMPRESS -> getUncompressedBinary(entry);
            default -> getCompressedBinary(entry);
        };
    }

    private BinaryDocValues getUncompressedBinary(BinaryEntry entry) throws IOException {
        final RandomAccessInput bytesSlice = data.randomAccessSlice(entry.dataOffset, entry.dataLength);

        if (entry.docsWithFieldOffset == -1) {
            // dense
            if (entry.minLength == entry.maxLength) {
                // fixed length
                final int length = entry.maxLength;
                return new DenseBinaryDocValues(maxDoc) {
                    final BytesRef bytes = new BytesRef(new byte[length], 0, length);

                    @Override
                    public BytesRef binaryValue() throws IOException {
                        bytesSlice.readBytes((long) doc * length, bytes.bytes, 0, length);
                        return bytes;
                    }

                    @Override
                    public BlockLoader.Block tryRead(
                        BlockLoader.BlockFactory factory,
                        BlockLoader.Docs docs,
                        int offset,
                        boolean nullsFiltered,
                        BlockDocValuesReader.ToDouble toDouble,
                        boolean toInt,
                        boolean binaryMultiValuedFormat
                    ) throws IOException {
                        if (docs.mayContainDuplicates()) {
                            return null;
                        }

                        int count = docs.count() - offset;
                        int firstDocId = docs.get(offset);
                        int lastDocId = docs.get(docs.count() - 1);
                        doc = lastDocId;

                        if (binaryMultiValuedFormat) {
                            try (var builder = factory.bytesRefs(count)) {
                                final var reader = new CustomBinaryDocValuesReader();
                                for (int i = offset; i < docs.count(); i++) {
                                    int docId = docs.get(i);
                                    bytesSlice.readBytes((long) docId * length, bytes.bytes, 0, length);
                                    reader.read(bytes, builder);
                                }
                                return builder.build();
                            }
                        } else if (isDense(firstDocId, lastDocId, count)) {
                            try (var builder = factory.singletonBytesRefs(count)) {
                                int bulkLength = length * count;
                                byte[] bytes = new byte[bulkLength];
                                bytesSlice.readBytes((long) firstDocId * length, bytes, 0, bulkLength);
                                builder.appendBytesRefs(bytes, length);
                                return builder.build();
                            }
                        } else {
                            try (var builder = factory.bytesRefs(count)) {
                                for (int i = offset; i < docs.count(); i++) {
                                    int docId = docs.get(i);
                                    bytesSlice.readBytes((long) docId * length, bytes.bytes, 0, length);
                                    builder.appendBytesRef(bytes);
                                }
                                return builder.build();
                            }
                        }
                    }

                    @Override
                    int getLength() {
                        return length;
                    }
                };
            } else {
                // variable length
                final RandomAccessInput addressesData = this.data.randomAccessSlice(entry.addressesOffset, entry.addressesLength);
                final LongValues addresses = DirectMonotonicReader.getInstance(entry.addressesMeta, addressesData, merging);
                return new DenseBinaryDocValues(maxDoc) {
                    final BytesRef bytes = new BytesRef(new byte[entry.maxLength], 0, entry.maxLength);

                    @Override
                    public BytesRef binaryValue() throws IOException {
                        long startOffset = addresses.get(doc);
                        bytes.length = (int) (addresses.get(doc + 1L) - startOffset);
                        bytesSlice.readBytes(startOffset, bytes.bytes, 0, bytes.length);
                        return bytes;
                    }

                    @Override
                    public BlockLoader.Block tryRead(
                        BlockLoader.BlockFactory factory,
                        BlockLoader.Docs docs,
                        int offset,
                        boolean nullsFiltered,
                        BlockDocValuesReader.ToDouble toDouble,
                        boolean toInt,
                        boolean binaryMultiValuedFormat
                    ) throws IOException {
                        if (docs.mayContainDuplicates()) {
                            return null;
                        }

                        int count = docs.count() - offset;
                        int firstDocId = docs.get(offset);
                        int lastDocId = docs.get(docs.count() - 1);
                        doc = lastDocId;

                        if (binaryMultiValuedFormat) {
                            try (var builder = factory.bytesRefs(count)) {
                                final var reader = new CustomBinaryDocValuesReader();
                                for (int i = offset; i < docs.count(); i++) {
                                    int docId = docs.get(i);
                                    long startOffset = addresses.get(docId);
                                    bytes.length = (int) (addresses.get(docId + 1L) - startOffset);
                                    bytesSlice.readBytes(startOffset, bytes.bytes, 0, bytes.length);
                                    reader.read(bytes, builder);
                                }
                                return builder.build();
                            }
                        } else if (isDense(firstDocId, lastDocId, count)) {
                            try (var builder = factory.singletonBytesRefs(count)) {
                                int[] offsets = new int[count + 1];

                                long startOffset = addresses.get(firstDocId);
                                for (int i = offset, j = 1; i < docs.count(); i++, j++) {
                                    int docId = docs.get(i);
                                    offsets[j] = Math.toIntExact(addresses.get(docId + 1) - startOffset);
                                }

                                int length = Math.toIntExact(addresses.get(lastDocId + 1L) - startOffset);
                                byte[] bytes = new byte[length];
                                bytesSlice.readBytes(startOffset, bytes, 0, length);
                                builder.appendBytesRefs(bytes, offsets);
                                return builder.build();
                            }
                        } else {
                            try (var builder = factory.bytesRefs(count)) {
                                for (int i = offset; i < docs.count(); i++) {
                                    int docId = docs.get(i);
                                    long startOffset = addresses.get(docId);
                                    bytes.length = (int) (addresses.get(docId + 1L) - startOffset);
                                    bytesSlice.readBytes(startOffset, bytes.bytes, 0, bytes.length);
                                    builder.appendBytesRef(bytes);
                                }
                                return builder.build();
                            }
                        }
                    }

                    @Override
                    int getLength() {
                        return (int) (addresses.get(doc + 1L) - addresses.get(doc));
                    }
                };
            }
        } else {
            // sparse
            final IndexedDISI disi = new IndexedDISI(
                data,
                entry.docsWithFieldOffset,
                entry.docsWithFieldLength,
                entry.jumpTableEntryCount,
                entry.denseRankPower,
                entry.numDocsWithField
            );
            if (entry.minLength == entry.maxLength) {
                // fixed length
                final int length = entry.maxLength;
                return new SparseBinaryDocValues(disi) {
                    final BytesRef bytes = new BytesRef(new byte[length], 0, length);

                    @Override
                    public BytesRef binaryValue() throws IOException {
                        bytesSlice.readBytes((long) disi.index() * length, bytes.bytes, 0, length);
                        return bytes;
                    }

                    @Override
                    int getLength() {
                        return length;
                    }
                };
            } else {
                // variable length
                final RandomAccessInput addressesData = this.data.randomAccessSlice(entry.addressesOffset, entry.addressesLength);
                final LongValues addresses = DirectMonotonicReader.getInstance(entry.addressesMeta, addressesData, merging);
                return new SparseBinaryDocValues(disi) {
                    final BytesRef bytes = new BytesRef(new byte[entry.maxLength], 0, entry.maxLength);

                    @Override
                    public BytesRef binaryValue() throws IOException {
                        final int index = disi.index();
                        long startOffset = addresses.get(index);
                        bytes.length = (int) (addresses.get(index + 1L) - startOffset);
                        bytesSlice.readBytes(startOffset, bytes.bytes, 0, bytes.length);
                        return bytes;
                    }

                    @Override
                    int getLength() {
                        final int index = disi.index();
                        return (int) (addresses.get(index + 1L) - addresses.get(index));
                    }
                };
            }
        }
    }

    private BinaryDocValues getCompressedBinary(BinaryEntry entry) throws IOException {
        final DocOffsetsCodec.Decoder offsetsDecoder = this.docOffsetsDecoder;
        if (entry.docsWithFieldOffset == -1) {
            // dense
            final RandomAccessInput addressesData = this.data.randomAccessSlice(entry.addressesOffset, entry.addressesLength);
            final LongValues addresses = DirectMonotonicReader.getInstance(entry.addressesMeta, addressesData);

            final RandomAccessInput docOffsetsData = this.data.randomAccessSlice(entry.docOffsetsOffset, entry.docOffsetLength);
            final DirectMonotonicReader docOffsets = DirectMonotonicReader.getInstance(entry.docOffsetMeta, docOffsetsData);
            return new DenseBinaryDocValues(maxDoc) {
                final BinaryDecoder decoder = new BinaryDecoder(
                    entry.compression.compressionMode().newDecompressor(),
                    addresses,
                    docOffsets,
                    data.clone(),
                    entry.maxUncompressedChunkSize,
                    entry.maxNumDocsInAnyBlock,
                    offsetsDecoder
                );

                @Override
                public BytesRef binaryValue() throws IOException {
                    return decoder.decode(doc, entry.numCompressedBlocks);
                }

                @Override
                public BlockLoader.Block tryRead(
                    BlockLoader.BlockFactory factory,
                    BlockLoader.Docs docs,
                    int offset,
                    boolean nullsFiltered,
                    BlockDocValuesReader.ToDouble toDouble,
                    boolean toInt,
                    boolean binaryMultiValuedFormat
                ) throws IOException {
                    if (docs.mayContainDuplicates()) {
                        return null;
                    }

                    int count = docs.count() - offset;
                    int firstDocId = docs.get(offset);
                    int lastDocId = docs.get(docs.count() - 1);
                    doc = lastDocId;

                    if (binaryMultiValuedFormat) {
                        try (var builder = factory.bytesRefs(count)) {
                            final var reader = new CustomBinaryDocValuesReader();
                            for (int i = offset; i < docs.count(); i++) {
                                BytesRef bytes = decoder.decode(docs.get(i), entry.numCompressedBlocks);
                                reader.read(bytes, builder);
                            }
                            return builder.build();
                        }
                    } else if (isDense(firstDocId, lastDocId, count)) {
                        try (var builder = factory.singletonBytesRefs(count)) {
                            decoder.decodeBulk(entry.numCompressedBlocks, firstDocId, lastDocId, count, builder);
                            return builder.build();
                        }
                    } else {
                        try (var builder = factory.bytesRefs(count)) {
                            for (int i = offset; i < docs.count(); i++) {
                                builder.appendBytesRef(decoder.decode(docs.get(i), entry.numCompressedBlocks));
                            }
                            return builder.build();
                        }
                    }
                }

                @Override
                int getLength() throws IOException {
                    return decoder.decodeLength(doc, entry.numCompressedBlocks);
                }

                @Override
                public DocIdSetIterator tryLengthIterator(int length) throws IOException {
                    return decoder.lengthsTwoPhase(entry.numCompressedBlocks, length, maxDoc);
                }

                @Override
                public DocIdSetIterator tryContainsIterator(BytesRef containsTerm) throws IOException {
                    return decoder.containsTermTwoPhase(entry.numCompressedBlocks, containsTerm, maxDoc);
                }

                @Override
                public DocIdSetIterator tryTermEqualIterator(BytesRef term) throws IOException {
                    return decoder.termEqualTwoPhase(entry.numCompressedBlocks, term, maxDoc);
                }
            };
        } else {
            // sparse
            final IndexedDISI disi = new IndexedDISI(
                data,
                entry.docsWithFieldOffset,
                entry.docsWithFieldLength,
                entry.jumpTableEntryCount,
                entry.denseRankPower,
                entry.numDocsWithField
            );
            final RandomAccessInput addressesData = this.data.randomAccessSlice(entry.addressesOffset, entry.addressesLength);
            final LongValues addresses = DirectMonotonicReader.getInstance(entry.addressesMeta, addressesData);

            final RandomAccessInput docOffsetsData = this.data.randomAccessSlice(entry.docOffsetsOffset, entry.docOffsetLength);
            final DirectMonotonicReader docOffsets = DirectMonotonicReader.getInstance(entry.docOffsetMeta, docOffsetsData);
            return new SparseBinaryDocValues(disi) {
                final BinaryDecoder decoder = new BinaryDecoder(
                    entry.compression.compressionMode().newDecompressor(),
                    addresses,
                    docOffsets,
                    data.clone(),
                    entry.maxUncompressedChunkSize,
                    entry.maxNumDocsInAnyBlock,
                    offsetsDecoder
                );

                @Override
                public BytesRef binaryValue() throws IOException {
                    return decoder.decode(disi.index(), entry.numCompressedBlocks);
                }

                @Override
                int getLength() throws IOException {
                    return decoder.decodeLength(disi.index(), entry.numCompressedBlocks);
                }
            };
        }
    }

    static final class BinaryDecoder {

        private final LongValues addresses;
        private final DirectMonotonicReader docOffsets;
        private final IndexInput compressedData;
        private final IndexInput readAhead;
        private long lastBlockId = -1;
        private final int[] uncompressedDocStarts;
        private final int biggestUncompressedBlockSize;
        // Lazily allocated to avoid eagerly over-consuming memory under a large query fan-out or a single outlier block
        private byte[] uncompressedBlock;
        private BytesRef uncompressedBytesRef;
        private long startDocNumForBlock = -1;
        private long limitDocNumForBlock = -1;
        private final Decompressor decompressor;
        private final DocOffsetsCodec.Decoder docOffsetsDecoder;

        BinaryDecoder(
            Decompressor decompressor,
            LongValues addresses,
            DirectMonotonicReader docOffsets,
            IndexInput compressedData,
            int biggestUncompressedBlockSize,
            int maxNumDocsInAnyBlock,
            DocOffsetsCodec.Decoder docOffsetsDecoder
        ) {
            this.decompressor = decompressor;
            this.addresses = addresses;
            this.docOffsets = docOffsets;
            this.compressedData = compressedData;
            this.readAhead = compressedData.clone();
            this.biggestUncompressedBlockSize = biggestUncompressedBlockSize;
            uncompressedDocStarts = new int[maxNumDocsInAnyBlock + 1];
            this.docOffsetsDecoder = docOffsetsDecoder;
        }

        private BinaryDVCompressionMode.BlockHeader decompressOffsets(long blockId, int numDocsInBlock) throws IOException {
            long blockStartOffset = addresses.get(blockId);
            compressedData.seek(blockStartOffset);

            var header = BinaryDVCompressionMode.BlockHeader.fromByte(compressedData.readByte());
            int uncompressedBlockLength = compressedData.readVInt();

            if (uncompressedBlockLength == 0) {
                Arrays.fill(uncompressedDocStarts, 0);
            } else {
                docOffsetsDecoder.decode(uncompressedDocStarts, numDocsInBlock, compressedData);
            }

            return header;
        }

        private void decompressBlock(long blockId, int numDocsInBlock) throws IOException {
            var header = decompressOffsets(blockId, numDocsInBlock);
            decompressValues(header.isCompressed(), numDocsInBlock);
        }

        /**
         * Decompresses the value bytes of the block whose offsets were just loaded by
         * {@link #decompressOffsets}. Precondition: {@code compressedData} is still positioned
         * immediately after that block's encoded offsets — no intervening seek or read on this
         * decoder since the offsets were loaded.
         */
        private void decompressValues(boolean compressed, int numDocsInBlock) throws IOException {
            int uncompressedBlockLength = uncompressedDocStarts[numDocsInBlock];
            if (uncompressedBlock == null || uncompressedBlock.length < uncompressedBlockLength) {
                // Size to the block we actually read, capped at the segment max, so one outlier block does not force every
                // decoder on the segment to allocate the outlier size.
                int size = Math.min(ArrayUtil.oversize(uncompressedBlockLength, Byte.BYTES), biggestUncompressedBlockSize);
                uncompressedBlock = new byte[size];
                uncompressedBytesRef = new BytesRef(uncompressedBlock);
            }
            assert uncompressedBlockLength <= uncompressedBlock.length;
            uncompressedBytesRef.offset = 0;
            uncompressedBytesRef.length = uncompressedBlock.length;

            if (compressed) {
                decompressor.decompress(compressedData, uncompressedBlockLength, 0, uncompressedBlockLength, uncompressedBytesRef);
            } else {
                compressedData.readBytes(uncompressedBlock, 0, uncompressedBlockLength);
            }
        }

        long findAndUpdateBlock(int docNumber, int numBlocks) {
            if (docNumber < limitDocNumForBlock && lastBlockId >= 0) {
                return lastBlockId;
            }

            long index = docOffsets.binarySearch(lastBlockId + 1, numBlocks, docNumber);
            if (index < 0) {
                index = -2 - index;
            }
            assert index < numBlocks : "invalid range " + index + " for doc " + docNumber + " in numBlocks " + numBlocks;

            startDocNumForBlock = docOffsets.get(index);
            limitDocNumForBlock = docOffsets.get(index + 1);
            return index;
        }

        BytesRef decode(int docNumber, int numBlocks) throws IOException {
            long blockId = findAndUpdateBlock(docNumber, numBlocks);

            int numDocsInBlock = (int) (limitDocNumForBlock - startDocNumForBlock);
            int idxInBlock = (int) (docNumber - startDocNumForBlock);
            assert idxInBlock < numDocsInBlock;

            if (blockId != lastBlockId) {
                decompressBlock(blockId, numDocsInBlock);
                lastBlockId = blockId;
            }

            int start = uncompressedDocStarts[idxInBlock];
            int end = uncompressedDocStarts[idxInBlock + 1];
            uncompressedBytesRef.offset = start;
            uncompressedBytesRef.length = end - start;
            return uncompressedBytesRef;
        }

        int decodeLength(int docNumber, int numBlocks) throws IOException {
            long blockId = findAndUpdateBlock(docNumber, numBlocks);

            int numDocsInBlock = (int) (limitDocNumForBlock - startDocNumForBlock);
            int idxInBlock = (int) (docNumber - startDocNumForBlock);
            assert idxInBlock < numDocsInBlock;

            if (blockId != lastBlockId) {
                decompressOffsets(blockId, numDocsInBlock);
                lastBlockId = blockId;
            }

            int start = uncompressedDocStarts[idxInBlock];
            int end = uncompressedDocStarts[idxInBlock + 1];
            return end - start;
        }

        /**
         * Base class for binary-doc-values predicates expressed as Lucene
         * {@link TwoPhaseIterator}s. Centralizes the "walk a dense approximation, lazily decompress
         * the block containing the current doc, ask the subclass whether it matches" loop so each
         * predicate-shaped iterator only has to provide {@link #loadBlock} (full bytes vs offsets
         * only) and {@link #matchesInBlock} (the per-doc check).
         *
         * <p><b>Why this shape:</b> the older "find next match by scanning inside
         * {@code advance(target)}" pattern over-scans past the caller's {@code max} when matches are
         * sparse — under sub-segment slicing ({@code DataPartitioning.DOC}), siblings on the same
         * leaf each re-scan empty regions, blowing up total CPU. A {@link TwoPhaseIterator} with a
         * dense approximation is detected by {@link org.apache.lucene.search.ConstantScoreScorer}
         * (via {@link org.apache.lucene.search.Scorer#twoPhaseIterator()}); Lucene's default
         * BulkScorer then drives {@code approximation.advance(min) + matches()} within {@code [min,
         * max)}, bounding per-driver cost to the slice size. Future per-doc binary-DV predicates on
         * this block layout should extend this class rather than building a custom iterator that
         * bakes matching into {@code advance()}.
         */
        abstract class BlockAwareTwoPhase extends TwoPhaseIterator {
            private final int numBlocks;
            private long blockId = -1;
            private int blockStart = 0;
            private int blockEnd = 0;

            BlockAwareTwoPhase(DocIdSetIterator approximation, int numBlocks) {
                super(approximation);
                this.numBlocks = numBlocks;
            }

            @Override
            public final boolean matches() throws IOException {
                int doc = approximation.docID();
                if (blockId == -1 || doc >= blockEnd) {
                    blockId = findBlock(doc, numBlocks, blockId == -1 ? 0 : blockId);
                    blockStart = (int) docOffsets.get(blockId);
                    blockEnd = (int) docOffsets.get(blockId + 1);
                    loadBlock(blockId, blockEnd - blockStart);
                }
                return matchesInBlock(doc - blockStart);
            }

            /**
             * Loads {@code blockId} into the decoder's per-block buffers. Subclasses that only need
             * offsets call {@link #decompressOffsets}; subclasses that read value bytes call
             * {@link #decompressBlock}.
             */
            abstract void loadBlock(long blockId, int numDocsInBlock) throws IOException;

            /** Per-doc predicate evaluated against the currently-loaded block. */
            abstract boolean matchesInBlock(int idxInBlock) throws IOException;
        }

        /**
         * Length-equality predicate: only block offsets are decompressed (no value bytes), so the
         * per-doc check is a single offsets subtraction.
         */
        DocIdSetIterator lengthsTwoPhase(int numBlocks, int requestedLength, int leafMaxDoc) {
            return TwoPhaseIterator.asDocIdSetIterator(new BlockAwareTwoPhase(DocIdSetIterator.all(leafMaxDoc), numBlocks) {
                @Override
                void loadBlock(long blockId, int numDocsInBlock) throws IOException {
                    decompressOffsets(blockId, numDocsInBlock);
                }

                @Override
                boolean matchesInBlock(int idx) {
                    return uncompressedDocStarts[idx + 1] - uncompressedDocStarts[idx] == requestedLength;
                }

                @Override
                public float matchCost() {
                    // One int subtraction inside an in-memory offsets array.
                    return 2f;
                }
            });
        }

        /**
         * Substring-contains predicate: the full block bytes are decompressed and {@code matches}
         * runs the SIMD substring check on each doc's slice of the block.
         */
        DocIdSetIterator containsTermTwoPhase(int numBlocks, BytesRef containsTerm, int leafMaxDoc) {
            return TwoPhaseIterator.asDocIdSetIterator(new BlockAwareTwoPhase(DocIdSetIterator.all(leafMaxDoc), numBlocks) {
                @Override
                void loadBlock(long blockId, int numDocsInBlock) throws IOException {
                    decompressBlock(blockId, numDocsInBlock);
                }

                @Override
                boolean matchesInBlock(int idx) {
                    int offset = uncompressedDocStarts[idx];
                    int length = uncompressedDocStarts[idx + 1] - offset;
                    return length >= containsTerm.length
                        && BinaryDocValuesContainsTermQuery.contains(uncompressedBlock, offset, length, containsTerm);
                }

                @Override
                public float matchCost() {
                    // SIMD substring check amortized over the decompressed block.
                    return 10f;
                }
            });
        }

        /**
         * Term-equality predicate: block offsets are decompressed eagerly; the value bytes are
         * decompressed lazily — only on the first doc in a block whose stored length matches
         * {@code term.length}. Blocks where no doc has the matching length are rejected on offsets
         * alone, paying only the offset decode and skipping the full Zstd/LZ4 decompression.
         *
         * <p>For a length-discriminating term (e.g. a hostname that is longer or shorter than most
         * values in the field), the majority of blocks never touch the value bytes. For a
         * non-discriminating length, the worst case is identical to decompressing every block
         * eagerly; no work is added.
         *
         * <p>Only valid for single-valued docs (no length-prefix framing from the multi-valued
         * encoding). The caller must gate on
         * {@code countsSkipper == null || countsSkipper.maxValue() == 1} before calling this.
         */
        DocIdSetIterator termEqualTwoPhase(int numBlocks, BytesRef term, int leafMaxDoc) {
            return TwoPhaseIterator.asDocIdSetIterator(new BlockAwareTwoPhase(DocIdSetIterator.all(leafMaxDoc), numBlocks) {
                private boolean valuesLoaded;
                private boolean blockCompressed;
                private int blockNumDocs;

                @Override
                void loadBlock(long blockId, int numDocsInBlock) throws IOException {
                    // Offsets only — value bytes are decompressed on demand in matchesInBlock.
                    blockCompressed = decompressOffsets(blockId, numDocsInBlock).isCompressed();
                    blockNumDocs = numDocsInBlock;
                    valuesLoaded = false;
                }

                @Override
                boolean matchesInBlock(int idx) throws IOException {
                    int offset = uncompressedDocStarts[idx];
                    int length = uncompressedDocStarts[idx + 1] - offset;
                    if (length != term.length) {
                        return false; // rejected on offsets alone — value bytes never touched
                    }
                    if (valuesLoaded == false) {
                        // First length-match in this block: decompress values now. compressedData is
                        // still positioned immediately after this block's encoded offsets.
                        decompressValues(blockCompressed, blockNumDocs);
                        valuesLoaded = true;
                    }
                    return Arrays.equals(uncompressedBlock, offset, offset + length, term.bytes, term.offset, term.offset + term.length);
                }

                @Override
                public float matchCost() {
                    // Most docs are rejected on offsets alone; the equality scan over matching-length
                    // docs is one intrinsified call — cheaper than contains' SIMD substring search (10f).
                    return 5f;
                }
            });
        }

        void decodeBulk(int numBlocks, int firstDocId, int lastDocId, int count, BlockLoader.SingletonBytesRefBuilder builder)
            throws IOException {
            final long firstBlockId = findBlock(firstDocId, numBlocks, lastBlockId == -1 ? 0 : lastBlockId);
            final long endBlockId = findBlock(lastDocId, numBlocks, firstBlockId);
            final int bufferSize = computeMultipleBlockBufferSize(firstBlockId, endBlockId);

            int offsetBufferIndex = 0;
            final int[] offsetBuffer = new int[count + 1];
            int valuesBufferIndex = 0;
            final byte[] valuesBuffer = new byte[bufferSize];

            for (long blockId = firstBlockId; blockId <= endBlockId; blockId++) {
                int blockStartDocId = (int) docOffsets.get(blockId);
                int blockEndDocId = (int) docOffsets.get(blockId + 1);
                int numDocsInBlock = blockEndDocId - blockStartDocId;
                if (blockId != lastBlockId) {
                    decompressBlock(blockId, numDocsInBlock);
                }

                int startDocId = blockId == firstBlockId ? firstDocId : blockStartDocId;
                int endDocId = blockId == endBlockId ? lastDocId + 1 : blockEndDocId;
                int offsetStart = uncompressedDocStarts[startDocId - blockStartDocId];
                int offsetEnd = uncompressedDocStarts[endDocId - blockStartDocId];

                for (int docId = startDocId; docId < endDocId; docId++) {
                    int index = docId - blockStartDocId;
                    int offset = valuesBufferIndex + uncompressedDocStarts[index] - offsetStart;
                    offsetBuffer[offsetBufferIndex++] = offset;
                }

                int length = offsetEnd - offsetStart;
                System.arraycopy(uncompressedBlock, offsetStart, valuesBuffer, valuesBufferIndex, length);
                valuesBufferIndex += length;
            }
            offsetBuffer[offsetBufferIndex] = valuesBufferIndex;

            lastBlockId = endBlockId;

            // TODO: This sets state for the decode(...), we should look into removing this.
            startDocNumForBlock = docOffsets.get(endBlockId);
            limitDocNumForBlock = docOffsets.get(endBlockId + 1);

            assert count == offsetBufferIndex;
            builder.appendBytesRefs(valuesBuffer, offsetBuffer);
        }

        long findBlock(int docNumber, int numBlocks, long fromIndex) {
            long index = docOffsets.binarySearch(fromIndex, numBlocks, docNumber);
            if (index < 0) {
                index = -2 - index;
            }
            assert index < numBlocks : "invalid range " + index + " for doc " + docNumber + " in numBlocks " + numBlocks;
            return index;
        }

        int computeMultipleBlockBufferSize(long firstBlockId, long lastBlockId) throws IOException {
            int requiredBufferSize = 0;
            for (long blockId = firstBlockId; blockId <= lastBlockId; blockId++) {
                long blockStartOffset = addresses.get(blockId);
                readAhead.seek(blockStartOffset);
                readAhead.readByte(); // skip BlockHeader
                int uncompressedBlockLength = readAhead.readVInt();
                requiredBufferSize += uncompressedBlockLength;
            }
            return requiredBufferSize;
        }
    }

    public abstract static class TSDBBinaryDocValues extends BinaryDocValues
        implements
            BlockLoader.OptionalColumnAtATimeReader,
            BlockLoader.OptionalLengthReader {}

    abstract static class DenseBinaryDocValues extends TSDBBinaryDocValues {

        final int maxDoc;
        int doc = -1;

        DenseBinaryDocValues(int maxDoc) {
            this.maxDoc = maxDoc;
        }

        @Override
        public int nextDoc() throws IOException {
            return advance(doc + 1);
        }

        @Override
        public int docID() {
            return doc;
        }

        @Override
        public long cost() {
            return maxDoc;
        }

        @Override
        public int advance(int target) throws IOException {
            if (target >= maxDoc) {
                return doc = NO_MORE_DOCS;
            }
            return doc = target;
        }

        @Override
        public boolean advanceExact(int target) throws IOException {
            doc = target;
            return true;
        }

        @Override
        public int docIDRunEnd() throws IOException {
            return maxDoc;
        }

        @Override
        public void intoBitSet(int upTo, FixedBitSet bitSet, int offset) throws IOException {
            assert offset <= doc;
            upTo = Math.min(upTo, maxDoc);
            if (upTo > doc) {
                bitSet.set(doc - offset, upTo - offset);
                advance(upTo);
            }
        }

        @Override
        @Nullable
        public BlockLoader.Block tryRead(
            BlockLoader.BlockFactory factory,
            BlockLoader.Docs docs,
            int offset,
            boolean nullsFiltered,
            BlockDocValuesReader.ToDouble toDouble,
            boolean toInt,
            boolean binaryMultiValuedFormat
        ) throws IOException {
            return null;
        }

        abstract int getLength() throws IOException;

        @Override
        @Nullable
        public BlockLoader.Block tryReadLength(BlockLoader.BlockFactory factory, BlockLoader.Docs docs, int offset, boolean nullsFiltered)
            throws IOException {
            int count = docs.count() - offset;
            try (var builder = factory.singletonInts(count)) {
                int countIndex = 0;
                int[] counts = new int[count];
                for (int i = offset; i < docs.count(); i++) {
                    int doc = docs.get(i);
                    boolean advance = advanceExact(doc);
                    assert advance;
                    counts[countIndex++] = getLength();
                }
                builder.appendInts(counts, 0, countIndex);
                return builder.build();
            }
        }

        @Override
        public NumericDocValues toLengthValues() {
            DenseBinaryDocValues binaryDocValues = this;
            return new NumericDocValues() {
                @Override
                public long longValue() throws IOException {
                    return binaryDocValues.getLength();
                }

                @Override
                public boolean advanceExact(int target) throws IOException {
                    return binaryDocValues.advanceExact(target);
                }

                @Override
                public int docID() {
                    return binaryDocValues.docID();
                }

                @Override
                public int nextDoc() throws IOException {
                    return binaryDocValues.nextDoc();
                }

                @Override
                public int advance(int target) throws IOException {
                    return binaryDocValues.advance(target);
                }

                @Override
                public long cost() {
                    return binaryDocValues.cost();
                }

                @Override
                public void intoBitSet(int upTo, FixedBitSet bitSet, int offset) throws IOException {
                    binaryDocValues.intoBitSet(upTo, bitSet, offset);
                }

                @Override
                public int docIDRunEnd() throws IOException {
                    return binaryDocValues.docIDRunEnd();
                }
            };
        }
    }

    abstract static class SparseBinaryDocValues extends TSDBBinaryDocValues {

        final IndexedDISI disi;

        SparseBinaryDocValues(IndexedDISI disi) {
            this.disi = disi;
        }

        @Override
        public int nextDoc() throws IOException {
            return disi.nextDoc();
        }

        @Override
        public int docID() {
            return disi.docID();
        }

        @Override
        public long cost() {
            return disi.cost();
        }

        @Override
        public int advance(int target) throws IOException {
            return disi.advance(target);
        }

        @Override
        public boolean advanceExact(int target) throws IOException {
            return disi.advanceExact(target);
        }

        @Override
        public int docIDRunEnd() throws IOException {
            return disi.docIDRunEnd();
        }

        @Override
        public void intoBitSet(int upTo, FixedBitSet bitSet, int offset) throws IOException {
            disi.intoBitSet(upTo, bitSet, offset);
        }

        @Override
        @Nullable
        public BlockLoader.Block tryRead(
            BlockLoader.BlockFactory factory,
            BlockLoader.Docs docs,
            int offset,
            boolean nullsFiltered,
            BlockDocValuesReader.ToDouble toDouble,
            boolean toInt,
            boolean binaryMultiValuedFormat
        ) throws IOException {
            return null;
        }

        abstract int getLength() throws IOException;

        @Override
        @Nullable
        public BlockLoader.Block tryReadLength(BlockLoader.BlockFactory factory, BlockLoader.Docs docs, int offset, boolean nullsFiltered)
            throws IOException {
            int count = docs.count() - offset;
            try (var builder = factory.ints(count)) {
                for (int i = offset; i < docs.count(); i++) {
                    int doc = docs.get(i);
                    if (advanceExact(doc)) {
                        builder.appendInt(getLength());
                    } else {
                        builder.appendNull();
                    }
                }
                return builder.build();
            }
        }

        @Override
        public NumericDocValues toLengthValues() {
            SparseBinaryDocValues binaryDocValues = this;
            return new NumericDocValues() {
                @Override
                public long longValue() throws IOException {
                    return binaryDocValues.getLength();
                }

                @Override
                public boolean advanceExact(int target) throws IOException {
                    return binaryDocValues.advanceExact(target);
                }

                @Override
                public int docID() {
                    return binaryDocValues.docID();
                }

                @Override
                public int nextDoc() throws IOException {
                    return binaryDocValues.nextDoc();
                }

                @Override
                public int advance(int target) throws IOException {
                    return binaryDocValues.advance(target);
                }

                @Override
                public long cost() {
                    return binaryDocValues.cost();
                }

                @Override
                public int docIDRunEnd() throws IOException {
                    return binaryDocValues.docIDRunEnd();
                }

                @Override
                public void intoBitSet(int upTo, FixedBitSet bitSet, int offset) throws IOException {
                    binaryDocValues.intoBitSet(upTo, bitSet, offset);
                }
            };
        }
    }

    @Override
    public SortedDocValues getSorted(FieldInfo field) throws IOException {
        SortedEntry entry = sorted.get(field.number);
        return getSorted(entry, field.number == primarySortFieldNumber);
    }

    private SortedDocValues getSorted(SortedEntry entry, boolean valuesSorted) throws IOException {
        if (entry.ordsEntry.docsWithFieldOffset == -2) {
            return DocValues.emptySorted();
        }

        final NumericDocValues ords = getNumeric(entry.ordsEntry, entry.termsDictEntry.termsDictSize, null);
        return new BaseSortedDocValues(entry) {

            @Override
            public int ordValue() throws IOException {
                return (int) ords.longValue();
            }

            @Override
            public boolean advanceExact(int target) throws IOException {
                return ords.advanceExact(target);
            }

            @Override
            public int docID() {
                return ords.docID();
            }

            @Override
            public int nextDoc() throws IOException {
                return ords.nextDoc();
            }

            @Override
            public int advance(int target) throws IOException {
                return ords.advance(target);
            }

            @Override
            public long cost() {
                return ords.cost();
            }

            @Override
            public int docIDRunEnd() throws IOException {
                return ords.docIDRunEnd();
            }

            @Override
            public void intoBitSet(int upTo, FixedBitSet bitSet, int offset) throws IOException {
                ords.intoBitSet(upTo, bitSet, offset);
            }

            @Override
            public BlockLoader.Block tryRead(
                BlockLoader.BlockFactory factory,
                BlockLoader.Docs docs,
                int offset,
                boolean nullsFiltered,
                BlockDocValuesReader.ToDouble toDouble,
                boolean toInt,
                boolean binaryMultiValuedFormat
            ) throws IOException {
                assert toDouble == null;
                if (ords instanceof BaseDenseNumericValues denseOrds) {
                    var block = tryReadAHead(factory, docs, offset);
                    if (block != null) {
                        return block;
                    }
                    try (var builder = factory.singletonOrdinalsBuilder(this, docs.count() - offset, true)) {
                        BlockLoader.SingletonLongBuilder delegate = new SingletonLongToSingletonOrdinalDelegate(
                            builder,
                            entry.ordsEntry.blockSize
                        );
                        var result = denseOrds.tryRead(delegate, docs, offset);
                        if (result != null) {
                            return result;
                        }
                    }
                }
                return null;
            }

            public BlockLoader.Block tryReadAHead(BlockLoader.BlockFactory factory, BlockLoader.Docs docs, int offset) throws IOException {
                if (ords instanceof BaseDenseNumericValues denseOrds) {
                    if (entry.termsDictEntry.termsDictSize == 1) {
                        int lastDoc = docs.get(docs.count() - 1);
                        denseOrds.advanceExact(lastDoc);
                        return factory.constantBytes(BytesRef.deepCopyOf(lookupOrd(0)), docs.count() - offset);
                    }
                    if (valuesSorted == false) {
                        return null;
                    }
                    int firstDoc = docs.get(offset);
                    denseOrds.advanceExact(firstDoc);
                    int startValue = Math.toIntExact(denseOrds.longValue());
                    final int docCount = docs.count();
                    int lastDoc = docs.get(docCount - 1);
                    int lastValue = Math.toIntExact(denseOrds.lookAheadValueAt(lastDoc));
                    if (lastValue == startValue) {
                        BytesRef b = lookupOrd(Math.toIntExact(startValue));
                        return factory.constantBytes(BytesRef.deepCopyOf(b), docCount - offset);
                    }
                    var ordinalReader = denseOrds.sortedOrdinalReader();
                    if (ordinalReader != null) {
                        try (var builder = factory.singletonOrdinalsBuilder(this, docCount - offset, true)) {
                            int docIndex = offset;
                            while (docIndex < docCount) {
                                int ord = Math.toIntExact(ordinalReader.readValueAndAdvance(docs.get(docIndex)));
                                if (lastDoc < ordinalReader.rangeEndExclusive) {
                                    builder.appendOrds(ord, docCount - docIndex);
                                    break;
                                }
                                final int startIndex = docIndex;
                                while (docIndex < docCount && docs.get(docIndex) < ordinalReader.rangeEndExclusive) {
                                    ++docIndex;
                                }
                                builder.appendOrds(ord, docIndex - startIndex);
                            }
                            return builder.build();
                        }
                    }
                }
                return null;
            }
        };
    }

    public abstract class BaseSortedDocValues extends SortedDocValues
        implements
            BlockLoader.OptionalColumnAtATimeReader,
            PartitionedDocValues {

        final SortedEntry entry;
        final TermsEnum termsEnum;

        BaseSortedDocValues(SortedEntry entry) throws IOException {
            this.entry = entry;
            this.termsEnum = termsEnum();
        }

        @Override
        public int getValueCount() {
            return Math.toIntExact(entry.termsDictEntry.termsDictSize);
        }

        @Override
        public BytesRef lookupOrd(int ord) throws IOException {
            termsEnum.seekExact(ord);
            return termsEnum.term();
        }

        @Override
        public int lookupTerm(BytesRef key) throws IOException {
            TermsEnum.SeekStatus status = termsEnum.seekCeil(key);
            return switch (status) {
                case FOUND -> Math.toIntExact(termsEnum.ord());
                default -> Math.toIntExact(-1L - termsEnum.ord());
            };
        }

        @Override
        public TermsEnum termsEnum() throws IOException {
            return new TermsDict(entry.termsDictEntry, data, merging, formatConfig.termsBlockLz4Shift());
        }

        @Override
        public BlockLoader.Block tryRead(
            BlockLoader.BlockFactory factory,
            BlockLoader.Docs docs,
            int offset,
            boolean nullsFiltered,
            BlockDocValuesReader.ToDouble toDouble,
            boolean toInt,
            boolean binaryMultiValuedFormat
        ) throws IOException {
            return null;
        }

        public BlockLoader.Block tryReadAHead(BlockLoader.BlockFactory factory, BlockLoader.Docs docs, int offset) throws IOException {
            return null;
        }

        @Override
        public int prefixPartitionBits() {
            if (entry instanceof PrefixPartitionedEntry partition) {
                return partition.partitionNumBits;
            }
            return 0;
        }

        @Override
        public PrefixPartitions prefixPartitions(PrefixPartitions reused) throws IOException {
            if (entry instanceof PrefixPartitionedEntry partitioned) {
                var slice = data.slice("partitioning", partitioned.partitionStartPointer, partitioned.partitionDataLength);
                return PrefixedPartitionsReader.prefixPartitions(slice, reused);
            } else {
                return null;
            }
        }
    }

    public abstract static class BaseDenseNumericValues extends NumericDocValues
        implements
            BlockLoader.OptionalColumnAtATimeReader,
            BlockLoader.OptionalNumericRangeReader {
        private final int maxDoc;
        protected int doc = -1;

        BaseDenseNumericValues(int maxDoc) {
            this.maxDoc = maxDoc;
        }

        @Override
        public final int docID() {
            return doc;
        }

        @Override
        public final int nextDoc() throws IOException {
            return advance(doc + 1);
        }

        @Override
        public final int advance(int target) throws IOException {
            if (target >= maxDoc) {
                return doc = NO_MORE_DOCS;
            }
            return doc = target;
        }

        @Override
        public final boolean advanceExact(int target) {
            doc = target;
            return true;
        }

        @Override
        public final long cost() {
            return maxDoc;
        }

        public final int docIDRunEnd() throws IOException {
            return maxDoc;
        }

        @Override
        public final void intoBitSet(int upTo, FixedBitSet bitSet, int offset) throws IOException {
            assert offset <= doc;
            upTo = Math.min(upTo, maxDoc);
            if (upTo > doc) {
                bitSet.set(doc - offset, upTo - offset);
                advance(upTo);
            }
        }

        @Override
        public BlockLoader.Block tryRead(
            BlockLoader.BlockFactory factory,
            BlockLoader.Docs docs,
            int offset,
            boolean nullsFiltered,
            BlockDocValuesReader.ToDouble toDouble,
            boolean toInt,
            boolean binaryMultiValuedFormat
        ) throws IOException {
            return null;
        }

        abstract long lookAheadValueAt(int targetDoc) throws IOException;

        BlockLoader.Block tryRead(BlockLoader.SingletonLongBuilder builder, BlockLoader.Docs docs, int offset) throws IOException {
            return null;
        }

        @Nullable
        abstract SortedOrdinalReader sortedOrdinalReader();
    }

    abstract static class BaseSparseNumericValues extends NumericDocValues implements BlockLoader.OptionalColumnAtATimeReader {
        protected final IndexedDISI disi;

        BaseSparseNumericValues(IndexedDISI disi) {
            this.disi = disi;
        }

        @Override
        public final int advance(int target) throws IOException {
            return disi.advance(target);
        }

        @Override
        public final boolean advanceExact(int target) throws IOException {
            return disi.advanceExact(target);
        }

        @Override
        public final int nextDoc() throws IOException {
            return disi.nextDoc();
        }

        @Override
        public final int docID() {
            return disi.docID();
        }

        @Override
        public final long cost() {
            return disi.cost();
        }

        @Override
        public final int docIDRunEnd() throws IOException {
            return disi.docIDRunEnd();
        }

        @Override
        public final void intoBitSet(int upTo, FixedBitSet bitSet, int offset) throws IOException {
            disi.intoBitSet(upTo, bitSet, offset);
        }

        @Override
        public BlockLoader.Block tryRead(
            BlockLoader.BlockFactory factory,
            BlockLoader.Docs docs,
            int offset,
            boolean nullsFiltered,
            BlockDocValuesReader.ToDouble toDouble,
            boolean toInt,
            boolean binaryMultiValuedFormat
        ) throws IOException {
            return null;
        }
    }

    abstract static class BaseSortedSetDocValues extends SortedSetDocValues {

        final SortedSetEntry entry;
        final IndexInput data;
        final boolean merging;
        final int termsDictBlockLz4Shift;
        final TermsEnum termsEnum;

        BaseSortedSetDocValues(SortedSetEntry entry, IndexInput data, boolean merging, int termsDictBlockLz4Shift) throws IOException {
            this.entry = entry;
            this.data = data;
            this.merging = merging;
            this.termsDictBlockLz4Shift = termsDictBlockLz4Shift;
            this.termsEnum = termsEnum();
        }

        @Override
        public long getValueCount() {
            return entry.termsDictEntry.termsDictSize;
        }

        @Override
        public BytesRef lookupOrd(long ord) throws IOException {
            termsEnum.seekExact(ord);
            return termsEnum.term();
        }

        @Override
        public long lookupTerm(BytesRef key) throws IOException {
            TermsEnum.SeekStatus status = termsEnum.seekCeil(key);
            return switch (status) {
                case FOUND -> termsEnum.ord();
                default -> -1L - termsEnum.ord();
            };
        }

        @Override
        public TermsEnum termsEnum() throws IOException {
            return new TermsDict(entry.termsDictEntry, data, merging, termsDictBlockLz4Shift);
        }
    }

    private static class TermsDict extends BaseTermsEnum {
        static final int LZ4_DECOMPRESSOR_PADDING = 7;

        final TermsDictEntry entry;
        final LongValues blockAddresses;
        final IndexInput bytes;
        final long blockMask;
        final LongValues indexAddresses;
        final RandomAccessInput indexBytes;
        final BytesRef term;
        final int termsDictBlockLz4Shift;
        long ord = -1;

        BytesRef blockBuffer = null;
        ByteArrayDataInput blockInput = null;
        long currentCompressedBlockStart = -1;
        long currentCompressedBlockEnd = -1;

        TermsDict(TermsDictEntry entry, IndexInput data, boolean merging, int termsDictBlockLz4Shift) throws IOException {
            this.entry = entry;
            this.termsDictBlockLz4Shift = termsDictBlockLz4Shift;
            RandomAccessInput addressesSlice = data.randomAccessSlice(entry.termsAddressesOffset, entry.termsAddressesLength);
            blockAddresses = DirectMonotonicReader.getInstance(entry.termsAddressesMeta, addressesSlice, merging);
            bytes = data.slice("terms", entry.termsDataOffset, entry.termsDataLength);
            blockMask = (1L << termsDictBlockLz4Shift) - 1;
            RandomAccessInput indexAddressesSlice = data.randomAccessSlice(
                entry.termsIndexAddressesOffset,
                entry.termsIndexAddressesLength
            );
            indexAddresses = DirectMonotonicReader.getInstance(entry.termsIndexAddressesMeta, indexAddressesSlice, merging);
            indexBytes = data.randomAccessSlice(entry.termsIndexOffset, entry.termsIndexLength);
            term = new BytesRef(entry.maxTermLength);

            int bufferSize = entry.maxBlockLength + entry.maxTermLength + LZ4_DECOMPRESSOR_PADDING;
            blockBuffer = new BytesRef(new byte[bufferSize], 0, bufferSize);
        }

        @Override
        public BytesRef next() throws IOException {
            if (++ord >= entry.termsDictSize) {
                return null;
            }

            if ((ord & blockMask) == 0L) {
                decompressBlock();
            } else {
                DataInput input = blockInput;
                final int token = Byte.toUnsignedInt(input.readByte());
                int prefixLength = token & 0x0F;
                int suffixLength = 1 + (token >>> 4);
                if (prefixLength == 15) {
                    prefixLength += input.readVInt();
                }
                if (suffixLength == 16) {
                    suffixLength += input.readVInt();
                }
                term.length = prefixLength + suffixLength;
                input.readBytes(term.bytes, prefixLength, suffixLength);
            }
            return term;
        }

        @Override
        public void seekExact(long ord) throws IOException {
            if (ord < 0 || ord >= entry.termsDictSize) {
                throw new IndexOutOfBoundsException();
            }
            final long currentBlockIndex = this.ord >> termsDictBlockLz4Shift;
            final long blockIndex = ord >> termsDictBlockLz4Shift;
            if (ord < this.ord || blockIndex != currentBlockIndex) {
                final long blockAddress = blockAddresses.get(blockIndex);
                bytes.seek(blockAddress);
                this.ord = (blockIndex << termsDictBlockLz4Shift) - 1;
            }
            while (this.ord < ord) {
                next();
            }
        }

        private BytesRef getTermFromIndex(long index) throws IOException {
            assert index >= 0 && index <= (entry.termsDictSize - 1) >>> entry.termsDictIndexShift;
            final long start = indexAddresses.get(index);
            term.length = (int) (indexAddresses.get(index + 1) - start);
            indexBytes.readBytes(start, term.bytes, 0, term.length);
            return term;
        }

        private long seekTermsIndex(BytesRef text) throws IOException {
            long lo = 0L;
            long hi = (entry.termsDictSize - 1) >> entry.termsDictIndexShift;
            while (lo <= hi) {
                final long mid = (lo + hi) >>> 1;
                getTermFromIndex(mid);
                final int cmp = term.compareTo(text);
                if (cmp <= 0) {
                    lo = mid + 1;
                } else {
                    hi = mid - 1;
                }
            }

            assert hi < 0 || getTermFromIndex(hi).compareTo(text) <= 0;
            assert hi == ((entry.termsDictSize - 1) >> entry.termsDictIndexShift) || getTermFromIndex(hi + 1).compareTo(text) > 0;

            return hi;
        }

        private BytesRef getFirstTermFromBlock(long block) throws IOException {
            assert block >= 0 && block <= (entry.termsDictSize - 1) >>> termsDictBlockLz4Shift;
            final long blockAddress = blockAddresses.get(block);
            bytes.seek(blockAddress);
            term.length = bytes.readVInt();
            bytes.readBytes(term.bytes, 0, term.length);
            return term;
        }

        private long seekBlock(BytesRef text) throws IOException {
            long index = seekTermsIndex(text);
            if (index == -1L) {
                return -1L;
            }

            long ordLo = index << entry.termsDictIndexShift;
            long ordHi = Math.min(entry.termsDictSize, ordLo + (1L << entry.termsDictIndexShift)) - 1L;

            long blockLo = ordLo >>> termsDictBlockLz4Shift;
            long blockHi = ordHi >>> termsDictBlockLz4Shift;

            while (blockLo <= blockHi) {
                final long blockMid = (blockLo + blockHi) >>> 1;
                getFirstTermFromBlock(blockMid);
                final int cmp = term.compareTo(text);
                if (cmp <= 0) {
                    blockLo = blockMid + 1;
                } else {
                    blockHi = blockMid - 1;
                }
            }

            assert blockHi < 0 || getFirstTermFromBlock(blockHi).compareTo(text) <= 0;
            assert blockHi == ((entry.termsDictSize - 1) >>> termsDictBlockLz4Shift)
                || getFirstTermFromBlock(blockHi + 1).compareTo(text) > 0;

            return blockHi;
        }

        @Override
        public SeekStatus seekCeil(BytesRef text) throws IOException {
            final long block = seekBlock(text);
            if (block == -1) {
                if (entry.termsDictSize == 0) {
                    ord = 0;
                    return SeekStatus.END;
                } else {
                    seekExact(0L);
                    return SeekStatus.NOT_FOUND;
                }
            }
            final long blockAddress = blockAddresses.get(block);
            this.ord = block << termsDictBlockLz4Shift;
            bytes.seek(blockAddress);
            decompressBlock();

            while (true) {
                int cmp = term.compareTo(text);
                if (cmp == 0) {
                    return SeekStatus.FOUND;
                } else if (cmp > 0) {
                    return SeekStatus.NOT_FOUND;
                }
                if (next() == null) {
                    return SeekStatus.END;
                }
            }
        }

        private void decompressBlock() throws IOException {
            term.length = bytes.readVInt();
            bytes.readBytes(term.bytes, 0, term.length);
            long offset = bytes.getFilePointer();
            if (offset < entry.termsDataLength - 1) {
                if (currentCompressedBlockStart != offset) {
                    blockBuffer.offset = term.length;
                    blockBuffer.length = bytes.readVInt();
                    System.arraycopy(term.bytes, 0, blockBuffer.bytes, 0, blockBuffer.offset);
                    LZ4.decompress(bytes, blockBuffer.length, blockBuffer.bytes, blockBuffer.offset);
                    currentCompressedBlockStart = offset;
                    currentCompressedBlockEnd = bytes.getFilePointer();
                } else {
                    bytes.seek(currentCompressedBlockEnd);
                }

                blockInput = new ByteArrayDataInput(blockBuffer.bytes, blockBuffer.offset, blockBuffer.length);
            }
        }

        @Override
        public BytesRef term() throws IOException {
            return term;
        }

        @Override
        public long ord() throws IOException {
            return ord;
        }

        @Override
        public long totalTermFreq() throws IOException {
            return -1L;
        }

        @Override
        public PostingsEnum postings(PostingsEnum reuse, int flags) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public ImpactsEnum impacts(int flags) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public int docFreq() throws IOException {
            throw new UnsupportedOperationException();
        }
    }

    @Override
    public SortedNumericDocValues getSortedNumeric(FieldInfo field) throws IOException {
        SortedNumericEntry entry = sortedNumerics.get(field.number);
        return getSortedNumeric(entry, AbstractTSDBDocValuesConsumer.NO_MAX_ORD, field);
    }

    @Override
    public SortedSetDocValues getSortedSet(FieldInfo field) throws IOException {
        SortedSetEntry entry = sortedSets.get(field.number);
        if (entry.singleValueEntry != null) {
            return DocValues.singleton(getSorted(entry.singleValueEntry, field.number == primarySortFieldNumber));
        }

        SortedNumericEntry ordsEntry = entry.ordsEntry;
        final SortedNumericDocValues ords = getSortedNumeric(ordsEntry, entry.termsDictEntry.termsDictSize, null);
        return new BaseSortedSetDocValues(entry, data, merging, formatConfig.termsBlockLz4Shift()) {

            int i = 0;
            int count = 0;
            boolean set = false;

            @Override
            public long nextOrd() throws IOException {
                if (set == false) {
                    set = true;
                    i = 0;
                    count = ords.docValueCount();
                }
                assert i < count;
                i++;
                return ords.nextValue();
            }

            @Override
            public int docValueCount() {
                return ords.docValueCount();
            }

            @Override
            public boolean advanceExact(int target) throws IOException {
                set = false;
                return ords.advanceExact(target);
            }

            @Override
            public int docID() {
                return ords.docID();
            }

            @Override
            public int nextDoc() throws IOException {
                set = false;
                return ords.nextDoc();
            }

            @Override
            public int advance(int target) throws IOException {
                set = false;
                return ords.advance(target);
            }

            @Override
            public long cost() {
                return ords.cost();
            }

            @Override
            public int docIDRunEnd() throws IOException {
                return ords.docIDRunEnd();
            }

            @Override
            public void intoBitSet(int upTo, FixedBitSet bitSet, int offset) throws IOException {
                if (upTo > docID()) {
                    set = false;
                    ords.intoBitSet(upTo, bitSet, offset);
                }
            }
        };
    }

    @Override
    public DocValuesSkipper getSkipper(FieldInfo field) throws IOException {
        final DocValuesSkipperEntry entry = skippers.get(field.number);

        return new DocValuesSkipper() {
            final int[] minDocID = new int[formatConfig.skipIndexMaxLevel()];
            final int[] maxDocID = new int[formatConfig.skipIndexMaxLevel()];

            IndexInput input;

            {
                for (int i = 0; i < formatConfig.skipIndexMaxLevel(); i++) {
                    minDocID[i] = maxDocID[i] = -1;
                }
            }

            final long[] minValue = new long[formatConfig.skipIndexMaxLevel()];
            final long[] maxValue = new long[formatConfig.skipIndexMaxLevel()];
            final int[] docCount = new int[formatConfig.skipIndexMaxLevel()];
            int levels = 1;

            @Override
            public void advance(int target) throws IOException {
                if (target > entry.maxDocId) {
                    for (int i = 0; i < formatConfig.skipIndexMaxLevel(); i++) {
                        minDocID[i] = maxDocID[i] = DocIdSetIterator.NO_MORE_DOCS;
                    }
                } else {
                    if (input == null) {
                        if (skip == null) {
                            input = data.slice("doc value skipper", entry.offset, entry.length);
                        } else {
                            input = skip.slice("doc value skipper", entry.offset, entry.length);
                        }
                    }
                    assert target > maxDocID[0] : "target must be bigger than current interval";
                    while (true) {
                        levels = input.readByte();
                        assert levels <= formatConfig.skipIndexMaxLevel() && levels > 0 : "level out of range [" + levels + "]";
                        boolean valid = true;
                        for (int level = levels - 1; level >= 0; level--) {
                            if ((maxDocID[level] = input.readInt()) < target) {
                                input.skipBytes(skipIndexJumpLengthPerLevel[level]);
                                valid = false;
                                break;
                            }
                            minDocID[level] = input.readInt();
                            maxValue[level] = input.readLong();
                            minValue[level] = input.readLong();
                            docCount[level] = input.readInt();
                        }
                        if (valid) {
                            while (levels < formatConfig.skipIndexMaxLevel() && maxDocID[levels] >= target) {
                                levels++;
                            }
                            break;
                        }
                    }
                }
            }

            @Override
            public int numLevels() {
                return levels;
            }

            @Override
            public int minDocID(int level) {
                return minDocID[level];
            }

            @Override
            public int maxDocID(int level) {
                return maxDocID[level];
            }

            @Override
            public long minValue(int level) {
                return minValue[level];
            }

            @Override
            public long maxValue(int level) {
                return maxValue[level];
            }

            @Override
            public int docCount(int level) {
                return docCount[level];
            }

            @Override
            public long minValue() {
                return entry.minValue;
            }

            @Override
            public long maxValue() {
                return entry.maxValue;
            }

            @Override
            public int docCount() {
                return entry.docCount;
            }

            @Override
            public int maxValueCount() {
                return entry.maxValueCount;
            }
        };
    }

    @Override
    public void checkIntegrity() throws IOException {
        CodecUtil.checksumEntireFile(data);
        if (skip != null) {
            CodecUtil.checksumEntireFile(skip);
        }
    }

    @Override
    public void close() throws IOException {
        IOUtils.close(data, skip);
    }

    /**
     * Returns the field number of the primary sort field for the given segment,
     * if the field is sorted in ascending order. Returns {@code -1} if not found.
     */
    public static int primarySortFieldNumber(SegmentInfo segmentInfo, FieldInfos fieldInfos) {
        final var indexSort = segmentInfo.getIndexSort();
        if (indexSort != null && indexSort.getSort().length > 0) {
            SortField sortField = indexSort.getSort()[0];
            if (sortField.getReverse() == false) {
                FieldInfo fieldInfo = fieldInfos.fieldInfo(sortField.getField());
                if (fieldInfo != null) {
                    return fieldInfo.number;
                }
            }
        }
        return -1;
    }

    private void readFields(IndexInput meta, FieldInfos infos, int version, int numericBlockShift) throws IOException {
        for (int fieldNumber = meta.readInt(); fieldNumber != -1; fieldNumber = meta.readInt()) {
            FieldInfo info = infos.fieldInfo(fieldNumber);
            if (info == null) {
                throw new CorruptIndexException("Invalid field number: " + fieldNumber, meta);
            }
            byte type = meta.readByte();
            if (info.docValuesSkipIndexType() != DocValuesSkipIndexType.NONE) {
                skippers.put(info.number, readDocValueSkipperMeta(meta, version));
            }
            if (type == AbstractTSDBDocValuesConsumer.NUMERIC) {
                numerics.put(info.number, readNumeric(meta, numericBlockShift));
            } else if (type == AbstractTSDBDocValuesConsumer.BINARY) {
                binaries.put(info.number, readBinary(meta, version));
            } else if (type == AbstractTSDBDocValuesConsumer.SORTED) {
                sorted.put(
                    info.number,
                    readSorted(version, meta, numericBlockShift, formatConfig.termsBlockLz4Shift(), info.number == primarySortFieldNumber)
                );
            } else if (type == AbstractTSDBDocValuesConsumer.SORTED_SET) {
                sortedSets.put(
                    info.number,
                    readSortedSet(
                        version,
                        meta,
                        numericBlockShift,
                        formatConfig.termsBlockLz4Shift(),
                        info.number == primarySortFieldNumber
                    )
                );
            } else if (type == AbstractTSDBDocValuesConsumer.SORTED_NUMERIC) {
                sortedNumerics.put(info.number, readSortedNumeric(meta, numericBlockShift));
            } else {
                throw new CorruptIndexException("invalid type: " + type, meta);
            }
        }
    }

    private NumericEntry readNumeric(IndexInput meta, int numericBlockShift) throws IOException {
        final NumericEntry entry = new NumericEntry();
        readNumericField(meta, entry, numericBlockShift);
        return entry;
    }

    private static DocValuesSkipperEntry readDocValueSkipperMeta(IndexInput meta, int version) throws IOException {
        long offset = meta.readLong();
        long length = meta.readLong();
        long maxValue = meta.readLong();
        long minValue = meta.readLong();
        int docCount = meta.readInt();
        int maxDocID = meta.readInt();
        int maxValueCount = docCount == 0 ? 0 : -1;
        if (version >= TSDBDocValuesFormatConfig.VERSION_SKIPPER_MAX_VALUE_COUNT) {
            maxValueCount = meta.readInt();
        }

        return new DocValuesSkipperEntry(offset, length, minValue, maxValue, docCount, maxDocID, maxValueCount);
    }

    private void inferMaxValueCounts(FieldInfos fieldInfos) {
        for (IntObjectHashMap.IntObjectCursor<DocValuesSkipperEntry> cursor : skippers) {
            DocValuesSkipperEntry entry = cursor.value;
            if (entry.maxValueCount == -1 && entry.docCount != 0) {
                FieldInfo info = fieldInfos.fieldInfo(cursor.key);
                if (info != null) {
                    int inferred = -1;
                    switch (info.getDocValuesType()) {
                        case NUMERIC, SORTED -> inferred = 1;
                        case SORTED_NUMERIC -> {
                            SortedNumericEntry sne = sortedNumerics.get(cursor.key);
                            if (sne != null && sne.numValues == sne.numDocsWithField) {
                                inferred = 1;
                            }
                        }
                        case SORTED_SET -> {
                            SortedSetEntry sse = sortedSets.get(cursor.key);
                            if (sse != null) {
                                if (sse.singleValueEntry != null) {
                                    inferred = 1;
                                } else if (sse.ordsEntry != null && sse.ordsEntry.numValues == sse.ordsEntry.numDocsWithField) {
                                    inferred = 1;
                                }
                            }
                        }
                        case BINARY, NONE -> {
                        }
                    }
                    if (inferred != -1) {
                        skippers.put(
                            cursor.key,
                            new DocValuesSkipperEntry(
                                entry.offset,
                                entry.length,
                                entry.minValue,
                                entry.maxValue,
                                entry.docCount,
                                entry.maxDocId,
                                inferred
                            )
                        );
                    }
                }
            }
        }
    }

    private void readNumericField(IndexInput meta, NumericEntry entry, int numericBlockShift) throws IOException {
        var numericFieldReader = numericCodec.createReader(readContext);
        numericFieldReader.readFieldEntry(meta, entry, numericBlockShift);
    }

    private void readOrdinalField(IndexInput meta, NumericEntry entry, int numericBlockShift) throws IOException {
        var ordinalFieldReader = ordinalCodec.createReader(readContext);
        ordinalFieldReader.readFieldEntry(meta, entry, numericBlockShift);
    }

    private BinaryEntry readBinary(IndexInput meta, int version) throws IOException {
        final BinaryDVCompressionMode compression;
        if (version >= TSDBDocValuesFormatConfig.VERSION_BINARY_DV_COMPRESSION) {
            compression = BinaryDVCompressionMode.fromMode(meta.readByte());
        } else {
            compression = BinaryDVCompressionMode.NO_COMPRESS;
        }
        final BinaryEntry entry = new BinaryEntry(compression);

        entry.dataOffset = meta.readLong();
        entry.dataLength = meta.readLong();
        entry.docsWithFieldOffset = meta.readLong();
        entry.docsWithFieldLength = meta.readLong();
        entry.jumpTableEntryCount = meta.readShort();
        entry.denseRankPower = meta.readByte();
        entry.numDocsWithField = meta.readInt();
        entry.minLength = meta.readInt();
        entry.maxLength = meta.readInt();

        if (compression == BinaryDVCompressionMode.NO_COMPRESS) {
            if (entry.minLength < entry.maxLength) {
                entry.addressesOffset = meta.readLong();
                long numAddresses = entry.numDocsWithField + 1L;
                final int blockShift = meta.readVInt();
                entry.addressesMeta = DirectMonotonicReader.loadMeta(meta, numAddresses, blockShift);
                entry.addressesLength = meta.readLong();
            }
        } else {
            if (entry.numDocsWithField > 0) {
                entry.addressesOffset = meta.readLong();
                int numCompressedChunks = meta.readVInt();
                entry.maxUncompressedChunkSize = meta.readVInt();
                entry.maxNumDocsInAnyBlock = meta.readVInt();
                final int blockShift = meta.readVInt();

                entry.addressesMeta = DirectMonotonicReader.loadMeta(meta, numCompressedChunks + 1, blockShift);
                entry.addressesLength = meta.readLong();

                entry.docOffsetsOffset = meta.readLong();
                entry.docOffsetMeta = DirectMonotonicReader.loadMeta(meta, numCompressedChunks + 1, blockShift);
                entry.docOffsetLength = meta.readLong();

                entry.numCompressedBlocks = numCompressedChunks;
            }
        }
        return entry;
    }

    private SortedNumericEntry readSortedNumeric(IndexInput meta, int numericBlockShift) throws IOException {
        final SortedNumericEntry entry = new SortedNumericEntry();
        readSortedNumeric(meta, entry, numericBlockShift);
        return entry;
    }

    private void readSortedNumeric(IndexInput meta, SortedNumericEntry entry, int numericBlockShift) throws IOException {
        readNumericField(meta, entry, numericBlockShift);
        if (entry.numDocsWithField != entry.numValues) {
            entry.addressesOffset = meta.readLong();
            final int blockShift = meta.readVInt();
            entry.addressesMeta = DirectMonotonicReader.loadMeta(meta, entry.numDocsWithField + 1, blockShift);
            entry.addressesLength = meta.readLong();
        }
    }

    private void readSortedOrdinal(IndexInput meta, SortedNumericEntry entry, int numericBlockShift) throws IOException {
        readOrdinalField(meta, entry, numericBlockShift);
        if (entry.numDocsWithField != entry.numValues) {
            entry.addressesOffset = meta.readLong();
            final int blockShift = meta.readVInt();
            entry.addressesMeta = DirectMonotonicReader.loadMeta(meta, entry.numDocsWithField + 1, blockShift);
            entry.addressesLength = meta.readLong();
        }
    }

    private SortedEntry readSorted(int version, IndexInput meta, int numericBlockShift, int termsDictBlockLz4Shift, boolean primarySorted)
        throws IOException {
        SortedEntry entry = new SortedEntry();
        entry.ordsEntry = new NumericEntry();
        entry.termsDictEntry = new TermsDictEntry();
        if (version >= TSDBDocValuesFormatConfig.VERSION_PREFIX_PARTITIONS) {
            readTermDict(meta, entry.termsDictEntry, termsDictBlockLz4Shift);
            readOrdinalField(meta, entry.ordsEntry, numericBlockShift);
            if (primarySorted && meta.readByte() == 1) {
                PrefixPartitionedEntry partitioned = new PrefixPartitionedEntry();
                partitioned.ordsEntry = entry.ordsEntry;
                partitioned.termsDictEntry = entry.termsDictEntry;
                partitioned.partitionNumBits = meta.readByte();
                partitioned.partitionStartPointer = meta.readLong();
                partitioned.partitionDataLength = meta.readVLong();
                return partitioned;
            }
        } else {
            readOrdinalField(meta, entry.ordsEntry, numericBlockShift);
            readTermDict(meta, entry.termsDictEntry, termsDictBlockLz4Shift);
        }
        return entry;
    }

    private SortedSetEntry readSortedSet(
        int version,
        IndexInput meta,
        int numericBlockShift,
        int termsDictBlockLz4Shift,
        boolean primarySorted
    ) throws IOException {
        SortedSetEntry entry = new SortedSetEntry();
        byte multiValued = meta.readByte();
        switch (multiValued) {
            case 0: // singlevalued
                entry.singleValueEntry = readSorted(version, meta, numericBlockShift, termsDictBlockLz4Shift, primarySorted);
                return entry;
            case 1: // multivalued
                break;
            default:
                throw new CorruptIndexException("Invalid multiValued flag: " + multiValued, meta);
        }
        entry.ordsEntry = new SortedNumericEntry();
        readSortedOrdinal(meta, entry.ordsEntry, numericBlockShift);
        entry.termsDictEntry = new TermsDictEntry();
        readTermDict(meta, entry.termsDictEntry, termsDictBlockLz4Shift);
        return entry;
    }

    private static void readTermDict(IndexInput meta, TermsDictEntry entry, int termsDictBlockLz4Shift) throws IOException {
        entry.termsDictSize = meta.readVLong();
        final int blockShift = meta.readInt();
        final long addressesSize = (entry.termsDictSize + (1L << termsDictBlockLz4Shift) - 1) >>> termsDictBlockLz4Shift;
        entry.termsAddressesMeta = DirectMonotonicReader.loadMeta(meta, addressesSize, blockShift);
        entry.maxTermLength = meta.readInt();
        entry.maxBlockLength = meta.readInt();
        entry.termsDataOffset = meta.readLong();
        entry.termsDataLength = meta.readLong();
        entry.termsAddressesOffset = meta.readLong();
        entry.termsAddressesLength = meta.readLong();
        entry.termsDictIndexShift = meta.readInt();
        final long indexSize = (entry.termsDictSize + (1L << entry.termsDictIndexShift) - 1) >>> entry.termsDictIndexShift;
        entry.termsIndexAddressesMeta = DirectMonotonicReader.loadMeta(meta, 1 + indexSize, blockShift);
        entry.termsIndexOffset = meta.readLong();
        entry.termsIndexLength = meta.readLong();
        entry.termsIndexAddressesOffset = meta.readLong();
        entry.termsIndexAddressesLength = meta.readLong();
    }

    @FunctionalInterface
    private interface NumericValues {
        long advance(long index) throws IOException;
    }

    @FunctionalInterface
    private interface BlockDecoder {
        void decode(DataInput input, long[] values) throws IOException;
    }

    private BlockDecoder blockDecoder(NumericEntry entry, long maxOrd) {
        if (maxOrd != AbstractTSDBDocValuesConsumer.NO_MAX_ORD) {
            final int bitsPerOrd = PackedInts.bitsRequired(maxOrd - 1);
            var ordinalFieldReader = ordinalCodec.createReader(readContext);
            final OrdinalFieldReader.Decoder decoder = ordinalFieldReader.decoder(entry.blockSize);
            return (input, values) -> decoder.decodeOrdinals(input, values, bitsPerOrd);
        } else {
            var numericFieldReader = numericCodec.createReader(readContext);
            final NumericFieldReader.Decoder decoder = numericFieldReader.decoder(entry.pipelineDescriptor);
            final int blockSize = entry.blockSize;
            return (input, values) -> decoder.decodeBlock(input, values, blockSize);
        }
    }

    static final class SortedOrdinalReader {
        final long maxOrd;
        final DirectMonotonicReader startDocs;
        private long currentIndex = -1;
        long rangeEndExclusive = -1;

        SortedOrdinalReader(long maxOrd, DirectMonotonicReader startDocs) {
            this.maxOrd = maxOrd;
            this.startDocs = startDocs;
        }

        long readValueAndAdvance(int doc) {
            if (doc < rangeEndExclusive) {
                return currentIndex;
            }
            if (doc == rangeEndExclusive) {
                currentIndex++;
            } else {
                currentIndex = searchRange(doc);
            }
            rangeEndExclusive = startDocs.get(currentIndex + 1);
            return currentIndex;
        }

        private long searchRange(int doc) {
            long index = startDocs.binarySearch(currentIndex + 1, maxOrd, doc);
            if (index < 0) {
                index = -2 - index;
            }
            assert index < maxOrd : "invalid range " + index + " for doc " + doc + " in maxOrd " + maxOrd;
            return index;
        }

        long lookAheadValue(int targetDoc) {
            if (targetDoc < rangeEndExclusive) {
                return currentIndex;
            } else {
                return searchRange(targetDoc);
            }
        }
    }

    private NumericDocValues getNumeric(NumericEntry entry, long maxOrd, @Nullable FieldInfo fieldInfo) throws IOException {
        if (entry.docsWithFieldOffset == -2) {
            return DocValues.emptyNumeric();
        }

        if (maxOrd == 1) {
            if (entry.docsWithFieldOffset == -1) {
                return new BaseDenseNumericValues(maxDoc) {
                    @Override
                    public long longValue() {
                        return 0L;
                    }

                    @Override
                    long lookAheadValueAt(int targetDoc) throws IOException {
                        return 0L;
                    }

                    @Override
                    SortedOrdinalReader sortedOrdinalReader() {
                        return null;
                    }
                };
            } else {
                final IndexedDISI disi = new IndexedDISI(
                    data,
                    entry.docsWithFieldOffset,
                    entry.docsWithFieldLength,
                    entry.jumpTableEntryCount,
                    entry.denseRankPower,
                    entry.numValues
                );
                return new BaseSparseNumericValues(disi) {
                    @Override
                    public long longValue() throws IOException {
                        return 0L;
                    }
                };
            }
        } else if (entry.sortedOrdinals != null) {
            return getRangeEncodedNumericDocValues(entry, maxOrd);
        }

        final RandomAccessInput indexSlice = data.randomAccessSlice(entry.indexOffset, entry.indexLength);
        final DirectMonotonicReader indexReader = DirectMonotonicReader.getInstance(entry.indexMeta, indexSlice, merging);
        final IndexInput valuesData = data.slice("values", entry.valuesOffset, entry.valuesLength);

        final int numericBlockSize = entry.blockSize;
        final int numericBlockShift = Integer.numberOfTrailingZeros(numericBlockSize);
        final int numericBlockMask = numericBlockSize - 1;

        if (entry.docsWithFieldOffset == -1) {
            // dense
            return new BaseDenseNumericValues(maxDoc) {
                private final BlockDecoder decoder = blockDecoder(entry, maxOrd);
                private long currentBlockIndex = -1;
                private final long[] currentBlock = new long[numericBlockSize];
                private long lookaheadBlockIndex = -1;
                private long[] lookaheadBlock;
                private IndexInput lookaheadData = null;

                @Override
                public long longValue() throws IOException {
                    final int index = doc;
                    final int blockIndex = index >>> numericBlockShift;
                    final int blockInIndex = index & numericBlockMask;
                    if (blockIndex == currentBlockIndex) {
                        return currentBlock[blockInIndex];
                    }
                    if (blockIndex == lookaheadBlockIndex) {
                        return lookaheadBlock[blockInIndex];
                    }
                    assert blockIndex > currentBlockIndex : blockIndex + " < " + currentBlockIndex;
                    if (currentBlockIndex + 1 != blockIndex) {
                        valuesData.seek(indexReader.get(blockIndex));
                    }
                    currentBlockIndex = blockIndex;
                    decoder.decode(valuesData, currentBlock);
                    return currentBlock[blockInIndex];
                }

                @Override
                public BlockLoader.Block tryRead(
                    BlockLoader.BlockFactory factory,
                    BlockLoader.Docs docs,
                    int offset,
                    boolean nullsFiltered,
                    BlockDocValuesReader.ToDouble toDouble,
                    boolean toInt,
                    boolean binaryMultiValuedFormat
                ) throws IOException {
                    try (var singletonLongBuilder = singletonLongBuilder(factory, toDouble, docs.count() - offset, toInt)) {
                        return tryRead(singletonLongBuilder, docs, offset);
                    }
                }

                @Override
                BlockLoader.Block tryRead(BlockLoader.SingletonLongBuilder builder, BlockLoader.Docs docs, int offset) throws IOException {
                    if (docs.mayContainDuplicates()) {
                        return null;
                    }
                    final int docsCount = docs.count();
                    doc = docs.get(docsCount - 1);
                    for (int i = offset; i < docsCount;) {
                        int index = docs.get(i);
                        final int blockIndex = index >>> numericBlockShift;
                        final int blockInIndex = index & numericBlockMask;
                        if (blockIndex != currentBlockIndex) {
                            assert blockIndex > currentBlockIndex : blockIndex + " < " + currentBlockIndex;
                            if (currentBlockIndex + 1 != blockIndex) {
                                valuesData.seek(indexReader.get(blockIndex));
                            }
                            currentBlockIndex = blockIndex;
                            decoder.decode(valuesData, currentBlock);
                        }

                        int length = 1;
                        int remainingBlockLength = Math.min(numericBlockSize - blockInIndex, docsCount - i);
                        for (int newLength = remainingBlockLength; newLength > 1; newLength = newLength >> 1) {
                            int lastIndex = i + newLength - 1;
                            if (isDense(index, docs.get(lastIndex), newLength)) {
                                length = newLength;
                                break;
                            }
                        }
                        builder.appendLongs(currentBlock, blockInIndex, length);
                        i += length;
                    }
                    return builder.build();
                }

                @Override
                long lookAheadValueAt(int targetDoc) throws IOException {
                    final int blockIndex = targetDoc >>> numericBlockShift;
                    final int valueIndex = targetDoc & numericBlockMask;
                    if (blockIndex == currentBlockIndex) {
                        return currentBlock[valueIndex];
                    }
                    if (lookaheadBlockIndex != blockIndex) {
                        if (lookaheadBlock == null) {
                            lookaheadBlock = new long[numericBlockSize];
                            lookaheadData = data.slice("look_ahead_values", entry.valuesOffset, entry.valuesLength);
                        }
                        if (lookaheadBlockIndex + 1 != blockIndex) {
                            lookaheadData.seek(indexReader.get(blockIndex));
                        }
                        decoder.decode(lookaheadData, lookaheadBlock);
                        lookaheadBlockIndex = blockIndex;
                    }
                    return lookaheadBlock[valueIndex];
                }

                @Override
                public DocIdSetIterator tryRangeIterator(long lowerValue, long upperValue) throws IOException {
                    // Real TwoPhaseIterator over a cheap DocIdSetIterator.all approximation. This is what
                    // lets ESQL run DataPartitioning.DOC here: a plain DocIdSetIterator matches eagerly in
                    // advance(min), so a slice [min, max) with no match scans past max (and every slice
                    // repeats it). The two-phase form makes advance(min) O(1) and confines block decoding
                    // to intoBitSet(upTo), which ConstantScoreBulkScorer caps at the slice's max.
                    //
                    // intoBitSet / docIDRunEnd still carry the dense bulk scan (bitSet.set(start, end+1)
                    // for all-in-range skipper blocks). They are overridable since Lucene 10.5
                    // (apache/lucene#16177); consumers reach them via TwoPhaseIterator.unwrap, not the
                    // returned DISI wrapper. matches()/intoBitSet are skipper-aware; partial-overlap
                    // blocks decode and scan the SIMD bitmask.
                    // Use a fresh instance: it shares decode state with the outer reader.

                    DocValuesSkipper skipper = fieldInfo != null && fieldInfo.docValuesSkipIndexType() == DocValuesSkipIndexType.RANGE
                        ? getSkipper(fieldInfo)
                        : null;
                    final FixedBitSet matches = new FixedBitSet(numericBlockSize);
                    final DocIdSetIterator approximation = DocIdSetIterator.all(maxDoc);
                    if (skipper != null) {
                        // Skips at two levels: skipper blocks (coarse min/max range check), then
                        // per-numeric-block SIMD bitmasks via inRangeBitmask.
                        return TwoPhaseIterator.asDocIdSetIterator(new TwoPhaseIterator(approximation) {
                            @Override
                            public boolean matches() throws IOException {
                                final int doc = approximation.docID();
                                if (skipper.maxDocID(0) < doc) {
                                    skipper.advance(doc);
                                }
                                long minVal = skipper.minValue(0);
                                long maxVal = skipper.maxValue(0);
                                if (lowerValue <= minVal && maxVal <= upperValue) {
                                    // Entire skipper block is in range: confirm without decoding values.
                                    return true;
                                }
                                if (minVal > upperValue || maxVal < lowerValue) {
                                    // No overlap.
                                    return false;
                                }
                                // Partial overlap: decode the numeric block and check the SIMD bitmask bit.
                                loadRangeBitmaskForBlock(doc >>> numericBlockShift, lowerValue, upperValue, matches);
                                return matches.get(doc & numericBlockMask);
                            }

                            @Override
                            public float matchCost() {
                                return RANGE_ITERATOR_MATCH_COST;
                            }

                            @Override
                            public void intoBitSet(int upTo, FixedBitSet bitSet, int offset) throws IOException {
                                int doc = approximation.docID();
                                while (doc < upTo) {
                                    if (skipper.maxDocID(0) < doc) {
                                        skipper.advance(doc);
                                        if (skipper.maxDocID(0) == NO_MORE_DOCS) {
                                            approximation.advance(maxDoc);
                                            return;
                                        }
                                    }
                                    int firstDocInSkipper = Math.max(doc, skipper.minDocID(0));
                                    // Cap at upTo-1 so we never write bits past the caller's window.
                                    int lastDocInSkipper = Math.min(skipper.maxDocID(0), upTo - 1);
                                    long minVal = skipper.minValue(0);
                                    long maxVal = skipper.maxValue(0);
                                    if (lowerValue <= minVal && maxVal <= upperValue) {
                                        // All docs in the (clipped) skipper block match, bulk set them without decoding.
                                        bitSet.set(firstDocInSkipper - offset, lastDocInSkipper + 1 - offset);
                                    } else if (minVal <= upperValue && lowerValue <= maxVal) {
                                        // Partial overlap: scan SIMD bitmask for each numeric block.
                                        int firstBlock = firstDocInSkipper >>> numericBlockShift;
                                        int lastBlock = lastDocInSkipper >>> numericBlockShift;
                                        for (int blockId = firstBlock; blockId <= lastBlock; blockId++) {
                                            loadRangeBitmaskForBlock(blockId, lowerValue, upperValue, matches);
                                            int firstInBlock = blockId == firstBlock ? firstDocInSkipper & numericBlockMask : 0;
                                            int lastInBlock = blockId == lastBlock ? lastDocInSkipper & numericBlockMask : numericBlockMask;
                                            // shift by blockBase to matching bitSet's coordinate space
                                            int blockBase = (blockId << numericBlockShift) - offset;
                                            matches.forEach(firstInBlock, lastInBlock + 1, blockBase, bitSet::set);
                                        }
                                    }
                                    // No overlap, all-in-range, or partial overlap: advance past this skipper block.
                                    doc = lastDocInSkipper + 1;
                                }
                                // Honor the intoBitSet contract: leave the approximation at the first
                                // candidate doc >= upTo (matches() then filters non-matching candidates).
                                if (approximation.docID() < upTo) {
                                    approximation.advance(upTo);
                                }
                            }

                            @Override
                            public int docIDRunEnd() throws IOException {
                                final int doc = approximation.docID();
                                if (skipper.maxDocID(0) < doc) {
                                    skipper.advance(doc);
                                }
                                // Whole skipper block in range: every doc matches, so the run extends to the block end.
                                if (lowerValue <= skipper.minValue(0) && skipper.maxValue(0) <= upperValue) {
                                    return skipper.maxDocID(0) + 1;
                                }
                                // Partial block: only extend a run from a confirmed match. docIDRunEnd() may be called
                                // on an unconfirmed candidate (e.g. DenseConjunctionBulkScorer), so never claim a run
                                // from a non-matching doc.
                                int blockId = doc >>> numericBlockShift;
                                if (currentBlockIndex == blockId && matches.get(doc & numericBlockMask)) {
                                    int firstClearBit = nextClearBit((doc & numericBlockMask) + 1, matches);
                                    return Math.min((blockId << numericBlockShift) + firstClearBit, maxDoc);
                                }
                                return doc;
                            }
                        });
                    } else {
                        // No skipper: scan SIMD bitmasks for every numeric block directly.
                        return TwoPhaseIterator.asDocIdSetIterator(new TwoPhaseIterator(approximation) {
                            @Override
                            public boolean matches() throws IOException {
                                final int doc = approximation.docID();
                                loadRangeBitmaskForBlock(doc >>> numericBlockShift, lowerValue, upperValue, matches);
                                return matches.get(doc & numericBlockMask);
                            }

                            @Override
                            public float matchCost() {
                                return RANGE_ITERATOR_MATCH_COST;
                            }

                            @Override
                            public void intoBitSet(int upTo, FixedBitSet bitSet, int offset) throws IOException {
                                int doc = approximation.docID();
                                upTo = Math.min(upTo, maxDoc);
                                if (doc >= upTo) {
                                    return;
                                }
                                int firstBlock = doc >>> numericBlockShift;
                                int lastBlock = (upTo - 1) >>> numericBlockShift;
                                for (int blockId = firstBlock; blockId <= lastBlock; blockId++) {
                                    loadRangeBitmaskForBlock(blockId, lowerValue, upperValue, matches);
                                    int firstInBlock = blockId == firstBlock ? doc & numericBlockMask : 0;
                                    int lastInBlock = blockId == lastBlock ? (upTo - 1) & numericBlockMask : numericBlockMask;
                                    // shift by blockBase to matching bitSet's coordinate space
                                    int blockBase = (blockId << numericBlockShift) - offset;
                                    matches.forEach(firstInBlock, lastInBlock + 1, blockBase, bitSet::set);
                                }
                                // Honor the intoBitSet contract: leave the approximation at the first
                                // candidate doc >= upTo (matches() then filters non-matching candidates).
                                approximation.advance(upTo);
                            }

                            @Override
                            public int docIDRunEnd() throws IOException {
                                final int doc = approximation.docID();
                                // Only extend a run from a confirmed match. docIDRunEnd() may be called on an
                                // unconfirmed candidate (e.g. DenseConjunctionBulkScorer), so never claim a run
                                // from a non-matching doc.
                                int blockId = doc >>> numericBlockShift;
                                if (currentBlockIndex == blockId && matches.get(doc & numericBlockMask)) {
                                    int firstClearBit = nextClearBit((doc & numericBlockMask) + 1, matches);
                                    return Math.min((blockId << numericBlockShift) + firstClearBit, maxDoc);
                                }
                                return doc;
                            }
                        });
                    }
                }

                private void loadRangeBitmaskForBlock(int blockIndex, long lowerValue, long upperValue, FixedBitSet matches)
                    throws IOException {
                    if (blockIndex != currentBlockIndex) {
                        // load block
                        assert blockIndex > currentBlockIndex : blockIndex + " < " + currentBlockIndex;
                        if (currentBlockIndex + 1 != blockIndex) {
                            valuesData.seek(indexReader.get(blockIndex));
                        }
                        decoder.decode(valuesData, currentBlock);
                        currentBlockIndex = blockIndex;

                        // run query and set matches bitset
                        matches.clear();
                        ESVectorUtil.inRangeBitmask(currentBlock, lowerValue, upperValue, matches.getBits());
                    }
                }

                // Equivalent to FixedBitSet.nextSetBit but for 0-bits (clear bits).
                private static int nextClearBit(int from, FixedBitSet matches) {
                    long[] bits = matches.getBits();
                    int wordIdx = from >>> 6;
                    if (wordIdx >= bits.length) {
                        return matches.length();
                    }
                    // Invert and right-shift to isolate clear bits at or after `from`.
                    long word = ~bits[wordIdx] >>> (from & 63);
                    if (word != 0) {
                        return from + Long.numberOfTrailingZeros(word);
                    }
                    for (int i = wordIdx + 1; i < bits.length; i++) {
                        word = ~bits[i];
                        if (word != 0) {
                            return (i << 6) + Long.numberOfTrailingZeros(word);
                        }
                    }
                    return matches.length();
                }

                @Override
                SortedOrdinalReader sortedOrdinalReader() {
                    return null;
                }
            };
        } else {
            final IndexedDISI disi = new IndexedDISI(
                data,
                entry.docsWithFieldOffset,
                entry.docsWithFieldLength,
                entry.jumpTableEntryCount,
                entry.denseRankPower,
                entry.numValues
            );
            return new BaseSparseNumericValues(disi) {
                private final BlockDecoder decoder = blockDecoder(entry, maxOrd);
                private IndexedDISI lookAheadDISI;
                private long currentBlockIndex = -1;
                private final long[] currentBlock = new long[numericBlockSize];

                @Override
                public long longValue() throws IOException {
                    final int index = disi.index();
                    final int blockIndex = index >>> numericBlockShift;
                    final int blockInIndex = index & numericBlockMask;
                    if (blockIndex != currentBlockIndex) {
                        assert blockIndex > currentBlockIndex : blockIndex + "<=" + currentBlockIndex;
                        if (currentBlockIndex + 1 != blockIndex) {
                            valuesData.seek(indexReader.get(blockIndex));
                        }
                        currentBlockIndex = blockIndex;
                        decoder.decode(valuesData, currentBlock);
                    }
                    return currentBlock[blockInIndex];
                }

                @Override
                public BlockLoader.Block tryRead(
                    BlockLoader.BlockFactory factory,
                    BlockLoader.Docs docs,
                    int offset,
                    boolean nullsFiltered,
                    BlockDocValuesReader.ToDouble toDouble,
                    boolean toInt,
                    boolean binaryMultiValuedFormat
                ) throws IOException {
                    if (nullsFiltered == false) {
                        return null;
                    }
                    final int firstDoc = docs.get(offset);
                    if (disi.advanceExact(firstDoc) == false) {
                        assert false : "nullsFiltered is true, but doc [" + firstDoc + "] has no value";
                        throw new IllegalStateException("nullsFiltered is true, but doc [" + firstDoc + "] has no value");
                    }
                    if (lookAheadDISI == null) {
                        lookAheadDISI = new IndexedDISI(
                            data,
                            entry.docsWithFieldOffset,
                            entry.docsWithFieldLength,
                            entry.jumpTableEntryCount,
                            entry.denseRankPower,
                            entry.numValues
                        );
                    }
                    final int lastDoc = docs.get(docs.count() - 1);
                    if (lookAheadDISI.advanceExact(lastDoc) == false) {
                        assert false : "nullsFiltered is true, but doc [" + lastDoc + "] has no value";
                        throw new IllegalStateException("nullsFiltered is true, but doc [" + lastDoc + "] has no value");
                    }
                    final int firstIndex = disi.index();
                    final int lastIndex = lookAheadDISI.index();
                    final int valueCount = lastIndex - firstIndex + 1;
                    if (valueCount != docs.count()) {
                        return null;
                    }
                    if (Assertions.ENABLED) {
                        for (int i = 0; i < docs.count(); i++) {
                            final int doc = docs.get(i + offset);
                            assert disi.advanceExact(doc) : "nullsFiltered is true, but doc [" + doc + "] has no value";
                            assert disi.index() == firstIndex + i : "unexpected disi index " + (firstIndex + i) + "!=" + disi.index();
                        }
                    }
                    try (var singletonLongBuilder = singletonLongBuilder(factory, toDouble, valueCount, toInt)) {
                        for (int i = 0; i < valueCount;) {
                            final int index = firstIndex + i;
                            final int blockIndex = index >>> numericBlockShift;
                            final int blockStartIndex = index & numericBlockMask;
                            if (blockIndex != currentBlockIndex) {
                                assert blockIndex > currentBlockIndex : blockIndex + "<=" + currentBlockIndex;
                                if (currentBlockIndex + 1 != blockIndex) {
                                    valuesData.seek(indexReader.get(blockIndex));
                                }
                                currentBlockIndex = blockIndex;
                                decoder.decode(valuesData, currentBlock);
                            }
                            final int count = Math.min(numericBlockSize - blockStartIndex, valueCount - i);
                            singletonLongBuilder.appendLongs(currentBlock, blockStartIndex, count);
                            i += count;
                        }
                        return singletonLongBuilder.build();
                    }
                }
            };
        }
    }

    private static boolean isDense(int firstDocId, int lastDocId, int length) {
        return lastDocId - firstDocId == length - 1;
    }

    private NumericDocValues getRangeEncodedNumericDocValues(NumericEntry entry, long maxOrd) throws IOException {
        final var ordinalsReader = new SortedOrdinalReader(
            maxOrd,
            DirectMonotonicReader.getInstance(entry.sortedOrdinals, data.randomAccessSlice(entry.valuesOffset, entry.valuesLength), true)
        );
        if (entry.docsWithFieldOffset == -1) {
            return new BaseDenseNumericValues(maxDoc) {
                @Override
                long lookAheadValueAt(int targetDoc) {
                    return ordinalsReader.lookAheadValue(targetDoc);
                }

                @Override
                public long longValue() {
                    return ordinalsReader.readValueAndAdvance(doc);
                }

                @Override
                SortedOrdinalReader sortedOrdinalReader() {
                    return ordinalsReader;
                }
            };
        } else {
            final var disi = new IndexedDISI(
                data,
                entry.docsWithFieldOffset,
                entry.docsWithFieldLength,
                entry.jumpTableEntryCount,
                entry.denseRankPower,
                entry.numValues
            );
            return new BaseSparseNumericValues(disi) {
                @Override
                public long longValue() {
                    return ordinalsReader.readValueAndAdvance(disi.docID());
                }
            };
        }
    }

    private NumericValues getValues(NumericEntry entry, final long maxOrd) throws IOException {
        assert entry.numValues > 0;
        final RandomAccessInput indexSlice = data.randomAccessSlice(entry.indexOffset, entry.indexLength);
        final DirectMonotonicReader indexReader = DirectMonotonicReader.getInstance(entry.indexMeta, indexSlice, merging);

        final IndexInput valuesData = data.slice("values", entry.valuesOffset, entry.valuesLength);

        final int numericBlockSize = entry.blockSize;
        final int numericBlockShift = Integer.numberOfTrailingZeros(numericBlockSize);
        final int numericBlockMask = numericBlockSize - 1;
        final long[] currentBlockIndex = { -1 };
        final long[] currentBlock = new long[numericBlockSize];
        final BlockDecoder decoder = blockDecoder(entry, maxOrd);
        return index -> {
            final long blockIndex = index >>> numericBlockShift;
            final int blockInIndex = (int) (index & numericBlockMask);
            if (blockIndex != currentBlockIndex[0]) {
                if (currentBlockIndex[0] + 1 != blockIndex) {
                    valuesData.seek(indexReader.get(blockIndex));
                }
                currentBlockIndex[0] = blockIndex;
                decoder.decode(valuesData, currentBlock);
            }
            return currentBlock[blockInIndex];
        };
    }

    private SortedNumericDocValues getSortedNumeric(SortedNumericEntry entry, long maxOrd, @Nullable FieldInfo fieldInfo)
        throws IOException {
        if (entry.numValues == entry.numDocsWithField) {
            return DocValues.singleton(getNumeric(entry, maxOrd, fieldInfo));
        }

        final RandomAccessInput addressesInput = data.randomAccessSlice(entry.addressesOffset, entry.addressesLength);
        final LongValues addresses = DirectMonotonicReader.getInstance(entry.addressesMeta, addressesInput, merging);

        assert entry.sortedOrdinals == null : "encoded ordinal range supports only one value per document";
        if (entry.sortedOrdinals != null) {
            NumericDocValues values = getRangeEncodedNumericDocValues(entry, maxOrd);
            return DocValues.singleton(values);
        }
        final NumericValues values = getValues(entry, maxOrd);

        if (entry.docsWithFieldOffset == -1) {
            // dense
            return new SortedNumericDocValues() {

                int doc = -1;
                long start, end;
                int count;

                @Override
                public int nextDoc() throws IOException {
                    return advance(doc + 1);
                }

                @Override
                public int docID() {
                    return doc;
                }

                @Override
                public long cost() {
                    return maxDoc;
                }

                @Override
                public int advance(int target) throws IOException {
                    if (target >= maxDoc) {
                        return doc = NO_MORE_DOCS;
                    }
                    start = addresses.get(target);
                    end = addresses.get(target + 1L);
                    count = (int) (end - start);
                    return doc = target;
                }

                @Override
                public boolean advanceExact(int target) throws IOException {
                    start = addresses.get(target);
                    end = addresses.get(target + 1L);
                    count = (int) (end - start);
                    doc = target;
                    return true;
                }

                @Override
                public long nextValue() throws IOException {
                    return values.advance(start++);
                }

                @Override
                public int docValueCount() {
                    return count;
                }

                @Override
                public int docIDRunEnd() {
                    return maxDoc;
                }

                @Override
                public void intoBitSet(int upTo, FixedBitSet bitSet, int offset) throws IOException {
                    assert offset <= doc;
                    upTo = Math.min(upTo, maxDoc);
                    if (upTo > doc) {
                        bitSet.set(doc - offset, upTo - offset);
                        advance(upTo);
                    }
                }
            };
        } else {
            // sparse
            final IndexedDISI disi = new IndexedDISI(
                data,
                entry.docsWithFieldOffset,
                entry.docsWithFieldLength,
                entry.jumpTableEntryCount,
                entry.denseRankPower,
                entry.numDocsWithField
            );
            return new SortedNumericDocValues() {

                boolean set;
                long start, end;
                int count;

                @Override
                public int nextDoc() throws IOException {
                    set = false;
                    return disi.nextDoc();
                }

                @Override
                public int docID() {
                    return disi.docID();
                }

                @Override
                public long cost() {
                    return disi.cost();
                }

                @Override
                public int advance(int target) throws IOException {
                    set = false;
                    return disi.advance(target);
                }

                @Override
                public boolean advanceExact(int target) throws IOException {
                    set = false;
                    return disi.advanceExact(target);
                }

                @Override
                public long nextValue() throws IOException {
                    set();
                    return values.advance(start++);
                }

                @Override
                public int docValueCount() {
                    set();
                    return count;
                }

                @Override
                public int docIDRunEnd() throws IOException {
                    return disi.docIDRunEnd();
                }

                @Override
                public void intoBitSet(int upTo, FixedBitSet bitSet, int offset) throws IOException {
                    disi.intoBitSet(upTo, bitSet, offset);
                }

                private void set() {
                    if (set == false) {
                        final int index = disi.index();
                        start = addresses.get(index);
                        end = addresses.get(index + 1L);
                        count = (int) (end - start);
                        set = true;
                    }
                }
            };
        }
    }

    private record DocValuesSkipperEntry(
        long offset,
        long length,
        long minValue,
        long maxValue,
        int docCount,
        int maxDocId,
        int maxValueCount
    ) {}

    public static class NumericEntry {
        public long docsWithFieldOffset;
        public long docsWithFieldLength;
        public short jumpTableEntryCount;
        public byte denseRankPower;
        public long numValues;
        public int numDocsWithField;
        public long indexOffset;
        public long indexLength;
        public DirectMonotonicReader.Meta indexMeta;
        public long valuesOffset;
        public long valuesLength;
        public DirectMonotonicReader.Meta sortedOrdinals;
        public PipelineDescriptor pipelineDescriptor;
        // NOTE: per-field block size. Equals pipelineDescriptor.blockSize() when present
        // (ES95 pipeline-encoded entries); otherwise the format-level default read from
        // the numeric block shift header byte, used by ES819 entries and ordinal-stream
        // entries that have no pipeline descriptor.
        public int blockSize;
    }

    static class BinaryEntry {
        final BinaryDVCompressionMode compression;

        long dataOffset;
        long dataLength;
        long docsWithFieldOffset;
        long docsWithFieldLength;
        short jumpTableEntryCount;
        byte denseRankPower;
        int numDocsWithField;
        int minLength;
        int maxLength;
        long addressesOffset;
        long addressesLength;
        long docOffsetsOffset;
        long docOffsetLength;
        int maxUncompressedChunkSize;
        int maxNumDocsInAnyBlock;
        int numCompressedBlocks;
        DirectMonotonicReader.Meta addressesMeta;
        DirectMonotonicReader.Meta docOffsetMeta;

        BinaryEntry(BinaryDVCompressionMode compression) {
            this.compression = compression;
        }
    }

    public static class SortedNumericEntry extends NumericEntry {
        public DirectMonotonicReader.Meta addressesMeta;
        public long addressesOffset;
        public long addressesLength;
    }

    static class SortedEntry {
        NumericEntry ordsEntry;
        TermsDictEntry termsDictEntry;
    }

    static class PrefixPartitionedEntry extends SortedEntry {
        int partitionNumBits;
        long partitionStartPointer;
        long partitionDataLength;
    }

    static class SortedSetEntry {
        SortedEntry singleValueEntry;
        SortedNumericEntry ordsEntry;
        TermsDictEntry termsDictEntry;
    }

    static class TermsDictEntry {
        long termsDictSize;
        DirectMonotonicReader.Meta termsAddressesMeta;
        int maxTermLength;
        long termsDataOffset;
        long termsDataLength;
        long termsAddressesOffset;
        long termsAddressesLength;
        int termsDictIndexShift;
        DirectMonotonicReader.Meta termsIndexAddressesMeta;
        long termsIndexOffset;
        long termsIndexLength;
        long termsIndexAddressesOffset;
        long termsIndexAddressesLength;

        int maxBlockLength;
    }

    static final class SingletonLongToSingletonOrdinalDelegate implements BlockLoader.SingletonLongBuilder {
        private final BlockLoader.SingletonOrdinalsBuilder builder;
        private final int[] buffer;

        SingletonLongToSingletonOrdinalDelegate(BlockLoader.SingletonOrdinalsBuilder builder, int bufferSize) {
            this.builder = builder;
            this.buffer = new int[bufferSize];
        }

        @Override
        public BlockLoader.SingletonLongBuilder appendLong(long value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public BlockLoader.SingletonLongBuilder appendLongs(long[] values, int from, int length) {
            assert length <= buffer.length;
            int minOrd = Integer.MAX_VALUE;
            int maxOrd = Integer.MIN_VALUE;
            int counter = 0;
            int end = from + length;
            for (int j = from; j < end; j++) {
                int ord = Math.toIntExact(values[j]);
                buffer[counter++] = ord;
                minOrd = Math.min(minOrd, ord);
                maxOrd = Math.max(maxOrd, ord);
            }
            assert counter == length;
            builder.appendOrds(buffer, 0, length, minOrd, maxOrd);
            return this;
        }

        @Override
        public BlockLoader.Block build() {
            return builder.build();
        }

        @Override
        public BlockLoader.Builder appendNull() {
            throw new UnsupportedOperationException();
        }

        @Override
        public BlockLoader.Builder beginPositionEntry() {
            throw new UnsupportedOperationException();
        }

        @Override
        public BlockLoader.Builder endPositionEntry() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {}
    }

    static BlockLoader.SingletonLongBuilder singletonLongBuilder(
        BlockLoader.BlockFactory factory,
        BlockDocValuesReader.ToDouble toDouble,
        int valueCount,
        boolean toInt
    ) {
        assert (toInt && toDouble != null) == false;

        if (toDouble != null) {
            return new SingletonLongToDoubleDelegate(factory.singletonDoubles(valueCount), toDouble);
        } else if (toInt) {
            return new SingletonLongtoIntDelegate(factory.singletonInts(valueCount));
        } else {
            return factory.singletonLongs(valueCount);
        }
    }

    static final class SingletonLongToDoubleDelegate implements BlockLoader.SingletonLongBuilder {
        private final BlockLoader.SingletonDoubleBuilder doubleBuilder;
        private final BlockDocValuesReader.ToDouble toDouble;

        SingletonLongToDoubleDelegate(BlockLoader.SingletonDoubleBuilder doubleBuilder, BlockDocValuesReader.ToDouble toDouble) {
            this.doubleBuilder = doubleBuilder;
            this.toDouble = toDouble;
        }

        @Override
        public BlockLoader.SingletonLongBuilder appendLong(long value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public BlockLoader.SingletonLongBuilder appendLongs(long[] values, int from, int length) {
            doubleBuilder.appendLongs(toDouble, values, from, length);
            return this;
        }

        @Override
        public BlockLoader.Block build() {
            return doubleBuilder.build();
        }

        @Override
        public BlockLoader.Builder appendNull() {
            throw new UnsupportedOperationException();
        }

        @Override
        public BlockLoader.Builder beginPositionEntry() {
            throw new UnsupportedOperationException();
        }

        @Override
        public BlockLoader.Builder endPositionEntry() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {
            doubleBuilder.close();
        }
    }

    static final class SingletonLongtoIntDelegate implements BlockLoader.SingletonLongBuilder {
        private final BlockLoader.SingletonIntBuilder builder;

        SingletonLongtoIntDelegate(BlockLoader.SingletonIntBuilder builder) {
            this.builder = builder;
        }

        @Override
        public BlockLoader.SingletonLongBuilder appendLong(long value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public BlockLoader.SingletonLongBuilder appendLongs(long[] values, int from, int length) {
            builder.appendLongs(values, from, length);
            return this;
        }

        @Override
        public BlockLoader.Block build() {
            return builder.build();
        }

        @Override
        public BlockLoader.Builder appendNull() {
            throw new UnsupportedOperationException();
        }

        @Override
        public BlockLoader.Builder beginPositionEntry() {
            throw new UnsupportedOperationException();
        }

        @Override
        public BlockLoader.Builder endPositionEntry() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {
            builder.close();
        }
    }
}
