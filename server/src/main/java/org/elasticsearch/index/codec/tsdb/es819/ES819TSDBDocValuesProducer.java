/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.es819;

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
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.GroupVIntUtil;
import org.apache.lucene.util.LongValues;
import org.apache.lucene.util.compress.LZ4;
import org.apache.lucene.util.packed.DirectMonotonicReader;
import org.apache.lucene.util.packed.PackedInts;
import org.elasticsearch.core.Assertions;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.codec.tsdb.BinaryDVCompressionMode;
import org.elasticsearch.index.codec.tsdb.TSDBDocValuesEncoder;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.blockloader.docvalues.BlockDocValuesReader;
import org.elasticsearch.index.mapper.blockloader.docvalues.CustomBinaryDocValuesReader;

import java.io.IOException;
import java.util.Arrays;

import static org.elasticsearch.index.codec.tsdb.es819.ES819TSDBDocValuesFormat.SKIP_INDEX_JUMP_LENGTH_PER_LEVEL;
import static org.elasticsearch.index.codec.tsdb.es819.ES819TSDBDocValuesFormat.SKIP_INDEX_MAX_LEVEL;
import static org.elasticsearch.index.codec.tsdb.es819.ES819TSDBDocValuesFormat.TERMS_DICT_BLOCK_LZ4_SHIFT;

final class ES819TSDBDocValuesProducer extends DocValuesProducer {
    final IntObjectHashMap<NumericEntry> numerics;
    private final int primarySortFieldNumber;
    final IntObjectHashMap<BinaryEntry> binaries;
    final IntObjectHashMap<SortedEntry> sorted;
    final IntObjectHashMap<SortedSetEntry> sortedSets;
    final IntObjectHashMap<SortedNumericEntry> sortedNumerics;
    private final IntObjectHashMap<DocValuesSkipperEntry> skippers;
    private final IndexInput data;
    private final int maxDoc;
    final int version;
    private final boolean merging;
    private final int numericBlockShift;
    private final int numericBlockSize;
    private final int numericBlockMask;

    ES819TSDBDocValuesProducer(SegmentReadState state, String dataCodec, String dataExtension, String metaCodec, String metaExtension)
        throws IOException {
        this.numerics = new IntObjectHashMap<>();
        this.binaries = new IntObjectHashMap<>();
        this.sorted = new IntObjectHashMap<>();
        this.sortedSets = new IntObjectHashMap<>();
        this.sortedNumerics = new IntObjectHashMap<>();
        this.skippers = new IntObjectHashMap<>();
        this.maxDoc = state.segmentInfo.maxDoc();
        this.primarySortFieldNumber = primarySortFieldNumber(state.segmentInfo, state.fieldInfos);
        this.merging = false;

        // read in the entries from the metadata file.
        int version = -1;
        int blockShift = ES819TSDBDocValuesFormat.NUMERIC_BLOCK_SHIFT;
        String metaName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, metaExtension);

        try (ChecksumIndexInput in = state.directory.openChecksumInput(metaName)) {
            Throwable priorE = null;

            try {
                version = CodecUtil.checkIndexHeader(
                    in,
                    metaCodec,
                    ES819TSDBDocValuesFormat.VERSION_START,
                    ES819TSDBDocValuesFormat.VERSION_CURRENT,
                    state.segmentInfo.getId(),
                    state.segmentSuffix
                );
                if (version >= ES819TSDBDocValuesFormat.VERSION_NUMERIC_LARGE_BLOCKS) {
                    blockShift = in.readByte();
                }
                readFields(in, state.fieldInfos, version, blockShift);
            } catch (Throwable exception) {
                priorE = exception;
            } finally {
                CodecUtil.checkFooter(in, priorE);
            }
        }

        this.numericBlockShift = blockShift;
        this.numericBlockSize = 1 << blockShift;
        this.numericBlockMask = numericBlockSize - 1;

        String dataName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, dataExtension);
        this.data = state.directory.openInput(dataName, state.context);
        boolean success = false;
        try {
            final int version2 = CodecUtil.checkIndexHeader(
                data,
                dataCodec,
                ES819TSDBDocValuesFormat.VERSION_START,
                ES819TSDBDocValuesFormat.VERSION_CURRENT,
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
    }

    private ES819TSDBDocValuesProducer(
        IntObjectHashMap<NumericEntry> numerics,
        IntObjectHashMap<BinaryEntry> binaries,
        IntObjectHashMap<SortedEntry> sorted,
        IntObjectHashMap<SortedSetEntry> sortedSets,
        IntObjectHashMap<SortedNumericEntry> sortedNumerics,
        IntObjectHashMap<DocValuesSkipperEntry> skippers,
        IndexInput data,
        int maxDoc,
        int version,
        int primarySortFieldNumber,
        boolean merging,
        int numericBlockShift
    ) {
        this.numerics = numerics;
        this.binaries = binaries;
        this.sorted = sorted;
        this.sortedSets = sortedSets;
        this.sortedNumerics = sortedNumerics;
        this.skippers = skippers;
        this.data = data.clone();
        this.maxDoc = maxDoc;
        this.version = version;
        this.primarySortFieldNumber = primarySortFieldNumber;
        this.merging = merging;
        this.numericBlockShift = numericBlockShift;
        this.numericBlockSize = 1 << numericBlockShift;
        this.numericBlockMask = numericBlockSize - 1;
    }

    @Override
    public DocValuesProducer getMergeInstance() {
        return new ES819TSDBDocValuesProducer(
            numerics,
            binaries,
            sorted,
            sortedSets,
            sortedNumerics,
            skippers,
            data,
            maxDoc,
            version,
            primarySortFieldNumber,
            true,
            numericBlockShift
        );
    }

    @Override
    public NumericDocValues getNumeric(FieldInfo field) throws IOException {
        NumericEntry entry = numerics.get(field.number);
        return getNumeric(entry, -1);
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

    public BinaryDocValues getUncompressedBinary(BinaryEntry entry) throws IOException {
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
                            // isCompressed assumes there aren't duplicates
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
                                long[] offsets = new long[count + 1];

                                // Normalize offsets so that first offset is 0
                                long startOffset = addresses.get(firstDocId);
                                for (int i = offset, j = 1; i < docs.count(); i++, j++) {
                                    int docId = docs.get(i);
                                    long nextOffset = addresses.get(docId + 1) - startOffset;
                                    offsets[j] = nextOffset;
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
                    entry.maxNumDocsInAnyBlock
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
                        // isCompressed assumes there aren't duplicates
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
                public DocIdSetIterator lengthIterator(int length) throws IOException {
                    return decoder.decodeLengthsBulk(entry.numCompressedBlocks, 0, maxDoc - 1, length);
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
                    entry.maxNumDocsInAnyBlock
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

    // Decompresses blocks of binary values to retrieve content
    static final class BinaryDecoder {

        private final LongValues addresses;
        private final DirectMonotonicReader docOffsets;
        private final IndexInput compressedData;
        private final IndexInput readAhead;
        // Cache of last uncompressed block
        private long lastBlockId = -1;
        private final int[] uncompressedDocStarts;
        private final byte[] uncompressedBlock;
        private final BytesRef uncompressedBytesRef;
        private long startDocNumForBlock = -1;
        private long limitDocNumForBlock = -1;
        private final Decompressor decompressor;

        BinaryDecoder(
            Decompressor decompressor,
            LongValues addresses,
            DirectMonotonicReader docOffsets,
            IndexInput compressedData,
            int biggestUncompressedBlockSize,
            int maxNumDocsInAnyBlock
        ) {
            this.decompressor = decompressor;
            this.addresses = addresses;
            this.docOffsets = docOffsets;
            this.compressedData = compressedData;
            this.readAhead = compressedData.clone();
            // pre-allocate a byte array large enough for the biggest uncompressed block needed.
            this.uncompressedBlock = new byte[biggestUncompressedBlockSize];
            uncompressedBytesRef = new BytesRef(uncompressedBlock);
            uncompressedDocStarts = new int[maxNumDocsInAnyBlock + 1];
        }

        private BinaryDVCompressionMode.BlockHeader decompressOffsets(long blockId, int numDocsInBlock) throws IOException {
            long blockStartOffset = addresses.get(blockId);
            compressedData.seek(blockStartOffset);

            var header = BinaryDVCompressionMode.BlockHeader.fromByte(compressedData.readByte());
            int uncompressedBlockLength = compressedData.readVInt();

            if (uncompressedBlockLength == 0) {
                Arrays.fill(uncompressedDocStarts, 0);
            } else {
                decompressDocOffsets(numDocsInBlock, compressedData);
            }

            return header;
        }

        // unconditionally decompress blockId into uncompressedDocStarts and uncompressedBlock
        private void decompressBlock(long blockId, int numDocsInBlock) throws IOException {
            var header = decompressOffsets(blockId, numDocsInBlock);

            int uncompressedBlockLength = uncompressedDocStarts[numDocsInBlock];
            assert uncompressedBlockLength <= uncompressedBlock.length;
            uncompressedBytesRef.offset = 0;
            uncompressedBytesRef.length = uncompressedBlock.length;

            if (header.isCompressed()) {
                decompressor.decompress(compressedData, uncompressedBlockLength, 0, uncompressedBlockLength, uncompressedBytesRef);
            } else {
                compressedData.readBytes(uncompressedBlock, 0, uncompressedBlockLength);
            }
        }

        void decompressDocOffsets(int numDocsInBlock, DataInput input) throws IOException {
            int numOffsets = numDocsInBlock + 1;
            GroupVIntUtil.readGroupVInts(input, uncompressedDocStarts, numOffsets);
            deltaDecode(uncompressedDocStarts, numOffsets);
        }

        // Borrowed from to TSDBDocValuesEncoder.decodeDelta
        // The `sum` variable helps compiler optimize method, should not be removed.
        void deltaDecode(int[] arr, int length) {
            int sum = 0;
            for (int i = 0; i < length; ++i) {
                sum += arr[i];
                arr[i] = sum;
            }
        }

        long findAndUpdateBlock(int docNumber, int numBlocks) {
            if (docNumber < limitDocNumForBlock && lastBlockId >= 0) {
                return lastBlockId;
            }

            long index = docOffsets.binarySearch(lastBlockId + 1, numBlocks, docNumber);
            // If index is found, index is inclusive lower bound of docNum range, so docNum is in blockId == index
            if (index < 0) {
                // If index was not found, insertion point (-index - 1) will be upper bound of docNum range.
                // Subtract additional 1 so that index points to lower bound of doc range, which is the blockId
                index = -2 - index;
            }
            assert index < numBlocks : "invalid range " + index + " for doc " + docNumber + " in numBlocks " + numBlocks;

            startDocNumForBlock = docOffsets.get(index);
            limitDocNumForBlock = docOffsets.get(index + 1);
            return index;
        }

        BytesRef decode(int docNumber, int numBlocks) throws IOException {
            // docNumber, rather than docId, because these are dense and could be indices from a DISI
            long blockId = findAndUpdateBlock(docNumber, numBlocks);

            int numDocsInBlock = (int) (limitDocNumForBlock - startDocNumForBlock);
            int idxInBlock = (int) (docNumber - startDocNumForBlock);
            assert idxInBlock < numDocsInBlock;

            if (blockId != lastBlockId) {
                decompressBlock(blockId, numDocsInBlock);
                // uncompressedBytesRef and uncompressedDocStarts now populated
                lastBlockId = blockId;
            }

            int start = uncompressedDocStarts[idxInBlock];
            int end = uncompressedDocStarts[idxInBlock + 1];
            uncompressedBytesRef.offset = start;
            uncompressedBytesRef.length = end - start;
            return uncompressedBytesRef;
        }

        /**
         * A BinaryDecoder should only call decode, decodeBulk, or decodeLength.
         * The same decoder should never be used to called multiple of these methods
         */
        int decodeLength(int docNumber, int numBlocks) throws IOException {
            // docNumber, rather than docId, because these are dense and could be indices from a DISI
            long blockId = findAndUpdateBlock(docNumber, numBlocks);

            int numDocsInBlock = (int) (limitDocNumForBlock - startDocNumForBlock);
            int idxInBlock = (int) (docNumber - startDocNumForBlock);
            assert idxInBlock < numDocsInBlock;

            if (blockId != lastBlockId) {
                decompressOffsets(blockId, numDocsInBlock);
                // uncompressedDocStarts now populated
                lastBlockId = blockId;
            }

            int start = uncompressedDocStarts[idxInBlock];
            int end = uncompressedDocStarts[idxInBlock + 1];
            return end - start;
        }

        DocIdSetIterator decodeLengthsBulk(int numBlocks, int firstDocId, int lastDocId, int requestedLength) throws IOException {
            final long firstBlockId = findBlock(firstDocId, numBlocks, 0);
            final long endBlockId = findBlock(lastDocId, numBlocks, firstBlockId);
            return new DocIdSetIterator() {

                int currentDocId = -1;
                int currentDocIdRunEnd = -1;

                @Override
                public int docID() {
                    return currentDocId;
                }

                @Override
                public int nextDoc() throws IOException {
                    return advance(currentDocId + 1);
                }

                @Override
                public int advance(int target) throws IOException {
                    return scanToTargetDocId(target);
                }

                @Override
                public long cost() {
                    int maxDoc = lastDocId + 1;
                    return maxDoc;
                }

                @Override
                public int docIDRunEnd() throws IOException {
                    if (currentDocIdRunEnd == -1) {
                        return super.docIDRunEnd();
                    } else {
                        return currentDocIdRunEnd;
                    }
                }

                long currentBlockId = -1;
                int blockStartDocId;
                int blockEndDocId;

                int scanToTargetDocId(int target) throws IOException {
                    // If target falls within the current known run of consecutive matches, return it directly
                    if (target < currentDocIdRunEnd) {
                        return currentDocId = target;
                    }

                    for (long blockId = currentBlockId == -1 ? firstBlockId : currentBlockId; blockId <= endBlockId; blockId++) {
                        if (blockId != currentBlockId) {
                            blockStartDocId = (int) docOffsets.get(blockId);
                            blockEndDocId = (int) docOffsets.get(blockId + 1);
                        }

                        if (blockEndDocId <= target) {
                            continue;
                        }

                        if (blockId != currentBlockId) {
                            int numDocsInBlock = blockEndDocId - blockStartDocId;
                            decompressOffsets(blockId, numDocsInBlock);
                        }

                        int startDocId = blockId == firstBlockId ? firstDocId : blockStartDocId;
                        if (startDocId < target) {
                            startDocId = target;
                        }
                        // lastDocId is inclusive and blockEndDocId is exclusive!
                        int endDocId = blockId == endBlockId ? lastDocId + 1 : blockEndDocId;

                        for (int docId = startDocId; docId < endDocId; docId++) {
                            int index = docId - blockStartDocId;
                            int length = uncompressedDocStarts[index + 1] - uncompressedDocStarts[index];
                            if (requestedLength == length) {
                                currentBlockId = blockId;
                                currentDocId = docId;
                                // Look ahead for consecutive matching docs within the current block
                                int runEnd = docId + 1;
                                while (runEnd < endDocId) {
                                    int runIndex = runEnd - blockStartDocId;
                                    int runLength = uncompressedDocStarts[runIndex + 1] - uncompressedDocStarts[runIndex];
                                    if (runLength != requestedLength) {
                                        break;
                                    }
                                    runEnd++;
                                }
                                currentDocIdRunEnd = runEnd;
                                return currentDocId;
                            }
                        }
                    }

                    currentBlockId = endBlockId;
                    return currentDocId = currentDocIdRunEnd = DocIdSetIterator.NO_MORE_DOCS;
                }
            };
        }

        void decodeBulk(int numBlocks, int firstDocId, int lastDocId, int count, BlockLoader.SingletonBytesRefBuilder builder)
            throws IOException {
            // Lookup the first blockId using binary search and subsequent blocks can be scanned because query and values are dense
            // Also lookup the last blockId using binary search so that the buffer for values to be collected can be sized accordingly.
            final long firstBlockId = findBlock(firstDocId, numBlocks, lastBlockId == -1 ? 0 : lastBlockId);
            final long endBlockId = findBlock(lastDocId, numBlocks, firstBlockId);
            final int bufferSize = computeMultipleBlockBufferSize(firstBlockId, endBlockId);

            int offsetBufferIndex = 0;
            // The last slot used as end offset for the last value
            final long[] offsetBuffer = new long[count + 1];
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
                // lastDocId is inclusive and blockEndDocId is exclusive!
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
            // Recording end offset for last value, so that builder knows where it ends
            offsetBuffer[offsetBufferIndex] = valuesBufferIndex;

            lastBlockId = endBlockId;

            // TODO: This sets state for the decode(...), we should look into removing this.
            // Either we will disallow invoking both bulkDecode and decode on the same BinaryDecoder instance
            // or decode should reuse decodeBulk.
            startDocNumForBlock = docOffsets.get(endBlockId);
            limitDocNumForBlock = docOffsets.get(endBlockId + 1);

            assert count == offsetBufferIndex;
            builder.appendBytesRefs(valuesBuffer, offsetBuffer);
        }

        long findBlock(int docNumber, int numBlocks, long fromIndex) {
            long index = docOffsets.binarySearch(fromIndex, numBlocks, docNumber);
            // If index is found, index is inclusive lower bound of docNum range, so docNum is in blockId == index
            if (index < 0) {
                // If index was not found, insertion point (-index - 1) will be upper bound of docNum range.
                // Subtract additional 1 so that index points to lower bound of doc range, which is the blockId
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

    abstract static class ES819BinaryDocValues extends BinaryDocValues
        implements
            BlockLoader.OptionalColumnAtATimeReader,
            BlockLoader.OptionalLengthReader {}

    abstract static class DenseBinaryDocValues extends ES819BinaryDocValues {

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
            };
        }
    }

    abstract static class SparseBinaryDocValues extends ES819BinaryDocValues {

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

        final NumericDocValues ords = getNumeric(entry.ordsEntry, entry.termsDictEntry.termsDictSize);
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
                    // Falling back to tryRead(...) is safe here, given that current block index wasn't altered by looking ahead.
                    try (var builder = factory.singletonOrdinalsBuilder(this, docs.count() - offset, true)) {
                        BlockLoader.SingletonLongBuilder delegate = new SingletonLongToSingletonOrdinalDelegate(builder, numericBlockSize);
                        var result = denseOrds.tryRead(delegate, docs, offset);
                        if (result != null) {
                            return result;
                        }
                    }
                }
                return null;
            }

            BlockLoader.Block tryReadAHead(BlockLoader.BlockFactory factory, BlockLoader.Docs docs, int offset) throws IOException {
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
                                // append all docs of the last range without checking
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

    abstract class BaseSortedDocValues extends SortedDocValues implements BlockLoader.OptionalColumnAtATimeReader {

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
            return new TermsDict(entry.termsDictEntry, data, merging);
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

        BlockLoader.Block tryReadAHead(BlockLoader.BlockFactory factory, BlockLoader.Docs docs, int offset) throws IOException {
            return null;
        }
    }

    abstract static class BaseDenseNumericValues extends NumericDocValues implements BlockLoader.OptionalColumnAtATimeReader {
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
        final TermsEnum termsEnum;

        BaseSortedSetDocValues(SortedSetEntry entry, IndexInput data, boolean merging) throws IOException {
            this.entry = entry;
            this.data = data;
            this.merging = merging;
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
            return new TermsDict(entry.termsDictEntry, data, merging);
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
        long ord = -1;

        BytesRef blockBuffer = null;
        ByteArrayDataInput blockInput = null;
        long currentCompressedBlockStart = -1;
        long currentCompressedBlockEnd = -1;

        TermsDict(TermsDictEntry entry, IndexInput data, boolean merging) throws IOException {
            this.entry = entry;
            RandomAccessInput addressesSlice = data.randomAccessSlice(entry.termsAddressesOffset, entry.termsAddressesLength);
            blockAddresses = DirectMonotonicReader.getInstance(entry.termsAddressesMeta, addressesSlice, merging);
            bytes = data.slice("terms", entry.termsDataOffset, entry.termsDataLength);
            blockMask = (1L << TERMS_DICT_BLOCK_LZ4_SHIFT) - 1;
            RandomAccessInput indexAddressesSlice = data.randomAccessSlice(
                entry.termsIndexAddressesOffset,
                entry.termsIndexAddressesLength
            );
            indexAddresses = DirectMonotonicReader.getInstance(entry.termsIndexAddressesMeta, indexAddressesSlice, merging);
            indexBytes = data.randomAccessSlice(entry.termsIndexOffset, entry.termsIndexLength);
            term = new BytesRef(entry.maxTermLength);

            // add the max term length for the dictionary
            // add 7 padding bytes can help decompression run faster.
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
            // Signed shift since ord is -1 when the terms enum is not positioned
            final long currentBlockIndex = this.ord >> TERMS_DICT_BLOCK_LZ4_SHIFT;
            final long blockIndex = ord >> TERMS_DICT_BLOCK_LZ4_SHIFT;
            if (ord < this.ord || blockIndex != currentBlockIndex) {
                // The looked up ord is before the current ord or belongs to a different block, seek again
                final long blockAddress = blockAddresses.get(blockIndex);
                bytes.seek(blockAddress);
                this.ord = (blockIndex << TERMS_DICT_BLOCK_LZ4_SHIFT) - 1;
            }
            // Scan to the looked up ord
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
            assert block >= 0 && block <= (entry.termsDictSize - 1) >>> TERMS_DICT_BLOCK_LZ4_SHIFT;
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

            long blockLo = ordLo >>> TERMS_DICT_BLOCK_LZ4_SHIFT;
            long blockHi = ordHi >>> TERMS_DICT_BLOCK_LZ4_SHIFT;

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
            assert blockHi == ((entry.termsDictSize - 1) >>> TERMS_DICT_BLOCK_LZ4_SHIFT)
                || getFirstTermFromBlock(blockHi + 1).compareTo(text) > 0;

            return blockHi;
        }

        @Override
        public SeekStatus seekCeil(BytesRef text) throws IOException {
            final long block = seekBlock(text);
            if (block == -1) {
                // before the first term, or empty terms dict
                if (entry.termsDictSize == 0) {
                    ord = 0;
                    return SeekStatus.END;
                } else {
                    seekExact(0L);
                    return SeekStatus.NOT_FOUND;
                }
            }
            final long blockAddress = blockAddresses.get(block);
            this.ord = block << TERMS_DICT_BLOCK_LZ4_SHIFT;
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
            // The first term is kept uncompressed, so no need to decompress block if only
            // look up the first term when doing seek block.
            term.length = bytes.readVInt();
            bytes.readBytes(term.bytes, 0, term.length);
            long offset = bytes.getFilePointer();
            if (offset < entry.termsDataLength - 1) {
                // Avoid decompress again if we are reading a same block.
                if (currentCompressedBlockStart != offset) {
                    blockBuffer.offset = term.length;
                    blockBuffer.length = bytes.readVInt();
                    // Decompress the remaining of current block, using the first term as a dictionary
                    System.arraycopy(term.bytes, 0, blockBuffer.bytes, 0, blockBuffer.offset);
                    LZ4.decompress(bytes, blockBuffer.length, blockBuffer.bytes, blockBuffer.offset);
                    currentCompressedBlockStart = offset;
                    currentCompressedBlockEnd = bytes.getFilePointer();
                } else {
                    // Skip decompression but need to re-seek to block end.
                    bytes.seek(currentCompressedBlockEnd);
                }

                // Reset the buffer.
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
        return getSortedNumeric(entry, -1);
    }

    @Override
    public SortedSetDocValues getSortedSet(FieldInfo field) throws IOException {
        SortedSetEntry entry = sortedSets.get(field.number);
        if (entry.singleValueEntry != null) {
            return DocValues.singleton(getSorted(entry.singleValueEntry, field.number == primarySortFieldNumber));
        }

        SortedNumericEntry ordsEntry = entry.ordsEntry;
        final SortedNumericDocValues ords = getSortedNumeric(ordsEntry, entry.termsDictEntry.termsDictSize);
        return new BaseSortedSetDocValues(entry, data, merging) {

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
        };
    }

    @Override
    public DocValuesSkipper getSkipper(FieldInfo field) throws IOException {
        final DocValuesSkipperEntry entry = skippers.get(field.number);

        // TODO: should we write to disk the actual max level for this segment?
        return new DocValuesSkipper() {
            final int[] minDocID = new int[SKIP_INDEX_MAX_LEVEL];
            final int[] maxDocID = new int[SKIP_INDEX_MAX_LEVEL];

            IndexInput input;

            {
                for (int i = 0; i < SKIP_INDEX_MAX_LEVEL; i++) {
                    minDocID[i] = maxDocID[i] = -1;
                }
            }

            final long[] minValue = new long[SKIP_INDEX_MAX_LEVEL];
            final long[] maxValue = new long[SKIP_INDEX_MAX_LEVEL];
            final int[] docCount = new int[SKIP_INDEX_MAX_LEVEL];
            int levels = 1;

            @Override
            public void advance(int target) throws IOException {
                if (target > entry.maxDocId) {
                    // skipper is exhausted
                    for (int i = 0; i < SKIP_INDEX_MAX_LEVEL; i++) {
                        minDocID[i] = maxDocID[i] = DocIdSetIterator.NO_MORE_DOCS;
                    }
                } else {
                    if (input == null) {
                        input = data.slice("doc value skipper", entry.offset, entry.length);
                    }
                    // find next interval
                    assert target > maxDocID[0] : "target must be bigger than current interval";
                    while (true) {
                        levels = input.readByte();
                        assert levels <= SKIP_INDEX_MAX_LEVEL && levels > 0 : "level out of range [" + levels + "]";
                        boolean valid = true;
                        // check if current interval is competitive or we can jump to the next position
                        for (int level = levels - 1; level >= 0; level--) {
                            if ((maxDocID[level] = input.readInt()) < target) {
                                input.skipBytes(SKIP_INDEX_JUMP_LENGTH_PER_LEVEL[level]); // the jump for the level
                                valid = false;
                                break;
                            }
                            minDocID[level] = input.readInt();
                            maxValue[level] = input.readLong();
                            minValue[level] = input.readLong();
                            docCount[level] = input.readInt();
                        }
                        if (valid) {
                            // adjust levels
                            while (levels < SKIP_INDEX_MAX_LEVEL && maxDocID[levels] >= target) {
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
        };
    }

    @Override
    public void checkIntegrity() throws IOException {
        CodecUtil.checksumEntireFile(data);
    }

    @Override
    public void close() throws IOException {
        data.close();
    }

    /**
     * Returns the field number of the primary sort field for the given segment,
     * if the field is sorted in ascending order. Returns {@code -1} if not found.
     */
    static int primarySortFieldNumber(SegmentInfo segmentInfo, FieldInfos fieldInfos) {
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
                skippers.put(info.number, readDocValueSkipperMeta(meta));
            }
            if (type == ES819TSDBDocValuesFormat.NUMERIC) {
                numerics.put(info.number, readNumeric(meta, numericBlockShift));
            } else if (type == ES819TSDBDocValuesFormat.BINARY) {
                binaries.put(info.number, readBinary(meta, version));
            } else if (type == ES819TSDBDocValuesFormat.SORTED) {
                sorted.put(info.number, readSorted(meta, numericBlockShift));
            } else if (type == ES819TSDBDocValuesFormat.SORTED_SET) {
                sortedSets.put(info.number, readSortedSet(meta, numericBlockShift));
            } else if (type == ES819TSDBDocValuesFormat.SORTED_NUMERIC) {
                sortedNumerics.put(info.number, readSortedNumeric(meta, numericBlockShift));
            } else {
                throw new CorruptIndexException("invalid type: " + type, meta);
            }
        }
    }

    private static NumericEntry readNumeric(IndexInput meta, int numericBlockShift) throws IOException {
        NumericEntry entry = new NumericEntry();
        readNumeric(meta, entry, numericBlockShift);
        return entry;
    }

    private static DocValuesSkipperEntry readDocValueSkipperMeta(IndexInput meta) throws IOException {
        long offset = meta.readLong();
        long length = meta.readLong();
        long maxValue = meta.readLong();
        long minValue = meta.readLong();
        int docCount = meta.readInt();
        int maxDocID = meta.readInt();

        return new DocValuesSkipperEntry(offset, length, minValue, maxValue, docCount, maxDocID);
    }

    private static void readNumeric(IndexInput meta, NumericEntry entry, int numericBlockShift) throws IOException {
        entry.numValues = meta.readLong();
        // Change compared to ES87TSDBDocValuesProducer:
        entry.numDocsWithField = meta.readInt();
        if (entry.numValues > 0) {
            final int indexBlockShift = meta.readInt();
            if (indexBlockShift == -1) {
                // single ordinal, no block index
            } else if (indexBlockShift == -2) {
                // encoded ordinal range, no block index
                final int numOrds = meta.readVInt();
                final int blockShift = meta.readByte();
                entry.sortedOrdinals = DirectMonotonicReader.loadMeta(meta, numOrds + 1, blockShift);
            } else {
                entry.indexMeta = DirectMonotonicReader.loadMeta(meta, 1 + ((entry.numValues - 1) >>> numericBlockShift), indexBlockShift);
            }
            entry.indexOffset = meta.readLong();
            entry.indexLength = meta.readLong();
            entry.valuesOffset = meta.readLong();
            entry.valuesLength = meta.readLong();
        }
        // Change compared to ES87TSDBDocValuesProducer:
        entry.docsWithFieldOffset = meta.readLong();
        entry.docsWithFieldLength = meta.readLong();
        entry.jumpTableEntryCount = meta.readShort();
        entry.denseRankPower = meta.readByte();
    }

    private BinaryEntry readBinary(IndexInput meta, int version) throws IOException {
        final BinaryDVCompressionMode compression;
        if (version >= ES819TSDBDocValuesFormat.VERSION_BINARY_DV_COMPRESSION) {
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
                // Old count of uncompressed addresses
                long numAddresses = entry.numDocsWithField + 1L;
                final int blockShift = meta.readVInt();
                entry.addressesMeta = DirectMonotonicReader.loadMeta(meta, numAddresses, blockShift);
                entry.addressesLength = meta.readLong();
            }
        } else {
            if (entry.numDocsWithField > 0) {
                entry.addressesOffset = meta.readLong();
                // New count of compressed addresses - the number of compressed blocks
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

    private static SortedNumericEntry readSortedNumeric(IndexInput meta, int numericBlockShift) throws IOException {
        SortedNumericEntry entry = new SortedNumericEntry();
        readSortedNumeric(meta, entry, numericBlockShift);
        return entry;
    }

    private static SortedNumericEntry readSortedNumeric(IndexInput meta, SortedNumericEntry entry, int numericBlockShift)
        throws IOException {
        readNumeric(meta, entry, numericBlockShift);
        // We don't read numDocsWithField here any more.
        if (entry.numDocsWithField != entry.numValues) {
            entry.addressesOffset = meta.readLong();
            final int blockShift = meta.readVInt();
            entry.addressesMeta = DirectMonotonicReader.loadMeta(meta, entry.numDocsWithField + 1, blockShift);
            entry.addressesLength = meta.readLong();
        }
        return entry;
    }

    private static SortedEntry readSorted(IndexInput meta, int numericBlockShift) throws IOException {
        SortedEntry entry = new SortedEntry();
        entry.ordsEntry = new NumericEntry();
        readNumeric(meta, entry.ordsEntry, numericBlockShift);
        entry.termsDictEntry = new TermsDictEntry();
        readTermDict(meta, entry.termsDictEntry);
        return entry;
    }

    private static SortedSetEntry readSortedSet(IndexInput meta, int numericBlockShift) throws IOException {
        SortedSetEntry entry = new SortedSetEntry();
        byte multiValued = meta.readByte();
        switch (multiValued) {
            case 0: // singlevalued
                entry.singleValueEntry = readSorted(meta, numericBlockShift);
                return entry;
            case 1: // multivalued
                break;
            default:
                throw new CorruptIndexException("Invalid multiValued flag: " + multiValued, meta);
        }
        entry.ordsEntry = new SortedNumericEntry();
        readSortedNumeric(meta, entry.ordsEntry, numericBlockShift);
        entry.termsDictEntry = new TermsDictEntry();
        readTermDict(meta, entry.termsDictEntry);
        return entry;
    }

    private static void readTermDict(IndexInput meta, TermsDictEntry entry) throws IOException {
        entry.termsDictSize = meta.readVLong();
        final int blockShift = meta.readInt();
        final long addressesSize = (entry.termsDictSize + (1L << TERMS_DICT_BLOCK_LZ4_SHIFT) - 1) >>> TERMS_DICT_BLOCK_LZ4_SHIFT;
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

    static final class SortedOrdinalReader {
        final long maxOrd;
        final DirectMonotonicReader startDocs;
        private long currentIndex = -1;
        private long rangeEndExclusive = -1;

        SortedOrdinalReader(long maxOrd, DirectMonotonicReader startDocs) {
            this.maxOrd = maxOrd;
            this.startDocs = startDocs;
        }

        long readValueAndAdvance(int doc) {
            if (doc < rangeEndExclusive) {
                return currentIndex;
            }
            // move to the next range
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

    private NumericDocValues getNumeric(NumericEntry entry, long maxOrd) throws IOException {
        if (entry.docsWithFieldOffset == -2) {
            // empty
            return DocValues.emptyNumeric();
        }

        if (maxOrd == 1) {
            // Special case for maxOrd 1, no need to read blocks and use ordinal 0 as only value
            if (entry.docsWithFieldOffset == -1) {
                // Special case when all docs have a value
                return new BaseDenseNumericValues(maxDoc) {
                    @Override
                    public long longValue() {
                        // Only one ordinal!
                        return 0L;
                    }

                    @Override
                    public int docIDRunEnd() {
                        return maxDoc;
                    }

                    @Override
                    long lookAheadValueAt(int targetDoc) throws IOException {
                        return 0L;  // Only one ordinal!
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
                        return 0L;  // Only one ordinal!
                    }

                    @Override
                    public int docIDRunEnd() throws IOException {
                        return disi.docIDRunEnd();
                    }
                };
            }
        } else if (entry.sortedOrdinals != null) {
            return getRangeEncodedNumericDocValues(entry, maxOrd);
        }

        // NOTE: we could make this a bit simpler by reusing #getValues but this
        // makes things slower.

        final RandomAccessInput indexSlice = data.randomAccessSlice(entry.indexOffset, entry.indexLength);
        final DirectMonotonicReader indexReader = DirectMonotonicReader.getInstance(entry.indexMeta, indexSlice, merging);
        final IndexInput valuesData = data.slice("values", entry.valuesOffset, entry.valuesLength);

        final int bitsPerOrd = maxOrd >= 0 ? PackedInts.bitsRequired(maxOrd - 1) : -1;
        if (entry.docsWithFieldOffset == -1) {
            // dense
            return new BaseDenseNumericValues(maxDoc) {
                private final TSDBDocValuesEncoder decoder = new TSDBDocValuesEncoder(numericBlockSize);
                private long currentBlockIndex = -1;
                private final long[] currentBlock = new long[numericBlockSize];
                // lookahead block
                private long lookaheadBlockIndex = -1;
                private long[] lookaheadBlock;
                private IndexInput lookaheadData = null;

                @Override
                public int docIDRunEnd() {
                    return maxDoc;
                }

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
                    // no need to seek if the loading block is the next block
                    if (currentBlockIndex + 1 != blockIndex) {
                        valuesData.seek(indexReader.get(blockIndex));
                    }
                    currentBlockIndex = blockIndex;
                    if (bitsPerOrd == -1) {
                        decoder.decode(valuesData, currentBlock);
                    } else {
                        decoder.decodeOrdinals(valuesData, currentBlock, bitsPerOrd);
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
                    try (var singletonLongBuilder = singletonLongBuilder(factory, toDouble, docs.count() - offset, toInt)) {
                        return tryRead(singletonLongBuilder, docs, offset);
                    }
                }

                @Override
                BlockLoader.Block tryRead(BlockLoader.SingletonLongBuilder builder, BlockLoader.Docs docs, int offset) throws IOException {
                    if (docs.mayContainDuplicates()) {
                        // isCompressed assumes there aren't duplicates
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
                            // no need to seek if the loading block is the next block
                            if (currentBlockIndex + 1 != blockIndex) {
                                valuesData.seek(indexReader.get(blockIndex));
                            }
                            currentBlockIndex = blockIndex;
                            if (bitsPerOrd == -1) {
                                decoder.decode(valuesData, currentBlock);
                            } else {
                                decoder.decodeOrdinals(valuesData, currentBlock, bitsPerOrd);
                            }
                        }

                        // Try to append more than just one value:
                        // Instead of iterating over docs and find the max length, take an optimistic approach to avoid as
                        // many comparisons as there are remaining docs and instead do at most 7 comparisons:
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
                    // load data to the lookahead block
                    if (lookaheadBlockIndex != blockIndex) {
                        if (lookaheadBlock == null) {
                            lookaheadBlock = new long[numericBlockSize];
                            lookaheadData = data.slice("look_ahead_values", entry.valuesOffset, entry.valuesLength);
                        }
                        if (lookaheadBlockIndex + 1 != blockIndex) {
                            lookaheadData.seek(indexReader.get(blockIndex));
                        }
                        if (bitsPerOrd == -1) {
                            decoder.decode(lookaheadData, lookaheadBlock);
                        } else {
                            decoder.decodeOrdinals(lookaheadData, lookaheadBlock, bitsPerOrd);
                        }
                        lookaheadBlockIndex = blockIndex;
                    }
                    return lookaheadBlock[valueIndex];
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
                private final TSDBDocValuesEncoder decoder = new TSDBDocValuesEncoder(numericBlockSize);
                private IndexedDISI lookAheadDISI;
                private long currentBlockIndex = -1;
                private final long[] currentBlock = new long[numericBlockSize];

                @Override
                public int docIDRunEnd() throws IOException {
                    return disi.docIDRunEnd();
                }

                @Override
                public long longValue() throws IOException {
                    final int index = disi.index();
                    final int blockIndex = index >>> numericBlockShift;
                    final int blockInIndex = index & numericBlockMask;
                    if (blockIndex != currentBlockIndex) {
                        assert blockIndex > currentBlockIndex : blockIndex + "<=" + currentBlockIndex;
                        // no need to seek if the loading block is the next block
                        if (currentBlockIndex + 1 != blockIndex) {
                            valuesData.seek(indexReader.get(blockIndex));
                        }
                        currentBlockIndex = blockIndex;
                        if (bitsPerOrd == -1) {
                            decoder.decode(valuesData, currentBlock);
                        } else {
                            decoder.decodeOrdinals(valuesData, currentBlock, bitsPerOrd);
                        }
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
                    // Assumes docIds are unique - if the number of value indices between the first
                    // and last doc equals the doc count, all values can be read and converted in bulk
                    // TODO: Pass docCount attr for enrich and lookup.
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
        // This does not detect duplicate docids (e.g [1, 1, 2, 4] would be detected as dense),
        // this can happen with enrich or lookup. However this codec isn't used for enrich / lookup.
        // This codec is only used in the context of logsdb and tsdb, so this is fine here.
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
                public int docIDRunEnd() throws IOException {
                    return maxDoc;
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

                @Override
                public int docIDRunEnd() throws IOException {
                    return disi.docIDRunEnd();
                }
            };
        }
    }

    private NumericValues getValues(NumericEntry entry, final long maxOrd) throws IOException {
        assert entry.numValues > 0;
        final RandomAccessInput indexSlice = data.randomAccessSlice(entry.indexOffset, entry.indexLength);
        final DirectMonotonicReader indexReader = DirectMonotonicReader.getInstance(entry.indexMeta, indexSlice, merging);

        final IndexInput valuesData = data.slice("values", entry.valuesOffset, entry.valuesLength);
        final int bitsPerOrd = maxOrd >= 0 ? PackedInts.bitsRequired(maxOrd - 1) : -1;

        final long[] currentBlockIndex = { -1 };
        final long[] currentBlock = new long[numericBlockSize];
        final TSDBDocValuesEncoder decoder = new TSDBDocValuesEncoder(numericBlockSize);
        return index -> {
            final long blockIndex = index >>> numericBlockShift;
            final int blockInIndex = (int) (index & numericBlockMask);
            if (blockIndex != currentBlockIndex[0]) {
                // no need to seek if the loading block is the next block
                if (currentBlockIndex[0] + 1 != blockIndex) {
                    valuesData.seek(indexReader.get(blockIndex));
                }
                currentBlockIndex[0] = blockIndex;
                if (bitsPerOrd == -1) {
                    decoder.decode(valuesData, currentBlock);
                } else {
                    decoder.decodeOrdinals(valuesData, currentBlock, bitsPerOrd);
                }
            }
            return currentBlock[blockInIndex];
        };
    }

    private SortedNumericDocValues getSortedNumeric(SortedNumericEntry entry, long maxOrd) throws IOException {
        if (entry.numValues == entry.numDocsWithField) {
            return DocValues.singleton(getNumeric(entry, maxOrd));
        }

        final RandomAccessInput addressesInput = data.randomAccessSlice(entry.addressesOffset, entry.addressesLength);
        final LongValues addresses = DirectMonotonicReader.getInstance(entry.addressesMeta, addressesInput, merging);

        assert entry.sortedOrdinals == null : "encoded ordinal range supports only one value per document";
        if (entry.sortedOrdinals != null) {
            // TODO: determine when this can be removed.
            // This is for the clusters that ended up using ordinal range encoding for multi-values fields. Only first value can be
            // returned.
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

    private record DocValuesSkipperEntry(long offset, long length, long minValue, long maxValue, int docCount, int maxDocId) {}

    static class NumericEntry {
        long docsWithFieldOffset;
        long docsWithFieldLength;
        short jumpTableEntryCount;
        byte denseRankPower;
        long numValues;
        // Change compared to ES87TSDBDocValuesProducer:
        int numDocsWithField;
        long indexOffset;
        long indexLength;
        DirectMonotonicReader.Meta indexMeta;
        long valuesOffset;
        long valuesLength;
        DirectMonotonicReader.Meta sortedOrdinals;
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
        // compression mode
        int maxUncompressedChunkSize;
        int maxNumDocsInAnyBlock;
        int numCompressedBlocks;
        DirectMonotonicReader.Meta addressesMeta;
        DirectMonotonicReader.Meta docOffsetMeta;

        BinaryEntry(BinaryDVCompressionMode compression) {
            this.compression = compression;
        }
    }

    static class SortedNumericEntry extends NumericEntry {
        DirectMonotonicReader.Meta addressesMeta;
        long addressesOffset;
        long addressesLength;
    }

    static class SortedEntry {
        NumericEntry ordsEntry;
        TermsDictEntry termsDictEntry;
    }

    static class SortedSetEntry {
        SortedEntry singleValueEntry;
        SortedNumericEntry ordsEntry;
        TermsDictEntry termsDictEntry;
    }

    private static class TermsDictEntry {
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
            // Unfortunately, no array copy here...
            // Since we need to loop here, let's also keep track of min/max.
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

    // Block builder that consumes long values and converts them to double using the provided converter function.
    static final class SingletonLongToDoubleDelegate implements BlockLoader.SingletonLongBuilder {
        private final BlockLoader.SingletonDoubleBuilder doubleBuilder;
        private final BlockDocValuesReader.ToDouble toDouble;

        // The passed builder is used to store the converted double values and produce the final block containing them.
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
