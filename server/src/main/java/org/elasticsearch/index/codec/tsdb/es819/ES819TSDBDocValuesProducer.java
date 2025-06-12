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
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.internal.hppc.IntObjectHashMap;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LongValues;
import org.apache.lucene.util.compress.LZ4;
import org.apache.lucene.util.packed.DirectMonotonicReader;
import org.apache.lucene.util.packed.PackedInts;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.index.codec.tsdb.TSDBDocValuesEncoder;

import java.io.IOException;

import static org.elasticsearch.index.codec.tsdb.es819.ES819TSDBDocValuesFormat.SKIP_INDEX_JUMP_LENGTH_PER_LEVEL;
import static org.elasticsearch.index.codec.tsdb.es819.ES819TSDBDocValuesFormat.SKIP_INDEX_MAX_LEVEL;
import static org.elasticsearch.index.codec.tsdb.es819.ES819TSDBDocValuesFormat.TERMS_DICT_BLOCK_LZ4_SHIFT;

final class ES819TSDBDocValuesProducer extends DocValuesProducer {
    final IntObjectHashMap<NumericEntry> numerics;
    final IntObjectHashMap<BinaryEntry> binaries;
    final IntObjectHashMap<SortedEntry> sorted;
    final IntObjectHashMap<SortedSetEntry> sortedSets;
    final IntObjectHashMap<SortedNumericEntry> sortedNumerics;
    private final IntObjectHashMap<DocValuesSkipperEntry> skippers;
    private final IndexInput data;
    private final int maxDoc;
    final int version;
    private final boolean merging;

    ES819TSDBDocValuesProducer(SegmentReadState state, String dataCodec, String dataExtension, String metaCodec, String metaExtension)
        throws IOException {
        this.numerics = new IntObjectHashMap<>();
        this.binaries = new IntObjectHashMap<>();
        this.sorted = new IntObjectHashMap<>();
        this.sortedSets = new IntObjectHashMap<>();
        this.sortedNumerics = new IntObjectHashMap<>();
        this.skippers = new IntObjectHashMap<>();
        this.maxDoc = state.segmentInfo.maxDoc();
        this.merging = false;

        // read in the entries from the metadata file.
        int version = -1;
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

                readFields(in, state.fieldInfos);

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
        boolean merging
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
        this.merging = merging;
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
            true
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
                };
            }
        }
    }

    private abstract static class DenseBinaryDocValues extends BinaryDocValues {

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
    }

    private abstract static class SparseBinaryDocValues extends BinaryDocValues {

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
    }

    @Override
    public SortedDocValues getSorted(FieldInfo field) throws IOException {
        SortedEntry entry = sorted.get(field.number);
        return getSorted(entry);
    }

    private SortedDocValues getSorted(SortedEntry entry) throws IOException {
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
        };
    }

    abstract class BaseSortedDocValues extends SortedDocValues {

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
            return DocValues.singleton(getSorted(entry.singleValueEntry));
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
        };
    }

    @Override
    public DocValuesSkipper getSkipper(FieldInfo field) throws IOException {
        final DocValuesSkipperEntry entry = skippers.get(field.number);

        final IndexInput input = data.slice("doc value skipper", entry.offset, entry.length);
        // Prefetch the first page of data. Following pages are expected to get prefetched through
        // read-ahead.
        if (input.length() > 0) {
            input.prefetch(0, 1);
        }
        // TODO: should we write to disk the actual max level for this segment?
        return new DocValuesSkipper() {
            final int[] minDocID = new int[SKIP_INDEX_MAX_LEVEL];
            final int[] maxDocID = new int[SKIP_INDEX_MAX_LEVEL];

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
                    // find next interval
                    assert target > maxDocID[0] : "target must be bigger that current interval";
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

    private void readFields(IndexInput meta, FieldInfos infos) throws IOException {
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
                numerics.put(info.number, readNumeric(meta));
            } else if (type == ES819TSDBDocValuesFormat.BINARY) {
                binaries.put(info.number, readBinary(meta));
            } else if (type == ES819TSDBDocValuesFormat.SORTED) {
                sorted.put(info.number, readSorted(meta));
            } else if (type == ES819TSDBDocValuesFormat.SORTED_SET) {
                sortedSets.put(info.number, readSortedSet(meta));
            } else if (type == ES819TSDBDocValuesFormat.SORTED_NUMERIC) {
                sortedNumerics.put(info.number, readSortedNumeric(meta));
            } else {
                throw new CorruptIndexException("invalid type: " + type, meta);
            }
        }
    }

    private static NumericEntry readNumeric(IndexInput meta) throws IOException {
        NumericEntry entry = new NumericEntry();
        readNumeric(meta, entry);
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

    private static void readNumeric(IndexInput meta, NumericEntry entry) throws IOException {
        entry.numValues = meta.readLong();
        // Change compared to ES87TSDBDocValuesProducer:
        entry.numDocsWithField = meta.readInt();
        if (entry.numValues > 0) {
            final int indexBlockShift = meta.readInt();
            // Special case, -1 means there are no blocks, so no need to load the metadata for it
            // -1 is written when there the cardinality of a field is exactly one.
            if (indexBlockShift != -1) {
                entry.indexMeta = DirectMonotonicReader.loadMeta(
                    meta,
                    1 + ((entry.numValues - 1) >>> ES819TSDBDocValuesFormat.NUMERIC_BLOCK_SHIFT),
                    indexBlockShift
                );
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

    private BinaryEntry readBinary(IndexInput meta) throws IOException {
        final BinaryEntry entry = new BinaryEntry();
        entry.dataOffset = meta.readLong();
        entry.dataLength = meta.readLong();
        entry.docsWithFieldOffset = meta.readLong();
        entry.docsWithFieldLength = meta.readLong();
        entry.jumpTableEntryCount = meta.readShort();
        entry.denseRankPower = meta.readByte();
        entry.numDocsWithField = meta.readInt();
        entry.minLength = meta.readInt();
        entry.maxLength = meta.readInt();
        if (entry.minLength < entry.maxLength) {
            entry.addressesOffset = meta.readLong();

            // Old count of uncompressed addresses
            long numAddresses = entry.numDocsWithField + 1L;

            final int blockShift = meta.readVInt();
            entry.addressesMeta = DirectMonotonicReader.loadMeta(meta, numAddresses, blockShift);
            entry.addressesLength = meta.readLong();
        }
        return entry;
    }

    private static SortedNumericEntry readSortedNumeric(IndexInput meta) throws IOException {
        SortedNumericEntry entry = new SortedNumericEntry();
        readSortedNumeric(meta, entry);
        return entry;
    }

    private static SortedNumericEntry readSortedNumeric(IndexInput meta, SortedNumericEntry entry) throws IOException {
        readNumeric(meta, entry);
        // We don't read numDocsWithField here any more.
        if (entry.numDocsWithField != entry.numValues) {
            entry.addressesOffset = meta.readLong();
            final int blockShift = meta.readVInt();
            entry.addressesMeta = DirectMonotonicReader.loadMeta(meta, entry.numDocsWithField + 1, blockShift);
            entry.addressesLength = meta.readLong();
        }
        return entry;
    }

    private SortedEntry readSorted(IndexInput meta) throws IOException {
        SortedEntry entry = new SortedEntry();
        entry.ordsEntry = new NumericEntry();
        readNumeric(meta, entry.ordsEntry);
        entry.termsDictEntry = new TermsDictEntry();
        readTermDict(meta, entry.termsDictEntry);
        return entry;
    }

    private SortedSetEntry readSortedSet(IndexInput meta) throws IOException {
        SortedSetEntry entry = new SortedSetEntry();
        byte multiValued = meta.readByte();
        switch (multiValued) {
            case 0: // singlevalued
                entry.singleValueEntry = readSorted(meta);
                return entry;
            case 1: // multivalued
                break;
            default:
                throw new CorruptIndexException("Invalid multiValued flag: " + multiValued, meta);
        }
        entry.ordsEntry = new SortedNumericEntry();
        readSortedNumeric(meta, entry.ordsEntry);
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

    private abstract static class NumericValues {
        abstract long advance(long index) throws IOException;
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
                return new NumericDocValues() {

                    private final int maxDoc = ES819TSDBDocValuesProducer.this.maxDoc;
                    private int doc = -1;

                    @Override
                    public long longValue() {
                        // Only one ordinal!
                        return 0L;
                    }

                    @Override
                    public int docID() {
                        return doc;
                    }

                    @Override
                    public int nextDoc() throws IOException {
                        return advance(doc + 1);
                    }

                    @Override
                    public int advance(int target) throws IOException {
                        if (target >= maxDoc) {
                            return doc = NO_MORE_DOCS;
                        }
                        return doc = target;
                    }

                    @Override
                    public boolean advanceExact(int target) {
                        doc = target;
                        return true;
                    }

                    @Override
                    public long cost() {
                        return maxDoc;
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
                return new NumericDocValues() {

                    @Override
                    public int advance(int target) throws IOException {
                        return disi.advance(target);
                    }

                    @Override
                    public boolean advanceExact(int target) throws IOException {
                        return disi.advanceExact(target);
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
                    public long longValue() {
                        return 0L;
                    }
                };
            }
        }

        // NOTE: we could make this a bit simpler by reusing #getValues but this
        // makes things slower.

        final RandomAccessInput indexSlice = data.randomAccessSlice(entry.indexOffset, entry.indexLength);
        final DirectMonotonicReader indexReader = DirectMonotonicReader.getInstance(entry.indexMeta, indexSlice, merging);
        final IndexInput valuesData = data.slice("values", entry.valuesOffset, entry.valuesLength);

        final int bitsPerOrd = maxOrd >= 0 ? PackedInts.bitsRequired(maxOrd - 1) : -1;
        if (entry.docsWithFieldOffset == -1) {
            // dense
            return new NumericDocValues() {

                private final int maxDoc = ES819TSDBDocValuesProducer.this.maxDoc;
                private int doc = -1;
                private final TSDBDocValuesEncoder decoder = new TSDBDocValuesEncoder(ES819TSDBDocValuesFormat.NUMERIC_BLOCK_SIZE);
                private long currentBlockIndex = -1;
                private final long[] currentBlock = new long[ES819TSDBDocValuesFormat.NUMERIC_BLOCK_SIZE];

                @Override
                public int docID() {
                    return doc;
                }

                @Override
                public int nextDoc() throws IOException {
                    return advance(doc + 1);
                }

                @Override
                public int advance(int target) throws IOException {
                    if (target >= maxDoc) {
                        return doc = NO_MORE_DOCS;
                    }
                    return doc = target;
                }

                @Override
                public boolean advanceExact(int target) {
                    doc = target;
                    return true;
                }

                @Override
                public long cost() {
                    return maxDoc;
                }

                @Override
                public long longValue() throws IOException {
                    final int index = doc;
                    final int blockIndex = index >>> ES819TSDBDocValuesFormat.NUMERIC_BLOCK_SHIFT;
                    final int blockInIndex = index & ES819TSDBDocValuesFormat.NUMERIC_BLOCK_MASK;
                    if (blockIndex != currentBlockIndex) {
                        assert blockIndex > currentBlockIndex : blockIndex + " < " + currentBlockIndex;
                        // no need to seek if the loading block is the next block
                        if (currentBlockIndex + 1 != blockIndex) {
                            valuesData.seek(indexReader.get(blockIndex));
                        }
                        currentBlockIndex = blockIndex;
                        if (maxOrd >= 0) {
                            decoder.decodeOrdinals(valuesData, currentBlock, bitsPerOrd);
                        } else {
                            decoder.decode(valuesData, currentBlock);
                        }
                    }
                    return currentBlock[blockInIndex];
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
            return new NumericDocValues() {

                private final TSDBDocValuesEncoder decoder = new TSDBDocValuesEncoder(ES819TSDBDocValuesFormat.NUMERIC_BLOCK_SIZE);
                private long currentBlockIndex = -1;
                private final long[] currentBlock = new long[ES819TSDBDocValuesFormat.NUMERIC_BLOCK_SIZE];

                @Override
                public int advance(int target) throws IOException {
                    return disi.advance(target);
                }

                @Override
                public boolean advanceExact(int target) throws IOException {
                    return disi.advanceExact(target);
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
                public long longValue() throws IOException {
                    final int index = disi.index();
                    final int blockIndex = index >>> ES819TSDBDocValuesFormat.NUMERIC_BLOCK_SHIFT;
                    final int blockInIndex = index & ES819TSDBDocValuesFormat.NUMERIC_BLOCK_MASK;
                    if (blockIndex != currentBlockIndex) {
                        assert blockIndex > currentBlockIndex : blockIndex + "<=" + currentBlockIndex;
                        // no need to seek if the loading block is the next block
                        if (currentBlockIndex + 1 != blockIndex) {
                            valuesData.seek(indexReader.get(blockIndex));
                        }
                        currentBlockIndex = blockIndex;
                        if (maxOrd >= 0) {
                            decoder.decodeOrdinals(valuesData, currentBlock, bitsPerOrd);
                        } else {
                            decoder.decode(valuesData, currentBlock);
                        }
                    }
                    return currentBlock[blockInIndex];
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
        return new NumericValues() {

            private final TSDBDocValuesEncoder decoder = new TSDBDocValuesEncoder(ES819TSDBDocValuesFormat.NUMERIC_BLOCK_SIZE);
            private long currentBlockIndex = -1;
            private final long[] currentBlock = new long[ES819TSDBDocValuesFormat.NUMERIC_BLOCK_SIZE];

            @Override
            long advance(long index) throws IOException {
                final long blockIndex = index >>> ES819TSDBDocValuesFormat.NUMERIC_BLOCK_SHIFT;
                final int blockInIndex = (int) (index & ES819TSDBDocValuesFormat.NUMERIC_BLOCK_MASK);
                if (blockIndex != currentBlockIndex) {
                    // no need to seek if the loading block is the next block
                    if (currentBlockIndex + 1 != blockIndex) {
                        valuesData.seek(indexReader.get(blockIndex));
                    }
                    currentBlockIndex = blockIndex;
                    if (bitsPerOrd >= 0) {
                        decoder.decodeOrdinals(valuesData, currentBlock, bitsPerOrd);
                    } else {
                        decoder.decode(valuesData, currentBlock);
                    }
                }
                return currentBlock[blockInIndex];
            }
        };
    }

    private SortedNumericDocValues getSortedNumeric(SortedNumericEntry entry, long maxOrd) throws IOException {
        if (entry.numValues == entry.numDocsWithField) {
            return DocValues.singleton(getNumeric(entry, maxOrd));
        }

        final RandomAccessInput addressesInput = data.randomAccessSlice(entry.addressesOffset, entry.addressesLength);
        final LongValues addresses = DirectMonotonicReader.getInstance(entry.addressesMeta, addressesInput, merging);

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
    }

    static class BinaryEntry {
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
        DirectMonotonicReader.Meta addressesMeta;
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

}
