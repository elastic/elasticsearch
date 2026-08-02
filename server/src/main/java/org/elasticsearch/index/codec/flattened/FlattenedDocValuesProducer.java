/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.flattened;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.codecs.compressing.Decompressor;
import org.apache.lucene.codecs.lucene90.IndexedDISI;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.index.codec.zstd.ZstdCompressionMode;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;
import static org.elasticsearch.index.codec.flattened.FlattenedDocValuesConsumer.FLATTENED_COLUMNAR_BINARY;
import static org.elasticsearch.index.codec.flattened.FlattenedDocValuesFormat.FLAG_ALL_SINGLE_SLOT;
import static org.elasticsearch.index.codec.flattened.FlattenedDocValuesFormat.FLAG_DOCS_CONTIGUOUS;
import static org.elasticsearch.index.codec.flattened.FlattenedDocValuesFormat.FLAG_VALUES_COMPRESSED;
import static org.elasticsearch.index.codec.flattened.FlattenedDocValuesFormat.VERSION_CURRENT;
import static org.elasticsearch.index.codec.flattened.FlattenedDocValuesFormat.VERSION_START;

/**
 * Doc values producer for the columnar flattened format.
 *
 * <p>See {@link FlattenedDocValuesFormat} for the full on-disk layout.
 *
 * <p>Reading one sub-field (via {@link ColumnarKeyedBinaryDocValues#advanceExactKey}) only
 * accesses that key's column. Reading the full blob (via {@link BinaryDocValues#binaryValue})
 * performs a lockstep walk over all columns in key-ordinal (lex) order.
 */
final class FlattenedDocValuesProducer extends DocValuesProducer {

    static final Decompressor DECOMPRESSOR = new ZstdCompressionMode(1).newDecompressor();

    private final IndexInput data;
    private final Map<String, BinaryEntry> entries;

    FlattenedDocValuesProducer(SegmentReadState state, String dataCodec, String dataExtension, String metaCodec, String metaExtension)
        throws IOException {
        final String dataName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, dataExtension);
        final String metaName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, metaExtension);

        this.entries = new HashMap<>();
        boolean success = false;
        IndexInput dataIn = null;
        try {
            dataIn = state.directory.openInput(dataName, state.context);
            CodecUtil.checkIndexHeader(dataIn, dataCodec, VERSION_START, VERSION_CURRENT, state.segmentInfo.getId(), state.segmentSuffix);

            try (ChecksumIndexInput metaIn = state.directory.openChecksumInput(metaName)) {
                CodecUtil.checkIndexHeader(
                    metaIn,
                    metaCodec,
                    VERSION_START,
                    VERSION_CURRENT,
                    state.segmentInfo.getId(),
                    state.segmentSuffix
                );
                Throwable priorE = null;
                try {
                    readFields(metaIn, dataIn, state.fieldInfos);
                } catch (Throwable e) {
                    priorE = e;
                } finally {
                    CodecUtil.checkFooter(metaIn, priorE);
                }
            }

            this.data = dataIn;
            success = true;
        } finally {
            if (success == false) {
                IOUtils.closeWhileHandlingException(dataIn);
            }
        }
    }

    /** Copy constructor used by {@link #getMergeInstance()}. */
    private FlattenedDocValuesProducer(FlattenedDocValuesProducer orig) throws IOException {
        this.data = orig.data.clone();
        this.entries = orig.entries;
    }

    private void readFields(ChecksumIndexInput meta, IndexInput data, FieldInfos fieldInfos) throws IOException {
        for (int fieldNumber = meta.readInt(); fieldNumber != -1; fieldNumber = meta.readInt()) {
            final FieldInfo info = fieldInfos.fieldInfo(fieldNumber);
            if (info == null) {
                throw new CorruptIndexException("invalid field number: " + fieldNumber, meta);
            }
            final byte type = meta.readByte();
            if (type != FLATTENED_COLUMNAR_BINARY) {
                throw new CorruptIndexException(
                    "unexpected DV type byte " + type + " for field " + info.name + "; expected " + FLATTENED_COLUMNAR_BINARY,
                    meta
                );
            }
            final BinaryEntry entry = readBinaryEntry(meta, data);
            entries.put(info.name, entry);
        }
    }

    private static BinaryEntry readBinaryEntry(ChecksumIndexInput meta, IndexInput data) throws IOException {
        final BinaryEntry e = new BinaryEntry();
        e.dataOffset = meta.readLong();
        e.docsWithFieldOffset = meta.readLong();
        e.docsWithFieldLength = meta.readLong();
        e.jumpTableEntryCount = meta.readShort();
        e.denseRankPower = meta.readByte();
        e.numDocsWithField = meta.readInt();
        e.numKeys = meta.readInt();
        e.keyDictOffset = meta.readLong();
        e.keyDictLength = meta.readLong();
        e.columnAddressTableOffset = meta.readLong();
        e.maxUncompressedBlockLen = meta.readVInt();
        e.maxDocsPerBlock = meta.readVInt();
        e.dataLength = meta.readLong();

        // Load key dictionary into memory (keys are in lex order; ordinal = lex rank).
        data.seek(e.keyDictOffset);
        loadKeyDictionary(data, e);

        // Load column address table into memory.
        data.seek(e.columnAddressTableOffset);
        loadColumnAddressTable(data, e);

        return e;
    }

    /**
     * Reads the key dictionary (keys in lex order, ordinal = lex rank) from the data file.
     *
     * <pre>
     * vint numKeys
     * per key in lex order:
     *   vint keyLen
     *   keyLen bytes
     * </pre>
     */
    private static void loadKeyDictionary(IndexInput data, BinaryEntry e) throws IOException {
        final int numKeysOnDisk = data.readVInt();
        if (numKeysOnDisk != e.numKeys) {
            throw new CorruptIndexException("key dictionary numKeys mismatch: expected " + e.numKeys + " but got " + numKeysOnDisk, data);
        }
        final int n = e.numKeys;
        if (n == 0) {
            e.keyBytes = new byte[0];
            e.keyOffsets = new int[1];
            return;
        }
        // Read key lengths first to know total size.
        final int[] keyLens = new int[n];
        final byte[][] rawKeys = new byte[n][];
        for (int ord = 0; ord < n; ord++) {
            keyLens[ord] = data.readVInt();
            rawKeys[ord] = new byte[keyLens[ord]];
            data.readBytes(rawKeys[ord], 0, keyLens[ord]);
        }
        e.keyOffsets = new int[n + 1];
        int totalBytes = 0;
        for (int ord = 0; ord < n; ord++) {
            e.keyOffsets[ord] = totalBytes;
            totalBytes += keyLens[ord];
        }
        e.keyOffsets[n] = totalBytes;
        e.keyBytes = new byte[totalBytes];
        for (int ord = 0; ord < n; ord++) {
            System.arraycopy(rawKeys[ord], 0, e.keyBytes, e.keyOffsets[ord], keyLens[ord]);
        }
    }

    /**
     * Reads the column address table (one 16-byte entry per key, in lex ordinal order) from the
     * data file into three parallel arrays on {@code e}.
     */
    private static void loadColumnAddressTable(IndexInput data, BinaryEntry e) throws IOException {
        final int n = e.numKeys;
        e.columnStartOffsets = new long[n];
        e.blockIndexRelOffsets = new int[n];
        e.numColumnBlocks = new int[n];
        for (int ord = 0; ord < n; ord++) {
            e.columnStartOffsets[ord] = data.readLong();
            e.blockIndexRelOffsets[ord] = data.readInt();
            e.numColumnBlocks[ord] = data.readInt();
        }
    }

    /**
     * Returns the {@link BinaryEntry} for {@code fieldName}, or {@code null} if this producer
     * has no entry for that field (e.g. the field was absent in this segment).
     */
    BinaryEntry entryFor(String fieldName) {
        return entries.get(fieldName);
    }

    /**
     * Returns a clone of the data {@link IndexInput} for independent sequential reading.
     * The caller is responsible for closing the returned clone.
     */
    IndexInput cloneDataInput() {
        return data.clone();
    }

    @Override
    public BinaryDocValues getBinary(FieldInfo field) throws IOException {
        final BinaryEntry entry = entries.get(field.name);
        if (entry == null) {
            throw new IllegalArgumentException("Unknown field: " + field.name);
        }
        if (entry.docsWithFieldOffset == -2L) {
            return emptyBinary();
        }
        final IndexInput dataIn = data.clone();
        if (entry.docsWithFieldOffset == -1L) {
            return new DenseFlattenedBinaryDocValues(entry, dataIn);
        } else {
            final IndexedDISI disi = new IndexedDISI(
                dataIn,
                entry.docsWithFieldOffset,
                entry.docsWithFieldLength,
                entry.jumpTableEntryCount,
                entry.denseRankPower,
                entry.numDocsWithField
            );
            return new SparseFlattenedBinaryDocValues(entry, dataIn, disi);
        }
    }

    @Override
    public void checkIntegrity() throws IOException {
        CodecUtil.checksumEntireFile(data);
    }

    @Override
    public DocValuesProducer getMergeInstance() {
        try {
            return new FlattenedDocValuesProducer(this);
        } catch (IOException e) {
            throw new RuntimeException("Failed to clone data file for merge", e);
        }
    }

    @Override
    public org.apache.lucene.index.DocValuesSkipper getSkipper(FieldInfo field) throws IOException {
        return null;
    }

    @Override
    public void close() throws IOException {
        data.close();
    }

    @Override
    public NumericDocValues getNumeric(FieldInfo field) {
        throw unsupported(field, "NUMERIC");
    }

    @Override
    public SortedDocValues getSorted(FieldInfo field) {
        throw unsupported(field, "SORTED");
    }

    @Override
    public SortedNumericDocValues getSortedNumeric(FieldInfo field) {
        throw unsupported(field, "SORTED_NUMERIC");
    }

    @Override
    public SortedSetDocValues getSortedSet(FieldInfo field) {
        throw unsupported(field, "SORTED_SET");
    }

    private static UnsupportedOperationException unsupported(FieldInfo field, String type) {
        return new UnsupportedOperationException(
            "["
                + FlattenedDocValuesFormat.CODEC_NAME
                + "] only supports BINARY doc values for flattened keyed fields, got ["
                + type
                + "] for field ["
                + field.name
                + "]; this indicates a PerFieldFormatSupplier#getDocValuesFormatForField dispatch bug"
        );
    }

    // ---------------------------------------------------------------------------------
    // BinaryEntry: per-field metadata loaded from the meta file.
    // ---------------------------------------------------------------------------------

    static final class BinaryEntry {
        long dataOffset;
        long dataLength;

        long docsWithFieldOffset;   // -2 = empty, -1 = dense, else DISI offset
        long docsWithFieldLength;
        short jumpTableEntryCount;
        byte denseRankPower;
        int numDocsWithField;

        int numKeys;
        long keyDictOffset;
        long keyDictLength;
        long columnAddressTableOffset;
        int maxUncompressedBlockLen;
        int maxDocsPerBlock;

        // Key dictionary (lex order; ordinal = lex rank).
        byte[] keyBytes;    // all key bytes concatenated in lex ordinal order
        int[] keyOffsets;   // keyOffsets[ord] = start of key ord in keyBytes; length = numKeys + 1

        // Column address table (loaded into memory for fast cursor construction).
        long[] columnStartOffsets;   // [numKeys] absolute data-file offset of column's block 0
        int[] blockIndexRelOffsets;  // [numKeys] byte offset of block index from columnStartOffset
        int[] numColumnBlocks;       // [numKeys] number of blocks in this column
    }

    // ---------------------------------------------------------------------------------
    // ColumnCursor: reads one column for a single sub-field key.
    // ---------------------------------------------------------------------------------

    /**
     * Reads blocks from one column. Created lazily per key ordinal on first access.
     *
     * <p>The cursor maintains a block index (eagerly loaded: typically 1–10 entries) and a
     * lazily-decompressed payload for the current block. Forward scans within a block are
     * O(docs skipped); backwards movement (rare) reloads the block header and resets the cursor.
     */
    static final class ColumnCursor {
        private final IndexInput dataIn;          // cloned; independent file position
        private final long columnStartOff;        // absolute data-file offset of block 0
        private final long blockIndexOff;         // absolute offset of block index start
        private final int numBlocks;

        // Block index (eagerly loaded, always in-memory).
        private final int[] firstDocIds;         // firstDocIds[b] = first docId in block b
        private final int[] blockRelOffsets;     // blockRelOffsets[b] = byte offset from columnStartOff

        // Loaded block header (populated by loadBlockHeader).
        private int loadedBlock = -1;
        private int numDocsInBlock;
        private int firstDocInBlock;
        private boolean contiguous;
        private boolean allSingleSlot;
        private boolean payloadCompressed;
        private int[] docIds;        // resolved docIds (null when contiguous)
        private int[] slotCounts;    // slot counts per doc (null when allSingleSlot)
        private int uncompPayloadLen;
        private long payloadAbsOff;  // absolute position of [vint compressedLen?][payload bytes] in data file

        // Decompressed payload (lazy).
        private byte[] payload;
        private boolean payloadLoaded;

        // Cursor state within current block.
        private int docCursorIdx = -1;  // last doc index consumed (−1 = before first)
        private int payloadCursor = 0;  // byte offset in payload[]
        private int slotsRemaining = 0;

        // Reusable slot result (reset for each nextSlot() call that returns non-null).
        private byte[] slotBytes = new byte[64];
        private final BytesRef slotResult = new BytesRef(slotBytes);

        ColumnCursor(IndexInput data, long columnStartOff, int blockIndexRelOff, int numBlocks) throws IOException {
            this.dataIn = data.clone();
            this.columnStartOff = columnStartOff;
            this.blockIndexOff = columnStartOff + blockIndexRelOff;
            this.numBlocks = numBlocks;
            this.firstDocIds = new int[numBlocks];
            this.blockRelOffsets = new int[numBlocks];
            // Eagerly load the block index (8 bytes per block, typically very small).
            this.dataIn.seek(blockIndexOff);
            for (int b = 0; b < numBlocks; b++) {
                firstDocIds[b] = this.dataIn.readInt();
                blockRelOffsets[b] = this.dataIn.readInt();
            }
            this.payload = new byte[256];
            this.docIds = new int[8];
            this.slotCounts = new int[8];
        }

        /**
         * Finds the block index b such that {@code firstDocIds[b] <= docId < firstDocIds[b+1]},
         * or returns -1 if {@code docId < firstDocIds[0]}.
         */
        private int findBlockFor(int docId) {
            if (numBlocks == 0 || docId < firstDocIds[0]) return -1;
            int lo = 0, hi = numBlocks - 1;
            while (lo < hi) {
                final int mid = (lo + hi + 1) >>> 1;
                if (firstDocIds[mid] <= docId) lo = mid;
                else hi = mid - 1;
            }
            return lo;
        }

        /**
         * Loads the header of block {@code blockIdx} (flags, numDocs, docIds, slotCounts).
         * The payload is not decompressed yet.
         */
        private void loadBlockHeader(int blockIdx) throws IOException {
            if (loadedBlock == blockIdx) return;
            loadedBlock = blockIdx;
            payloadLoaded = false;
            docCursorIdx = -1;
            payloadCursor = 0;
            slotsRemaining = 0;

            dataIn.seek(columnStartOff + blockRelOffsets[blockIdx]);

            final byte flags = dataIn.readByte();
            contiguous = (flags & FLAG_DOCS_CONTIGUOUS) != 0;
            allSingleSlot = (flags & FLAG_ALL_SINGLE_SLOT) != 0;
            payloadCompressed = (flags & FLAG_VALUES_COMPRESSED) != 0;

            numDocsInBlock = dataIn.readVInt();
            firstDocInBlock = firstDocIds[blockIdx];

            if (contiguous == false) {
                if (docIds.length < numDocsInBlock) docIds = new int[numDocsInBlock];
                docIds[0] = firstDocInBlock;
                for (int i = 1; i < numDocsInBlock; i++) {
                    docIds[i] = docIds[i - 1] + dataIn.readVInt() + 1;
                }
            }
            if (allSingleSlot == false) {
                if (slotCounts.length < numDocsInBlock) slotCounts = new int[numDocsInBlock];
                for (int i = 0; i < numDocsInBlock; i++) {
                    slotCounts[i] = dataIn.readVInt();
                }
            }
            uncompPayloadLen = dataIn.readVInt();
            // payloadAbsOff points at [vint compressedLen][bytes] for compressed blocks,
            // or directly at [bytes] for raw blocks. The decompressor reads the vint itself.
            payloadAbsOff = dataIn.getFilePointer();
        }

        /** Decompresses (or reads) the payload for the current loaded block, if not already done. */
        private void ensurePayloadLoaded() throws IOException {
            if (payloadLoaded) return;
            if (payload.length < uncompPayloadLen) {
                payload = new byte[uncompPayloadLen];
            }
            dataIn.seek(payloadAbsOff);
            if (payloadCompressed) {
                // ZstdCompressionMode wrote [vint compressedLen][compressedBytes]; the decompressor
                // reads the vint prefix from dataIn itself.
                final BytesRef decompRef = new BytesRef(payload, 0, uncompPayloadLen);
                DECOMPRESSOR.decompress(dataIn, uncompPayloadLen, 0, uncompPayloadLen, decompRef);
                payload = decompRef.bytes;
            } else {
                dataIn.readBytes(payload, 0, uncompPayloadLen);
            }
            payloadLoaded = true;
        }

        /**
         * Returns the index of {@code docId} within the current loaded block, or -1 if absent.
         * Caller must have called {@link #loadBlockHeader} first.
         */
        private int findDocInBlock(int docId) {
            if (contiguous) {
                final int idx = docId - firstDocInBlock;
                return (idx >= 0 && idx < numDocsInBlock) ? idx : -1;
            }
            int lo = 0, hi = numDocsInBlock - 1;
            while (lo <= hi) {
                final int mid = (lo + hi) >>> 1;
                if (docIds[mid] < docId) lo = mid + 1;
                else if (docIds[mid] > docId) hi = mid - 1;
                else return mid;
            }
            return -1;
        }

        /** Skips {@code count} slots in {@link #payload} starting at {@link #payloadCursor}. */
        private void skipSlots(int count) {
            for (int i = 0; i < count; i++) {
                int prefix = 0, shift = 0;
                while (true) {
                    final int b = payload[payloadCursor++] & 0xFF;
                    prefix |= (b & 0x7F) << shift;
                    if ((b & 0x80) == 0) break;
                    shift += 7;
                }
                if (prefix > 0) payloadCursor += prefix - 1;
            }
        }

        /**
         * Positions this cursor on {@code docId}.
         *
         * @return the slot count for this doc (1 if allSingleSlot), or 0 if not present
         */
        int advanceToDoc(int docId) throws IOException {
            final int blockIdx = findBlockFor(docId);
            if (blockIdx < 0) return 0;
            loadBlockHeader(blockIdx);
            final int docIdx = findDocInBlock(docId);
            if (docIdx < 0) return 0;
            ensurePayloadLoaded();
            if (docIdx < docCursorIdx) {
                // Backwards movement: reset payload cursor to start of block payload.
                payloadCursor = 0;
                docCursorIdx = -1;
                slotsRemaining = 0;
            }
            // Skip payload for docs before docIdx.
            for (int i = docCursorIdx + 1; i < docIdx; i++) {
                skipSlots(allSingleSlot ? 1 : slotCounts[i]);
            }
            docCursorIdx = docIdx;
            slotsRemaining = allSingleSlot ? 1 : slotCounts[docIdx];
            return slotsRemaining;
        }

        /**
         * Returns the next slot value for the current doc, or {@code null} for a null slot.
         * Returns {@code null} with {@link BytesRef#length} == -1 when all slots are exhausted.
         */
        BytesRef nextSlot() {
            if (slotsRemaining <= 0) {
                slotResult.length = -1;
                return null;
            }
            slotsRemaining--;
            int prefix = 0, shift = 0;
            while (true) {
                final int b = payload[payloadCursor++] & 0xFF;
                prefix |= (b & 0x7F) << shift;
                if ((b & 0x80) == 0) break;
                shift += 7;
            }
            if (prefix == 0) {
                // null slot
                return null;
            }
            final int valLen = prefix - 1;
            if (slotBytes.length < valLen) {
                slotBytes = new byte[valLen];
                slotResult.bytes = slotBytes;
            }
            System.arraycopy(payload, payloadCursor, slotBytes, 0, valLen);
            payloadCursor += valLen;
            slotResult.offset = 0;
            slotResult.length = valLen;
            return slotResult;
        }
    }

    // ---------------------------------------------------------------------------------
    // ColumnarKeyedBinaryDocValues base class (dense and sparse share this).
    // ---------------------------------------------------------------------------------

    private abstract static class FlattenedBinaryDocValues extends ColumnarKeyedBinaryDocValues {

        protected final BinaryEntry entry;
        protected final IndexInput dataIn;

        protected int currentDocId = -1;

        // One ColumnCursor per key ordinal, lazily allocated.
        private final ColumnCursor[] columnCursors;

        // Active key ordinal after advanceExactKey().
        private int activeKeyOrd = -1;

        // Output buffers for binaryValue().
        private byte[] bvBuf = new byte[256];
        private final BytesRef bvResult = new BytesRef();

        FlattenedBinaryDocValues(BinaryEntry entry, IndexInput dataIn) {
            this.entry = entry;
            this.dataIn = dataIn;
            this.columnCursors = new ColumnCursor[entry.numKeys];
        }

        /** Returns (or lazily creates) the ColumnCursor for key ordinal {@code ord}. */
        private ColumnCursor cursor(int ord) throws IOException {
            ColumnCursor c = columnCursors[ord];
            if (c == null) {
                c = new ColumnCursor(dataIn, entry.columnStartOffsets[ord], entry.blockIndexRelOffsets[ord], entry.numColumnBlocks[ord]);
                columnCursors[ord] = c;
            }
            return c;
        }

        @Override
        public int docID() {
            return currentDocId;
        }

        @Override
        public long cost() {
            return entry.numDocsWithField;
        }

        // ---- ColumnarKeyedBinaryDocValues API ----

        /**
         * Binary search for {@code key} in the lex-ordered dictionary.
         * Since ordinal = lex rank, the returned value is the ordinal directly.
         */
        @Override
        public int lookupKeyOrdinal(BytesRef key) {
            int lo = 0, hi = entry.numKeys - 1;
            while (lo <= hi) {
                final int mid = (lo + hi) >>> 1;
                final int keyStart = entry.keyOffsets[mid];
                final int keyLen = entry.keyOffsets[mid + 1] - keyStart;
                final int cmp = compareKey(key, entry.keyBytes, keyStart, keyLen);
                if (cmp < 0) hi = mid - 1;
                else if (cmp > 0) lo = mid + 1;
                else return mid;
            }
            return -1;
        }

        private static int compareKey(BytesRef key, byte[] dictBytes, int dictStart, int dictLen) {
            final int minLen = Math.min(key.length, dictLen);
            for (int i = 0; i < minLen; i++) {
                final int diff = (key.bytes[key.offset + i] & 0xFF) - (dictBytes[dictStart + i] & 0xFF);
                if (diff != 0) return diff;
            }
            return key.length - dictLen;
        }

        @Override
        public int advanceExactKey(int keyOrdinal) throws IOException {
            if (keyOrdinal < 0 || keyOrdinal >= entry.numKeys) {
                activeKeyOrd = -1;
                return 0;
            }
            activeKeyOrd = keyOrdinal;
            return cursor(keyOrdinal).advanceToDoc(currentDocId);
        }

        @Override
        public BytesRef nextKeyValue() throws IOException {
            if (activeKeyOrd < 0) return null;
            final BytesRef slot = columnCursors[activeKeyOrd].nextSlot();
            // slot.length == -1 signals exhaustion (per ColumnCursor contract).
            if (slot != null && slot.length == -1) return null;
            return slot;
        }

        @Override
        public BytesRef binaryValue() throws IOException {
            final int docId = currentDocId;
            int off = 0;

            for (int ord = 0; ord < entry.numKeys; ord++) {
                final int slotCount = cursor(ord).advanceToDoc(docId);
                if (slotCount == 0) continue;

                final int keyStart = entry.keyOffsets[ord];
                final int keyLen = entry.keyOffsets[ord + 1] - keyStart;

                for (int s = 0; s < slotCount; s++) {
                    final BytesRef slot = columnCursors[ord].nextSlot();
                    final boolean isNull = (slot == null);
                    final int valLen = isNull ? 0 : slot.length;
                    final int prefix = isNull ? 0 : valLen + 1;

                    bvBuf = ensureCap(bvBuf, off + 5 + keyLen + 1 + valLen);
                    off = writeVInt(bvBuf, off, prefix);
                    System.arraycopy(entry.keyBytes, keyStart, bvBuf, off, keyLen);
                    off += keyLen;
                    bvBuf[off++] = 0; // \0 separator
                    if (isNull == false && valLen > 0) {
                        System.arraycopy(slot.bytes, slot.offset, bvBuf, off, valLen);
                        off += valLen;
                    }
                }
            }

            bvResult.bytes = bvBuf;
            bvResult.offset = 0;
            bvResult.length = off;
            return bvResult;
        }

        private static byte[] ensureCap(byte[] buf, int needed) {
            if (buf.length >= needed) return buf;
            final byte[] n = new byte[Math.max(needed, buf.length * 2)];
            System.arraycopy(buf, 0, n, 0, buf.length);
            return n;
        }

        private static int writeVInt(byte[] buf, int off, int v) {
            while ((v & ~0x7F) != 0) {
                buf[off++] = (byte) ((v & 0x7F) | 0x80);
                v >>>= 7;
            }
            buf[off++] = (byte) v;
            return off;
        }
    }

    // ---------------------------------------------------------------------------------
    // Dense (all docs have this field) implementation.
    // ---------------------------------------------------------------------------------

    private static final class DenseFlattenedBinaryDocValues extends FlattenedBinaryDocValues {

        private int nextDocId = 0;
        private final int maxDocId;

        DenseFlattenedBinaryDocValues(BinaryEntry entry, IndexInput dataIn) {
            super(entry, dataIn);
            this.maxDocId = entry.numDocsWithField; // = maxDoc for dense
        }

        @Override
        public int nextDoc() throws IOException {
            if (nextDocId >= maxDocId) {
                currentDocId = NO_MORE_DOCS;
                return NO_MORE_DOCS;
            }
            currentDocId = nextDocId++;
            return currentDocId;
        }

        @Override
        public int advance(int target) throws IOException {
            if (target >= maxDocId) {
                currentDocId = NO_MORE_DOCS;
                return NO_MORE_DOCS;
            }
            currentDocId = target;
            nextDocId = target + 1;
            return currentDocId;
        }

        @Override
        public boolean advanceExact(int target) throws IOException {
            currentDocId = target;
            if (target >= maxDocId) return false;
            nextDocId = target + 1;
            return true;
        }
    }

    // ---------------------------------------------------------------------------------
    // Sparse (IndexedDISI) implementation.
    // ---------------------------------------------------------------------------------

    private static final class SparseFlattenedBinaryDocValues extends FlattenedBinaryDocValues {

        private final IndexedDISI disi;

        SparseFlattenedBinaryDocValues(BinaryEntry entry, IndexInput dataIn, IndexedDISI disi) {
            super(entry, dataIn);
            this.disi = disi;
        }

        @Override
        public int nextDoc() throws IOException {
            final int doc = disi.nextDoc();
            currentDocId = doc;
            return doc;
        }

        @Override
        public int advance(int target) throws IOException {
            final int doc = disi.advance(target);
            currentDocId = doc;
            return doc;
        }

        @Override
        public boolean advanceExact(int target) throws IOException {
            currentDocId = target;
            return disi.advanceExact(target);
        }
    }

    private static BinaryDocValues emptyBinary() {
        return new BinaryDocValues() {
            @Override
            public int nextDoc() {
                return NO_MORE_DOCS;
            }

            @Override
            public int docID() {
                return NO_MORE_DOCS;
            }

            @Override
            public long cost() {
                return 0;
            }

            @Override
            public int advance(int target) {
                return NO_MORE_DOCS;
            }

            @Override
            public boolean advanceExact(int target) {
                return false;
            }

            @Override
            public BytesRef binaryValue() {
                throw new IllegalStateException("advanceExact was false");
            }
        };
    }
}
