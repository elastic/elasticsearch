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
import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocIDMerger;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.ByteBlockPool;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefHash;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.index.codec.FilterDocValuesProducer;
import org.elasticsearch.index.codec.perfield.XPerFieldDocValuesFormat;
import org.elasticsearch.index.codec.tsdb.DISIAccumulator;
import org.elasticsearch.index.engine.PruningMergePolicy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;
import static org.elasticsearch.index.codec.flattened.FlattenedDocValuesFormat.COLUMN_ADDRESS_ENTRY_BYTES;
import static org.elasticsearch.index.codec.flattened.FlattenedDocValuesFormat.VERSION_CURRENT;

/**
 * Doc values consumer for the columnar flattened format.
 *
 * <p>Only {@link org.apache.lucene.index.DocValuesType#BINARY} doc values are supported. Any
 * attempt to write other types throws {@link UnsupportedOperationException}, because this format
 * is only dispatched for the {@code ._keyed} binary field of a {@code flattened} field with
 * {@code layout: columnar}.
 *
 * <p>Merge is handled by {@link #mergeBinaryField}: when all source segments are columnar, it
 * performs a column-wise merge that reads each sub-field's column sequentially and writes blocks
 * directly into the output, avoiding the expensive round-trip through {@link BinaryDocValues}
 * blobs and {@link #addBinaryField}. When any source segment is not columnar (mixed merge), or
 * when the kill switch {@code es.flattened.mergeColumnWise=false} is set, the inherited default
 * from {@link DocValuesConsumer} is used instead.
 *
 * <h2>Meta layout per field</h2>
 *
 * <pre>
 * int   fieldNumber
 * byte  FLATTENED_COLUMNAR_BINARY (0)
 * long  dataOffset                  — start of this field's data region in the data file
 * long  docsWithFieldOffset         — -2 = empty, -1 = dense, else IndexedDISI offset in data
 * long  docsWithFieldLength
 * short jumpTableEntryCount         — -1 for dense/empty
 * byte  denseRankPower              — -1 for dense/empty
 * int   numDocsWithField
 * int   numKeys
 * long  keyDictOffset               — key dictionary offset in data file
 * long  keyDictLength
 * long  columnAddressTableOffset    — column address table offset in data file
 * vint  maxUncompressedBlockLen     — for reader buffer pre-sizing
 * vint  maxDocsPerBlock             — for reader buffer pre-sizing
 * long  dataLength                  — total bytes of this field's data region
 * </pre>
 */
final class FlattenedDocValuesConsumer extends DocValuesConsumer {

    /** Sentinel in meta to mark end-of-fields. */
    private static final int FIELD_EOF = -1;

    /** DV type code written to the meta file for our one supported type. */
    static final byte FLATTENED_COLUMNAR_BINARY = 0;

    /**
     * Default dense-rank power (every 512 docIDs). Valid range for {@link DISIAccumulator}: 7–15 or -1.
     */
    static final byte DEFAULT_DENSE_RANK_POWER = (byte) 9;

    private IndexOutput data;
    private IndexOutput meta;

    private final int maxDoc;
    private final SegmentWriteState state;
    private final int targetBlockBytes;
    private final int maxDocsPerBlock;
    private final int minCompressBytes;
    private final int maxBufferedBytes;

    FlattenedDocValuesConsumer(
        SegmentWriteState state,
        String dataCodec,
        String dataExtension,
        String metaCodec,
        String metaExtension,
        int targetBlockBytes,
        int maxDocsPerBlock,
        int minCompressBytes,
        int maxBufferedBytes
    ) throws IOException {
        this.state = state;
        this.maxDoc = state.segmentInfo.maxDoc();
        this.targetBlockBytes = targetBlockBytes;
        this.maxDocsPerBlock = maxDocsPerBlock;
        this.minCompressBytes = minCompressBytes;
        this.maxBufferedBytes = maxBufferedBytes;

        boolean success = false;
        try {
            final String dataName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, dataExtension);
            data = state.directory.createOutput(dataName, state.context);
            CodecUtil.writeIndexHeader(data, dataCodec, VERSION_CURRENT, state.segmentInfo.getId(), state.segmentSuffix);

            final String metaName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, metaExtension);
            meta = state.directory.createOutput(metaName, state.context);
            CodecUtil.writeIndexHeader(meta, metaCodec, VERSION_CURRENT, state.segmentInfo.getId(), state.segmentSuffix);

            success = true;
        } finally {
            if (success == false) {
                IOUtils.closeWhileHandlingException(this);
            }
        }
    }

    @Override
    public void addBinaryField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
        meta.writeInt(field.number);
        meta.writeByte(FLATTENED_COLUMNAR_BINARY);
        final long dataOffset = data.getFilePointer();
        meta.writeLong(dataOffset);

        final BytesRefHash keyHash = new BytesRefHash(new ByteBlockPool(new ByteBlockPool.DirectAllocator()));
        final DISIAccumulator disiAcc = new DISIAccumulator(state.directory, state.context, data, DEFAULT_DENSE_RANK_POWER);
        // Accumulates (keyOrd, docId, preEncodedSlot) triples for the external sort.
        final SortedSlotAccumulator slotAcc = new SortedSlotAccumulator(state.directory, state.context, maxBufferedBytes);

        int numDocsWithField = 0;
        int prevDocId = -1;
        boolean isDense = true;
        // Reusable buffer for pre-encoding one slot as [vint prefix][value bytes] for the accumulator.
        byte[] slotBuf = new byte[64];

        try {
            final BinaryDocValues values = valuesProducer.getBinary(field);

            for (int doc = values.nextDoc(); doc != NO_MORE_DOCS; doc = values.nextDoc()) {
                numDocsWithField++;
                if (doc != prevDocId + 1) isDense = false;
                prevDocId = doc;
                disiAcc.addDocId(doc);

                final BytesRef blob = values.binaryValue();
                // Parse KeyedArrayOrderInlineNull framing: [vint prefix][key bytes]\0[value bytes]...
                int pos = blob.offset;
                final int end = blob.offset + blob.length;
                while (pos < end) {
                    int prefix = 0, b, shift = 0;
                    do {
                        b = blob.bytes[pos++] & 0xFF;
                        prefix |= (b & 0x7F) << shift;
                        shift += 7;
                    } while ((b & 0x80) != 0);
                    final boolean isNull = (prefix == 0);
                    final int valueLen = isNull ? 0 : (prefix - 1);

                    final int keyStart = pos;
                    int sep = keyStart;
                    while (sep < end && blob.bytes[sep] != 0) {
                        sep++;
                    }
                    final int keyLen = sep - keyStart;
                    pos = sep + 1; // skip \0; pos now points to value bytes

                    int ord = keyHash.add(new BytesRef(blob.bytes, keyStart, keyLen));
                    if (ord < 0) ord = -ord - 1; // already present

                    // Store slot in accumulator as [vint (isNull?0:valueLen+1)][value bytes]
                    // so the cursor drain loop can decode it when assembling the writer call.
                    final int encodedPrefix = isNull ? 0 : (valueLen + 1);
                    if (slotBuf.length < 5 + valueLen) slotBuf = new byte[5 + valueLen];
                    int pLen = writeVIntToArray(slotBuf, 0, encodedPrefix);
                    if (valueLen > 0) {
                        System.arraycopy(blob.bytes, pos, slotBuf, pLen, valueLen);
                        pLen += valueLen;
                    }
                    slotAcc.add(ord, doc, slotBuf, 0, pLen);
                    pos += valueLen;
                }
            }

            // -----------------------------------------------------------------------
            // Sort slot records by (lexRank, docId) and write columns directly into data.
            // -----------------------------------------------------------------------
            final int numKeys = keyHash.size();
            final int[] sortedOrds = keyHash.sort(); // sortedOrds[lexRank] = hashOrd
            final int[] lexRankOf = new int[numKeys];
            for (int lr = 0; lr < numKeys; lr++) {
                lexRankOf[sortedOrds[lr]] = lr;
            }

            final FieldBlockWriter.ColumnAddress[] addresses = new FieldBlockWriter.ColumnAddress[numKeys];
            int maxUncompressedBlockLen = 0;
            int maxDocsPerBlockSeen = 0;

            try (SortedSlotAccumulator.SortedCursor cursor = slotAcc.sortedCursor(lexRankOf)) {
                // Allocate one writer and reuse it across all columns via reset(). This avoids
                // allocating ~128 KiB of block-accumulation arrays and a ZSTD compressor per key.
                final FieldBlockWriter writer = new FieldBlockWriter(data, targetBlockBytes, maxDocsPerBlock, minCompressBytes);
                int prevLexRank = -1;
                int prevDoc = -1;
                int slotCount = 0;
                // Per-doc accumulation buffers in the new format: decoded lengths and raw value bytes.
                int[] docSlotLens = new int[8];
                byte[] docValues = new byte[256];
                int docValuesLen = 0;

                while (cursor.next()) {
                    final int lr = cursor.lexRank();
                    final int curDoc = cursor.docId();

                    if (lr != prevLexRank) {
                        if (prevLexRank >= 0) {
                            if (prevDoc >= 0) {
                                writer.addDocSlots(prevDoc, slotCount, docSlotLens, 0, docValues, 0, docValuesLen);
                            }
                            writer.finish();
                            maxUncompressedBlockLen = Math.max(maxUncompressedBlockLen, writer.maxUncompressedBlockLen);
                            maxDocsPerBlockSeen = Math.max(maxDocsPerBlockSeen, writer.maxDocsPerBlockSeen);
                            addresses[prevLexRank] = writer.columnAddress();
                            writer.reset(data);
                        }
                        prevLexRank = lr;
                        prevDoc = -1;
                        slotCount = 0;
                        docValuesLen = 0;
                    }

                    if (curDoc != prevDoc) {
                        if (prevDoc >= 0) {
                            writer.addDocSlots(prevDoc, slotCount, docSlotLens, 0, docValues, 0, docValuesLen);
                        }
                        prevDoc = curDoc;
                        slotCount = 0;
                        docValuesLen = 0;
                    }

                    // Decode the accumulator record: [vint prefix][value bytes].
                    // prefix == 0 → null; prefix == N+1 → N value bytes.
                    final byte[] recBytes = cursor.payloadBytes();
                    int recPos = cursor.payloadOffset();
                    int prefix = 0, shift = 0, b;
                    do {
                        b = recBytes[recPos++] & 0xFF;
                        prefix |= (b & 0x7F) << shift;
                        shift += 7;
                    } while ((b & 0x80) != 0);
                    final int valueLen = (prefix == 0) ? -1 : (prefix - 1); // -1 = null
                    docSlotLens = ArrayUtil.grow(docSlotLens, slotCount + 1);
                    docSlotLens[slotCount] = valueLen;
                    if (valueLen > 0) {
                        docValues = ArrayUtil.grow(docValues, docValuesLen + valueLen);
                        System.arraycopy(recBytes, recPos, docValues, docValuesLen, valueLen);
                        docValuesLen += valueLen;
                    }
                    slotCount++;
                }

                // Flush the last doc and last key.
                if (prevLexRank >= 0) {
                    if (prevDoc >= 0) {
                        writer.addDocSlots(prevDoc, slotCount, docSlotLens, 0, docValues, 0, docValuesLen);
                    }
                    writer.finish();
                    maxUncompressedBlockLen = Math.max(maxUncompressedBlockLen, writer.maxUncompressedBlockLen);
                    maxDocsPerBlockSeen = Math.max(maxDocsPerBlockSeen, writer.maxDocsPerBlockSeen);
                    addresses[prevLexRank] = writer.columnAddress();
                }
            }

            // Write DISI (sparse doc presence) after all column data.
            final long docsWithFieldOffset;
            final long docsWithFieldLength;
            final short jumpTableEntryCount;
            final byte denseRankPower;
            if (numDocsWithField == 0) {
                docsWithFieldOffset = -2L;
                docsWithFieldLength = 0L;
                jumpTableEntryCount = -1;
                denseRankPower = -1;
            } else if (numDocsWithField == maxDoc && isDense) {
                docsWithFieldOffset = -1L;
                docsWithFieldLength = 0L;
                jumpTableEntryCount = -1;
                denseRankPower = -1;
            } else {
                final long disiStart = data.getFilePointer();
                docsWithFieldOffset = disiStart;
                jumpTableEntryCount = disiAcc.build(data);
                docsWithFieldLength = data.getFilePointer() - disiStart;
                denseRankPower = DEFAULT_DENSE_RANK_POWER;
            }

            // Write key dictionary in lex order.
            final long keyDictOffset = data.getFilePointer();
            writeKeyDictionary(keyHash, numKeys, sortedOrds);
            final long keyDictLength = data.getFilePointer() - keyDictOffset;

            // Write column address table: one fixed-width entry per key, in lex ordinal order.
            final long columnAddressTableOffset = data.getFilePointer();
            assert COLUMN_ADDRESS_ENTRY_BYTES == 16; // long + int + int
            for (int lexRank = 0; lexRank < numKeys; lexRank++) {
                final FieldBlockWriter.ColumnAddress addr = addresses[lexRank];
                if (addr != null) {
                    data.writeLong(addr.columnStartOffset());
                    data.writeInt(addr.blockIndexRelativeOffset());
                    data.writeInt(addr.numBlocks());
                } else {
                    data.writeLong(0L);
                    data.writeInt(0);
                    data.writeInt(0);
                }
            }

            final long dataLength = data.getFilePointer() - dataOffset;

            meta.writeLong(docsWithFieldOffset);
            meta.writeLong(docsWithFieldLength);
            meta.writeShort(jumpTableEntryCount);
            meta.writeByte(denseRankPower);
            meta.writeInt(numDocsWithField);
            meta.writeInt(numKeys);
            meta.writeLong(keyDictOffset);
            meta.writeLong(keyDictLength);
            meta.writeLong(columnAddressTableOffset);
            meta.writeVInt(maxUncompressedBlockLen);
            meta.writeVInt(maxDocsPerBlockSeen);
            meta.writeLong(dataLength);

        } finally {
            IOUtils.close(disiAcc, slotAcc);
        }
    }

    /**
     * {@inheritDoc}
     *
     * <p>When all source segments use the columnar flattened format, performs a column-wise merge:
     * for each merged sub-field key (in lex order), a {@link DocIDMerger} walks source documents
     * in target-docID order and bulk-copies each doc's slot bytes directly from the decompressed
     * source block into a new block. Blocks are written straight into the data output — no temp
     * files, no splice pass, no {@link BytesRefHash} re-hashing. Work is O(total slots) with
     * sequential I/O per source segment.
     *
     * <p>Falls back to the inherited row-based merge (which calls {@link #addBinaryField}) when:
     * <ul>
     *   <li>The kill switch {@code es.flattened.mergeColumnWise=false} is set.</li>
     *   <li>Any source segment's producer cannot be unwrapped to a {@link FlattenedDocValuesProducer}
     *       (e.g. a mixed row/columnar merge, or a {@link PruningMergePolicy} pruning the field).</li>
     * </ul>
     */
    @Override
    public void mergeBinaryField(FieldInfo mergeFieldInfo, MergeState mergeState) throws IOException {
        if (FlattenedDocValuesFormat.MERGE_COLUMN_WISE_ENABLED == false || tryColumnWiseMerge(mergeFieldInfo, mergeState) == false) {
            super.mergeBinaryField(mergeFieldInfo, mergeState);
        }
    }

    /**
     * Attempts the column-wise merge. Returns {@code true} on success, {@code false} if any
     * source segment cannot be unwrapped to a {@link FlattenedDocValuesProducer} (caller
     * should fall back to the inherited merge).
     */
    private boolean tryColumnWiseMerge(FieldInfo mergeFieldInfo, MergeState mergeState) throws IOException {
        final int numSources = mergeState.docValuesProducers.length;
        final FlattenedDocValuesProducer[] producers = new FlattenedDocValuesProducer[numSources];

        for (int i = 0; i < numSources; i++) {
            final FieldInfo fi = mergeState.fieldInfos[i].fieldInfo(mergeFieldInfo.name);
            if (fi == null) {
                producers[i] = null; // segment does not have this field — OK
                continue;
            }
            DocValuesProducer p = mergeState.docValuesProducers[i];
            if (p == null) {
                producers[i] = null;
                continue;
            }
            // Check for pruning before unwrapping (PruningDocValuesProducer IS a FilterDocValuesProducer).
            if (p instanceof PruningMergePolicy.PruningDocValuesProducer pdv) {
                if (pdv.shouldPruneNumericDocValues(mergeFieldInfo.name)) {
                    return false; // field is being pruned; fall back
                }
                // Not pruning — fall through to the FilterDocValuesProducer unwrap below.
            }
            if (p instanceof FilterDocValuesProducer fdv) {
                p = fdv.getIn();
            }
            if (p instanceof XPerFieldDocValuesFormat.FieldsReader pfr) {
                p = pfr.getDocValuesProducer(fi);
                if (p == null) {
                    producers[i] = null;
                    continue;
                }
            }
            if (p instanceof FlattenedDocValuesProducer fdvp) {
                producers[i] = fdvp;
            } else {
                return false; // unrecognised producer — fall back
            }
        }

        doColumnWiseMerge(mergeFieldInfo, mergeState, producers);
        return true;
    }

    /**
     * Executes the column-wise merge: builds the merged key dictionary, performs the DISI
     * presence pass, emits one column per merged key directly into the data output, and
     * writes the DISI, key dictionary, address table, and meta fields.
     *
     * @param producers per-segment {@link FlattenedDocValuesProducer} instances (null for segments
     *                  that lack the field)
     */
    private void doColumnWiseMerge(FieldInfo mergeFieldInfo, MergeState mergeState, FlattenedDocValuesProducer[] producers)
        throws IOException {
        meta.writeInt(mergeFieldInfo.number);
        meta.writeByte(FLATTENED_COLUMNAR_BINARY);
        final long dataOffset = data.getFilePointer();
        meta.writeLong(dataOffset);

        final int numSources = producers.length;

        // Resolve BinaryEntry for each segment (null when the segment lacks the field).
        final FlattenedDocValuesProducer.BinaryEntry[] entries = new FlattenedDocValuesProducer.BinaryEntry[numSources];
        for (int i = 0; i < numSources; i++) {
            if (producers[i] != null) {
                entries[i] = producers[i].entryFor(mergeFieldInfo.name);
            }
        }

        // -----------------------------------------------------------------------
        // 1. Presence pass: walk merged binary doc values without calling binaryValue().
        // This correctly handles deleted docs and index sorting via DocIDMerger.
        // -----------------------------------------------------------------------
        final DISIAccumulator disiAcc = new DISIAccumulator(state.directory, state.context, data, DEFAULT_DENSE_RANK_POWER);
        int numDocsWithField = 0;
        int prevDocId = -1;
        boolean isDense = true;
        try {
            final BinaryDocValues merged = getMergedBinaryDocValues(mergeFieldInfo, mergeState);
            for (int doc = merged.nextDoc(); doc != NO_MORE_DOCS; doc = merged.nextDoc()) {
                numDocsWithField++;
                if (doc != prevDocId + 1) isDense = false;
                prevDocId = doc;
                disiAcc.addDocId(doc);
            }
        } catch (Throwable t) {
            IOUtils.closeWhileHandlingException(disiAcc);
            throw t;
        }

        // -----------------------------------------------------------------------
        // 2. Build the merged lex-ordered key dictionary via a k-way merge.
        // Each segment's key dict is already in lex order (ordinal = lex rank).
        // For each merged key, record the source ordinal per segment (-1 if absent).
        // -----------------------------------------------------------------------
        final int[] keyCursors = new int[numSources]; // current key ordinal in each segment
        final List<byte[]> mergedKeys = new ArrayList<>();
        // srcOrdsByKey.get(mergedOrd)[segIdx] = source ordinal in that segment, or -1.
        final List<int[]> srcOrdsByKey = new ArrayList<>();

        while (true) {
            // Find the lex-minimum key across all segment cursors.
            byte[] minBuf = null;
            int minStart = 0, minLen = 0;
            for (int i = 0; i < numSources; i++) {
                if (entries[i] == null || keyCursors[i] >= entries[i].numKeys) continue;
                final int ks = entries[i].keyOffsets[keyCursors[i]];
                final int kl = entries[i].keyOffsets[keyCursors[i] + 1] - ks;
                if (minBuf == null || compareBytes(entries[i].keyBytes, ks, kl, minBuf, minStart, minLen) < 0) {
                    minBuf = entries[i].keyBytes;
                    minStart = ks;
                    minLen = kl;
                }
            }
            if (minBuf == null) break; // all cursors exhausted

            final byte[] kCopy = Arrays.copyOfRange(minBuf, minStart, minStart + minLen);
            mergedKeys.add(kCopy);

            // Advance every cursor that equals the minimum key.
            final int[] srcOrds = new int[numSources];
            Arrays.fill(srcOrds, -1);
            for (int i = 0; i < numSources; i++) {
                if (entries[i] == null || keyCursors[i] >= entries[i].numKeys) continue;
                final int ks = entries[i].keyOffsets[keyCursors[i]];
                final int kl = entries[i].keyOffsets[keyCursors[i] + 1] - ks;
                if (compareBytes(entries[i].keyBytes, ks, kl, kCopy, 0, kCopy.length) == 0) {
                    srcOrds[i] = keyCursors[i];
                    keyCursors[i]++;
                }
            }
            srcOrdsByKey.add(srcOrds);
        }

        final int numMergedKeys = mergedKeys.size();

        // -----------------------------------------------------------------------
        // 3. Emit one column per merged key, writing blocks straight into data.
        // -----------------------------------------------------------------------
        int maxUncompressedBlockLen = 0;
        int maxDocsPerBlockSeen = 0;
        final FieldBlockWriter.ColumnAddress[] addresses = new FieldBlockWriter.ColumnAddress[numMergedKeys];

        // Allocate one writer and reuse it across all merged keys via reset(). This avoids
        // allocating ~128 KiB of block-accumulation arrays and a ZSTD compressor per key.
        final FieldBlockWriter mergeWriter = new FieldBlockWriter(data, targetBlockBytes, maxDocsPerBlock, minCompressBytes);
        boolean firstMergedKey = true;

        for (int mergedOrd = 0; mergedOrd < numMergedKeys; mergedOrd++) {
            final int[] srcOrds = srcOrdsByKey.get(mergedOrd);

            if (firstMergedKey == false) {
                mergeWriter.reset(data);
            }
            firstMergedKey = false;

            // Build a DocIDMerger.Sub for each segment that has this key.
            final List<ColumnMergeSub> subs = new ArrayList<>();
            try {
                for (int i = 0; i < numSources; i++) {
                    if (srcOrds[i] < 0 || entries[i] == null) continue;
                    final FlattenedDocValuesProducer.BinaryEntry e = entries[i];
                    final int srcOrd = srcOrds[i];
                    final IndexInput dataIn = producers[i].cloneDataInput();
                    final SequentialColumnReader reader = new SequentialColumnReader(
                        dataIn,
                        e.columnStartOffsets[srcOrd],
                        e.blockIndexRelOffsets[srcOrd],
                        e.numColumnBlocks[srcOrd]
                    );
                    subs.add(new ColumnMergeSub(mergeState.docMaps[i], reader));
                }

                // Write the column directly into the data output (no temp file, no splice).
                if (subs.isEmpty() == false) {
                    final DocIDMerger<ColumnMergeSub> merger = DocIDMerger.of(subs, mergeState.needsIndexSort);
                    ColumnMergeSub sub;
                    while ((sub = merger.next()) != null) {
                        final SequentialColumnReader reader = sub.reader;
                        mergeWriter.addDocSlots(
                            sub.mappedDocID,
                            reader.slotCount(),
                            reader.slotLens(),
                            reader.firstSlotIndex(),
                            reader.payload(),
                            reader.docSlotsOffset(),
                            reader.docSlotsLength()
                        );
                    }
                }
                mergeWriter.finish();
                maxUncompressedBlockLen = Math.max(maxUncompressedBlockLen, mergeWriter.maxUncompressedBlockLen);
                maxDocsPerBlockSeen = Math.max(maxDocsPerBlockSeen, mergeWriter.maxDocsPerBlockSeen);
                addresses[mergedOrd] = mergeWriter.columnAddress();

            } finally {
                IOUtils.close(subs);
            }
        }

        // -----------------------------------------------------------------------
        // 4. Write DISI (sparse doc presence).
        // -----------------------------------------------------------------------
        final long docsWithFieldOffset;
        final long docsWithFieldLength;
        final short jumpTableEntryCount;
        final byte denseRankPower;
        if (numDocsWithField == 0) {
            docsWithFieldOffset = -2L;
            docsWithFieldLength = 0L;
            jumpTableEntryCount = -1;
            denseRankPower = -1;
            IOUtils.close(disiAcc);
        } else if (numDocsWithField == maxDoc && isDense) {
            docsWithFieldOffset = -1L;
            docsWithFieldLength = 0L;
            jumpTableEntryCount = -1;
            denseRankPower = -1;
            IOUtils.close(disiAcc);
        } else {
            final long disiStart = data.getFilePointer();
            docsWithFieldOffset = disiStart;
            jumpTableEntryCount = disiAcc.build(data);
            docsWithFieldLength = data.getFilePointer() - disiStart;
            denseRankPower = DEFAULT_DENSE_RANK_POWER;
            IOUtils.close(disiAcc);
        }

        // -----------------------------------------------------------------------
        // 5. Write key dictionary in merged lex order.
        // -----------------------------------------------------------------------
        final long keyDictOffset = data.getFilePointer();
        data.writeVInt(numMergedKeys);
        for (byte[] kBytes : mergedKeys) {
            data.writeVInt(kBytes.length);
            data.writeBytes(kBytes, 0, kBytes.length);
        }
        final long keyDictLength = data.getFilePointer() - keyDictOffset;

        // -----------------------------------------------------------------------
        // 6. Write column address table.
        // -----------------------------------------------------------------------
        final long columnAddressTableOffset = data.getFilePointer();
        assert FlattenedDocValuesFormat.COLUMN_ADDRESS_ENTRY_BYTES == 16; // long + int + int
        for (int ord = 0; ord < numMergedKeys; ord++) {
            final FieldBlockWriter.ColumnAddress addr = addresses[ord];
            if (addr != null) {
                data.writeLong(addr.columnStartOffset());
                data.writeInt(addr.blockIndexRelativeOffset());
                data.writeInt(addr.numBlocks());
            } else {
                // Key has no segments with data (should not happen: keys come from segments).
                data.writeLong(0L);
                data.writeInt(0);
                data.writeInt(0);
            }
        }

        final long dataLength = data.getFilePointer() - dataOffset;

        // -----------------------------------------------------------------------
        // 7. Write meta fields (must match the layout written by addBinaryField).
        // -----------------------------------------------------------------------
        meta.writeLong(docsWithFieldOffset);
        meta.writeLong(docsWithFieldLength);
        meta.writeShort(jumpTableEntryCount);
        meta.writeByte(denseRankPower);
        meta.writeInt(numDocsWithField);
        meta.writeInt(numMergedKeys);
        meta.writeLong(keyDictOffset);
        meta.writeLong(keyDictLength);
        meta.writeLong(columnAddressTableOffset);
        meta.writeVInt(maxUncompressedBlockLen);
        meta.writeVInt(maxDocsPerBlockSeen);
        meta.writeLong(dataLength);
    }

    // ---------------------------------------------------------------------------------
    // Helpers for column-wise merge
    // ---------------------------------------------------------------------------------

    /**
     * A {@link DocIDMerger.Sub} that drives a {@link SequentialColumnReader} and implements
     * {@code Closeable} so {@link IOUtils#close} can clean up all readers at once.
     */
    private static final class ColumnMergeSub extends DocIDMerger.Sub implements java.io.Closeable {
        final SequentialColumnReader reader;

        ColumnMergeSub(MergeState.DocMap docMap, SequentialColumnReader reader) {
            super(docMap);
            this.reader = reader;
        }

        @Override
        public int nextDoc() throws IOException {
            return reader.nextDoc();
        }

        @Override
        public void close() throws IOException {
            reader.close();
        }
    }

    /**
     * Lexicographic comparison of two byte-array slices.
     * Returns negative, zero, or positive as {@code a} is less than, equal to, or greater
     * than {@code b}.
     */
    private static int compareBytes(byte[] a, int aOff, int aLen, byte[] b, int bOff, int bLen) {
        final int minLen = Math.min(aLen, bLen);
        for (int i = 0; i < minLen; i++) {
            final int diff = (a[aOff + i] & 0xFF) - (b[bOff + i] & 0xFF);
            if (diff != 0) return diff;
        }
        return aLen - bLen;
    }

    /**
     * Writes the segment key dictionary to the data file. Keys are stored in lexicographic order;
     * ordinal = lex rank, enabling direct binary-search lookup without a sorted-ordinals side array.
     *
     * <pre>
     * vint numKeys
     * per key in lex order (ordinal = lex rank):
     *   vint keyLen
     *   keyLen bytes
     * </pre>
     */
    private void writeKeyDictionary(BytesRefHash keyHash, int numKeys, int[] sortedOrds) throws IOException {
        data.writeVInt(numKeys);
        final BytesRef scratch = new BytesRef();
        for (int lexRank = 0; lexRank < numKeys; lexRank++) {
            keyHash.get(sortedOrds[lexRank], scratch);
            data.writeVInt(scratch.length);
            data.writeBytes(scratch.bytes, scratch.offset, scratch.length);
        }
    }

    @Override
    public void close() throws IOException {
        boolean success = false;
        try {
            if (meta != null) {
                meta.writeInt(FIELD_EOF);
                CodecUtil.writeFooter(meta);
            }
            if (data != null) {
                CodecUtil.writeFooter(data);
            }
            success = true;
        } finally {
            if (success) {
                IOUtils.close(data, meta);
            } else {
                IOUtils.closeWhileHandlingException(data, meta);
            }
            data = meta = null;
        }
    }

    @Override
    public void addNumericField(FieldInfo field, DocValuesProducer valuesProducer) {
        throw unsupported(field, "NUMERIC");
    }

    @Override
    public void addSortedField(FieldInfo field, DocValuesProducer valuesProducer) {
        throw unsupported(field, "SORTED");
    }

    @Override
    public void addSortedNumericField(FieldInfo field, DocValuesProducer valuesProducer) {
        throw unsupported(field, "SORTED_NUMERIC");
    }

    @Override
    public void addSortedSetField(FieldInfo field, DocValuesProducer valuesProducer) {
        throw unsupported(field, "SORTED_SET");
    }

    /** Encodes {@code v} as a VInt into {@code buf[off..]} and returns the new offset. */
    private static int writeVIntToArray(byte[] buf, int off, int v) {
        while ((v & ~0x7F) != 0) {
            buf[off++] = (byte) ((v & 0x7F) | 0x80);
            v >>>= 7;
        }
        buf[off++] = (byte) v;
        return off;
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
}
