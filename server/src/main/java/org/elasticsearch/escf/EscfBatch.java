/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.escf;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.CompositeBytesReference;
import org.elasticsearch.common.util.ByteUtils;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.sourcebatch.SourceBatch;
import org.elasticsearch.sourcebatch.SourceRow;
import org.elasticsearch.sourcebatch.SourceSchema;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * An ESCF (Elasticsearch Column Format) batch: a column-major {@link SourceBatch} backed by an array
 * of {@link ElasticsearchColumnData}. Built in memory by {@link EscfEncoder} or reconstructed from
 * serialized bytes via {@link #EscfBatch(BytesReference, Releasable)}.
 *
 * <p>Serialized layout (32-byte header, all multi-byte integers little-endian):
 * <pre>
 * magic('escf') version(i32) flags(i32) doc_count(i32)
 * schema_offset(i32) column_index_offset(i32) data_offset(i32) total_size(i32)
 * [Schema]        same binary format as EIRF (non_leaf_count + entries, leaf_count + entries)
 * [Column Index]  per leaf: kind(u8) present_flags(u8) base_offset(i32)
 *                 absent_len(i32) typevec_len(i32) offsets_len(i32) data_len(i32)   [= 22 bytes]
 * [Column Data]   per leaf, present fields concatenated: [absent_bitset] [type_vector] [offsets] [data]
 * </pre>
 * {@code present_flags} bit 0 = absent bitset, bit 1 = type vector, bit 2 = offsets; the data field is
 * always present. {@code base_offset} is relative to {@code data_offset}.
 */
public final class EscfBatch implements SourceBatch {

    /** Magic as a little-endian int: bytes 'e','s','c','f' read as LE i32. */
    public static final int MAGIC_LE = ('e' & 0xFF) | (('s' & 0xFF) << 8) | (('c' & 0xFF) << 16) | (('f' & 0xFF) << 24);
    public static final int VERSION = 1;

    private static final int HEADER_SIZE = 32;
    private static final int COLUMN_INDEX_ENTRY_SIZE = 22;

    private static final int FLAG_ABSENT = 0x1;
    private static final int FLAG_TYPE_VECTOR = 0x2;
    private static final int FLAG_OFFSETS = 0x4;

    private final SourceSchema schema;
    private final int docCount;
    private final ElasticsearchColumnData[] columns;
    private final ElasticsearchColumn[] columnCache;
    private final Releasable releasable;
    private BytesReference serialized;

    /** In-memory construction path used by {@link EscfEncoder#buildPartition(int)}. */
    EscfBatch(SourceSchema schema, int docCount, ElasticsearchColumnData[] columns, Releasable releasable) {
        this.schema = schema;
        this.docCount = docCount;
        this.columns = columns;
        this.columnCache = new ElasticsearchColumn[columns.length];
        this.releasable = releasable;
        this.serialized = null;
    }

    /** Serialized construction path: parse a batch from its wire/translog bytes. */
    public EscfBatch(BytesReference data, Releasable releasable) {
        this.releasable = releasable;
        this.serialized = data;

        int magic = data.getIntLE(0);
        if (magic != MAGIC_LE) {
            throw new IllegalArgumentException(
                "Invalid magic: expected 'escf', got '"
                    + (char) (magic & 0xFF)
                    + (char) ((magic >> 8) & 0xFF)
                    + (char) ((magic >> 16) & 0xFF)
                    + (char) ((magic >> 24) & 0xFF)
                    + "'"
            );
        }
        int version = data.getIntLE(4);
        if (version != VERSION) {
            throw new IllegalArgumentException("Unsupported ESCF version: " + version);
        }
        this.docCount = data.getIntLE(12);
        int schemaOffset = data.getIntLE(16);
        int columnIndexOffset = data.getIntLE(20);
        int dataOffset = data.getIntLE(24);

        this.schema = parseSchema(data, schemaOffset);

        int colCount = schema.leafCount();
        this.columns = new ElasticsearchColumnData[colCount];
        this.columnCache = new ElasticsearchColumn[colCount];
        for (int c = 0; c < colCount; c++) {
            int entryBase = columnIndexOffset + c * COLUMN_INDEX_ENTRY_SIZE;
            byte kind = data.get(entryBase);
            int flags = data.get(entryBase + 1) & 0xFF;
            int base = dataOffset + data.getIntLE(entryBase + 2);
            int absentLen = data.getIntLE(entryBase + 6);
            int typeVecLen = data.getIntLE(entryBase + 10);
            int offsetsLen = data.getIntLE(entryBase + 14);
            int dataLen = data.getIntLE(entryBase + 18);

            int pos = base;
            FixedBitSet absent = null;
            if ((flags & FLAG_ABSENT) != 0) {
                absent = bytesToFixedBitSet(data, pos, docCount);
                pos += absentLen;
            }
            byte[] typeVector = null;
            if ((flags & FLAG_TYPE_VECTOR) != 0) {
                typeVector = bytesToByteArray(data, pos, typeVecLen);
                pos += typeVecLen;
            }
            int[] offsets = null;
            if ((flags & FLAG_OFFSETS) != 0) {
                offsets = bytesToOffsets(data, pos, docCount);
                pos += offsetsLen;
            }
            // For BOOL the data field carries the value bitset; every other kind keeps its payload as a byte slice.
            FixedBitSet values = null;
            BytesReference colData = null;
            if (kind == ElasticsearchColumnKind.BOOL) {
                values = bytesToFixedBitSet(data, pos, docCount);
            } else {
                colData = data.slice(pos, dataLen);
            }
            columns[c] = new ElasticsearchColumnData(kind, docCount, absent, values, typeVector, offsets, colData);
        }
    }

    @Override
    public int docCount() {
        return docCount;
    }

    @Override
    public SourceSchema schema() {
        return schema;
    }

    @Override
    public BytesReference data() {
        if (serialized == null) {
            serialized = serialize(schema, docCount, columns);
        }
        return serialized;
    }

    @Override
    public int columnCount() {
        return schema.leafCount();
    }

    @Override
    public SourceRow row(int docIndex) {
        if (docIndex < 0 || docIndex >= docCount) {
            throw new IndexOutOfBoundsException("docIndex " + docIndex + " out of range [0, " + docCount + ")");
        }
        return new EscfRow(this, docIndex);
    }

    /** The typed view for {@code columnIndex}, lazily built and cached. Package-private: used by {@link EscfRow}. */
    ElasticsearchColumn column(int columnIndex) {
        ElasticsearchColumn cached = columnCache[columnIndex];
        if (cached != null) {
            return cached;
        }
        ElasticsearchColumn built = ElasticsearchColumn.from(columns[columnIndex]);
        columnCache[columnIndex] = built;
        return built;
    }

    @Override
    public SourceBatch slice(int from, int to) {
        if (from < 0 || to > docCount || from > to) {
            throw new IndexOutOfBoundsException("slice [" + from + ", " + to + ") out of [0, " + docCount + ")");
        }
        if (from == 0 && to == docCount) {
            return new EscfBatch(schema, docCount, columns, () -> {});
        }
        int newDocCount = to - from;
        ElasticsearchColumnData[] newColumns = new ElasticsearchColumnData[columns.length];
        for (int c = 0; c < columns.length; c++) {
            newColumns[c] = sliceColumn(columns[c], from, newDocCount);
        }
        return new EscfBatch(schema, newDocCount, newColumns, () -> {});
    }

    @Override
    public void close() {
        releasable.close();
    }

    @Override
    public long ramBytesUsed() {
        if (serialized != null) {
            return serialized.length() + 64L;
        }
        long total = 64L;
        for (ElasticsearchColumnData col : columns) {
            total += bitsetRam(col.absent()) + bitsetRam(col.values()) + (col.typeVector() != null ? col.typeVector().length : 0L) + (col
                .offsets() != null ? col.offsets().length * 4L : 0L) + refLen(col.data());
        }
        return total;
    }

    private static long bitsetRam(FixedBitSet bs) {
        return bs == null ? 0L : (long) bs.getBits().length * 8;
    }

    private static ElasticsearchColumnData sliceColumn(ElasticsearchColumnData col, int from, int newCount) {
        FixedBitSet absent = col.absent() != null ? sliceBitset(col.absent(), from, newCount) : null;
        if (col.kind() == ElasticsearchColumnKind.ARRAY) {
            return sliceArrayColumn(col, from, newCount, absent);
        }
        if (col.offsets() != null) {
            byte[] typeVector = col.typeVector() != null ? Arrays.copyOfRange(col.typeVector(), from, from + newCount) : null;
            int[] srcOffsets = col.offsets();
            int byteFrom = srcOffsets[from];
            int byteTo = srcOffsets[from + newCount];
            BytesReference data = copyRange(col.data(), byteFrom, byteTo - byteFrom);
            int[] offsets = rebasedOffsets(srcOffsets, from, newCount, byteFrom);
            return new ElasticsearchColumnData(col.kind(), newCount, absent, null, typeVector, offsets, data);
        }
        if (col.kind() == ElasticsearchColumnKind.BOOL) {
            FixedBitSet values = col.values() != null ? sliceBitset(col.values(), from, newCount) : null;
            return new ElasticsearchColumnData(col.kind(), newCount, absent, values, null, null, null);
        }
        // LONG / DOUBLE: 8-byte slots
        BytesReference data = copyRange(col.data(), from * 8, newCount * 8);
        return new ElasticsearchColumnData(col.kind(), newCount, absent, null, null, null, data);
    }

    /** Slices an Arrow array column: the offsets are element ranges and the data is {@code child_kind | child_values}. */
    private static ElasticsearchColumnData sliceArrayColumn(ElasticsearchColumnData col, int from, int newCount, FixedBitSet absent) {
        int[] rowOffsets = col.offsets();
        int elemFrom = rowOffsets[from];
        int elemTo = rowOffsets[from + newCount];
        int newElemCount = elemTo - elemFrom;
        int[] newRowOffsets = rebasedOffsets(rowOffsets, from, newCount, elemFrom);

        BytesRef d = col.data().toBytesRef();
        byte childKind = d.bytes[d.offset];
        int childBase = d.offset + 1;
        byte[] newData;
        if (childKind == ElasticsearchColumnKind.STRING) {
            int oldElemCount = rowOffsets[col.docCount()];
            int byteFrom = readIntLE(d.bytes, childBase + elemFrom * 4);
            int byteTo = readIntLE(d.bytes, childBase + elemTo * 4);
            int childDataBase = childBase + (oldElemCount + 1) * 4;
            int prefix = 1 + (newElemCount + 1) * 4;
            newData = new byte[prefix + (byteTo - byteFrom)];
            newData[0] = childKind;
            for (int j = 0; j <= newElemCount; j++) {
                ByteUtils.writeIntLE(readIntLE(d.bytes, childBase + (elemFrom + j) * 4) - byteFrom, newData, 1 + j * 4);
            }
            System.arraycopy(d.bytes, childDataBase + byteFrom, newData, prefix, byteTo - byteFrom);
        } else {
            // LONG / DOUBLE child: 8-byte slots
            newData = new byte[1 + newElemCount * 8];
            newData[0] = childKind;
            System.arraycopy(d.bytes, childBase + elemFrom * 8, newData, 1, newElemCount * 8);
        }
        return new ElasticsearchColumnData(col.kind(), newCount, absent, null, null, newRowOffsets, new BytesArray(newData));
    }

    private static int readIntLE(byte[] b, int off) {
        return (b[off] & 0xFF) | ((b[off + 1] & 0xFF) << 8) | ((b[off + 2] & 0xFF) << 16) | ((b[off + 3] & 0xFF) << 24);
    }

    private static int[] rebasedOffsets(int[] offsets, int from, int newCount, int rebase) {
        int[] out = new int[newCount + 1];
        for (int i = 0; i <= newCount; i++) {
            out[i] = offsets[from + i] - rebase;
        }
        return out;
    }

    private static BytesReference copyRange(BytesReference src, int from, int length) {
        BytesRef ref = src.slice(from, length).toBytesRef();
        return new BytesArray(Arrays.copyOfRange(ref.bytes, ref.offset, ref.offset + length));
    }

    /** Copies bits {@code [from, from + count)} of {@code src} into a fresh bitset at {@code [0, count)}; out-of-range bits read as clear. */
    private static FixedBitSet sliceBitset(FixedBitSet src, int from, int count) {
        FixedBitSet out = new FixedBitSet(Math.max(1, count));
        int cap = src.length();
        for (int i = 0; i < count; i++) {
            int idx = from + i;
            if (idx < cap && src.get(idx)) {
                out.set(i);
            }
        }
        return out;
    }

    private static BytesReference serialize(SourceSchema schema, int docCount, ElasticsearchColumnData[] columns) {
        int colCount = schema.leafCount();
        int nonLeafCount = schema.nonLeafCount();

        byte[][] nonLeafNameBytes = new byte[nonLeafCount][];
        int schemaSize = 2;
        for (int i = 0; i < nonLeafCount; i++) {
            nonLeafNameBytes[i] = schema.getNonLeafName(i).getBytes(StandardCharsets.UTF_8);
            schemaSize += 2 + 2 + nonLeafNameBytes[i].length;
        }
        schemaSize += 2;
        byte[][] leafNameBytes = new byte[colCount][];
        for (int i = 0; i < colCount; i++) {
            leafNameBytes[i] = schema.getLeafName(i).getBytes(StandardCharsets.UTF_8);
            schemaSize += 2 + 2 + leafNameBytes[i].length;
        }

        int columnIndexSize = colCount * COLUMN_INDEX_ENTRY_SIZE;
        int schemaOffset = HEADER_SIZE;
        int columnIndexOffset = schemaOffset + schemaSize;
        int dataOffset = columnIndexOffset + columnIndexSize;

        // Encode each column's native fields into their wire byte parts — this is the only place ESCF serializes.
        BytesReference[] absentPart = new BytesReference[colCount];
        BytesReference[] typeVecPart = new BytesReference[colCount];
        BytesReference[] offsetsPart = new BytesReference[colCount];
        BytesReference[] dataPart = new BytesReference[colCount];
        for (int c = 0; c < colCount; c++) {
            ElasticsearchColumnData col = columns[c];
            absentPart[c] = col.absent() != null ? bitsetToRef(col.absent(), docCount) : null;
            typeVecPart[c] = col.typeVector() != null ? new BytesArray(col.typeVector()) : null;
            offsetsPart[c] = col.offsets() != null ? intArrayToRef(col.offsets()) : null;
            // BOOL keeps its value bitset in the data slot; every other kind already has a byte payload.
            dataPart[c] = col.kind() == ElasticsearchColumnKind.BOOL ? bitsetToRef(col.values(), docCount) : col.data();
        }

        int[] flags = new int[colCount];
        int[] baseOffsets = new int[colCount];
        int cumDataOffset = 0;
        for (int c = 0; c < colCount; c++) {
            baseOffsets[c] = cumDataOffset;
            int f = 0;
            if (absentPart[c] != null) {
                f |= FLAG_ABSENT;
                cumDataOffset += absentPart[c].length();
            }
            if (typeVecPart[c] != null) {
                f |= FLAG_TYPE_VECTOR;
                cumDataOffset += typeVecPart[c].length();
            }
            if (offsetsPart[c] != null) {
                f |= FLAG_OFFSETS;
                cumDataOffset += offsetsPart[c].length();
            }
            cumDataOffset += dataPart[c].length();
            flags[c] = f;
        }
        int totalSize = dataOffset + cumDataOffset;

        byte[] header = new byte[dataOffset];
        ByteUtils.writeIntLE(MAGIC_LE, header, 0);
        ByteUtils.writeIntLE(VERSION, header, 4);
        ByteUtils.writeIntLE(0, header, 8);
        ByteUtils.writeIntLE(docCount, header, 12);
        ByteUtils.writeIntLE(schemaOffset, header, 16);
        ByteUtils.writeIntLE(columnIndexOffset, header, 20);
        ByteUtils.writeIntLE(dataOffset, header, 24);
        ByteUtils.writeIntLE(totalSize, header, 28);

        int pos = schemaOffset;
        writeShortLE(header, pos, nonLeafCount);
        pos += 2;
        for (int i = 0; i < nonLeafCount; i++) {
            writeShortLE(header, pos, schema.getNonLeafParent(i));
            pos += 2;
            writeShortLE(header, pos, nonLeafNameBytes[i].length);
            pos += 2;
            System.arraycopy(nonLeafNameBytes[i], 0, header, pos, nonLeafNameBytes[i].length);
            pos += nonLeafNameBytes[i].length;
        }
        writeShortLE(header, pos, colCount);
        pos += 2;
        for (int i = 0; i < colCount; i++) {
            writeShortLE(header, pos, schema.getLeafParent(i));
            pos += 2;
            writeShortLE(header, pos, leafNameBytes[i].length);
            pos += 2;
            System.arraycopy(leafNameBytes[i], 0, header, pos, leafNameBytes[i].length);
            pos += leafNameBytes[i].length;
        }

        pos = columnIndexOffset;
        for (int c = 0; c < colCount; c++) {
            header[pos] = columns[c].kind();
            header[pos + 1] = (byte) flags[c];
            ByteUtils.writeIntLE(baseOffsets[c], header, pos + 2);
            ByteUtils.writeIntLE(absentPart[c] != null ? absentPart[c].length() : 0, header, pos + 6);
            ByteUtils.writeIntLE(typeVecPart[c] != null ? typeVecPart[c].length() : 0, header, pos + 10);
            ByteUtils.writeIntLE(offsetsPart[c] != null ? offsetsPart[c].length() : 0, header, pos + 14);
            ByteUtils.writeIntLE(dataPart[c].length(), header, pos + 18);
            pos += COLUMN_INDEX_ENTRY_SIZE;
        }

        List<BytesReference> parts = new ArrayList<>(1 + colCount * 4);
        parts.add(new BytesArray(header));
        for (int c = 0; c < colCount; c++) {
            if (absentPart[c] != null) {
                parts.add(absentPart[c]);
            }
            if (typeVecPart[c] != null) {
                parts.add(typeVecPart[c]);
            }
            if (offsetsPart[c] != null) {
                parts.add(offsetsPart[c]);
            }
            parts.add(dataPart[c]);
        }
        return CompositeBytesReference.of(parts.toArray(new BytesReference[0]));
    }

    private static void writeShortLE(byte[] buf, int offset, int value) {
        buf[offset] = (byte) value;
        buf[offset + 1] = (byte) (value >>> 8);
    }

    private static long refLen(BytesReference ref) {
        return ref == null ? 0L : ref.length();
    }

    /** Number of bytes needed to hold {@code docCount} bits as little-endian 64-bit words. */
    static int bitsetBytes(int docCount) {
        return ((docCount + 63) / 64) * 8;
    }

    /** Serialises {@code bs} (or an all-clear bitset when {@code bs == null}) to {@code bitsetBytes(docCount)} LE bytes. */
    private static BytesReference bitsetToRef(FixedBitSet bs, int docCount) {
        int n = bitsetBytes(docCount);
        byte[] out = new byte[n];
        if (bs != null) {
            long[] words = bs.getBits();
            int wordCount = n / 8;
            for (int w = 0; w < wordCount; w++) {
                long value = w < words.length ? words[w] : 0L;
                ByteUtils.writeLongLE(value, out, w * 8);
            }
        }
        return new BytesArray(out);
    }

    private static BytesReference intArrayToRef(int[] values) {
        byte[] out = new byte[values.length * 4];
        for (int i = 0; i < values.length; i++) {
            ByteUtils.writeIntLE(values[i], out, i * 4);
        }
        return new BytesArray(out);
    }

    /** Parses {@code bitsetBytes(docCount)} LE bytes at {@code pos} into a {@link FixedBitSet}. */
    private static FixedBitSet bytesToFixedBitSet(BytesReference data, int pos, int docCount) {
        int words = bitsetBytes(docCount) / 8;
        long[] bits = new long[words];
        for (int w = 0; w < words; w++) {
            bits[w] = data.getLongLE(pos + w * 8);
        }
        return new FixedBitSet(bits, words * 64);
    }

    private static byte[] bytesToByteArray(BytesReference data, int pos, int len) {
        BytesRef ref = data.slice(pos, len).toBytesRef();
        return Arrays.copyOfRange(ref.bytes, ref.offset, ref.offset + len);
    }

    /** Parses {@code (count + 1)} LE i32 values at {@code pos} into an {@code int[]}. */
    private static int[] bytesToOffsets(BytesReference data, int pos, int count) {
        int[] offsets = new int[count + 1];
        for (int i = 0; i <= count; i++) {
            offsets[i] = data.getIntLE(pos + i * 4);
        }
        return offsets;
    }

    private static SourceSchema parseSchema(BytesReference data, int offset) {
        int nonLeafCount = readU16LE(data, offset);
        offset += 2;
        List<String> nonLeafNames = new ArrayList<>(nonLeafCount);
        int[] nonLeafParents = new int[nonLeafCount];
        for (int i = 0; i < nonLeafCount; i++) {
            nonLeafParents[i] = readU16LE(data, offset);
            offset += 2;
            int nameLen = readU16LE(data, offset);
            offset += 2;
            if (nameLen > 0) {
                var ref = data.slice(offset, nameLen).toBytesRef();
                nonLeafNames.add(new String(ref.bytes, ref.offset, ref.length, StandardCharsets.UTF_8));
            } else {
                nonLeafNames.add("");
            }
            offset += nameLen;
        }
        int leafCount = readU16LE(data, offset);
        offset += 2;
        List<String> leafNames = new ArrayList<>(leafCount);
        int[] leafParents = new int[leafCount];
        for (int i = 0; i < leafCount; i++) {
            leafParents[i] = readU16LE(data, offset);
            offset += 2;
            int nameLen = readU16LE(data, offset);
            offset += 2;
            var ref = data.slice(offset, nameLen).toBytesRef();
            leafNames.add(new String(ref.bytes, ref.offset, ref.length, StandardCharsets.UTF_8));
            offset += nameLen;
        }
        return new SourceSchema(nonLeafNames, nonLeafParents, leafNames, leafParents);
    }

    // TODO: Optimize onto bytes reference
    private static int readU16LE(BytesReference data, int offset) {
        return (data.get(offset) & 0xFF) | ((data.get(offset + 1) & 0xFF) << 8);
    }
}
