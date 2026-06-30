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
            BytesReference absent = null;
            if ((flags & FLAG_ABSENT) != 0) {
                absent = data.slice(pos, absentLen);
                pos += absentLen;
            }
            BytesReference typeVector = null;
            if ((flags & FLAG_TYPE_VECTOR) != 0) {
                typeVector = data.slice(pos, typeVecLen);
                pos += typeVecLen;
            }
            BytesReference offsets = null;
            if ((flags & FLAG_OFFSETS) != 0) {
                offsets = data.slice(pos, offsetsLen);
                pos += offsetsLen;
            }
            BytesReference colData = data.slice(pos, dataLen);
            columns[c] = new ElasticsearchColumnData(kind, docCount, absent, typeVector, offsets, colData);
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
        ElasticsearchColumn built = ElasticsearchColumn.from(columnIndex, columns[columnIndex]);
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
            total += refLen(col.absentBitset()) + refLen(col.typeVector()) + refLen(col.offsets()) + refLen(col.data());
        }
        return total;
    }

    private static ElasticsearchColumnData sliceColumn(ElasticsearchColumnData col, int from, int newCount) {
        BytesReference absent = newCount > 0 && col.absentBitset() != null ? copyBitset(col.absentBitset(), from, newCount) : null;
        if (col.kind() == ElasticsearchColumnKind.ARRAY) {
            return sliceArrayColumn(col, from, newCount, absent);
        }
        BytesReference typeVector = col.typeVector() != null ? copyRange(col.typeVector(), from, newCount) : null;
        BytesReference offsets;
        BytesReference data;
        if (col.offsets() != null) {
            int byteFrom = col.offsets().getIntLE(from * 4);
            int byteTo = col.offsets().getIntLE((from + newCount) * 4);
            data = copyRange(col.data(), byteFrom, byteTo - byteFrom);
            offsets = rebasedOffsets(col.offsets(), from, newCount, byteFrom);
        } else if (col.kind() == ElasticsearchColumnKind.BOOL) {
            offsets = null;
            data = copyBitset(col.data(), from, newCount);
        } else {
            offsets = null;
            data = copyRange(col.data(), from * 8, newCount * 8);
        }
        return new ElasticsearchColumnData(col.kind(), newCount, absent, typeVector, offsets, data);
    }

    /** Slices an Arrow array column: the offsets are element ranges and the data is {@code child_kind | child_values}. */
    private static ElasticsearchColumnData sliceArrayColumn(ElasticsearchColumnData col, int from, int newCount, BytesReference absent) {
        BytesReference rowOffsets = col.offsets();
        int elemFrom = rowOffsets.getIntLE(from * 4);
        int elemTo = rowOffsets.getIntLE((from + newCount) * 4);
        int newElemCount = elemTo - elemFrom;
        BytesReference newRowOffsets = rebasedOffsets(rowOffsets, from, newCount, elemFrom);

        BytesRef d = col.data().toBytesRef();
        byte childKind = d.bytes[d.offset];
        int childBase = d.offset + 1;
        byte[] newData;
        if (childKind == ElasticsearchColumnKind.STRING) {
            int byteFrom = readIntLE(d.bytes, childBase + elemFrom * 4);
            int byteTo = readIntLE(d.bytes, childBase + elemTo * 4);
            int childDataBase = childBase + (oldElemCount(rowOffsets, col.docCount()) + 1) * 4;
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
        return new ElasticsearchColumnData(col.kind(), newCount, absent, null, newRowOffsets, new BytesArray(newData));
    }

    private static int oldElemCount(BytesReference rowOffsets, int docCount) {
        return rowOffsets.getIntLE(docCount * 4);
    }

    private static int readIntLE(byte[] b, int off) {
        return (b[off] & 0xFF) | ((b[off + 1] & 0xFF) << 8) | ((b[off + 2] & 0xFF) << 16) | ((b[off + 3] & 0xFF) << 24);
    }

    private static BytesReference rebasedOffsets(BytesReference offsets, int from, int newCount, int rebase) {
        byte[] out = new byte[(newCount + 1) * 4];
        for (int i = 0; i <= newCount; i++) {
            ByteUtils.writeIntLE(offsets.getIntLE((from + i) * 4) - rebase, out, i * 4);
        }
        return new BytesArray(out);
    }

    private static BytesReference copyRange(BytesReference src, int from, int length) {
        BytesRef ref = src.slice(from, length).toBytesRef();
        return new BytesArray(Arrays.copyOfRange(ref.bytes, ref.offset, ref.offset + length));
    }

    private static BytesReference copyBitset(BytesReference src, int from, int count) {
        byte[] out = new byte[ElasticsearchColumnBuilder.bitsetBytes(count)];
        for (int i = 0; i < count; i++) {
            long word = src.getLongLE((from + i) / 64 * 8);
            if (((word >>> ((from + i) & 63)) & 1L) != 0) {
                out[i / 64 * 8 + (i & 63) / 8] |= (byte) (1 << ((i & 63) & 7));
            }
        }
        return new BytesArray(out);
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

        int[] flags = new int[colCount];
        int[] baseOffsets = new int[colCount];
        int cumDataOffset = 0;
        for (int c = 0; c < colCount; c++) {
            ElasticsearchColumnData col = columns[c];
            baseOffsets[c] = cumDataOffset;
            int f = 0;
            if (col.absentBitset() != null) {
                f |= FLAG_ABSENT;
                cumDataOffset += col.absentBitset().length();
            }
            if (col.typeVector() != null) {
                f |= FLAG_TYPE_VECTOR;
                cumDataOffset += col.typeVector().length();
            }
            if (col.offsets() != null) {
                f |= FLAG_OFFSETS;
                cumDataOffset += col.offsets().length();
            }
            cumDataOffset += col.data().length();
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
            ElasticsearchColumnData col = columns[c];
            header[pos] = col.kind();
            header[pos + 1] = (byte) flags[c];
            ByteUtils.writeIntLE(baseOffsets[c], header, pos + 2);
            ByteUtils.writeIntLE(col.absentBitset() != null ? col.absentBitset().length() : 0, header, pos + 6);
            ByteUtils.writeIntLE(col.typeVector() != null ? col.typeVector().length() : 0, header, pos + 10);
            ByteUtils.writeIntLE(col.offsets() != null ? col.offsets().length() : 0, header, pos + 14);
            ByteUtils.writeIntLE(col.data().length(), header, pos + 18);
            pos += COLUMN_INDEX_ENTRY_SIZE;
        }

        List<BytesReference> parts = new ArrayList<>(1 + colCount * 4);
        parts.add(new BytesArray(header));
        for (int c = 0; c < colCount; c++) {
            ElasticsearchColumnData col = columns[c];
            if (col.absentBitset() != null) {
                parts.add(col.absentBitset());
            }
            if (col.typeVector() != null) {
                parts.add(col.typeVector());
            }
            if (col.offsets() != null) {
                parts.add(col.offsets());
            }
            parts.add(col.data());
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

    private static int readU16LE(BytesReference data, int offset) {
        return (data.get(offset) & 0xFF) | ((data.get(offset + 1) & 0xFF) << 8);
    }
}
