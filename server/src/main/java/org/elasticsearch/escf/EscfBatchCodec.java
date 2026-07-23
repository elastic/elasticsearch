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
import org.elasticsearch.sourcebatch.SourceSchema;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * The ESCF wire format: serializes an {@link EscfBatch}'s columns to bytes and parses them back.
 * {@link EscfBatch} is the in-memory container; this class owns the on-disk/on-wire layout and the
 * field-level codecs (bitsets, offset arrays, array children).
 *
 * <p>Serialized layout (32-byte header, all multi-byte integers little-endian):
 * <pre>
 * magic('escf') version(i32) flags(i32) doc_count(i32)
 * schema_offset(i32) column_index_offset(i32) data_offset(i32) total_size(i32)
 * [Schema]        non_leaf_count(u16) { parent(u16) name_len(u16) name }* leaf_count(u16) { parent(u16) name_len(u16) name }*
 * [Column Index]  per leaf: kind(u8) present_flags(u8) base_offset(i32)
 *                 validity_len(i32) typevec_len(i32) offsets_len(i32) data_len(i32)   [= 22 bytes]
 * [Column Data]   per leaf, present fields concatenated: [validity_bitset] [type_vector] [offsets] [data]
 * </pre>
 * {@code present_flags} bit 0 = Arrow-style validity bitset (bit set = present), bit 1 = type vector,
 * bit 2 = offsets; the data field is always present. {@code base_offset} is relative to {@code data_offset}.
 */
final class EscfBatchCodec {

    /** Magic as a little-endian int: bytes 'e','s','c','f' read as LE i32. */
    public static final int MAGIC_LE = ('e' & 0xFF) | (('s' & 0xFF) << 8) | (('c' & 0xFF) << 16) | (('f' & 0xFF) << 24);
    public static final int VERSION = 1;

    private static final int HEADER_SIZE = 32;
    private static final int COLUMN_INDEX_ENTRY_SIZE = 22;

    private static final int FLAG_VALIDITY = 0x1;
    private static final int FLAG_TYPE_VECTOR = 0x2;
    private static final int FLAG_OFFSETS = 0x4;

    private EscfBatchCodec() {}

    // TODO: This is not very optimized at the moment. Lots of random reads against composite bytes references. Eventually we'll want to
    // implement stream reads with slices out for column data.
    static EscfBatch parse(BytesReference data, Releasable releasable) {
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
        int docCount = data.getIntLE(12);
        int schemaOffset = data.getIntLE(16);
        int columnIndexOffset = data.getIntLE(20);
        int dataOffset = data.getIntLE(24);

        SourceSchema schema = parseSchema(data, schemaOffset);

        int colCount = schema.leafCount();
        EscfColumnData[] columns = new EscfColumnData[colCount];
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
            FixedBitSet validity = null;
            if ((flags & FLAG_VALIDITY) != 0) {
                validity = bytesToFixedBitSet(data, pos, docCount);
                pos += absentLen;
            }
            BytesRef typeVector = null;
            if ((flags & FLAG_TYPE_VECTOR) != 0) {
                typeVector = new BytesRef(bytesToByteArray(data, pos, typeVecLen));
                pos += typeVecLen;
            }
            int[] offsets = null;
            if ((flags & FLAG_OFFSETS) != 0) {
                offsets = bytesToOffsets(data, pos, docCount);
                pos += offsetsLen;
            }
            // For BOOL the data field carries the value bitset; ARRAY carries a nested child column;
            // every other kind keeps its payload as a byte slice.
            columns[c] = switch (kind) {
                case EscfColumnKind.BOOL -> EscfColumnData.ofBool(docCount, validity, bytesToFixedBitSet(data, pos, docCount));
                case EscfColumnKind.ARRAY -> EscfColumnData.ofArray(
                    docCount,
                    validity,
                    offsets,
                    decodeArrayChild(data, pos, dataLen, offsets[docCount])
                );
                case EscfColumnKind.UNION -> EscfColumnData.ofUnion(docCount, validity, typeVector, offsets, data.slice(pos, dataLen));
                case EscfColumnKind.STRING, EscfColumnKind.BINARY -> EscfColumnData.ofVarWidth(
                    kind,
                    docCount,
                    validity,
                    offsets,
                    data.slice(pos, dataLen)
                );
                case EscfColumnKind.LONG, EscfColumnKind.DOUBLE -> EscfColumnData.ofFixed64(
                    kind,
                    docCount,
                    validity,
                    data.slice(pos, dataLen)
                );
                default -> throw new IllegalStateException("Unknown ESCF column kind: " + EscfColumnKind.name(kind));
            };
        }
        return new EscfBatch(schema, docCount, columns, data, releasable);
    }

    static BytesReference serialize(SourceSchema schema, int docCount, EscfColumnData[] columns) {
        int colCount = schema.leafCount();

        int schemaSize = schemaSize(schema);
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
            EscfColumnData col = columns[c];
            absentPart[c] = col.validity() != null ? bitsetToRef(col.validity(), docCount) : null;
            typeVecPart[c] = col.typeVector() != null
                ? new BytesArray(col.typeVector().bytes, col.typeVector().offset, col.typeVector().length)
                : null;
            offsetsPart[c] = col.offsets() != null ? intArrayToRef(col.offsets()) : null;
            // BOOL keeps its value bitset in the data slot; ARRAY flattens its native child column to
            // child_kind(1) | child_values bytes here (the only place ESCF serializes an ARRAY's child);
            // every other kind already has a byte payload.
            if (col.kind() == EscfColumnKind.BOOL) {
                dataPart[c] = bitsetToRef(col.values(), docCount);
            } else if (col.kind() == EscfColumnKind.ARRAY) {
                dataPart[c] = encodeArrayChild(col.child());
            } else {
                dataPart[c] = col.data();
            }
        }

        int[] flags = new int[colCount];
        int[] baseOffsets = new int[colCount];
        int cumDataOffset = 0;
        for (int c = 0; c < colCount; c++) {
            baseOffsets[c] = cumDataOffset;
            int f = 0;
            if (absentPart[c] != null) {
                f |= FLAG_VALIDITY;
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

        writeSchema(schema, header, schemaOffset);

        int pos = columnIndexOffset;
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

    /** Number of bytes the serialized schema occupies (a {@code u16} count and, per field, parent + name-length + name). */
    static int schemaSize(SourceSchema schema) {
        int size = 2;
        for (int i = 0; i < schema.nonLeafCount(); i++) {
            size += 2 + 2 + schema.getNonLeafName(i).getBytes(StandardCharsets.UTF_8).length;
        }
        size += 2;
        for (int i = 0; i < schema.leafCount(); i++) {
            size += 2 + 2 + schema.getLeafName(i).getBytes(StandardCharsets.UTF_8).length;
        }
        return size;
    }

    /** Writes {@code schema} into {@code buf} starting at {@code pos}; returns the position just past the schema. */
    static int writeSchema(SourceSchema schema, byte[] buf, int pos) {
        int nonLeafCount = schema.nonLeafCount();
        int leafCount = schema.leafCount();
        writeShortLE(buf, pos, nonLeafCount);
        pos += 2;
        for (int i = 0; i < nonLeafCount; i++) {
            byte[] name = schema.getNonLeafName(i).getBytes(StandardCharsets.UTF_8);
            writeShortLE(buf, pos, schema.getNonLeafParent(i));
            pos += 2;
            writeShortLE(buf, pos, name.length);
            pos += 2;
            System.arraycopy(name, 0, buf, pos, name.length);
            pos += name.length;
        }
        writeShortLE(buf, pos, leafCount);
        pos += 2;
        for (int i = 0; i < leafCount; i++) {
            byte[] name = schema.getLeafName(i).getBytes(StandardCharsets.UTF_8);
            writeShortLE(buf, pos, schema.getLeafParent(i));
            pos += 2;
            writeShortLE(buf, pos, name.length);
            pos += 2;
            System.arraycopy(name, 0, buf, pos, name.length);
            pos += name.length;
        }
        return pos;
    }

    static SourceSchema parseSchema(BytesReference data, int offset) {
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

    /** Number of bytes needed to hold {@code docCount} bits as little-endian 64-bit words. */
    static int bitsetBytes(int docCount) {
        return ((docCount + 63) / 64) * 8;
    }

    /** Serialises {@code bs} (or an all-clear bitset when {@code bs == null}) to {@code bitsetBytes(docCount)} LE bytes. */
    static BytesReference bitsetToRef(FixedBitSet bs, int docCount) {
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

    static BytesReference intArrayToRef(int[] values) {
        byte[] out = new byte[values.length * 4];
        for (int i = 0; i < values.length; i++) {
            ByteUtils.writeIntLE(values[i], out, i * 4);
        }
        return new BytesArray(out);
    }

    /**
     * Flattens an ARRAY column's native {@code child} into the on-disk {@code child_kind(1) | child_values}
     * bytes (child offsets, for a STRING child, are written right after the kind byte). This is the only
     * place ESCF serializes an array's child; {@link #decodeArrayChild} is its exact inverse.
     */
    static BytesReference encodeArrayChild(EscfColumnData child) {
        BytesReference kindByte = new BytesArray(new byte[] { child.kind() });
        if (child.kind() == EscfColumnKind.STRING) {
            return CompositeBytesReference.of(kindByte, intArrayToRef(child.offsets()), child.data());
        }
        return CompositeBytesReference.of(kindByte, child.data());
    }

    /** Parses {@code bitsetBytes(docCount)} LE bytes at {@code pos} into a {@link FixedBitSet}. */
    static FixedBitSet bytesToFixedBitSet(BytesReference data, int pos, int docCount) {
        int words = bitsetBytes(docCount) / 8;
        long[] bits = new long[words];
        for (int w = 0; w < words; w++) {
            bits[w] = data.getLongLE(pos + w * 8);
        }
        return new FixedBitSet(bits, words * 64);
    }

    static byte[] bytesToByteArray(BytesReference data, int pos, int len) {
        BytesRef ref = data.slice(pos, len).toBytesRef();
        return Arrays.copyOfRange(ref.bytes, ref.offset, ref.offset + len);
    }

    /** Parses {@code (count + 1)} LE i32 values at {@code pos} into an {@code int[]}. */
    static int[] bytesToOffsets(BytesReference data, int pos, int count) {
        int[] offsets = new int[count + 1];
        for (int i = 0; i <= count; i++) {
            offsets[i] = data.getIntLE(pos + i * 4);
        }
        return offsets;
    }

    /**
     * Parses the {@code child_kind(1) | child_values} bytes at {@code [pos, pos + dataLen)} into a native
     * {@code child} column with {@code totalElems} elements. Exact inverse of {@link #encodeArrayChild}.
     */
    static EscfColumnData decodeArrayChild(BytesReference data, int pos, int dataLen, int totalElems) {
        byte childKind = data.get(pos);
        int childBase = pos + 1;
        if (childKind == EscfColumnKind.STRING) {
            int[] childOffsets = bytesToOffsets(data, childBase, totalElems);
            int childDataBase = childBase + (totalElems + 1) * 4;
            BytesReference childData = data.slice(childDataBase, pos + dataLen - childDataBase);
            return EscfColumnData.ofVarWidth(EscfColumnKind.STRING, totalElems, null, childOffsets, childData);
        }
        BytesReference childData = data.slice(childBase, pos + dataLen - childBase);
        return EscfColumnData.ofFixed64(childKind, totalElems, null, childData);
    }

    static void writeShortLE(byte[] buf, int offset, int value) {
        if (value < 0 || value > 0xFFFF) {
            throw new IllegalArgumentException("value [" + value + "] does not fit in an unsigned 16-bit field");
        }
        buf[offset] = (byte) value;
        buf[offset + 1] = (byte) (value >>> 8);
    }

    // TODO: Optimize onto bytes reference
    static int readU16LE(BytesReference data, int offset) {
        return (data.get(offset) & 0xFF) | ((data.get(offset + 1) & 0xFF) << 8);
    }
}
