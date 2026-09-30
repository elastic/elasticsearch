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
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.CompositeBytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.io.stream.RecyclerBytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.sourcebatch.SourceSchema;

import java.io.IOException;
import java.io.UncheckedIOException;
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

    static ReleasableBytesReference serialize(SourceSchema schema, int docCount, EscfColumnData[] columns, Recycler<BytesRef> recycler) {
        int colCount = schema.leafCount();

        int schemaSize = schemaSize(schema);
        int columnIndexSize = colCount * COLUMN_INDEX_ENTRY_SIZE;
        int schemaOffset = HEADER_SIZE;
        int columnIndexOffset = schemaOffset + schemaSize;
        int dataOffset = columnIndexOffset + columnIndexSize;

        int[] validityLen = new int[colCount];
        int[] typeVecLen = new int[colCount];
        int[] offsetsLen = new int[colCount];
        int[] dataLen = new int[colCount];
        int cumDataOffset = 0;
        for (int c = 0; c < colCount; c++) {
            EscfColumnData col = columns[c];
            validityLen[c] = col.validity() != null ? bitsetBytes(docCount) : 0;
            typeVecLen[c] = col.typeVector() != null ? col.typeVector().length : 0;
            offsetsLen[c] = col.offsets() != null ? col.offsets().length * 4 : 0;
            dataLen[c] = dataLength(col, docCount);
            cumDataOffset += validityLen[c] + typeVecLen[c] + offsetsLen[c] + dataLen[c];
        }
        int totalSize = dataOffset + cumDataOffset;

        // Header, column index and every column's metadata go into one recycler stream; payloads are
        // joined by reference. This is the only place ESCF serializes.
        RecyclerBytesStreamOutput out = new RecyclerBytesStreamOutput(recycler);
        boolean success = false;
        try {
            out.writeIntLE(MAGIC_LE);
            out.writeIntLE(VERSION);
            out.writeIntLE(0);
            out.writeIntLE(docCount);
            out.writeIntLE(schemaOffset);
            out.writeIntLE(columnIndexOffset);
            out.writeIntLE(dataOffset);
            out.writeIntLE(totalSize);
            writeSchema(schema, out);

            int baseOffset = 0;
            for (int c = 0; c < colCount; c++) {
                EscfColumnData col = columns[c];
                out.writeByte(col.kind());
                out.writeByte((byte) presentFlags(col));
                out.writeIntLE(baseOffset);
                out.writeIntLE(validityLen[c]);
                out.writeIntLE(typeVecLen[c]);
                out.writeIntLE(offsetsLen[c]);
                out.writeIntLE(dataLen[c]);
                baseOffset += validityLen[c] + typeVecLen[c] + offsetsLen[c] + dataLen[c];
            }

            BytesReference[] payloads = new BytesReference[colCount];
            long[] metadataEnds = new long[colCount];
            for (int c = 0; c < colCount; c++) {
                EscfColumnData col = columns[c];
                if (col.validity() != null) {
                    writeBitset(out, col.validity(), docCount);
                }
                if (col.typeVector() != null) {
                    out.writeBytes(col.typeVector().bytes, col.typeVector().offset, col.typeVector().length);
                }
                if (col.offsets() != null) {
                    writeOffsets(out, col.offsets());
                }
                // BOOL keeps its value bitset in the data slot; ARRAY flattens its native child column to
                // child_kind(1) | child_values bytes here (the only place ESCF serializes an ARRAY's child);
                // every other kind already has a byte payload.
                payloads[c] = switch (col.kind()) {
                    case EscfColumnKind.BOOL -> {
                        writeBitset(out, col.values(), docCount);
                        yield null;
                    }
                    case EscfColumnKind.ARRAY -> {
                        writeArrayChildPrefix(out, col.child());
                        yield col.child().data();
                    }
                    case EscfColumnKind.LONG, EscfColumnKind.DOUBLE, EscfColumnKind.STRING, EscfColumnKind.BINARY, EscfColumnKind.UNION ->
                        col.data();
                    default -> throw new IllegalStateException("Unknown ESCF column kind: " + EscfColumnKind.name(col.kind()));
                };
                metadataEnds[c] = out.position();
            }
            assert out.position() + payloadsLength(payloads) == totalSize : out.position() + " + payloads != " + totalSize;

            BytesReference written = out.bytes();
            ReleasableBytesReference pages = out.moveToBytesReference();
            success = true;
            List<BytesReference> parts = new ArrayList<>(1 + 2 * colCount);
            int from = 0;
            for (int c = 0; c < colCount; c++) {
                int to = Math.toIntExact(metadataEnds[c]);
                addNonEmpty(parts, written.slice(from, to - from));
                addNonEmpty(parts, payloads[c]);
                from = to;
            }
            addNonEmpty(parts, written.slice(from, written.length() - from));
            Releasable releasePages = pages;
            return new ReleasableBytesReference(CompositeBytesReference.of(parts.toArray(new BytesReference[0])), releasePages);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            if (success == false) {
                out.close();
            }
        }
    }

    private static int presentFlags(EscfColumnData col) {
        int flags = 0;
        if (col.validity() != null) {
            flags |= FLAG_VALIDITY;
        }
        if (col.typeVector() != null) {
            flags |= FLAG_TYPE_VECTOR;
        }
        if (col.offsets() != null) {
            flags |= FLAG_OFFSETS;
        }
        return flags;
    }

    private static int dataLength(EscfColumnData col, int docCount) {
        return switch (col.kind()) {
            case EscfColumnKind.BOOL -> bitsetBytes(docCount);
            case EscfColumnKind.ARRAY -> arrayChildLength(col.child());
            case EscfColumnKind.LONG, EscfColumnKind.DOUBLE, EscfColumnKind.STRING, EscfColumnKind.BINARY, EscfColumnKind.UNION -> col
                .data()
                .length();
            default -> throw new IllegalStateException("Unknown ESCF column kind: " + EscfColumnKind.name(col.kind()));
        };
    }

    private static long payloadsLength(BytesReference[] payloads) {
        long length = 0;
        for (BytesReference payload : payloads) {
            length += payload == null ? 0 : payload.length();
        }
        return length;
    }

    private static void addNonEmpty(List<BytesReference> parts, @Nullable BytesReference part) {
        if (part != null && part.length() > 0) {
            parts.add(part);
        }
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

    /** Writes {@code schema} to {@code out}, exactly {@link #schemaSize} bytes. */
    static void writeSchema(SourceSchema schema, StreamOutput out) throws IOException {
        int nonLeafCount = schema.nonLeafCount();
        int leafCount = schema.leafCount();
        writeU16LE(out, nonLeafCount);
        for (int i = 0; i < nonLeafCount; i++) {
            byte[] name = schema.getNonLeafName(i).getBytes(StandardCharsets.UTF_8);
            writeU16LE(out, schema.getNonLeafParent(i));
            writeU16LE(out, name.length);
            out.writeBytes(name);
        }
        writeU16LE(out, leafCount);
        for (int i = 0; i < leafCount; i++) {
            byte[] name = schema.getLeafName(i).getBytes(StandardCharsets.UTF_8);
            writeU16LE(out, schema.getLeafParent(i));
            writeU16LE(out, name.length);
            out.writeBytes(name);
        }
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

    /** Writes {@code bs} (or an all-clear bitset when {@code bs == null}) to {@code out} as {@code bitsetBytes(docCount)} LE bytes. */
    static void writeBitset(StreamOutput out, @Nullable FixedBitSet bs, int docCount) throws IOException {
        int wordCount = bitsetBytes(docCount) / 8;
        int presentWords = bs != null ? Math.min(bs.getBits().length, wordCount) : 0;
        if (presentWords > 0) {
            out.writeLongsLE(bs.getBits(), 0, presentWords);
        }
        for (int w = presentWords; w < wordCount; w++) {
            out.writeLongLE(0L);
        }
    }

    static void writeOffsets(StreamOutput out, int[] values) throws IOException {
        out.writeIntsLE(values, 0, values.length);
    }

    /**
     * Writes the {@code child_kind(1)} prefix of an ARRAY column's native {@code child} (child offsets, for a
     * STRING child, are written right after the kind byte); the {@code child_values} follow it by reference.
     * This is the only place ESCF serializes an array's child; {@link #decodeArrayChild} is its exact inverse.
     */
    static void writeArrayChildPrefix(StreamOutput out, EscfColumnData child) throws IOException {
        // BINARY as an array child is var-width but decodeArrayChild treats every non-STRING child as
        // fixed-width; a round-trip would corrupt the column. Fail loudly until the codec gap is closed.
        assert child.kind() != EscfColumnKind.BINARY : "BINARY array child does not round-trip through the codec; see decodeArrayChild";
        out.writeByte(child.kind());
        if (child.kind() == EscfColumnKind.STRING) {
            writeOffsets(out, child.offsets());
        }
    }

    static int arrayChildLength(EscfColumnData child) {
        int prefix = 1 + (child.kind() == EscfColumnKind.STRING ? child.offsets().length * 4 : 0);
        return prefix + child.data().length();
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
     * {@code child} column with {@code totalElems} elements. Exact inverse of {@link #writeArrayChildPrefix}
     * followed by the child values.
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

    static void writeU16LE(StreamOutput out, int value) throws IOException {
        if (value < 0 || value > 0xFFFF) {
            throw new IllegalArgumentException("value [" + value + "] does not fit in an unsigned 16-bit field");
        }
        out.writeByte((byte) value);
        out.writeByte((byte) (value >>> 8));
    }

    // TODO: Optimize onto bytes reference
    static int readU16LE(BytesReference data, int offset) {
        return (data.get(offset) & 0xFF) | ((data.get(offset + 1) & 0xFF) << 8);
    }
}
