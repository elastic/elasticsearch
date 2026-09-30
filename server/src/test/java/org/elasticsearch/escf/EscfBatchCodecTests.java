/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.escf;

import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.CompositeBytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.util.ByteUtils;
import org.elasticsearch.sourcebatch.SourceSchema;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Direct unit tests for the ESCF wire-format pieces in {@link EscfBatchCodec}. These exercise each
 * codec in isolation (schema, bitset, offsets, array child) — edge cases that the full-batch
 * round-trip in {@link EscfBatchSerializationTests} can only reach indirectly.
 */
public class EscfBatchCodecTests extends ESTestCase {

    /** A leaf name longer than 255 bytes forces the high byte of the {@code u16} name-length field to be non-zero. */
    public void testSchemaRoundTripWithLongName() throws IOException {
        String longName = "f".repeat(300);
        // Mirrors the on-wire schema shape: non-leaf 0 is the self-parented root, "nested" hangs off it.
        SourceSchema schema = new SourceSchema(
            List.of("", "nested"),
            new int[] { 0, 0 },
            List.of(longName, "short", ""),
            new int[] { 0, 1, 0 }
        );

        SourceSchema parsed = writeAndParseSchema(schema);

        assertEquals(schema.nonLeafCount(), parsed.nonLeafCount());
        for (int i = 0; i < schema.nonLeafCount(); i++) {
            assertEquals("non-leaf name " + i, schema.getNonLeafName(i), parsed.getNonLeafName(i));
            assertEquals("non-leaf parent " + i, schema.getNonLeafParent(i), parsed.getNonLeafParent(i));
        }
        assertEquals(schema.leafCount(), parsed.leafCount());
        for (int i = 0; i < schema.leafCount(); i++) {
            assertEquals("leaf name " + i, schema.getLeafName(i), parsed.getLeafName(i));
            assertEquals("leaf parent " + i, schema.getLeafParent(i), parsed.getLeafParent(i));
        }
        // A 300-byte name really does span both bytes of the length field.
        assertTrue("expected name longer than a single byte can encode", longName.getBytes(StandardCharsets.UTF_8).length > 0xFF);
    }

    public void testSchemaRoundTripRootOnly() throws IOException {
        // A fresh schema holds only the self-parented root non-leaf and no leaves.
        SourceSchema parsed = writeAndParseSchema(new SourceSchema());
        assertEquals(1, parsed.nonLeafCount());
        assertEquals("", parsed.getNonLeafName(0));
        assertEquals(0, parsed.getNonLeafParent(0));
        assertEquals(0, parsed.leafCount());
    }

    public void testSchemaSizeMatchesBytesWritten() throws IOException {
        SourceSchema schema = new SourceSchema(List.of("", "a"), new int[] { 0, 0 }, List.of("x", "yy"), new int[] { 0, 1 });
        BytesStreamOutput out = new BytesStreamOutput();
        EscfBatchCodec.writeSchema(schema, out);
        assertEquals("schemaSize must equal the bytes writeSchema writes", EscfBatchCodec.schemaSize(schema), out.size());
    }

    public void testBitsetRoundTrip() throws IOException {
        for (int docCount : new int[] { 1, 63, 64, 65, 130, 200 }) {
            FixedBitSet bits = new FixedBitSet(docCount);
            for (int i = 0; i < docCount; i++) {
                if (randomBoolean()) {
                    bits.set(i);
                }
            }
            BytesReference ref = writeBitset(bits, docCount);
            assertEquals("bitset byte length for docCount " + docCount, EscfBatchCodec.bitsetBytes(docCount), ref.length());

            FixedBitSet parsed = EscfBatchCodec.bytesToFixedBitSet(ref, 0, docCount);
            for (int i = 0; i < docCount; i++) {
                assertEquals("bit " + i + " (docCount " + docCount + ")", bits.get(i), parsed.get(i));
            }
        }
    }

    public void testNullBitsetSerializesAllClear() throws IOException {
        int docCount = 70;
        BytesReference ref = writeBitset(null, docCount);
        assertEquals(EscfBatchCodec.bitsetBytes(docCount), ref.length());
        FixedBitSet parsed = EscfBatchCodec.bytesToFixedBitSet(ref, 0, docCount);
        assertEquals(0, parsed.cardinality());
    }

    public void testBitsetBytesBoundaries() {
        assertEquals(0, EscfBatchCodec.bitsetBytes(0));
        assertEquals(8, EscfBatchCodec.bitsetBytes(1));
        assertEquals(8, EscfBatchCodec.bitsetBytes(63));
        assertEquals(8, EscfBatchCodec.bitsetBytes(64));
        assertEquals(16, EscfBatchCodec.bitsetBytes(65));
    }

    public void testOffsetsRoundTrip() throws IOException {
        int[] offsets = { 0, 3, 3, 10, 42 };
        BytesReference ref = writeOffsets(offsets);
        assertEquals(offsets.length * 4, ref.length());
        int[] parsed = EscfBatchCodec.bytesToOffsets(ref, 0, offsets.length - 1);
        assertArrayEquals(offsets, parsed);
    }

    public void testReadU16LEHighByte() throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        EscfBatchCodec.writeU16LE(out, 300);
        assertEquals(300, EscfBatchCodec.readU16LE(out.bytes(), 0));
    }

    /** A field name longer than a u16 can encode overflows the on-wire name-length field and must be rejected. */
    public void testWriteSchemaRejectsNameLongerThanU16() {
        String hugeName = "f".repeat(0x10000);
        SourceSchema schema = new SourceSchema(List.of(""), new int[] { 0 }, List.of(hugeName), new int[] { 0 });
        expectThrows(IllegalArgumentException.class, () -> EscfBatchCodec.writeSchema(schema, new BytesStreamOutput()));
    }

    public void testEncodeDecodeStringArrayChild() throws IOException {
        // child of a STRING array: two elements "hi" and "world".
        int[] childOffsets = { 0, 2, 7 };
        byte[] childBytes = "hiworld".getBytes(StandardCharsets.UTF_8);
        EscfColumnData child = EscfColumnData.ofVarWidth(EscfColumnKind.STRING, 2, null, childOffsets, new BytesArray(childBytes));

        BytesReference encoded = encodeArrayChild(child);
        EscfColumnData decoded = EscfBatchCodec.decodeArrayChild(encoded, 0, encoded.length(), 2);

        assertEquals(EscfColumnKind.STRING, decoded.kind());
        assertArrayEquals(childOffsets, decoded.offsets());
        assertEquals(new BytesArray(childBytes), decoded.data());
    }

    public void testEncodeDecodeLongArrayChild() throws IOException {
        byte[] childBytes = new byte[3 * 8];
        ByteUtils.writeLongLE(1L, childBytes, 0);
        ByteUtils.writeLongLE(-2L, childBytes, 8);
        ByteUtils.writeLongLE(Long.MAX_VALUE, childBytes, 16);
        EscfColumnData child = EscfColumnData.ofFixed64(EscfColumnKind.LONG, 3, null, new BytesArray(childBytes));

        BytesReference encoded = encodeArrayChild(child);
        EscfColumnData decoded = EscfBatchCodec.decodeArrayChild(encoded, 0, encoded.length(), 3);

        assertEquals(EscfColumnKind.LONG, decoded.kind());
        assertNull(decoded.offsets());
        assertEquals(new BytesArray(childBytes), decoded.data());
    }

    public void testParseOfInMemoryBatchBytes() throws IOException {
        String[] docs = {
            "{\"i\":1,\"s\":\"alice\",\"arr\":[1,2,3]}",
            "{\"i\":2,\"s\":\"bob\",\"tags\":[\"x\",\"y\"]}",
            "{\"i\":3,\"d\":2.5,\"flag\":true}" };
        List<BytesReference> sources = new ArrayList<>(docs.length);
        for (String doc : docs) {
            sources.add(new BytesArray(doc));
        }
        try (
            EscfBatch batch = EscfEncoder.encode(sources, XContentType.JSON);
            EscfBatch parsed = EscfBatchCodec.parse(batch.data(), () -> {})
        ) {
            assertEquals(batch.docCount(), parsed.docCount());
            assertEquals(batch.columnCount(), parsed.columnCount());
            assertEquals(batch.schema().leafCount(), parsed.schema().leafCount());
        }
    }

    public void testParseRejectsBadMagic() {
        byte[] buf = new byte[EscfBatchCodec.bitsetBytes(0) + 32];
        ByteUtils.writeIntLE(0xDEADBEEF, buf, 0);
        expectThrows(IllegalArgumentException.class, () -> EscfBatchCodec.parse(new BytesArray(buf), () -> {}));
    }

    public void testParseRejectsBadVersion() {
        byte[] buf = new byte[32];
        ByteUtils.writeIntLE(EscfBatchCodec.MAGIC_LE, buf, 0);
        ByteUtils.writeIntLE(EscfBatchCodec.VERSION + 1, buf, 4);
        expectThrows(IllegalArgumentException.class, () -> EscfBatchCodec.parse(new BytesArray(buf), () -> {}));
    }

    private static SourceSchema writeAndParseSchema(SourceSchema schema) throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        EscfBatchCodec.writeSchema(schema, out);
        return EscfBatchCodec.parseSchema(out.bytes(), 0);
    }

    private static BytesReference writeBitset(FixedBitSet bits, int docCount) throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        EscfBatchCodec.writeBitset(out, bits, docCount);
        return out.bytes();
    }

    private static BytesReference writeOffsets(int[] offsets) throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        EscfBatchCodec.writeOffsets(out, offsets);
        return out.bytes();
    }

    private static BytesReference encodeArrayChild(EscfColumnData child) throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        EscfBatchCodec.writeArrayChildPrefix(out, child);
        BytesReference encoded = CompositeBytesReference.of(out.bytes(), child.data());
        assertEquals(EscfBatchCodec.arrayChildLength(child), encoded.length());
        return encoded;
    }
}
