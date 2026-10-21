/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.numeric;

import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ByteArrayDataOutput;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

/**
 * Covers {@link MetadataBuffer} directly: every write method round-trips through the matching
 * {@link org.apache.lucene.store.DataInput} read, and the fixed-width writes are asserted byte-for-byte
 * against Lucene's own {@link ByteArrayDataOutput}. That byte-layout assertion is what pins the class
 * javadoc's compatibility claim: {@code writeInt}/{@code writeLong} are hand-rolled here, so a byte
 * order change would otherwise corrupt any transform using them without failing a test.
 */
public class MetadataBufferTests extends ESTestCase {

    public void testNewBufferIsEmpty() {
        final MetadataBuffer buffer = new MetadataBuffer();
        assertEquals(0, buffer.size());
    }

    public void testClearResetsBuffer() {
        final MetadataBuffer buffer = new MetadataBuffer();
        buffer.writeVInt(randomIntBetween(1, 10000));
        buffer.writeVLong(randomLongBetween(1L, 100000L));

        buffer.clear();

        assertEquals(0, buffer.size());
    }

    public void testBufferReusableAfterClear() throws IOException {
        final MetadataBuffer buffer = new MetadataBuffer();
        buffer.writeVInt(randomIntBetween(1, 10000));
        buffer.clear();

        final long expected = randomLongBetween(1L, 100000L);
        buffer.writeVLong(expected);

        final ByteArrayDataInput in = flush(buffer);
        assertEquals(expected, in.readVLong());
    }

    public void testBufferGrowsWithLargeData() throws IOException {
        final MetadataBuffer buffer = new MetadataBuffer();
        // Each value stays under 0x80, so one byte apiece: enough of them to outgrow the default capacity.
        final int count = randomIntBetween(100, 127);

        for (int i = 0; i < count; i++) {
            buffer.writeVInt(i);
        }

        assertTrue("Buffer should have grown beyond default capacity", buffer.size() > 64);
        assertEquals(count, buffer.size());

        final ByteArrayDataInput in = flush(buffer);
        for (int i = 0; i < count; i++) {
            assertEquals(i, in.readVInt());
        }
    }

    public void testRoundtrip() throws IOException {
        final MetadataBuffer buffer = new MetadataBuffer();
        final byte byteVal = randomByte();
        final int vintVal = randomIntBetween(0, Integer.MAX_VALUE);
        final long vlongVal = randomLongBetween(0L, Long.MAX_VALUE);
        final int zintVal = randomInt();
        final long zlongVal = randomLong();

        buffer.writeByte(byteVal);
        buffer.writeVInt(vintVal);
        buffer.writeVLong(vlongVal);
        buffer.writeZInt(zintVal);
        buffer.writeZLong(zlongVal);

        final ByteArrayDataInput in = flush(buffer);
        assertEquals(byteVal, in.readByte());
        assertEquals(vintVal, in.readVInt());
        assertEquals(vlongVal, in.readVLong());
        assertEquals(zintVal, in.readZInt());
        assertEquals(zlongVal, in.readZLong());
    }

    public void testWriteIntBoundaryValues() throws IOException {
        final int[] values = new int[] { 0, 1, -1, Integer.MIN_VALUE, Integer.MAX_VALUE };

        for (final int value : values) {
            final MetadataBuffer buffer = new MetadataBuffer();
            buffer.writeInt(value);
            assertEquals(Integer.BYTES, buffer.size());

            final byte[] written = toArray(buffer);
            assertEquals("Roundtrip failed for value " + Integer.toHexString(value), value, new ByteArrayDataInput(written).readInt());

            final byte[] luceneOutput = new byte[Integer.BYTES];
            new ByteArrayDataOutput(luceneOutput).writeInt(value);
            assertArrayEquals("Byte layout mismatch for value " + Integer.toHexString(value), luceneOutput, written);
        }
    }

    public void testWriteLongBoundaryValues() throws IOException {
        final long[] values = new long[] { 0L, 1L, -1L, Long.MIN_VALUE, Long.MAX_VALUE, 0x00000000FFFFFFFFL, 0xFFFFFFFF00000000L };

        for (final long value : values) {
            final MetadataBuffer buffer = new MetadataBuffer();
            buffer.writeLong(value);
            assertEquals(Long.BYTES, buffer.size());

            final byte[] written = toArray(buffer);
            assertEquals("Roundtrip failed for value " + Long.toHexString(value), value, new ByteArrayDataInput(written).readLong());

            final byte[] luceneOutput = new byte[Long.BYTES];
            new ByteArrayDataOutput(luceneOutput).writeLong(value);
            assertArrayEquals("Byte layout mismatch for value " + Long.toHexString(value), luceneOutput, written);
        }
    }

    public void testWriteZIntBoundaryValues() throws IOException {
        final int[] values = new int[] { 0, 1, -1, Integer.MIN_VALUE, Integer.MAX_VALUE };

        for (final int value : values) {
            final MetadataBuffer buffer = new MetadataBuffer();
            buffer.writeZInt(value);

            final ByteArrayDataInput in = flush(buffer);
            assertEquals("Roundtrip failed for value " + Integer.toHexString(value), value, in.readZInt());
        }
    }

    public void testWriteZLongBoundaryValues() throws IOException {
        final long[] values = new long[] { 0L, 1L, -1L, Long.MIN_VALUE, Long.MAX_VALUE };

        for (final long value : values) {
            final MetadataBuffer buffer = new MetadataBuffer();
            buffer.writeZLong(value);

            final ByteArrayDataInput in = flush(buffer);
            assertEquals("Roundtrip failed for value " + Long.toHexString(value), value, in.readZLong());
        }
    }

    public void testWriteLongMultipleValues() throws IOException {
        final MetadataBuffer buffer = new MetadataBuffer();
        final int count = randomIntBetween(5, 20);
        final long[] values = new long[count];

        for (int i = 0; i < count; i++) {
            values[i] = randomLong();
            buffer.writeLong(values[i]);
        }

        assertEquals(count * Long.BYTES, buffer.size());

        final ByteArrayDataInput in = flush(buffer);
        for (int i = 0; i < count; i++) {
            assertEquals("Value at index " + i, values[i], in.readLong());
        }
    }

    public void testWriteLongMixedWithOtherTypes() throws IOException {
        final MetadataBuffer buffer = new MetadataBuffer();
        final byte byteVal = randomByte();
        final long longVal = randomLong();
        final int vintVal = randomIntBetween(0, Integer.MAX_VALUE);
        final int intVal = randomInt();
        final long longVal2 = randomLong();

        buffer.writeByte(byteVal);
        buffer.writeLong(longVal);
        buffer.writeVInt(vintVal);
        buffer.writeInt(intVal);
        buffer.writeLong(longVal2);

        final ByteArrayDataInput in = flush(buffer);
        assertEquals(byteVal, in.readByte());
        assertEquals(longVal, in.readLong());
        assertEquals(vintVal, in.readVInt());
        assertEquals(intVal, in.readInt());
        assertEquals(longVal2, in.readLong());
    }

    public void testWriteToFlushesExactlySizeBytes() throws IOException {
        final MetadataBuffer buffer = new MetadataBuffer();
        buffer.writeByte(randomByte());
        buffer.writeLong(randomLong());
        buffer.writeVInt(randomIntBetween(0, Integer.MAX_VALUE));

        // Deliberately oversized, so a writeTo that flushed the whole backing array would overshoot.
        final ByteArrayDataOutput out = new ByteArrayDataOutput(new byte[256]);
        buffer.writeTo(out);

        assertEquals("writeTo must write exactly size() bytes", buffer.size(), out.getPosition());
    }

    public void testWriteToOnEmptyBufferWritesNothing() throws IOException {
        final ByteArrayDataOutput out = new ByteArrayDataOutput(new byte[64]);
        new MetadataBuffer().writeTo(out);

        assertEquals(0, out.getPosition());
    }

    public void testGetBytesRawArrayContract() throws IOException {
        final MetadataBuffer buffer = new MetadataBuffer();
        final long expected = randomLong();
        buffer.writeLong(expected);

        // getBytes returns the raw backing array, which is grown in steps and so is generally longer
        // than the written prefix; only [0, size()) is meaningful.
        final byte[] raw = buffer.getBytes();
        assertTrue("Backing array must cover the written prefix", raw.length >= buffer.size());
        assertEquals(expected, new ByteArrayDataInput(raw, 0, buffer.size()).readLong());
    }

    public void testWriteVIntNegativeTriggersAssertion() {
        final MetadataBuffer buffer = new MetadataBuffer();
        final AssertionError e = expectThrows(AssertionError.class, () -> buffer.writeVInt(randomIntBetween(Integer.MIN_VALUE, -1)));
        assertTrue(e.getMessage(), e.getMessage().contains("Use writeZInt for signed values"));
    }

    public void testWriteVLongNegativeTriggersAssertion() {
        final MetadataBuffer buffer = new MetadataBuffer();
        final AssertionError e = expectThrows(AssertionError.class, () -> buffer.writeVLong(randomLongBetween(Long.MIN_VALUE, -1L)));
        assertTrue(e.getMessage(), e.getMessage().contains("Use writeZLong for signed values"));
    }

    /** The bytes {@code buffer} flushes through {@link MetadataBuffer#writeTo}, exactly {@code size()} long. */
    private static byte[] toArray(MetadataBuffer buffer) throws IOException {
        final byte[] output = new byte[buffer.size()];
        buffer.writeTo(new ByteArrayDataOutput(output));
        return output;
    }

    /** A reader over exactly the bytes {@code buffer} flushes, so an over-read hits the end of the input. */
    private static ByteArrayDataInput flush(MetadataBuffer buffer) throws IOException {
        return new ByteArrayDataInput(toArray(buffer));
    }
}
