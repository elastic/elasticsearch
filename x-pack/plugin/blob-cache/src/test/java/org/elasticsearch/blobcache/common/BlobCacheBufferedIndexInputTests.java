/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.blobcache.common;

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.ArrayUtil;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Random;

// Copy of {@link org.apache.lucene.store.TestBufferedIndexInput} adapted slightly for BlobCacheBufferedIndexInput
public class BlobCacheBufferedIndexInputTests extends ESTestCase {

    private static final long TEST_FILE_LENGTH = 100 * 1024;

    // Call readByte() repeatedly, past the buffer boundary, and see that it
    // is working as expected.
    // Our input comes from a dynamically generated/ "file" - see
    // MyBufferedIndexInput below.
    public void testReadByte() throws Exception {
        var input = new MyBlobCacheBufferedIndexInput(Long.MAX_VALUE);
        for (int i = 0; i < BlobCacheBufferedIndexInput.BUFFER_SIZE * 10; i++) {
            assertEquals(input.readByte(), byten(i));
        }
    }

    // Call readBytes() repeatedly, with various chunk sizes (from 1 byte to
    // larger than the buffer size), and see that it returns the bytes we expect.
    // Our input comes from a dynamically generated "file" -
    // see MyBlobCacheBufferedIndexInput below.
    public void testReadBytes() throws Exception {
        var input = new MyBlobCacheBufferedIndexInput(Long.MAX_VALUE);
        runReadBytes(input, MyBlobCacheBufferedIndexInput.BUFFER_SIZE, random());
    }

    private void runReadBytes(IndexInput input, int bufferSize, Random r) throws IOException {

        int pos = 0;
        // gradually increasing size:
        for (int size = 1; size < bufferSize * 10; size = size + size / 200 + 1) {
            checkReadBytes(input, size, pos);
            pos += size;
            if (pos >= TEST_FILE_LENGTH) {
                // wrap
                pos = 0;
                input.seek(0L);
            }
        }
        // wildly fluctuating size:
        for (long i = 0; i < 100; i++) {
            final int size = r.nextInt(10000);
            checkReadBytes(input, 1 + size, pos);
            pos += 1 + size;
            if (pos >= TEST_FILE_LENGTH) {
                // wrap
                pos = 0;
                input.seek(0L);
            }
        }
        // constant small size (7 bytes):
        for (int i = 0; i < bufferSize; i++) {
            checkReadBytes(input, 7, pos);
            pos += 7;
            if (pos >= TEST_FILE_LENGTH) {
                // wrap
                pos = 0;
                input.seek(0L);
            }
        }
    }

    private byte[] buffer = new byte[10];

    private void checkReadBytes(IndexInput input, int size, int pos) throws IOException {
        // Just to see that "offset" is treated properly in readBytes(), we
        // add an arbitrary offset at the beginning of the array
        int offset = size % 10; // arbitrary
        buffer = ArrayUtil.grow(buffer, offset + size);
        assertEquals(pos, input.getFilePointer());
        long left = TEST_FILE_LENGTH - input.getFilePointer();
        if (left <= 0) {
            return;
        } else if (left < size) {
            size = (int) left;
        }
        input.readBytes(buffer, offset, size);
        assertEquals(pos + size, input.getFilePointer());
        for (int i = 0; i < size; i++) {
            assertEquals("pos=" + i + " filepos=" + (pos + i), byten(pos + i), buffer[offset + i]);
        }
    }

    // This tests that attempts to readBytes() past an EOF will fail, while
    // reads up to the EOF will succeed. The EOF is determined by the
    // BufferedIndexInput's arbitrary length() value.
    public void testEOF() throws Exception {
        var input = new MyBlobCacheBufferedIndexInput(1024);
        // see that we can read all the bytes at one go:
        checkReadBytes(input, (int) input.length(), 0);
        // go back and see that we can't read more than that, for small and
        // large overflows:
        int pos = (int) input.length() - 10;
        input.seek(pos);
        checkReadBytes(input, 10, pos);
        input.seek(pos);
        // block read past end of file
        expectThrows(IOException.class, () -> { checkReadBytes(input, 11, pos); });

        input.seek(pos);

        // block read past end of file
        expectThrows(IOException.class, () -> { checkReadBytes(input, 50, pos); });

        input.seek(pos);

        // block read past end of file
        expectThrows(IOException.class, () -> { checkReadBytes(input, 100000, pos); });
    }

    // Test that when reading backwards, we page backwards rather than refilling
    // on every call
    public void testBackwardsByteReads() throws IOException {
        var input = new MyBlobCacheBufferedIndexInput(1024 * 8);
        for (int i = 2048; i > 0; i -= random().nextInt(16)) {
            assertEquals(byten(i), input.readByte(i));
        }
        assertEquals(3, input.readCount);
    }

    public void testBackwardsShortReads() throws IOException {
        var input = new MyBlobCacheBufferedIndexInput(1024 * 8);
        ByteBuffer bb = ByteBuffer.allocate(2);
        bb.order(ByteOrder.LITTLE_ENDIAN);
        for (int i = 2048; i > 0; i -= (random().nextInt(16) + 1)) {
            bb.clear();
            bb.put(byten(i));
            bb.put(byten(i + 1));
            assertEquals(bb.getShort(0), input.readShort(i));
        }
        // readCount can be three or four, depending on whether or not we had to adjust the bufferStart
        // to include a whole short
        assertTrue("Expected 4 or 3, got " + input.readCount, input.readCount == 4 || input.readCount == 3);
    }

    public void testBackwardsIntReads() throws IOException {
        var input = new MyBlobCacheBufferedIndexInput(1024 * 8);
        ByteBuffer bb = ByteBuffer.allocate(4);
        bb.order(ByteOrder.LITTLE_ENDIAN);
        for (int i = 2048; i > 0; i -= (random().nextInt(16) + 3)) {
            bb.clear();
            bb.put(byten(i));
            bb.put(byten(i + 1));
            bb.put(byten(i + 2));
            bb.put(byten(i + 3));
            assertEquals(bb.getInt(0), input.readInt(i));
        }
        // readCount can be three or four, depending on whether or not we had to adjust the bufferStart
        // to include a whole int
        assertTrue("Expected 4 or 3, got " + input.readCount, input.readCount == 4 || input.readCount == 3);
    }

    public void testBackwardsLongReads() throws IOException {
        var input = new MyBlobCacheBufferedIndexInput(1024 * 8);
        ByteBuffer bb = ByteBuffer.allocate(8);
        bb.order(ByteOrder.LITTLE_ENDIAN);
        for (int i = 2048; i > 0; i -= (random().nextInt(16) + 7)) {
            bb.clear();
            bb.put(byten(i));
            bb.put(byten(i + 1));
            bb.put(byten(i + 2));
            bb.put(byten(i + 3));
            bb.put(byten(i + 4));
            bb.put(byten(i + 5));
            bb.put(byten(i + 6));
            bb.put(byten(i + 7));
            assertEquals(bb.getLong(0), input.readLong(i));
        }
        // readCount can be three or four, depending on whether or not we had to adjust the bufferStart
        // to include a whole long
        assertTrue("Expected 4 or 3, got " + input.readCount, input.readCount == 4 || input.readCount == 3);
    }

    public void testReadFloats() throws IOException {
        final int length = 1024 * 8;
        var input = new MyBlobCacheBufferedIndexInput(length);
        ByteBuffer bb = ByteBuffer.allocate(Float.BYTES).order(ByteOrder.LITTLE_ENDIAN);
        final int bufferLength = 128;
        float[] floatBuffer = new float[bufferLength];

        for (int alignment = 0; alignment < Float.BYTES; alignment++) {
            input.seek(0);
            for (int i = 0; i < alignment; i++) {
                input.readByte();
            }
            final int bulkReads = length / (bufferLength * Float.BYTES) - 1;
            for (int i = 0; i < bulkReads; i++) {
                int pos = alignment + i * bufferLength * Float.BYTES;
                final int floatOffset = random().nextInt(3);
                input.skipBytes(floatOffset * Float.BYTES);
                input.readFloats(floatBuffer, floatOffset, bufferLength - floatOffset);
                for (int idx = floatOffset; idx < bufferLength; idx++) {
                    final int offset = pos + idx * Float.BYTES;
                    bb.position(0).put(byten(offset)).put(byten(offset + 1)).put(byten(offset + 2)).put(byten(offset + 3));
                    assertEquals(Float.floatToRawIntBits(bb.getFloat(0)), Float.floatToRawIntBits(floatBuffer[idx]));
                }
            }
        }
    }

    public void testReadInts() throws IOException {
        final int length = 1024 * 8;
        var input = new MyBlobCacheBufferedIndexInput(length);
        ByteBuffer bb = ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN);
        final int bufferLength = 128;
        int[] intBuffer = new int[bufferLength];

        for (int alignment = 0; alignment < Integer.BYTES; alignment++) {
            input.seek(0);
            for (int i = 0; i < alignment; i++) {
                input.readByte();
            }
            final int bulkReads = length / (bufferLength * Integer.BYTES) - 1;
            for (int i = 0; i < bulkReads; i++) {
                int pos = alignment + i * bufferLength * Integer.BYTES;
                final int intOffset = random().nextInt(3);
                input.skipBytes(intOffset * Integer.BYTES);
                input.readInts(intBuffer, intOffset, bufferLength - intOffset);
                for (int idx = intOffset; idx < bufferLength; idx++) {
                    final int offset = pos + idx * Integer.BYTES;
                    bb.position(0).put(byten(offset)).put(byten(offset + 1)).put(byten(offset + 2)).put(byten(offset + 3));
                    assertEquals(bb.getInt(0), intBuffer[idx]);
                }
            }
        }
    }

    public void testReadLongs() throws IOException {
        final int length = 1024 * 8;
        var input = new MyBlobCacheBufferedIndexInput(length);
        ByteBuffer bb = ByteBuffer.allocate(Long.BYTES).order(ByteOrder.LITTLE_ENDIAN);
        final int bufferLength = 128;
        long[] longBuffer = new long[bufferLength];

        for (int alignment = 0; alignment < Long.BYTES; alignment++) {
            input.seek(0);
            for (int i = 0; i < alignment; i++) {
                input.readByte();
            }
            final int bulkReads = length / (bufferLength * Long.BYTES) - 1;
            for (int i = 0; i < bulkReads; i++) {
                int pos = alignment + i * bufferLength * Long.BYTES;
                final int longOffset = random().nextInt(3);
                input.skipBytes(longOffset * Long.BYTES);
                input.readLongs(longBuffer, longOffset, bufferLength - longOffset);
                for (int idx = longOffset; idx < bufferLength; idx++) {
                    final int offset = pos + idx * Long.BYTES;
                    bb.position(0)
                        .put(byten(offset))
                        .put(byten(offset + 1))
                        .put(byten(offset + 2))
                        .put(byten(offset + 3))
                        .put(byten(offset + 4))
                        .put(byten(offset + 5))
                        .put(byten(offset + 6))
                        .put(byten(offset + 7));
                    assertEquals(bb.getLong(0), longBuffer[idx]);
                }
            }
        }
    }

    // byten emulates a file - byten(n) returns the n'th byte in that file.
    // MyBufferedIndexInput reads this "file".
    private static byte byten(long n) {
        return (byte) (n * n % 256);
    }

    static class MyBlobCacheBufferedIndexInput extends BlobCacheBufferedIndexInput {
        private long pos;
        private final long len;
        private long readCount = 0;

        MyBlobCacheBufferedIndexInput(long len) {
            super("MyBlobCacheBufferedIndexInput(len=" + len + ")", MyBlobCacheBufferedIndexInput.BUFFER_SIZE, len);
            this.len = len;
            this.pos = 0;
        }

        MyBlobCacheBufferedIndexInput() {
            // an infinite file
            this(Long.MAX_VALUE);
        }

        @Override
        protected void readInternal(ByteBuffer b) {
            readCount++;
            while (b.hasRemaining()) {
                b.put(byten(pos++));
            }
        }

        @Override
        protected void seekInternal(long pos) throws IOException {
            this.pos = pos;
        }

        @Override
        public void close() throws IOException {}

        @Override
        public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
            throw new UnsupportedOperationException();
        }
    }
}
