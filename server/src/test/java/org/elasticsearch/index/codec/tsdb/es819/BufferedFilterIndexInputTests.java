/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.es819;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.tests.util.LuceneTestCase;

import java.io.IOException;

public class BufferedFilterIndexInputTests extends LuceneTestCase {

    public void testReadByte() throws IOException {
        try (Directory dir = newDirectory()) {
            try (IndexOutput out = dir.createOutput("test", IOContext.DEFAULT)) {
                for (int i = 0; i < 256; i++) {
                    out.writeByte((byte) i);
                }
            }

            try (IndexInput raw = dir.openInput("test", IOContext.DEFAULT)) {
                try (BufferedFilterIndexInput in = new BufferedFilterIndexInput(raw)) {
                    for (int i = 0; i < 256; i++) {
                        assertEquals((byte) i, in.readByte());
                    }
                }
            }
        }
    }

    public void testReadBytes() throws IOException {
        try (Directory dir = newDirectory()) {
            byte[] data = new byte[10000];
            random().nextBytes(data);

            try (IndexOutput out = dir.createOutput("test", IOContext.DEFAULT)) {
                out.writeBytes(data, data.length);
            }

            try (IndexInput raw = dir.openInput("test", IOContext.DEFAULT)) {
                try (BufferedFilterIndexInput in = new BufferedFilterIndexInput(raw)) {
                    byte[] result = new byte[data.length];
                    in.readBytes(result, 0, result.length);
                    assertArrayEquals(data, result);
                }
            }
        }
    }

    public void testReadBytesLargerThanBuffer() throws IOException {
        try (Directory dir = newDirectory()) {
            // Create data larger than the default buffer size
            byte[] data = new byte[BufferedFilterIndexInput.BUFFER_SIZE * 3];
            random().nextBytes(data);

            try (IndexOutput out = dir.createOutput("test", IOContext.DEFAULT)) {
                out.writeBytes(data, data.length);
            }

            try (IndexInput raw = dir.openInput("test", IOContext.DEFAULT)) {
                try (BufferedFilterIndexInput in = new BufferedFilterIndexInput(raw)) {
                    byte[] result = new byte[data.length];
                    in.readBytes(result, 0, result.length);
                    assertArrayEquals(data, result);
                }
            }
        }
    }

    public void testReadInt() throws IOException {
        try (Directory dir = newDirectory()) {
            try (IndexOutput out = dir.createOutput("test", IOContext.DEFAULT)) {
                out.writeInt(12345);
                out.writeInt(-98765);
                out.writeInt(Integer.MAX_VALUE);
                out.writeInt(Integer.MIN_VALUE);
            }

            try (IndexInput raw = dir.openInput("test", IOContext.DEFAULT)) {
                try (BufferedFilterIndexInput in = new BufferedFilterIndexInput(raw)) {
                    assertEquals(12345, in.readInt());
                    assertEquals(-98765, in.readInt());
                    assertEquals(Integer.MAX_VALUE, in.readInt());
                    assertEquals(Integer.MIN_VALUE, in.readInt());
                }
            }
        }
    }

    public void testReadLong() throws IOException {
        try (Directory dir = newDirectory()) {
            try (IndexOutput out = dir.createOutput("test", IOContext.DEFAULT)) {
                out.writeLong(123456789012345L);
                out.writeLong(-987654321098765L);
                out.writeLong(Long.MAX_VALUE);
                out.writeLong(Long.MIN_VALUE);
            }

            try (IndexInput raw = dir.openInput("test", IOContext.DEFAULT)) {
                try (BufferedFilterIndexInput in = new BufferedFilterIndexInput(raw)) {
                    assertEquals(123456789012345L, in.readLong());
                    assertEquals(-987654321098765L, in.readLong());
                    assertEquals(Long.MAX_VALUE, in.readLong());
                    assertEquals(Long.MIN_VALUE, in.readLong());
                }
            }
        }
    }

    public void testReadShort() throws IOException {
        try (Directory dir = newDirectory()) {
            try (IndexOutput out = dir.createOutput("test", IOContext.DEFAULT)) {
                out.writeShort((short) 12345);
                out.writeShort((short) -9876);
                out.writeShort(Short.MAX_VALUE);
                out.writeShort(Short.MIN_VALUE);
            }

            try (IndexInput raw = dir.openInput("test", IOContext.DEFAULT)) {
                try (BufferedFilterIndexInput in = new BufferedFilterIndexInput(raw)) {
                    assertEquals((short) 12345, in.readShort());
                    assertEquals((short) -9876, in.readShort());
                    assertEquals(Short.MAX_VALUE, in.readShort());
                    assertEquals(Short.MIN_VALUE, in.readShort());
                }
            }
        }
    }

    public void testReadVInt() throws IOException {
        try (Directory dir = newDirectory()) {
            try (IndexOutput out = dir.createOutput("test", IOContext.DEFAULT)) {
                out.writeVInt(0);
                out.writeVInt(127);
                out.writeVInt(128);
                out.writeVInt(16383);
                out.writeVInt(Integer.MAX_VALUE);
            }

            try (IndexInput raw = dir.openInput("test", IOContext.DEFAULT)) {
                try (BufferedFilterIndexInput in = new BufferedFilterIndexInput(raw)) {
                    assertEquals(0, in.readVInt());
                    assertEquals(127, in.readVInt());
                    assertEquals(128, in.readVInt());
                    assertEquals(16383, in.readVInt());
                    assertEquals(Integer.MAX_VALUE, in.readVInt());
                }
            }
        }
    }

    public void testReadVLong() throws IOException {
        try (Directory dir = newDirectory()) {
            try (IndexOutput out = dir.createOutput("test", IOContext.DEFAULT)) {
                out.writeVLong(0L);
                out.writeVLong(127L);
                out.writeVLong(128L);
                out.writeVLong(Long.MAX_VALUE);
            }

            try (IndexInput raw = dir.openInput("test", IOContext.DEFAULT)) {
                try (BufferedFilterIndexInput in = new BufferedFilterIndexInput(raw)) {
                    assertEquals(0L, in.readVLong());
                    assertEquals(127L, in.readVLong());
                    assertEquals(128L, in.readVLong());
                    assertEquals(Long.MAX_VALUE, in.readVLong());
                }
            }
        }
    }

    public void testSeek() throws IOException {
        try (Directory dir = newDirectory()) {
            try (IndexOutput out = dir.createOutput("test", IOContext.DEFAULT)) {
                for (int i = 0; i < 1000; i++) {
                    out.writeInt(i);
                }
            }

            try (IndexInput raw = dir.openInput("test", IOContext.DEFAULT)) {
                try (BufferedFilterIndexInput in = new BufferedFilterIndexInput(raw)) {
                    // Seek forward
                    in.seek(400); // 100th int
                    assertEquals(100, in.readInt());

                    // Seek backward
                    in.seek(0);
                    assertEquals(0, in.readInt());

                    // Seek to end
                    in.seek(3996); // Last int
                    assertEquals(999, in.readInt());
                }
            }
        }
    }

    public void testSeekWithinBuffer() throws IOException {
        try (Directory dir = newDirectory()) {
            try (IndexOutput out = dir.createOutput("test", IOContext.DEFAULT)) {
                for (int i = 0; i < 100; i++) {
                    out.writeByte((byte) i);
                }
            }

            try (IndexInput raw = dir.openInput("test", IOContext.DEFAULT)) {
                try (BufferedFilterIndexInput in = new BufferedFilterIndexInput(raw)) {
                    // Read to populate buffer
                    in.readByte();
                    in.readByte();

                    // Seek within buffer (should not refill)
                    in.seek(0);
                    assertEquals(0, in.getFilePointer());
                    assertEquals((byte) 0, in.readByte());
                }
            }
        }
    }

    public void testGetFilePointer() throws IOException {
        try (Directory dir = newDirectory()) {
            try (IndexOutput out = dir.createOutput("test", IOContext.DEFAULT)) {
                for (int i = 0; i < 100; i++) {
                    out.writeByte((byte) i);
                }
            }

            try (IndexInput raw = dir.openInput("test", IOContext.DEFAULT)) {
                try (BufferedFilterIndexInput in = new BufferedFilterIndexInput(raw)) {
                    assertEquals(0, in.getFilePointer());
                    in.readByte();
                    assertEquals(1, in.getFilePointer());
                    in.readInt();
                    assertEquals(5, in.getFilePointer());
                    in.seek(50);
                    assertEquals(50, in.getFilePointer());
                }
            }
        }
    }

    public void testLength() throws IOException {
        try (Directory dir = newDirectory()) {
            try (IndexOutput out = dir.createOutput("test", IOContext.DEFAULT)) {
                for (int i = 0; i < 100; i++) {
                    out.writeByte((byte) i);
                }
            }

            try (IndexInput raw = dir.openInput("test", IOContext.DEFAULT)) {
                try (BufferedFilterIndexInput in = new BufferedFilterIndexInput(raw)) {
                    assertEquals(100, in.length());
                }
            }
        }
    }

    public void testSlice() throws IOException {
        try (Directory dir = newDirectory()) {
            try (IndexOutput out = dir.createOutput("test", IOContext.DEFAULT)) {
                for (int i = 0; i < 100; i++) {
                    out.writeByte((byte) i);
                }
            }

            try (IndexInput raw = dir.openInput("test", IOContext.DEFAULT)) {
                try (BufferedFilterIndexInput in = new BufferedFilterIndexInput(raw)) {
                    try (IndexInput slice = in.slice("slice", 10, 20)) {
                        assertEquals(20, slice.length());
                        assertEquals((byte) 10, slice.readByte());
                        assertEquals((byte) 11, slice.readByte());
                    }
                }
            }
        }
    }

    public void testClone() throws IOException {
        try (Directory dir = newDirectory()) {
            try (IndexOutput out = dir.createOutput("test", IOContext.DEFAULT)) {
                for (int i = 0; i < 100; i++) {
                    out.writeByte((byte) i);
                }
            }

            try (IndexInput raw = dir.openInput("test", IOContext.DEFAULT)) {
                try (BufferedFilterIndexInput in = new BufferedFilterIndexInput(raw)) {
                    in.readByte();
                    in.readByte();

                    IndexInput clone = in.clone();
                    // Clone should start at same position
                    assertEquals(in.getFilePointer(), clone.getFilePointer());
                    // Clone reads independently
                    assertEquals((byte) 2, clone.readByte());
                    assertEquals(3, clone.getFilePointer());
                    assertEquals(2, in.getFilePointer()); // Original unchanged
                }
            }
        }
    }

    public void testRandomAccessReadByte() throws IOException {
        try (Directory dir = newDirectory()) {
            byte[] data = new byte[1000];
            random().nextBytes(data);

            try (IndexOutput out = dir.createOutput("test", IOContext.DEFAULT)) {
                out.writeBytes(data, data.length);
            }

            try (IndexInput raw = dir.openInput("test", IOContext.DEFAULT)) {
                try (BufferedFilterIndexInput in = new BufferedFilterIndexInput(raw)) {
                    RandomAccessInput rai = in;
                    for (int i = 0; i < 100; i++) {
                        int pos = random().nextInt(data.length);
                        assertEquals(data[pos], rai.readByte(pos));
                    }
                }
            }
        }
    }

    public void testRandomAccessReadInt() throws IOException {
        try (Directory dir = newDirectory()) {
            int[] data = new int[250];
            for (int i = 0; i < data.length; i++) {
                data[i] = random().nextInt();
            }

            try (IndexOutput out = dir.createOutput("test", IOContext.DEFAULT)) {
                for (int value : data) {
                    out.writeInt(value);
                }
            }

            try (IndexInput raw = dir.openInput("test", IOContext.DEFAULT)) {
                try (BufferedFilterIndexInput in = new BufferedFilterIndexInput(raw)) {
                    RandomAccessInput rai = in;
                    for (int i = 0; i < 100; i++) {
                        int index = random().nextInt(data.length);
                        assertEquals(data[index], rai.readInt(index * 4L));
                    }
                }
            }
        }
    }

    public void testRandomAccessReadLong() throws IOException {
        try (Directory dir = newDirectory()) {
            long[] data = new long[125];
            for (int i = 0; i < data.length; i++) {
                data[i] = random().nextLong();
            }

            try (IndexOutput out = dir.createOutput("test", IOContext.DEFAULT)) {
                for (long value : data) {
                    out.writeLong(value);
                }
            }

            try (IndexInput raw = dir.openInput("test", IOContext.DEFAULT)) {
                try (BufferedFilterIndexInput in = new BufferedFilterIndexInput(raw)) {
                    RandomAccessInput rai = in;
                    for (int i = 0; i < 100; i++) {
                        int index = random().nextInt(data.length);
                        assertEquals(data[index], rai.readLong(index * 8L));
                    }
                }
            }
        }
    }

    public void testRandomAccessReadShort() throws IOException {
        try (Directory dir = newDirectory()) {
            short[] data = new short[500];
            for (int i = 0; i < data.length; i++) {
                data[i] = (short) random().nextInt();
            }

            try (IndexOutput out = dir.createOutput("test", IOContext.DEFAULT)) {
                for (short value : data) {
                    out.writeShort(value);
                }
            }

            try (IndexInput raw = dir.openInput("test", IOContext.DEFAULT)) {
                try (BufferedFilterIndexInput in = new BufferedFilterIndexInput(raw)) {
                    RandomAccessInput rai = in;
                    for (int i = 0; i < 100; i++) {
                        int index = random().nextInt(data.length);
                        assertEquals(data[index], rai.readShort(index * 2L));
                    }
                }
            }
        }
    }

    public void testReadInts() throws IOException {
        try (Directory dir = newDirectory()) {
            int[] data = new int[500];
            for (int i = 0; i < data.length; i++) {
                data[i] = random().nextInt();
            }

            try (IndexOutput out = dir.createOutput("test", IOContext.DEFAULT)) {
                for (int value : data) {
                    out.writeInt(value);
                }
            }

            try (IndexInput raw = dir.openInput("test", IOContext.DEFAULT)) {
                try (BufferedFilterIndexInput in = new BufferedFilterIndexInput(raw)) {
                    int[] result = new int[data.length];
                    in.readInts(result, 0, result.length);
                    assertArrayEquals(data, result);
                }
            }
        }
    }

    public void testReadLongs() throws IOException {
        try (Directory dir = newDirectory()) {
            long[] data = new long[500];
            for (int i = 0; i < data.length; i++) {
                data[i] = random().nextLong();
            }

            try (IndexOutput out = dir.createOutput("test", IOContext.DEFAULT)) {
                for (long value : data) {
                    out.writeLong(value);
                }
            }

            try (IndexInput raw = dir.openInput("test", IOContext.DEFAULT)) {
                try (BufferedFilterIndexInput in = new BufferedFilterIndexInput(raw)) {
                    long[] result = new long[data.length];
                    in.readLongs(result, 0, result.length);
                    assertArrayEquals(data, result);
                }
            }
        }
    }

    public void testReadFloats() throws IOException {
        try (Directory dir = newDirectory()) {
            float[] data = new float[500];
            for (int i = 0; i < data.length; i++) {
                data[i] = random().nextFloat();
            }

            try (IndexOutput out = dir.createOutput("test", IOContext.DEFAULT)) {
                for (float value : data) {
                    out.writeInt(Float.floatToIntBits(value));
                }
            }

            try (IndexInput raw = dir.openInput("test", IOContext.DEFAULT)) {
                try (BufferedFilterIndexInput in = new BufferedFilterIndexInput(raw)) {
                    float[] result = new float[data.length];
                    in.readFloats(result, 0, result.length);
                    for (int i = 0; i < data.length; i++) {
                        assertEquals(data[i], result[i], 0.0f);
                    }
                }
            }
        }
    }

    public void testBufferSize() throws IOException {
        try (Directory dir = newDirectory()) {
            // Create a file larger than the requested buffer size
            try (IndexOutput out = dir.createOutput("test", IOContext.DEFAULT)) {
                for (int i = 0; i < 1000; i++) {
                    out.writeByte((byte) i);
                }
            }

            try (IndexInput raw = dir.openInput("test", IOContext.DEFAULT)) {
                try (BufferedFilterIndexInput in = new BufferedFilterIndexInput(raw, 512)) {
                    assertEquals(512, in.getBufferSize());
                }
            }
        }
    }

    public void testGetDelegate() throws IOException {
        try (Directory dir = newDirectory()) {
            try (IndexOutput out = dir.createOutput("test", IOContext.DEFAULT)) {
                out.writeByte((byte) 1);
            }

            try (IndexInput raw = dir.openInput("test", IOContext.DEFAULT)) {
                try (BufferedFilterIndexInput in = new BufferedFilterIndexInput(raw)) {
                    assertSame(raw, in.getDelegate());
                }
            }
        }
    }

    public void testEmptyFile() throws IOException {
        try (Directory dir = newDirectory()) {
            try (IndexOutput out = dir.createOutput("test", IOContext.DEFAULT)) {
                // Write nothing
            }

            try (IndexInput raw = dir.openInput("test", IOContext.DEFAULT)) {
                try (BufferedFilterIndexInput in = new BufferedFilterIndexInput(raw)) {
                    assertEquals(0, in.length());
                    assertEquals(0, in.getFilePointer());
                }
            }
        }
    }

    public void testBackwardSeekRefillsBuffer() throws IOException {
        try (Directory dir = newDirectory()) {
            // Create data larger than buffer
            int size = BufferedFilterIndexInput.BUFFER_SIZE * 3;
            try (IndexOutput out = dir.createOutput("test", IOContext.DEFAULT)) {
                for (int i = 0; i < size; i++) {
                    out.writeByte((byte) (i % 256));
                }
            }

            try (IndexInput raw = dir.openInput("test", IOContext.DEFAULT)) {
                try (BufferedFilterIndexInput in = new BufferedFilterIndexInput(raw)) {
                    // Read past buffer
                    in.seek(BufferedFilterIndexInput.BUFFER_SIZE * 2);
                    in.readByte();

                    // Seek backward
                    in.seek(100);
                    assertEquals((byte) 100, in.readByte());
                }
            }
        }
    }
}
