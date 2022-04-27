/*
 * Copyright 2020 Adrien Grand and the lz4-java contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Modifications copyright (C) 2021 Elasticsearch B.V.
 */

package org.elasticsearch.lz4;

import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4CompressorWithLength;
import net.jpountz.lz4.LZ4DecompressorWithLength;
import net.jpountz.lz4.LZ4FastDecompressor;
import net.jpountz.lz4.LZ4SafeDecompressor;

import org.elasticsearch.test.ESTestCase;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

/**
 * This file is forked from https://github.com/lz4/lz4-java. In particular, it forks the following file
 * net.jpountz.lz4.AbstractLZ4Test.
 *
 * It modifies the abstract test case to only test byte arrays and byte array backed byte buffers. These are
 * the only bytes we support.
 */
abstract class AbstractLZ4TestCase extends ESTestCase {

    public interface TesterBase<T> {

        T allocate(int length);

        T copyOf(byte[] array);

        byte[] copyOf(T data, int off, int len);

        int maxCompressedLength(int len);

        void fill(T instance, byte b);

        // Modified to remove redundant modifiers
        class ByteArrayTesterBase implements TesterBase<byte[]> {

            @Override
            public byte[] allocate(int length) {
                return new byte[length];
            }

            @Override
            public byte[] copyOf(byte[] array) {
                return Arrays.copyOf(array, array.length);
            }

            @Override
            public byte[] copyOf(byte[] data, int off, int len) {
                return Arrays.copyOfRange(data, off, off + len);
            }

            @Override
            public int maxCompressedLength(int len) {
                return LZ4Utils.maxCompressedLength(len);
            }

            @Override
            public void fill(byte[] instance, byte b) {
                Arrays.fill(instance, b);
            }
        }

        // Modified to remove redundant modifiers
        class ByteBufferTesterBase implements TesterBase<ByteBuffer> {

            @Override
            public ByteBuffer allocate(int length) {
                ByteBuffer bb;
                int slice = randomInt(5);
                // Modified to only test heap ByteBuffers
                bb = ByteBuffer.allocate(length + slice);
                bb.position(slice);
                bb = bb.slice();
                if (randomBoolean()) {
                    bb.order(ByteOrder.LITTLE_ENDIAN);
                } else {
                    bb.order(ByteOrder.BIG_ENDIAN);
                }
                return bb;
            }

            @Override
            public ByteBuffer copyOf(byte[] array) {
                ByteBuffer bb = allocate(array.length).put(array);
                // Modified to not test read only buffers as they do not make the array accessible
                bb.position(0);
                return bb;
            }

            @Override
            public byte[] copyOf(ByteBuffer data, int off, int len) {
                byte[] copy = new byte[len];
                data.position(off);
                data.get(copy);
                return copy;
            }

            @Override
            public int maxCompressedLength(int len) {
                return LZ4Utils.maxCompressedLength(len);
            }

            @Override
            public void fill(ByteBuffer instance, byte b) {
                for (int i = 0; i < instance.capacity(); ++i) {
                    instance.put(i, b);
                }
            }
        }
    }

    public interface Tester<T> extends TesterBase<T> {

        int compress(LZ4Compressor compressor, T src, int srcOff, int srcLen, T dest, int destOff, int maxDestLen);

        int decompress(LZ4FastDecompressor decompressor, T src, int srcOff, T dest, int destOff, int destLen);

        int decompress(LZ4SafeDecompressor decompressor, T src, int srcOff, int srcLen, T dest, int destOff, int maxDestLen);

        // Modified to remove redundant modifiers
        class ByteArrayTester extends ByteArrayTesterBase implements Tester<byte[]> {

            @Override
            public int compress(LZ4Compressor compressor, byte[] src, int srcOff, int srcLen, byte[] dest, int destOff, int maxDestLen) {
                return compressor.compress(src, srcOff, srcLen, dest, destOff, maxDestLen);
            }

            @Override
            public int decompress(LZ4FastDecompressor decompressor, byte[] src, int srcOff, byte[] dest, int destOff, int destLen) {
                return decompressor.decompress(src, srcOff, dest, destOff, destLen);
            }

            @Override
            public int decompress(
                LZ4SafeDecompressor decompressor,
                byte[] src,
                int srcOff,
                int srcLen,
                byte[] dest,
                int destOff,
                int maxDestLen
            ) {
                return decompressor.decompress(src, srcOff, srcLen, dest, destOff, maxDestLen);
            }
        }

        // Modified to remove redundant modifiers
        Tester<byte[]> BYTE_ARRAY = new ByteArrayTester();
        // Modified to remove redundant modifiers
        Tester<byte[]> BYTE_ARRAY_WITH_LENGTH = new ByteArrayTester() {
            @Override
            public int compress(LZ4Compressor compressor, byte[] src, int srcOff, int srcLen, byte[] dest, int destOff, int maxDestLen) {
                return new LZ4CompressorWithLength(compressor).compress(src, srcOff, srcLen, dest, destOff, maxDestLen);
            }

            @Override
            public int decompress(LZ4FastDecompressor decompressor, byte[] src, int srcOff, byte[] dest, int destOff, int destLen) {
                return new LZ4DecompressorWithLength(decompressor).decompress(src, srcOff, dest, destOff);
            }

            @Override
            public int decompress(
                LZ4SafeDecompressor decompressor,
                byte[] src,
                int srcOff,
                int srcLen,
                byte[] dest,
                int destOff,
                int maxDestLen
            ) {
                return new LZ4DecompressorWithLength(decompressor).decompress(src, srcOff, srcLen, dest, destOff);
            }
        };

        // Modified to remove redundant modifiers
        class ByteBufferTester extends ByteBufferTesterBase implements Tester<ByteBuffer> {

            @Override
            public int compress(
                LZ4Compressor compressor,
                ByteBuffer src,
                int srcOff,
                int srcLen,
                ByteBuffer dest,
                int destOff,
                int maxDestLen
            ) {
                return compressor.compress(src, srcOff, srcLen, dest, destOff, maxDestLen);
            }

            @Override
            public int decompress(LZ4FastDecompressor decompressor, ByteBuffer src, int srcOff, ByteBuffer dest, int destOff, int destLen) {
                return decompressor.decompress(src, srcOff, dest, destOff, destLen);
            }

            @Override
            public int decompress(
                LZ4SafeDecompressor decompressor,
                ByteBuffer src,
                int srcOff,
                int srcLen,
                ByteBuffer dest,
                int destOff,
                int maxDestLen
            ) {
                return decompressor.decompress(src, srcOff, srcLen, dest, destOff, maxDestLen);
            }
        }

        // Modified to remove redundant modifiers
        Tester<ByteBuffer> BYTE_BUFFER = new ByteBufferTester();
        // Modified to remove redundant modifiers
        Tester<ByteBuffer> BYTE_BUFFER_WITH_LENGTH = new ByteBufferTester() {
            @Override
            public int compress(
                LZ4Compressor compressor,
                ByteBuffer src,
                int srcOff,
                int srcLen,
                ByteBuffer dest,
                int destOff,
                int maxDestLen
            ) {
                return new LZ4CompressorWithLength(compressor).compress(src, srcOff, srcLen, dest, destOff, maxDestLen);
            }

            @Override
            public int decompress(LZ4FastDecompressor decompressor, ByteBuffer src, int srcOff, ByteBuffer dest, int destOff, int destLen) {
                return new LZ4DecompressorWithLength(decompressor).decompress(src, srcOff, dest, destOff);
            }

            @Override
            public int decompress(
                LZ4SafeDecompressor decompressor,
                ByteBuffer src,
                int srcOff,
                int srcLen,
                ByteBuffer dest,
                int destOff,
                int maxDestLen
            ) {
                return new LZ4DecompressorWithLength(decompressor).decompress(src, srcOff, srcLen, dest, destOff);
            }
        };
    }

    // Tester to test a simple compress/decompress(src, dest) type of APIs
    public interface SrcDestTester<T> extends TesterBase<T> {

        int compress(LZ4Compressor compressor, T src, T dest);

        int decompress(LZ4FastDecompressor decompressor, T src, T dest);

        int decompress(LZ4SafeDecompressor decompressor, T src, T dest);

        // Modified to remove redundant modifiers
        class ByteArrayTester extends ByteArrayTesterBase implements SrcDestTester<byte[]> {

            @Override
            public int compress(LZ4Compressor compressor, byte[] src, byte[] dest) {
                return compressor.compress(src, dest);
            }

            @Override
            public int decompress(LZ4FastDecompressor decompressor, byte[] src, byte[] dest) {
                return decompressor.decompress(src, dest);
            }

            @Override
            public int decompress(LZ4SafeDecompressor decompressor, byte[] src, byte[] dest) {
                return decompressor.decompress(src, dest);
            }
        }

        // Modified to remove redundant modifiers
        SrcDestTester<byte[]> BYTE_ARRAY = new ByteArrayTester();
        // Modified to remove redundant modifiers
        SrcDestTester<byte[]> BYTE_ARRAY_WITH_LENGTH = new ByteArrayTester() {
            @Override
            public int compress(LZ4Compressor compressor, byte[] src, byte[] dest) {
                return new LZ4CompressorWithLength(compressor).compress(src, dest);
            }

            @Override
            public int decompress(LZ4FastDecompressor decompressor, byte[] src, byte[] dest) {
                return new LZ4DecompressorWithLength(decompressor).decompress(src, dest);
            }

            @Override
            public int decompress(LZ4SafeDecompressor decompressor, byte[] src, byte[] dest) {
                return new LZ4DecompressorWithLength(decompressor).decompress(src, dest);
            }
        };

        // Modified to remove redundant modifiers
        class ByteBufferTester extends ByteBufferTesterBase implements SrcDestTester<ByteBuffer> {

            @Override
            public int compress(LZ4Compressor compressor, ByteBuffer src, ByteBuffer dest) {
                final int pos = dest.position();
                compressor.compress(src, dest);
                return dest.position() - pos;
            }

            @Override
            public int decompress(LZ4FastDecompressor decompressor, ByteBuffer src, ByteBuffer dest) {
                final int pos = src.position();
                decompressor.decompress(src, dest);
                return src.position() - pos;
            }

            @Override
            public int decompress(LZ4SafeDecompressor decompressor, ByteBuffer src, ByteBuffer dest) {
                final int pos = dest.position();
                decompressor.decompress(src, dest);
                return dest.position() - pos;
            }
        }

        // Modified to remove redundant modifiers
        SrcDestTester<ByteBuffer> BYTE_BUFFER = new ByteBufferTester();
        // Modified to remove redundant modifiers
        SrcDestTester<ByteBuffer> BYTE_BUFFER_WITH_LENGTH = new ByteBufferTester() {
            @Override
            public int compress(LZ4Compressor compressor, ByteBuffer src, ByteBuffer dest) {
                final int pos = dest.position();
                new LZ4CompressorWithLength(compressor).compress(src, dest);
                return dest.position() - pos;
            }

            @Override
            public int decompress(LZ4FastDecompressor decompressor, ByteBuffer src, ByteBuffer dest) {
                final int pos = src.position();
                new LZ4DecompressorWithLength(decompressor).decompress(src, dest);
                return src.position() - pos;
            }

            @Override
            public int decompress(LZ4SafeDecompressor decompressor, ByteBuffer src, ByteBuffer dest) {
                final int pos = dest.position();
                new LZ4DecompressorWithLength(decompressor).decompress(src, dest);
                return dest.position() - pos;
            }
        };
    }

    protected class RandomBytes {
        private final byte[] bytes;

        RandomBytes(int n) {
            assert n > 0 && n <= 256;
            bytes = new byte[n];
            for (int i = 0; i < n; ++i) {
                bytes[i] = (byte) randomInt(255);
            }
        }

        byte next() {
            final int i = randomInt(bytes.length - 1);
            return bytes[i];
        }
    }

    protected static byte[] readResource(String resource) throws IOException {
        InputStream is = AbstractLZ4TestCase.class.getResourceAsStream(resource);
        if (is == null) {
            throw new IllegalStateException("Cannot find " + resource);
        }
        byte[] buf = new byte[4096];
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try {
            while (true) {
                final int read = is.read(buf);
                if (read == -1) {
                    break;
                }
                baos.write(buf, 0, read);
            }
        } finally {
            is.close();
        }
        return baos.toByteArray();
    }

    protected byte[] randomArray(int len, int n) {
        byte[] result = new byte[len];
        RandomBytes randomBytes = new RandomBytes(n);
        for (int i = 0; i < result.length; ++i) {
            result[i] = randomBytes.next();
        }
        return result;
    }

    protected ByteBuffer copyOf(byte[] bytes, int offset, int len) {
        ByteBuffer buffer;
        // Modified to only test heap ByteBuffers
        buffer = ByteBuffer.allocate(bytes.length);
        buffer.put(bytes);
        buffer.position(offset);
        buffer.limit(offset + len);
        if (randomBoolean()) {
            buffer = buffer.asReadOnlyBuffer();
        }
        return buffer;
    }
}
