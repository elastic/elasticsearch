/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.transport;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressorFactory;
import org.elasticsearch.common.io.stream.BytesStream;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;

import java.io.EOFException;
import java.io.IOException;

public class CompressibleBytesOutputStreamTests extends ESTestCase {

    public void testStreamWithoutCompression() throws IOException {
        BytesStream bStream = new ZeroOutOnCloseStream();
        CompressibleBytesOutputStream stream = new CompressibleBytesOutputStream(bStream, false);

        byte[] expectedBytes = randomBytes(randomInt(30));
        stream.write(expectedBytes);

        BytesReference bytesRef = stream.materializeBytes();
        // Closing compression stream does not close underlying stream
        stream.close();

        assertFalse(CompressorFactory.COMPRESSOR.isCompressed(bytesRef));

        StreamInput streamInput = bytesRef.streamInput();
        byte[] actualBytes = new byte[expectedBytes.length];
        streamInput.readBytes(actualBytes, 0, expectedBytes.length);

        assertEquals(-1, streamInput.read());
        assertArrayEquals(expectedBytes, actualBytes);

        bStream.close();

        // The bytes should be zeroed out on close
        for (byte b : bytesRef.toBytesRef().bytes) {
            assertEquals((byte) 0, b);
        }
    }

    public void testStreamWithCompression() throws IOException {
        BytesStream bStream = new ZeroOutOnCloseStream();
        CompressibleBytesOutputStream stream = new CompressibleBytesOutputStream(bStream, true);

        byte[] expectedBytes = randomBytes(randomInt(30));
        stream.write(expectedBytes);

        BytesReference bytesRef = stream.materializeBytes();
        stream.close();

        assertTrue(CompressorFactory.COMPRESSOR.isCompressed(bytesRef));

        StreamInput streamInput = new InputStreamStreamInput(CompressorFactory.COMPRESSOR.threadLocalInputStream(bytesRef.streamInput()));
        byte[] actualBytes = new byte[expectedBytes.length];
        streamInput.readBytes(actualBytes, 0, expectedBytes.length);

        assertEquals(-1, streamInput.read());
        assertArrayEquals(expectedBytes, actualBytes);

        bStream.close();

        // The bytes should be zeroed out on close
        for (byte b : bytesRef.toBytesRef().bytes) {
            assertEquals((byte) 0, b);
        }
    }

    public void testCompressionWithCallingMaterializeFails() throws IOException {
        BytesStream bStream = new ZeroOutOnCloseStream();
        CompressibleBytesOutputStream stream = new CompressibleBytesOutputStream(bStream, true);

        byte[] expectedBytes = randomBytes(between(1, 30));
        stream.write(expectedBytes);


        StreamInput streamInput =
                new InputStreamStreamInput(CompressorFactory.COMPRESSOR.threadLocalInputStream(bStream.bytes().streamInput()));
        byte[] actualBytes = new byte[expectedBytes.length];
        EOFException e = expectThrows(EOFException.class, () -> streamInput.readBytes(actualBytes, 0, expectedBytes.length));
        assertEquals("Unexpected end of ZLIB input stream", e.getMessage());

        stream.close();
    }

    private static byte[] randomBytes(int length) {
        byte[] bytes = new byte[length];
        for (int i = 0; i < bytes.length; ++i) {
            bytes[i] = randomByte();
        }
        return bytes;
    }

    private static class ZeroOutOnCloseStream extends BytesStreamOutput {

        @Override
        public void close() {
            if (bytes != null) {
                int size = (int) bytes.size();
                bytes.set(0, new byte[size], 0, size);
            }
        }
    }
}
