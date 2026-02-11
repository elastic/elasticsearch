/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.lz4;

import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Exception;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class ESLZ4DecompressorTests extends ESTestCase {

    public void testDecompressRealisticUnicode() {
        for (int i = 0; i < 15; ++i) {
            int stringLengthMultiplier = randomFrom(5, 10, 20, 40, 80, 160, 320);

            final String uncompressedString = randomRealisticUnicodeOfCodepointLength(stringLengthMultiplier * 1024);
            byte[] uncompressed = uncompressedString.getBytes(StandardCharsets.UTF_8);

            byte[] compressed = new byte[uncompressed.length + uncompressed.length / 255 + 16];
            LZ4Compressor compressor = LZ4Factory.safeInstance().fastCompressor();
            int unForkedDestinationBytes = compressor.compress(uncompressed, compressed);

            LZ4FastDecompressor decompressor = ESLZ4Decompressor.INSTANCE;
            byte[] output = new byte[uncompressed.length];
            int forkedDestinationBytes = decompressor.decompress(compressed, output);

            assertEquals(unForkedDestinationBytes, forkedDestinationBytes);
            assertArrayEquals(uncompressed, output);
        }
    }

    public void testDecompressRandomBytes() throws IOException {
        for (int i = 0; i < 15; ++i) {
            int uncompressedBytesLength = randomFrom(16, 32, 64, 128, 256, 512, 1024) * 1024;

            BytesStreamOutput bytesStreamOutput = new BytesStreamOutput(uncompressedBytesLength);
            for (int j = 0; j < uncompressedBytesLength / 4; ++j) {
                bytesStreamOutput.writeInt(randomFrom(0, 1, randomInt()));
            }
            byte[] uncompressed = new byte[uncompressedBytesLength];
            bytesStreamOutput.bytes().streamInput().read(uncompressed);

            byte[] compressed = new byte[uncompressed.length + uncompressed.length / 255 + 16];
            LZ4Compressor compressor = LZ4Factory.safeInstance().fastCompressor();
            int unForkedDestinationBytes = compressor.compress(uncompressed, compressed);

            LZ4FastDecompressor decompressor = ESLZ4Decompressor.INSTANCE;
            byte[] output = new byte[uncompressed.length];
            int forkedDestinationBytes = decompressor.decompress(compressed, output);

            assertEquals(unForkedDestinationBytes, forkedDestinationBytes);
            assertArrayEquals(uncompressed, output);
        }
    }

    /**
     * Test that decompression produces deterministic output regardless
     * of prior buffer contents when the match offset equals the write position.
     */
    public void testMatchDecZero() {
        byte[] input = new byte[] {
            // First sequence: 0 literals, matchLen=18, matchDec=0
            0x0E,          // token: literalLen=0, matchLen=14 (14+4=18 total)
            0,
            0,          // matchDec=0
            // Second sequence: final literals to complete the block
            0x70,          // token: literalLen=7, matchLen=0
            0,
            0,
            0,
            0,
            0,
            0,
            0  // 7 literal bytes
        };

        int length = 25;
        byte[] output = new byte[length];

        // first decompression with clean buffer
        ESLZ4Decompressor.INSTANCE.decompress(input, 0, output, 0, length);
        byte[] decompressed1 = Arrays.copyOf(output, length);

        // second decompression with polluted buffer
        Arrays.fill(output, (byte) 0xFF);
        ESLZ4Decompressor.INSTANCE.decompress(input, 0, output, 0, length);
        byte[] decompressed2 = Arrays.copyOf(output, length);

        // both should be identical - all zeros in first 18 bytes, then 7 zero literals
        assertArrayEquals(decompressed1, decompressed2);
    }

    /**
     * Test handling of large extension byte sequences.
     * Verifies that extremely large length values are properly validated
     * and rejected when they exceed buffer capacity.
     */
    public void testLargeExtensionBytes() {
        // Create compressed data with large literalLen
        byte[] compressedData = new byte[300];
        compressedData[0] = (byte) 0xF0; // Token: literalLen = 15, matchLen = 0
        // Add 100 extension bytes: literalLen = 15 + (100 * 255) = 25,515
        for (int i = 1; i <= 100; i++) {
            compressedData[i] = (byte) 0xFF;
        }
        compressedData[101] = 0x00; // Final extension byte

        // Output buffer is too small for the literal length
        byte[] output = new byte[1024];
        expectThrows(
            LZ4Exception.class,
            Matchers.containsString("Malformed input"),
            () -> ESLZ4Decompressor.INSTANCE.decompress(compressedData, 0, output, 0, output.length)
        );
    }

    /**
     * Test handling of truncated source data.
     * Verifies proper error handling when source buffer is exhausted
     * before decompression completes.
     */
    public void testTruncatedSource() {
        byte[] output = new byte[100];

        byte[] emptySource = new byte[0];
        expectThrows(
            LZ4Exception.class,
            Matchers.containsString("Malformed input"),
            () -> ESLZ4Decompressor.INSTANCE.decompress(emptySource, 0, output, 0, output.length)
        );

        // valid first sequence but no continuation token
        byte[] truncatedSource = new byte[] {
            0x00,  // token: literalLen=0, matchLen=0
            0x01,
            0x00  // matchDec=1
            // Missing: next token - loop will check sOff >= srcEnd
        };

        expectThrows(
            LZ4Exception.class,
            Matchers.containsString("Malformed input"),
            () -> ESLZ4Decompressor.INSTANCE.decompress(truncatedSource, 0, output, 0, output.length)
        );
    }

    public void testInsufficientLiterals() {
        byte[] output = new byte[100];

        // token specifies 5 literals but only provides 2
        byte[] compressedData = new byte[] {
            0x50,  // token: literalLen=5, matchLen=0
            0x01,
            0x02  // only 2 literal bytes (specified 5)
        };

        expectThrows(
            LZ4Exception.class,
            Matchers.containsString("Malformed input"),
            () -> ESLZ4Decompressor.INSTANCE.decompress(compressedData, 0, output, 0, output.length)
        );
    }

}
