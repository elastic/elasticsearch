/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.lz4;

import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

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
}
