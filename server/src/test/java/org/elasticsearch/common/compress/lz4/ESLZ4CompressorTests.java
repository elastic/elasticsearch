/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.compress.lz4;

import net.jpountz.lz4.LZ4Compressor;

import net.jpountz.lz4.LZ4Factory;

import net.jpountz.lz4.LZ4FastDecompressor;

import org.elasticsearch.test.ESTestCase;

import java.nio.charset.StandardCharsets;

public class ESLZ4CompressorTests extends ESTestCase {

    public void testCompress() {
        for (int i = 0; i < 15; ++i) {
            int stringLengthMultiplier = randomFrom(5, 10, 20, 40, 80, 160, 320);

            final String uncompressedString = randomRealisticUnicodeOfCodepointLength(stringLengthMultiplier * 1024);
            byte[] uncompressed = uncompressedString.getBytes(StandardCharsets.UTF_8);

            byte[] compressed = new byte[uncompressed.length + uncompressed.length / 255 + 16];
            LZ4Compressor compressor = ESLZ4Compressor.INSTANCE;
            compressor.compress(uncompressed, compressed);

            LZ4FastDecompressor decompressor = LZ4Factory.safeInstance().fastDecompressor();
            byte[] output = new byte[uncompressed.length];
            decompressor.decompress(compressed, output);

            assertArrayEquals(uncompressed, output);
        }
    }
}
