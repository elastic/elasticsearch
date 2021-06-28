/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.transport;

import net.jpountz.lz4.LZ4BlockOutputStream;

import net.jpountz.lz4.LZ4Factory;

import org.elasticsearch.Version;
import org.elasticsearch.common.compress.DeflateCompressor;

import java.io.IOException;
import java.io.OutputStream;

public class Compression {

    public enum Scheme {
        LZ4,
        DEFLATE;
        // TODO: Change after backport
        static final Version LZ4_VERSION = Version.V_8_0_0;
        static final byte[] DEFLATE_HEADER = DeflateCompressor.HEADER;
        static final byte[] LZ4_HEADER = new byte[]{'L', 'Z', '4', '\0'};
        static final int HEADER_LENGTH = 4;
        private static final int LZ4_BLOCK_SIZE;

        static {
            String blockSizeString = System.getProperty("es.transport.compression.lz4_block_size");
            if (blockSizeString != null) {
                int lz4BlockSize = Integer.parseInt(blockSizeString);
                if (lz4BlockSize < 1024 || lz4BlockSize > (512 * 1024)) {
                    throw new IllegalArgumentException("lz4_block_size must be >= 1KB and <= 512KB");
                }
                LZ4_BLOCK_SIZE = lz4BlockSize;
            } else {
                // 16KB block size to minimize the allocation of large buffers
                LZ4_BLOCK_SIZE = 16 * 1024;
            }
        }

        public static OutputStream lz4OutputStream(OutputStream outputStream) throws IOException {
            outputStream.write(LZ4_HEADER);
            // 16KB block size to minimize the allocation of large buffers
            return new LZ4BlockOutputStream(outputStream, LZ4_BLOCK_SIZE, LZ4Factory.safeInstance().fastCompressor());
        }
    }

    public enum Enabled {
        TRUE,
        INDEXING_DATA,
        FALSE
    }

}
