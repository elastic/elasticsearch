/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.transport;

import net.jpountz.lz4.LZ4Factory;

import org.elasticsearch.Version;
import org.elasticsearch.common.bytes.BytesReference;

import java.io.IOException;
import java.io.OutputStream;

public class Compression {

    public enum Scheme {
        LZ4,
        DEFLATE;
        
        static final Version LZ4_VERSION = Version.V_7_14_0;
        static final int HEADER_LENGTH = 4;
        private static final byte[] DEFLATE_HEADER = new byte[]{'D', 'F', 'L', '\0'};
        private static final byte[] LZ4_HEADER = new byte[]{'L', 'Z', '4', '\0'};
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
                LZ4_BLOCK_SIZE = 64 * 1024;
            }
        }

        public static boolean isDeflate(BytesReference bytes) {
            byte firstByte = bytes.get(0);
            if (firstByte != Compression.Scheme.DEFLATE_HEADER[0]) {
                return false;
            } else {
                return validateHeader(bytes, DEFLATE_HEADER);
            }
        }

        public static boolean isLZ4(BytesReference bytes) {
            byte firstByte = bytes.get(0);
            if (firstByte != Scheme.LZ4_HEADER[0]) {
                return false;
            } else {
                return validateHeader(bytes, LZ4_HEADER);
            }
        }

        private static boolean validateHeader(BytesReference bytes, byte[] header) {
            for (int i = 1; i < Compression.Scheme.HEADER_LENGTH; ++i) {
                if (bytes.get(i) != header[i]) {
                    return false;
                }
            }
            return true;
        }

        public static OutputStream lz4OutputStream(OutputStream outputStream) throws IOException {
            outputStream.write(LZ4_HEADER);
            return new ReuseBuffersLZ4BlockOutputStream(outputStream, LZ4_BLOCK_SIZE, LZ4Factory.safeInstance().fastCompressor());
        }
    }

    public enum Enabled {
        TRUE,
        INDEXING_DATA,
        FALSE
    }

}
