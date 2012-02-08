/* Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under
 * the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS
 * OF ANY KIND, either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */

package org.elasticsearch.common.compress.lzf;

import org.elasticsearch.common.compress.lzf.util.ChunkDecoderFactory;

import java.io.IOException;

/**
 * Decoder that handles decoding of sequence of encoded LZF chunks,
 * combining them into a single contiguous result byte array.
 * As of version 0.9, this class has been mostly replaced by
 * {@link ChunkDecoder}, although static methods are left here
 * and may still be used.
 * All static methods use {@link ChunkDecoderFactory#optimalInstance}
 * to find actual {@link ChunkDecoder} instance to use.
 *
 * @author Tatu Saloranta (tatu@ning.com)
 */
public class LZFDecoder {
    /*
    ///////////////////////////////////////////////////////////////////////
    // Old API
    ///////////////////////////////////////////////////////////////////////
     */

    public static byte[] decode(final byte[] inputBuffer) throws IOException {
        return decode(inputBuffer, 0, inputBuffer.length);
    }

    public static byte[] decode(final byte[] inputBuffer, int offset, int length) throws IOException {
        return ChunkDecoderFactory.optimalInstance().decode(inputBuffer, offset, length);
    }

    public static int decode(final byte[] inputBuffer, final byte[] targetBuffer) throws IOException {
        return decode(inputBuffer, 0, inputBuffer.length, targetBuffer);
    }

    public static int decode(final byte[] sourceBuffer, int offset, int length, final byte[] targetBuffer) throws IOException {
        return ChunkDecoderFactory.optimalInstance().decode(sourceBuffer, offset, length, targetBuffer);
    }

    public static int calculateUncompressedSize(byte[] data, int offset, int length) throws IOException {
        return ChunkDecoder.calculateUncompressedSize(data, offset, length);
    }
}
