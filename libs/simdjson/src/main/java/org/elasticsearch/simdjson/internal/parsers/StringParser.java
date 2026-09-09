/*
 * @notice
 *
 * Based on a modification of https://github.com/simdjson/simdjson-java,
 * licensed under the Apache License, Version 2.0.
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
 * Modifications copyright (C) 2026 Elasticsearch B.V.
 */

package org.elasticsearch.simdjson.internal.parsers;

import jdk.incubator.vector.ByteVector;
import jdk.incubator.vector.VectorSpecies;

import org.elasticsearch.simdjson.JsonParsingException;
import org.elasticsearch.simdjson.SimdJsonSupport;

import static org.elasticsearch.simdjson.internal.parsers.CharacterUtils.escape;
import static org.elasticsearch.simdjson.internal.parsers.CharacterUtils.hexToInt;

/**
 * Parses JSON string values from a byte buffer into a UTF-8 string buffer.
 *
 * <p>Originally derived from
 * <a href="https://github.com/simdjson/simdjson-java">simdjson-java</a>'s {@code StringParser}.
 * Elasticsearch changes:
 * <ul>
 *   <li>Vector loop bounded by {@code buffer.length - BYTES_PROCESSED} with a scalar
 *       {@link #doParseStringScalar} tail for remaining bytes.</li>
 *   <li>Upstream speculatively {@code intoArray}s every vector chunk before checking for
 *       {@code "} or {@code \}; quote and escape paths copy only the literal prefix via
 *       {@code System.arraycopy}, and {@code intoArray} runs only on the no-special-characters
 *       fast path.</li>
 *   <li>Uses {@link SimdJsonVectorSupport} for vector width selection instead of upstream
 *       {@code VectorUtils}.</li>
 *   <li>Omits upstream {@code parseChar} and length-prefixed {@code parseString} overloads not
 *       needed by the ESCF walker.</li>
 * </ul>
 */
public final class StringParser {

    private static final VectorSpecies<Byte> BYTE_SPECIES = bootstrapVectorSpecies();
    private static final byte BACKSLASH = '\\';
    private static final byte QUOTE = '"';
    private static final int BYTES_PROCESSED = BYTE_SPECIES.vectorByteSize();
    private static final int MIN_HIGH_SURROGATE = 0xD800;
    private static final int MAX_HIGH_SURROGATE = 0xDBFF;
    private static final int MIN_LOW_SURROGATE = 0xDC00;
    private static final int MAX_LOW_SURROGATE = 0xDFFF;

    public int parseString(byte[] buffer, int idx, byte[] stringBuffer) {
        return doParseString(buffer, idx, stringBuffer, 0);
    }

    private int doParseString(byte[] buffer, int idx, byte[] stringBuffer, int offset) {
        int src = idx + 1;
        int dst = offset;
        int loopBound = buffer.length - BYTES_PROCESSED;
        while (src <= loopBound) {
            ByteVector srcVec = ByteVector.fromArray(BYTE_SPECIES, buffer, src);
            long backslashBits = srcVec.eq(BACKSLASH).toLong();
            long quoteBits = srcVec.eq(QUOTE).toLong();

            if (hasQuoteFirst(backslashBits, quoteBits)) {
                int quoteDist = Long.numberOfTrailingZeros(quoteBits);
                System.arraycopy(buffer, src, stringBuffer, dst, quoteDist);
                return dst + quoteDist;
            }
            if (hasBackslash(backslashBits, quoteBits)) {
                int backslashDist = Long.numberOfTrailingZeros(backslashBits);
                System.arraycopy(buffer, src, stringBuffer, dst, backslashDist);
                byte escapeChar = buffer[src + backslashDist + 1];
                if (escapeChar == 'u') {
                    src += backslashDist;
                    dst += backslashDist;
                    int codePoint = parseUnicodeCodePoint(buffer, src);
                    src += 6;
                    if (codePoint >= MIN_HIGH_SURROGATE && codePoint <= MAX_HIGH_SURROGATE) {
                        codePoint = parseLowSurrogate(buffer, src, codePoint);
                        src += 6;
                    } else if (codePoint >= MIN_LOW_SURROGATE && codePoint <= MAX_LOW_SURROGATE) {
                        throw new JsonParsingException("Invalid code point. The range U+DC00–U+DFFF is reserved for low surrogate.");
                    }
                    dst += storeCodePointInStringBuffer(codePoint, dst, stringBuffer);
                } else {
                    stringBuffer[dst + backslashDist] = escape(escapeChar);
                    src += backslashDist + 2;
                    dst += backslashDist + 1;
                }
            } else {
                // Full vector chunk has no quote or escape — bulk-copy literal UTF-8 bytes.
                srcVec.intoArray(stringBuffer, dst);
                src += BYTES_PROCESSED;
                dst += BYTES_PROCESSED;
            }
        }
        return doParseStringScalar(buffer, src, stringBuffer, dst);
    }

    /** Byte-at-a-time fallback for the tail when fewer than BYTES_PROCESSED bytes remain in the buffer. */
    private int doParseStringScalar(byte[] buffer, int src, byte[] stringBuffer, int dst) {
        while (true) {
            byte b = buffer[src];
            if (b == QUOTE) {
                return dst;
            }
            if (b == BACKSLASH) {
                byte escapeChar = buffer[src + 1];
                if (escapeChar == 'u') {
                    int codePoint = parseUnicodeCodePoint(buffer, src);
                    src += 6;
                    if (codePoint >= MIN_HIGH_SURROGATE && codePoint <= MAX_HIGH_SURROGATE) {
                        codePoint = parseLowSurrogate(buffer, src, codePoint);
                        src += 6;
                    } else if (codePoint >= MIN_LOW_SURROGATE && codePoint <= MAX_LOW_SURROGATE) {
                        throw new JsonParsingException("Invalid code point. The range U+DC00–U+DFFF is reserved for low surrogate.");
                    }
                    dst += storeCodePointInStringBuffer(codePoint, dst, stringBuffer);
                } else {
                    stringBuffer[dst] = escape(escapeChar);
                    src += 2;
                    dst++;
                }
            } else {
                stringBuffer[dst] = b;
                src++;
                dst++;
            }
        }
    }

    private int parseLowSurrogate(byte[] buffer, int src, int codePoint) {
        if (src + 6 > buffer.length) {
            throw new JsonParsingException("Low surrogate should start with '\\u'");
        }
        if ((buffer[src] << 8 | buffer[src + 1]) != ('\\' << 8 | 'u')) {
            throw new JsonParsingException("Low surrogate should start with '\\u'");
        } else {
            int codePoint2 = parseUnicodeCodePoint(buffer, src);
            int lowBit = codePoint2 - MIN_LOW_SURROGATE;
            if (lowBit >> 10 == 0) {
                return (((codePoint - MIN_HIGH_SURROGATE) << 10) | lowBit) + 0x10000;
            } else {
                throw new JsonParsingException("Invalid code point. Low surrogate should be in the range U+DC00–U+DFFF.");
            }
        }
    }

    private static int parseUnicodeCodePoint(byte[] buffer, int backslashIndex) {
        if (backslashIndex + 6 > buffer.length) {
            throw new JsonParsingException("Invalid unicode escape sequence: expected four hex digits after \\u");
        }
        return hexToInt(buffer, backslashIndex + 2);
    }

    private int storeCodePointInStringBuffer(int codePoint, int dst, byte[] stringBuffer) {
        if (codePoint < 0) {
            throw new JsonParsingException("Invalid unicode escape sequence: expected four hex digits after \\u");
        }
        if (codePoint <= 0x7F) {
            stringBuffer[dst] = (byte) codePoint;
            return 1;
        }
        if (codePoint <= 0x7FF) {
            stringBuffer[dst] = (byte) ((codePoint >> 6) + 192);
            stringBuffer[dst + 1] = (byte) ((codePoint & 63) + 128);
            return 2;
        }
        if (codePoint <= 0xFFFF) {
            stringBuffer[dst] = (byte) ((codePoint >> 12) + 224);
            stringBuffer[dst + 1] = (byte) (((codePoint >> 6) & 63) + 128);
            stringBuffer[dst + 2] = (byte) ((codePoint & 63) + 128);
            return 3;
        }
        if (codePoint <= 0x10FFFF) {
            stringBuffer[dst] = (byte) ((codePoint >> 18) + 240);
            stringBuffer[dst + 1] = (byte) (((codePoint >> 12) & 63) + 128);
            stringBuffer[dst + 2] = (byte) (((codePoint >> 6) & 63) + 128);
            stringBuffer[dst + 3] = (byte) ((codePoint & 63) + 128);
            return 4;
        }
        throw new IllegalStateException("Code point is greater than 0x110000.");
    }

    private boolean hasQuoteFirst(long backslashBits, long quoteBits) {
        return ((backslashBits - 1) & quoteBits) != 0;
    }

    private boolean hasBackslash(long backslashBits, long quoteBits) {
        return ((quoteBits - 1) & backslashBits) != 0;
    }

    private static VectorSpecies<Byte> bootstrapVectorSpecies() {
        if (SimdJsonSupport.isSupported() == false) {
            throw new ExceptionInInitializerError("StringParser requires simdjson support");
        }
        return SimdJsonVectorSupport.byteSpecies();
    }

}
