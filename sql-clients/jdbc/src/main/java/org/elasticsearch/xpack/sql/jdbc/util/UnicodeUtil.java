/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc.util;

public class UnicodeUtil {


    public static final int UNI_SUR_HIGH_START = 0xD800;
    public static final int UNI_SUR_HIGH_END = 0xDBFF;
    public static final int UNI_SUR_LOW_START = 0xDC00;
    public static final int UNI_SUR_LOW_END = 0xDFFF;
    public static final int UNI_REPLACEMENT_CHAR = 0xFFFD;

    private static final long HALF_SHIFT = 10;

    private static final int SURROGATE_OFFSET = Character.MIN_SUPPLEMENTARY_CODE_POINT - (UNI_SUR_HIGH_START << HALF_SHIFT) - UNI_SUR_LOW_START;

    /** Encode characters from this String, starting at offset
     *  for length characters. Output to the destination array
     *  will begin at {@code outOffset}. It is the responsibility of the
     *  caller to make sure that the destination array is large enough.
     *  <p>
     *  note this method returns the final output offset (outOffset + number of bytes written)
     */
    public static int UTF16toUTF8(final CharSequence s, final int offset, final int length, byte[] out, int outOffset) {
        final int end = offset + length;

        int upto = outOffset;
        for (int i = offset; i < end; i++) {
            final int code = (int) s.charAt(i);

            if (code < 0x80)
                out[upto++] = (byte) code;
            else if (code < 0x800) {
                out[upto++] = (byte) (0xC0 | (code >> 6));
                out[upto++] = (byte) (0x80 | (code & 0x3F));
            }
            else if (code < 0xD800 || code > 0xDFFF) {
                out[upto++] = (byte) (0xE0 | (code >> 12));
                out[upto++] = (byte) (0x80 | ((code >> 6) & 0x3F));
                out[upto++] = (byte) (0x80 | (code & 0x3F));
            }
            else {
                // surrogate pair
                // confirm valid high surrogate
                if (code < 0xDC00 && (i < end - 1)) {
                    int utf32 = (int) s.charAt(i + 1);
                    // confirm valid low surrogate and write pair
                    if (utf32 >= 0xDC00 && utf32 <= 0xDFFF) {
                        utf32 = (code << 10) + utf32 + SURROGATE_OFFSET;
                        i++;
                        out[upto++] = (byte) (0xF0 | (utf32 >> 18));
                        out[upto++] = (byte) (0x80 | ((utf32 >> 12) & 0x3F));
                        out[upto++] = (byte) (0x80 | ((utf32 >> 6) & 0x3F));
                        out[upto++] = (byte) (0x80 | (utf32 & 0x3F));
                        continue;
            }
                }
                // replace unpaired surrogate or out-of-order low surrogate
                // with substitution character
                out[upto++] = (byte) 0xEF;
                out[upto++] = (byte) 0xBF;
                out[upto++] = (byte) 0xBD;
        }
        }
        //assert matches(s, offset, length, out, upto);
        return upto;
    }
}
