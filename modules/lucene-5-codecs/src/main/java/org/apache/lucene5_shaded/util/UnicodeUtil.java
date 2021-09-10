/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.lucene5_shaded.util;




/*
 * Some of this code came from the excellent Unicode
 * conversion examples from:
 *
 *   http://www.unicode.org/Public/PROGRAMS/CVTUTF
 *
 * Full Copyright for that code follows:
*/

/*
 * Copyright 2001-2004 Unicode, Inc.
 * 
 * Disclaimer
 * 
 * This source code is provided as is by Unicode, Inc. No claims are
 * made as to fitness for any particular purpose. No warranties of any
 * kind are expressed or implied. The recipient agrees to determine
 * applicability of information provided. If this file has been
 * purchased on magnetic or optical media from Unicode, Inc., the
 * sole remedy for any claim will be exchange of defective media
 * within 90 days of receipt.
 * 
 * Limitations on Rights to Redistribute This Code
 * 
 * Unicode, Inc. hereby grants the right to freely use the information
 * supplied in this file in the creation of products supporting the
 * Unicode Standard, and to make copies of this file in any form
 * for internal or external distribution as long as this notice
 * remains attached.
 */

/*
 * Additional code came from the IBM ICU library.
 *
 *  http://www.icu-project.org
 *
 * Full Copyright for that code follows.
 */

/*
 * Copyright (C) 1999-2010, International Business Machines
 * Corporation and others.  All Rights Reserved.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, and/or sell copies of the
 * Software, and to permit persons to whom the Software is furnished to do so,
 * provided that the above copyright notice(s) and this permission notice appear
 * in all copies of the Software and that both the above copyright notice(s) and
 * this permission notice appear in supporting documentation.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT OF THIRD PARTY RIGHTS.
 * IN NO EVENT SHALL THE COPYRIGHT HOLDER OR HOLDERS INCLUDED IN THIS NOTICE BE
 * LIABLE FOR ANY CLAIM, OR ANY SPECIAL INDIRECT OR CONSEQUENTIAL DAMAGES, OR
 * ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER
 * IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT
 * OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 *
 * Except as contained in this notice, the name of a copyright holder shall not
 * be used in advertising or otherwise to promote the sale, use or other
 * dealings in this Software without prior written authorization of the
 * copyright holder.
 */

/**
 * Class to encode java's UTF16 char[] into UTF8 byte[]
 * without always allocating a new byte[] as
 * String.getBytes(StandardCharsets.UTF_8) does.
 *
 * @lucene.internal
 */

public final class UnicodeUtil {
  
  /** A binary term consisting of a number of 0xff bytes, likely to be bigger than other terms
   *  (e.g. collation keys) one would normally encounter, and definitely bigger than any UTF-8 terms.
   *  <p>
   *  WARNING: This is not a valid UTF8 Term  
   **/
  public static final BytesRef BIG_TERM = new BytesRef(
      new byte[] {-1,-1,-1,-1,-1,-1,-1,-1,-1,-1}
  ); // TODO this is unrelated here find a better place for it
  
  private UnicodeUtil() {} // no instance

  public static final int UNI_SUR_HIGH_START = 0xD800;
  public static final int UNI_SUR_HIGH_END = 0xDBFF;
  public static final int UNI_SUR_LOW_START = 0xDC00;
  public static final int UNI_SUR_LOW_END = 0xDFFF;
  public static final int UNI_REPLACEMENT_CHAR = 0xFFFD;

  private static final long UNI_MAX_BMP = 0x0000FFFF;

  private static final long HALF_SHIFT = 10;
  private static final long HALF_MASK = 0x3FFL;
  
  private static final int SURROGATE_OFFSET = 
    Character.MIN_SUPPLEMENTARY_CODE_POINT - 
    (UNI_SUR_HIGH_START << HALF_SHIFT) - UNI_SUR_LOW_START;

  /** Maximum number of UTF8 bytes per UTF16 character. */
  public static final int MAX_UTF8_BYTES_PER_CHAR = 3;

  /** Encode characters from a char[] source, starting at
   *  offset for length chars. It is the responsibility of the
   *  caller to make sure that the destination array is large enough.
   */
  public static int UTF16toUTF8(final char[] source, final int offset, final int length, byte[] out) {

    int upto = 0;
    int i = offset;
    final int end = offset + length;

    while(i < end) {
      
      final int code = (int) source[i++];

      if (code < 0x80)
        out[upto++] = (byte) code;
      else if (code < 0x800) {
        out[upto++] = (byte) (0xC0 | (code >> 6));
        out[upto++] = (byte)(0x80 | (code & 0x3F));
      } else if (code < 0xD800 || code > 0xDFFF) {
        out[upto++] = (byte)(0xE0 | (code >> 12));
        out[upto++] = (byte)(0x80 | ((code >> 6) & 0x3F));
        out[upto++] = (byte)(0x80 | (code & 0x3F));
      } else {
        // surrogate pair
        // confirm valid high surrogate
        if (code < 0xDC00 && i < end) {
          int utf32 = (int) source[i];
          // confirm valid low surrogate and write pair
          if (utf32 >= 0xDC00 && utf32 <= 0xDFFF) { 
            utf32 = (code << 10) + utf32 + SURROGATE_OFFSET;
            i++;
            out[upto++] = (byte)(0xF0 | (utf32 >> 18));
            out[upto++] = (byte)(0x80 | ((utf32 >> 12) & 0x3F));
            out[upto++] = (byte)(0x80 | ((utf32 >> 6) & 0x3F));
            out[upto++] = (byte)(0x80 | (utf32 & 0x3F));
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
    //assert matches(source, offset, length, out, upto);
    return upto;
  }

  /** Encode characters from this String, starting at offset
   *  for length characters. It is the responsibility of the
   *  caller to make sure that the destination array is large enough.
   */
  public static int UTF16toUTF8(final CharSequence s, final int offset, final int length, byte[] out) {
    return UTF16toUTF8(s, offset, length, out, 0);
  }

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
    for(int i=offset;i<end;i++) {
      final int code = (int) s.charAt(i);

      if (code < 0x80)
        out[upto++] = (byte) code;
      else if (code < 0x800) {
        out[upto++] = (byte) (0xC0 | (code >> 6));
        out[upto++] = (byte)(0x80 | (code & 0x3F));
      } else if (code < 0xD800 || code > 0xDFFF) {
        out[upto++] = (byte)(0xE0 | (code >> 12));
        out[upto++] = (byte)(0x80 | ((code >> 6) & 0x3F));
        out[upto++] = (byte)(0x80 | (code & 0x3F));
      } else {
        // surrogate pair
        // confirm valid high surrogate
        if (code < 0xDC00 && (i < end-1)) {
          int utf32 = (int) s.charAt(i+1);
          // confirm valid low surrogate and write pair
          if (utf32 >= 0xDC00 && utf32 <= 0xDFFF) { 
            utf32 = (code << 10) + utf32 + SURROGATE_OFFSET;
            i++;
            out[upto++] = (byte)(0xF0 | (utf32 >> 18));
            out[upto++] = (byte)(0x80 | ((utf32 >> 12) & 0x3F));
            out[upto++] = (byte)(0x80 | ((utf32 >> 6) & 0x3F));
            out[upto++] = (byte)(0x80 | (utf32 & 0x3F));
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

  /**
   * Calculates the number of UTF8 bytes necessary to write a UTF16 string.
   *
   * @return the number of bytes written
   */
  public static int calcUTF16toUTF8Length(final CharSequence s, final int offset, final int len) {
    final int end = offset + len;

    int res = 0;
    for (int i = offset; i < end; i++) {
      final int code = (int) s.charAt(i);

      if (code < 0x80)
        res++;
      else if (code < 0x800) {
        res += 2;
      } else if (code < 0xD800 || code > 0xDFFF) {
        res += 3;
      } else {
        // surrogate pair
        // confirm valid high surrogate
        if (code < 0xDC00 && (i < end - 1)) {
          int utf32 = (int) s.charAt(i + 1);
          // confirm valid low surrogate and write pair
          if (utf32 >= 0xDC00 && utf32 <= 0xDFFF) {
            i++;
            res += 4;
            continue;
          }
        }
        res += 3;
      }
    }

    return res;
  }

  // Only called from assert
  /*
  private static boolean matches(char[] source, int offset, int length, byte[] result, int upto) {
    try {
      String s1 = new String(source, offset, length);
      String s2 = new String(result, 0, upto, StandardCharsets.UTF_8);
      if (!s1.equals(s2)) {
        //System.out.println("DIFF: s1 len=" + s1.length());
        //for(int i=0;i<s1.length();i++)
        //  System.out.println("    " + i + ": " + (int) s1.charAt(i));
        //System.out.println("s2 len=" + s2.length());
        //for(int i=0;i<s2.length();i++)
        //  System.out.println("    " + i + ": " + (int) s2.charAt(i));

        // If the input string was invalid, then the
        // difference is OK
        if (!validUTF16String(s1))
          return true;

        return false;
      }
      return s1.equals(s2);
    } catch (UnsupportedEncodingException uee) {
      return false;
    }
  }

  // Only called from assert
  private static boolean matches(String source, int offset, int length, byte[] result, int upto) {
    try {
      String s1 = source.substring(offset, offset+length);
      String s2 = new String(result, 0, upto, StandardCharsets.UTF_8);
      if (!s1.equals(s2)) {
        // Allow a difference if s1 is not valid UTF-16

        //System.out.println("DIFF: s1 len=" + s1.length());
        //for(int i=0;i<s1.length();i++)
        //  System.out.println("    " + i + ": " + (int) s1.charAt(i));
        //System.out.println("  s2 len=" + s2.length());
        //for(int i=0;i<s2.length();i++)
        //  System.out.println("    " + i + ": " + (int) s2.charAt(i));

        // If the input string was invalid, then the
        // difference is OK
        if (!validUTF16String(s1))
          return true;

        return false;
      }
      return s1.equals(s2);
    } catch (UnsupportedEncodingException uee) {
      return false;
    }
  }
  */
  public static boolean validUTF16String(CharSequence s) {
    final int size = s.length();
    for(int i=0;i<size;i++) {
      char ch = s.charAt(i);
      if (ch >= UNI_SUR_HIGH_START && ch <= UNI_SUR_HIGH_END) {
        if (i < size-1) {
          i++;
          char nextCH = s.charAt(i);
          if (nextCH >= UNI_SUR_LOW_START && nextCH <= UNI_SUR_LOW_END) {
            // Valid surrogate pair
          } else
            // Unmatched high surrogate
            return false;
        } else
          // Unmatched high surrogate
          return false;
      } else if (ch >= UNI_SUR_LOW_START && ch <= UNI_SUR_LOW_END)
        // Unmatched low surrogate
        return false;
    }

    return true;
  }

  public static boolean validUTF16String(char[] s, int size) {
    for(int i=0;i<size;i++) {
      char ch = s[i];
      if (ch >= UNI_SUR_HIGH_START && ch <= UNI_SUR_HIGH_END) {
        if (i < size-1) {
          i++;
          char nextCH = s[i];
          if (nextCH >= UNI_SUR_LOW_START && nextCH <= UNI_SUR_LOW_END) {
            // Valid surrogate pair
          } else
            return false;
        } else
          return false;
      } else if (ch >= UNI_SUR_LOW_START && ch <= UNI_SUR_LOW_END)
        // Unmatched low surrogate
        return false;
    }

    return true;
  }

  // Borrowed from Python's 3.1.2 sources,
  // Objects/unicodeobject.c, and modified (see commented
  // out section, and the -1s) to disallow the reserved for
  // future (RFC 3629) 5/6 byte sequence characters, and
  // invalid 0xFE and 0xFF bytes.

  /* Map UTF-8 encoded prefix byte to sequence length.  -1 (0xFF)
   * means illegal prefix.  see RFC 2279 for details */
  static final int [] utf8CodeLength;
  static {
    final int v = Integer.MIN_VALUE;
    utf8CodeLength = new int [] {
        1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
        1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
        1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
        1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
        1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
        1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
        1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
        1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
        v, v, v, v, v, v, v, v, v, v, v, v, v, v, v, v,
        v, v, v, v, v, v, v, v, v, v, v, v, v, v, v, v,
        v, v, v, v, v, v, v, v, v, v, v, v, v, v, v, v,
        v, v, v, v, v, v, v, v, v, v, v, v, v, v, v, v,
        2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2,
        2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2,
        3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
        4, 4, 4, 4, 4, 4, 4, 4 //, 5, 5, 5, 5, 6, 6, 0, 0
      };
  }

  /** 
   * Returns the number of code points in this UTF8 sequence.
   * 
   * <p>This method assumes valid UTF8 input. This method 
   * <strong>does not perform</strong> full UTF8 validation, it will check only the 
   * first byte of each codepoint (for multi-byte sequences any bytes after 
   * the head are skipped).  
   * 
   * @throws IllegalArgumentException If invalid codepoint header byte occurs or the 
   *    content is prematurely truncated.
   */
  public static int codePointCount(BytesRef utf8) {
    int pos = utf8.offset;
    final int limit = pos + utf8.length;
    final byte[] bytes = utf8.bytes;

    int codePointCount = 0;
    for (; pos < limit; codePointCount++) {
      int v = bytes[pos] & 0xFF;
      if (v <   /* 0xxx xxxx */ 0x80) { pos += 1; continue; }
      if (v >=  /* 110x xxxx */ 0xc0) {
        if (v < /* 111x xxxx */ 0xe0) { pos += 2; continue; } 
        if (v < /* 1111 xxxx */ 0xf0) { pos += 3; continue; } 
        if (v < /* 1111 1xxx */ 0xf8) { pos += 4; continue; }
        // fallthrough, consider 5 and 6 byte sequences invalid. 
      }

      // Anything not covered above is invalid UTF8.
      throw new IllegalArgumentException();
    }

    // Check if we didn't go over the limit on the last character.
    if (pos > limit) throw new IllegalArgumentException();

    return codePointCount;
  }

  /**
   * <p>This method assumes valid UTF8 input. This method 
   * <strong>does not perform</strong> full UTF8 validation, it will check only the 
   * first byte of each codepoint (for multi-byte sequences any bytes after 
   * the head are skipped). It is the responsibility of the caller to make sure
   * that the destination array is large enough.
   * 
   * @throws IllegalArgumentException If invalid codepoint header byte occurs or the 
   *    content is prematurely truncated.
   */
  public static int UTF8toUTF32(final BytesRef utf8, final int[] ints) {
    // TODO: ints cannot be null, should be an assert
    int utf32Count = 0;
    int utf8Upto = utf8.offset;
    final byte[] bytes = utf8.bytes;
    final int utf8Limit = utf8.offset + utf8.length;
    while(utf8Upto < utf8Limit) {
      final int numBytes = utf8CodeLength[bytes[utf8Upto] & 0xFF];
      int v = 0;
      switch(numBytes) {
      case 1:
        ints[utf32Count++] = bytes[utf8Upto++];
        continue;
      case 2:
        // 5 useful bits
        v = bytes[utf8Upto++] & 31;
        break;
      case 3:
        // 4 useful bits
        v = bytes[utf8Upto++] & 15;
        break;
      case 4:
        // 3 useful bits
        v = bytes[utf8Upto++] & 7;
        break;
      default :
        throw new IllegalArgumentException("invalid utf8");
      }

      // TODO: this may read past utf8's limit.
      final int limit = utf8Upto + numBytes-1;
      while(utf8Upto < limit) {
        v = v << 6 | bytes[utf8Upto++]&63;
      }
      ints[utf32Count++] = v;
    }
    
    return utf32Count;
  }

  /** Shift value for lead surrogate to form a supplementary character. */
  private static final int LEAD_SURROGATE_SHIFT_ = 10;
  /** Mask to retrieve the significant value from a trail surrogate.*/
  private static final int TRAIL_SURROGATE_MASK_ = 0x3FF;
  /** Trail surrogate minimum value */
  private static final int TRAIL_SURROGATE_MIN_VALUE = 0xDC00;
  /** Lead surrogate minimum value */
  private static final int LEAD_SURROGATE_MIN_VALUE = 0xD800;
  /** The minimum value for Supplementary code points */
  private static final int SUPPLEMENTARY_MIN_VALUE = 0x10000;
  /** Value that all lead surrogate starts with */
  private static final int LEAD_SURROGATE_OFFSET_ = LEAD_SURROGATE_MIN_VALUE
          - (SUPPLEMENTARY_MIN_VALUE >> LEAD_SURROGATE_SHIFT_);

  /**
   * Cover JDK 1.5 API. Create a String from an array of codePoints.
   *
   * @param codePoints The code array
   * @param offset The start of the text in the code point array
   * @param count The number of code points
   * @return a String representing the code points between offset and count
   * @throws IllegalArgumentException If an invalid code point is encountered
   * @throws IndexOutOfBoundsException If the offset or count are out of bounds.
   */
  public static String newString(int[] codePoints, int offset, int count) {
      if (count < 0) {
          throw new IllegalArgumentException();
      }
      char[] chars = new char[count];
      int w = 0;
      for (int r = offset, e = offset + count; r < e; ++r) {
          int cp = codePoints[r];
          if (cp < 0 || cp > 0x10ffff) {
              throw new IllegalArgumentException();
          }
          while (true) {
              try {
                  if (cp < 0x010000) {
                      chars[w] = (char) cp;
                      w++;
                  } else {
                      chars[w] = (char) (LEAD_SURROGATE_OFFSET_ + (cp >> LEAD_SURROGATE_SHIFT_));
                      chars[w + 1] = (char) (TRAIL_SURROGATE_MIN_VALUE + (cp & TRAIL_SURROGATE_MASK_));
                      w += 2;
                  }
                  break;
              } catch (IndexOutOfBoundsException ex) {
                  int newlen = (int) (Math.ceil((double) codePoints.length * (w + 2)
                          / (r - offset + 1)));
                  char[] temp = new char[newlen];
                  System.arraycopy(chars, 0, temp, 0, w);
                  chars = temp;
              }
          }
      }
      return new String(chars, 0, w);
  }

  // for debugging
  public static String toHexString(String s) {
    StringBuilder sb = new StringBuilder();
    for(int i=0;i<s.length();i++) {
      char ch = s.charAt(i);
      if (i > 0) {
        sb.append(' ');
      }
      if (ch < 128) {
        sb.append(ch);
      } else {
        if (ch >= UNI_SUR_HIGH_START && ch <= UNI_SUR_HIGH_END) {
          sb.append("H:");
        } else if (ch >= UNI_SUR_LOW_START && ch <= UNI_SUR_LOW_END) {
          sb.append("L:");
        } else if (ch > UNI_SUR_LOW_END) {
          if (ch == 0xffff) {
            sb.append("F:");
          } else {
            sb.append("E:");
          }
        }
        
        sb.append("0x" + Integer.toHexString(ch));
      }
    }
    return sb.toString();
  }
  
  /**
   * Interprets the given byte array as UTF-8 and converts to UTF-16. It is the
   * responsibility of the caller to make sure that the destination array is large enough.
   * <p>
   * NOTE: Full characters are read, even if this reads past the length passed (and
   * can result in an ArrayOutOfBoundsException if invalid UTF-8 is passed).
   * Explicit checks for valid UTF-8 are not performed. 
   */
  // TODO: broken if chars.offset != 0
  public static int UTF8toUTF16(byte[] utf8, int offset, int length, char[] out) {
    int out_offset = 0;
    final int limit = offset + length;
    while (offset < limit) {
      int b = utf8[offset++]&0xff;
      if (b < 0xc0) {
        assert b < 0x80;
        out[out_offset++] = (char)b;
      } else if (b < 0xe0) {
        out[out_offset++] = (char)(((b&0x1f)<<6) + (utf8[offset++]&0x3f));
      } else if (b < 0xf0) {
        out[out_offset++] = (char)(((b&0xf)<<12) + ((utf8[offset]&0x3f)<<6) + (utf8[offset+1]&0x3f));
        offset += 2;
      } else {
        assert b < 0xf8: "b = 0x" + Integer.toHexString(b);
        int ch = ((b&0x7)<<18) + ((utf8[offset]&0x3f)<<12) + ((utf8[offset+1]&0x3f)<<6) + (utf8[offset+2]&0x3f);
        offset += 3;
        if (ch < UNI_MAX_BMP) {
          out[out_offset++] = (char)ch;
        } else {
          int chHalf = ch - 0x0010000;
          out[out_offset++] = (char) ((chHalf >> 10) + 0xD800);
          out[out_offset++] = (char) ((chHalf & HALF_MASK) + 0xDC00);          
        }
      }
    }
    return out_offset;
  }
  
  /**
   * Utility method for {@link #UTF8toUTF16(byte[], int, int, char[])}
   * @see #UTF8toUTF16(byte[], int, int, char[])
   */
  public static int UTF8toUTF16(BytesRef bytesRef, char[] chars) {
    return UTF8toUTF16(bytesRef.bytes, bytesRef.offset, bytesRef.length, chars);
  }

}
