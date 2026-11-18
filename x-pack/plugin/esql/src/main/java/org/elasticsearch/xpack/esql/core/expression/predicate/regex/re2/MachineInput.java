/*
 * Copyright (c) 2020 The Go Authors. All rights reserved.
 *
 * Use of this source code is governed by a BSD-style
 * license that can be found in the LICENSE file.
 */
// Original Go source here:
// http://code.google.com/p/go/source/browse/src/pkg/regexp/regexp.go

package com.google.re2j;

/**
 * MachineInput abstracts different representations of the input text supplied to the Machine. It
 * provides one-character lookahead.
 */
abstract class MachineInput {

  static final int EOF = (-1 << 3);

  static MachineInput fromUTF8(byte[] b) {
    return new UTF8Input(b);
  }

  static MachineInput fromUTF8(byte[] b, int start, int end) {
    return new UTF8Input(b, start, end);
  }

  static MachineInput fromUTF16(CharSequence s) {
    return new UTF16Input(s, 0, s.length());
  }

  static MachineInput fromUTF16(CharSequence s, int start, int end) {
    return new UTF16Input(s, start, end);
  }

  //// Interface

  // Returns the rune at the specified index; the units are
  // unspecified, but could be UTF-8 byte, UTF-16 char, or rune
  // indices.  Returns the width (in the same units) of the rune in
  // the lower 3 bits, and the rune (Unicode code point) in the high
  // bits.  Never negative, except for EOF which is represented as -1
  // << 3 | 0.
  abstract int step(int pos);

  // can we look ahead without losing info?
  abstract boolean canCheckPrefix();

  // Returns the index relative to |pos| at which |re2.prefix| is found
  // in this input stream, or a negative value if not found.
  abstract int index(RE2 re2, int pos);

  // Returns a bitmask of EMPTY_* flags.
  abstract int context(int pos);

  // Returns the end position in the same units as step().
  abstract int endPos();

  //// Implementations

  // An implementation of MachineInput for UTF-8 byte arrays.
  // |pos| and |width| are byte indices.
  private static class UTF8Input extends MachineInput {

    final byte[] b;
    final int start;
    final int end;

    UTF8Input(byte[] b) {
      this.b = b;
      start = 0;
      end = b.length;
    }

    UTF8Input(byte[] b, int start, int end) {
      if (end > b.length) {
        throw new ArrayIndexOutOfBoundsException(
            "end is greater than length: " + end + " > " + b.length);
      }
      this.b = b;
      this.start = start;
      this.end = end;
    }

    @Override
    int step(int i) {
      i += start;
      if (i >= end) {
        return EOF;
      }

      // UTF-8.  RFC 3629 in five lines:
      //
      // Unicode code points            UTF-8 encoding (binary)
      //         00-7F  (7 bits)   0tuvwxyz
      //     0080-07FF (11 bits)   110pqrst 10uvwxyz
      //     0800-FFFF (16 bits)   1110jklm 10npqrst 10uvwxyz
      // 010000-10FFFF (21 bits)   11110efg 10hijklm 10npqrst 10uvwxyz
      int x = b[i++] & 0xff; // zero extend
      if ((x & 0x80) == 0) {
        return x << 3 | 1;
      } else if ((x & 0xE0) == 0xC0) { // 110xxxxx
        x = x & 0x1F;
        if (i >= end) {
          return EOF;
        }
        x = x << 6 | (b[i++] & 0x3F);
        return x << 3 | 2;
      } else if ((x & 0xF0) == 0xE0) { // 1110xxxx
        x = x & 0x0F;
        if (i + 1 >= end) {
          return EOF;
        }
        x = x << 6 | (b[i++] & 0x3F);
        x = x << 6 | (b[i++] & 0x3F);
        return x << 3 | 3;
      } else { // 11110xxx
        x = x & 0x07;
        if (i + 2 >= end) {
          return EOF;
        }
        x = x << 6 | (b[i++] & 0x3F);
        x = x << 6 | (b[i++] & 0x3F);
        x = x << 6 | (b[i++] & 0x3F);
        return x << 3 | 4;
      }
    }

    @Override
    boolean canCheckPrefix() {
      return true;
    }

    @Override
    int index(RE2 re2, int pos) {
      pos += start;
      int i = Utils.indexOf(b, re2.prefixUTF8, pos);
      return i < 0 ? i : i - pos;
    }

    @Override
    int context(int pos) {
      pos += this.start;
      int r1 = -1;
      if (pos > this.start && pos <= this.end) {
        int start = pos - 1;
        r1 = b[start--];
        if (r1 >= 0x80) { // decode UTF-8
          // Find start, up to 4 bytes earlier.
          int lim = pos - 4;
          if (lim < this.start) {
            lim = this.start;
          }
          while (start >= lim && (b[start] & 0xC0) == 0x80) { // 10xxxxxx
            start--;
          }
          if (start < this.start) {
            start = this.start;
          }
          r1 = step(start) >> 3;
        }
      }
      int r2 = pos < this.end ? (step(pos) >> 3) : -1;
      return Utils.emptyOpContext(r1, r2);
    }

    @Override
    int endPos() {
      return end;
    }
  }

  // |pos| and |width| are in Java "char" units.
  private static class UTF16Input extends MachineInput {
    final CharSequence str;
    final int start;
    final int end;

    public UTF16Input(CharSequence str, int start, int end) {
      this.str = str;
      this.start = start;
      this.end = end;
    }

    @Override
    int step(int pos) {
      pos += start;
      if (pos < end) {
        int rune = Character.codePointAt(str, pos);
        return rune << 3 | Character.charCount(rune);
      } else {
        return EOF;
      }
    }

    @Override
    boolean canCheckPrefix() {
      return true;
    }

    @Override
    int index(RE2 re2, int pos) {
      pos += start;
      int i = indexOf(str, re2.prefix, pos);
      return i < 0 ? i : i - pos;
    }

    @Override
    int context(int pos) {
      pos += start;
      int r1 = pos > 0 && pos <= str.length() ? Character.codePointBefore(str, pos) : -1;
      int r2 = pos < str.length() ? Character.codePointAt(str, pos) : -1;
      return Utils.emptyOpContext(r1, r2);
    }

    @Override
    int endPos() {
      return end;
    }

    private int indexOf(CharSequence hayStack, String needle, int pos) {
      if (hayStack instanceof String) {
        return ((String) hayStack).indexOf(needle, pos);
      }
      if (hayStack instanceof StringBuilder) {
        return ((StringBuilder) hayStack).indexOf(needle, pos);
      }
      return indexOfFallback(hayStack, needle, pos);
    }

    // Modified version of {@link String#indexOf(String) that allows a CharSequence.
    private int indexOfFallback(CharSequence hayStack, String needle, int fromIndex) {
      if (fromIndex >= hayStack.length()) {
        return needle.isEmpty() ? 0 : -1;
      }
      if (fromIndex < 0) {
        fromIndex = 0;
      }
      if (needle.isEmpty()) {
        return fromIndex;
      }

      char first = needle.charAt(0);
      int max = hayStack.length() - needle.length();

      for (int i = fromIndex; i <= max; i++) {
        /* Look for first character. */
        if (hayStack.charAt(i) != first) {
          while (++i <= max && hayStack.charAt(i) != first) {}
        }

        /* Found first character, now look at the rest of v2 */
        if (i <= max) {
          int j = i + 1;
          int end = j + needle.length() - 1;
          for (int k = 1; j < end && hayStack.charAt(j) == needle.charAt(k); j++, k++) {}

          if (j == end) {
            /* Found whole string. */
            return i;
          }
        }
      }
      return -1;
    }
  }
}
