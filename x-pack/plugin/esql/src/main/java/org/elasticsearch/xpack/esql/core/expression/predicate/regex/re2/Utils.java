/*
 * Copyright (c) 2020 The Go Authors. All rights reserved.
 *
 * Use of this source code is governed by a BSD-style
 * license that can be found in the LICENSE file.
 */
package com.google.re2j;

/**
 * Various constants and helper utilities.
 */
abstract class Utils {

  static final int[] EMPTY_INTS = {};

  // Returns true iff |c| is an ASCII letter or decimal digit.
  static boolean isalnum(int c) {
    return ('0' <= c && c <= '9') || ('A' <= c && c <= 'Z') || ('a' <= c && c <= 'z');
  }

  // If |c| is an ASCII hex digit, returns its value, otherwise -1.
  static int unhex(int c) {
    if ('0' <= c && c <= '9') {
      return c - '0';
    }
    if ('a' <= c && c <= 'f') {
      return c - 'a' + 10;
    }
    if ('A' <= c && c <= 'F') {
      return c - 'A' + 10;
    }
    return -1;
  }

  private static final String METACHARACTERS = "\\.+*?()|[]{}^$";

  // Appends a RE2 literal to |out| for rune |rune|,
  // with regexp metacharacters escaped.
  static void escapeRune(StringBuilder out, int rune) {
    if (Unicode.isPrint(rune)) {
      if (METACHARACTERS.indexOf((char) rune) >= 0) {
        out.append('\\');
      }
      out.appendCodePoint(rune);
      return;
    }

    switch (rune) {
      case '"':
        out.append("\\\"");
        break;
      case '\\':
        out.append("\\\\");
        break;
      case '\t':
        out.append("\\t");
        break;
      case '\n':
        out.append("\\n");
        break;
      case '\r':
        out.append("\\r");
        break;
      case '\b':
        out.append("\\b");
        break;
      case '\f':
        out.append("\\f");
        break;
      default:
        {
          String s = Integer.toHexString(rune);
          if (rune < 0x100) {
            out.append("\\x");
            if (s.length() == 1) {
              out.append('0');
            }
            out.append(s);
          } else {
            out.append("\\x{").append(s).append('}');
          }
          break;
        }
    }
  }

  // Returns the array of runes in the specified Java UTF-16 string.
  static int[] stringToRunes(String str) {
    int charlen = str.length();
    int runelen = str.codePointCount(0, charlen);
    int[] runes = new int[runelen];
    int r = 0, c = 0;
    while (c < charlen) {
      int rune = str.codePointAt(c);
      runes[r++] = rune;
      c += Character.charCount(rune);
    }
    return runes;
  }

  // Returns the Java UTF-16 string containing the single rune |r|.
  static String runeToString(int r) {
    char c = (char) r;
    return r == c ? String.valueOf(c) : new String(Character.toChars(c));
  }

  // Returns a new copy of the specified subarray.
  static int[] subarray(int[] array, int start, int end) {
    int[] r = new int[end - start];
    for (int i = start; i < end; ++i) {
      r[i - start] = array[i];
    }
    return r;
  }

  // Returns a new copy of the specified subarray.
  static byte[] subarray(byte[] array, int start, int end) {
    byte[] r = new byte[end - start];
    for (int i = start; i < end; ++i) {
      r[i - start] = array[i];
    }
    return r;
  }

  // Returns the index of the first occurrence of array |target| within
  // array |source| after |fromIndex|, or -1 if not found.
  static int indexOf(byte[] source, byte[] target, int fromIndex) {
    if (fromIndex >= source.length) {
      return target.length == 0 ? source.length : -1;
    }
    if (fromIndex < 0) {
      fromIndex = 0;
    }
    if (target.length == 0) {
      return fromIndex;
    }

    byte first = target[0];
    for (int i = fromIndex, max = source.length - target.length; i <= max; i++) {
      // Look for first byte.
      if (source[i] != first) {
        while (++i <= max && source[i] != first) {}
      }

      // Found first byte, now look at the rest of v2.
      if (i <= max) {
        int j = i + 1;
        int end = j + target.length - 1;
        for (int k = 1; j < end && source[j] == target[k]; j++, k++) {}

        if (j == end) {
          return i; // found whole array
        }
      }
    }
    return -1;
  }

  // isWordRune reports whether r is consider a ``word character''
  // during the evaluation of the \b and \B zero-width assertions.
  // These assertions are ASCII-only: the word characters are [A-Za-z0-9_].
  static boolean isWordRune(int r) {
    return (('A' <= r && r <= 'Z') || ('a' <= r && r <= 'z') || ('0' <= r && r <= '9') || r == '_');
  }

  //// EMPTY_* flags

  static final int EMPTY_BEGIN_LINE = 0x01;
  static final int EMPTY_END_LINE = 0x02;
  static final int EMPTY_BEGIN_TEXT = 0x04;
  static final int EMPTY_END_TEXT = 0x08;
  static final int EMPTY_WORD_BOUNDARY = 0x10;
  static final int EMPTY_NO_WORD_BOUNDARY = 0x20;
  static final int EMPTY_ALL = -1; // (impossible)

  // emptyOpContext returns the zero-width assertions satisfied at the position
  // between the runes r1 and r2, a bitmask of EMPTY_* flags.
  // Passing r1 == -1 indicates that the position is at the beginning of the
  // text.
  // Passing r2 == -1 indicates that the position is at the end of the text.
  // TODO(adonovan): move to Machine.
  static int emptyOpContext(int r1, int r2) {
    int op = 0;
    if (r1 < 0) {
      op |= EMPTY_BEGIN_TEXT | EMPTY_BEGIN_LINE;
    }
    if (r1 == '\n') {
      op |= EMPTY_BEGIN_LINE;
    }
    if (r2 < 0) {
      op |= EMPTY_END_TEXT | EMPTY_END_LINE;
    }
    if (r2 == '\n') {
      op |= EMPTY_END_LINE;
    }
    if (isWordRune(r1) != isWordRune(r2)) {
      op |= EMPTY_WORD_BOUNDARY;
    } else {
      op |= EMPTY_NO_WORD_BOUNDARY;
    }
    return op;
  }

  private Utils() {} // uninstantiable
}
