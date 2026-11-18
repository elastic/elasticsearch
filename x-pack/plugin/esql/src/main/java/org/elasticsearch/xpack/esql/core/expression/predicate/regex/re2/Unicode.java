/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 *
 * Forked from RE2/J 1.8, see package-info.java.
 *
 * Copyright (c) 2020 The Go Authors. All rights reserved.
 *
 * Use of this source code is governed by a BSD-style
 * license that can be found in the LICENSE file.
 */
// Many of these were derived from the corresponding Go functions in
// http://code.google.com/p/go/source/browse/src/pkg/unicode/letter.go

package org.elasticsearch.xpack.esql.core.expression.predicate.regex.re2;

import org.elasticsearch.xpack.esql.core.expression.predicate.regex.Re2UnicodeTables;

/**
 * Utilities for dealing with Unicode better than Java does.
 *
 * @author adonovan@google.com (Alan Donovan)
 */
class Unicode {

  // The highest legal rune value.
  static final int MAX_RUNE = 0x10FFFF;

  // The highest legal ASCII value.
  static final int MAX_ASCII = 0x7f;

  // The highest legal Latin-1 value.
  static final int MAX_LATIN1 = 0xFF;

  // Minimum and maximum runes involved in folding.
  // Checked during test.
  // ES: from the JDK, see Re2UnicodeTables
  static final int MIN_FOLD = Re2UnicodeTables.MIN_FOLD;
  static final int MAX_FOLD = Re2UnicodeTables.MAX_FOLD;

  // is32 uses binary search to test whether rune is in the specified
  // slice of 32-bit ranges.
  // TODO(adonovan): opt: consider using int[n*3] instead of int[n][3].
  private static boolean is32(int[][] ranges, int r) {
    // binary search over ranges
    for (int lo = 0, hi = ranges.length; lo < hi; ) {
      int m = lo + (hi - lo) / 2;
      int[] range = ranges[m]; // [lo, hi, stride]
      if (range[0] <= r && r <= range[1]) {
        return ((r - range[0]) % range[2]) == 0;
      }
      if (r < range[0]) {
        hi = m;
      } else {
        lo = m + 1;
      }
    }
    return false;
  }

  // is tests whether rune is in the specified table of ranges.
  private static boolean is(int[][] ranges, int r) {
    // common case: rune is ASCII or Latin-1, so use linear search.
    if (r <= MAX_LATIN1) {
      for (int[] range : ranges) { // range = [lo, hi, stride]
        if (r > range[1]) {
          continue;
        }
        if (r < range[0]) {
          return false;
        }
        return ((r - range[0]) % range[2]) == 0;
      }
      return false;
    }
    return ranges.length > 0 && r >= ranges[0][0] && is32(ranges, r);
  }

  // isUpper reports whether the rune is an upper case letter.
  static boolean isUpper(int r) {
    // See comment in isGraphic.
    if (r <= MAX_LATIN1) {
      return Character.isUpperCase((char) r);
    }
    // ES: from the JDK
    return Character.isUpperCase(r);
  }

  // isPrint reports whether the rune is printable (Unicode L/M/N/P/S or ' ').
  static boolean isPrint(int r) {
    if (r <= MAX_LATIN1) {
      return (r >= 0x20 && r < 0x7F) || (r >= 0xA1 && r != 0xAD);
    }
    // ES: from the JDK, see Re2UnicodeTables
    return Re2UnicodeTables.isPrint(r);
  }

  // simpleFold iterates over Unicode code points equivalent under
  // the Unicode-defined simple case folding.  Among the code points
  // equivalent to rune (including rune itself), SimpleFold returns the
  // smallest r >= rune if one exists, or else the smallest r >= 0.
  //
  // For example:
  //      SimpleFold('A') = 'a'
  //      SimpleFold('a') = 'A'
  //
  //      SimpleFold('K') = 'k'
  //      SimpleFold('k') = '\u212A' (Kelvin symbol, K)
  //      SimpleFold('\u212A') = 'K'
  //
  //      SimpleFold('1') = '1'
  //
  // Derived from Go's unicode.SimpleFold.
  //
  static int simpleFold(int r) {
    // ES: from the JDK, see Re2UnicodeTables
    return Re2UnicodeTables.simpleFold(r);
  }

  // equalsIgnoreCase performs case-insensitive equality comparison
  // on the given runes |r1| and |r2|, with special consideration
  // for the likely scenario where both runes are ASCII characters.
  // If non-ASCII, Unicode case folding will be performed on |r1|
  // to compare it to |r2|.
  // -1 is interpreted as the end-of-file mark.
  static boolean equalsIgnoreCase(int r1, int r2) {
    // Runes already match, or one of them is EOF
    if (r1 < 0 || r2 < 0 || r1 == r2) {
      return true;
    }

    // Fast path for the common case where both runes are ASCII characters.
    // Coerces both runes to lowercase if applicable.
    if (r1 <= MAX_ASCII && r2 <= MAX_ASCII) {
      if ('A' <= r1 && r1 <= 'Z') {
        r1 |= 0x20;
      }

      if ('A' <= r2 && r2 <= 'Z') {
        r2 |= 0x20;
      }

      return r1 == r2;
    }

    // Fall back to full Unicode case folding otherwise.
    // Invariant: r1 must be non-negative
    for (int r = Unicode.simpleFold(r1); r != r1; r = Unicode.simpleFold(r)) {
      if (r == r2) {
        return true;
      }
    }

    return false;
  }

  private Unicode() {} // uninstantiable
}
