/*
 * Copyright (c) 2020 The Go Authors. All rights reserved.
 *
 * Use of this source code is governed by a BSD-style
 * license that can be found in the LICENSE file.
 */
// Original Go source here:
// http://code.google.com/p/go/source/browse/src/pkg/regexp/syntax/prog.go

package com.google.re2j;

/**
 * A single instruction in the regular expression virtual machine.
 *
 * @see http://swtch.com/~rsc/regexp/regexp2.html
 */
final class Inst {

  public static final int ALT = 1;
  public static final int ALT_MATCH = 2;
  public static final int CAPTURE = 3;
  public static final int EMPTY_WIDTH = 4;
  public static final int FAIL = 5;
  public static final int MATCH = 6;
  public static final int NOP = 7;
  public static final int RUNE = 8;
  public static final int RUNE1 = 9;
  public static final int RUNE_ANY = 10;
  public static final int RUNE_ANY_NOT_NL = 11;

  int op;
  int out; // all but MATCH, FAIL
  int arg; // ALT, ALT_MATCH, CAPTURE, EMPTY_WIDTH
  int[] runes; // length==1 => exact match
  // otherwise a list of [lo,hi] pairs.  hi is *inclusive*.
  // REVIEWERS: why not half-open intervals?

  Inst(int op) {
    this.op = op;
  }

  static boolean isRuneOp(int op) {
    return RUNE <= op && op <= RUNE_ANY_NOT_NL;
  }

  // MatchRune returns true if the instruction matches (and consumes) r.
  // It should only be called when op == InstRune.
  boolean matchRune(int r) {
    // Special case: single-rune slice is from literal string, not char
    // class.
    if (runes.length == 1) {
      int r0 = runes[0];

      // If this pattern is case-insensitive, apply Unicode case folding to compare the two runes.
      // Note that this may result in a case-folding loop when executed on a JVM newer than the version used
      // to generate Unicode data for re2j, so attempt to reduce the chance of that occurring
      // by performing case folding on |r0| from the pattern rather than |r| from the input.
      if ((arg & RE2.FOLD_CASE) != 0) {
        return Unicode.equalsIgnoreCase(r0, r);
      }
      return r == r0;
    }

    // Peek at the first few pairs.
    // Should handle ASCII well.
    for (int j = 0; j < runes.length && j <= 8; j += 2) {
      if (r < runes[j]) {
        return false;
      }
      if (r <= runes[j + 1]) {
        return true;
      }
    }

    // Otherwise binary search.
    for (int lo = 0, hi = runes.length / 2; lo < hi; ) {
      int m = lo + (hi - lo) / 2;
      int c = runes[2 * m];
      if (c <= r) {
        if (r <= runes[2 * m + 1]) {
          return true;
        }
        lo = m + 1;
      } else {
        hi = m;
      }
    }
    return false;
  }

  @Override
  public String toString() {
    switch (op) {
      case ALT:
        return "alt -> " + out + ", " + arg;
      case ALT_MATCH:
        return "altmatch -> " + out + ", " + arg;
      case CAPTURE:
        return "cap " + arg + " -> " + out;
      case EMPTY_WIDTH:
        return "empty " + arg + " -> " + out;
      case MATCH:
        return "match";
      case FAIL:
        return "fail";
      case NOP:
        return "nop -> " + out;
      case RUNE:
        if (runes == null) {
          return "rune <null>"; // can't happen
        }
        return "rune "
            + escapeRunes(runes)
            + (((arg & RE2.FOLD_CASE) != 0) ? "/i" : "")
            + " -> "
            + out;
      case RUNE1:
        return "rune1 " + escapeRunes(runes) + " -> " + out;
      case RUNE_ANY:
        return "any -> " + out;
      case RUNE_ANY_NOT_NL:
        return "anynotnl -> " + out;
      default:
        throw new IllegalStateException("unhandled case in Inst.toString");
    }
  }

  // Returns an RE2 expression matching exactly |runes|.
  private static String escapeRunes(int[] runes) {
    StringBuilder out = new StringBuilder();
    out.append('"');
    for (int rune : runes) {
      Utils.escapeRune(out, rune);
    }
    out.append('"');
    return out.toString();
  }
}
