/*
 * Copyright (c) 2020 The Go Authors. All rights reserved.
 *
 * Use of this source code is governed by a BSD-style
 * license that can be found in the LICENSE file.
 */
// Original Go source here:
// http://code.google.com/p/go/source/browse/src/pkg/regexp/syntax/regexp.go

package com.google.re2j;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

/**
 * Regular expression abstract syntax tree. Produced by parser, used by compiler. NB, this
 * corresponds to {@code syntax.regexp} in the Go implementation; Go's {@code regexp} is called
 * {@code RE2} in Java.
 */
class Regexp {

  enum Op {
    NO_MATCH, // Matches no strings.
    EMPTY_MATCH, // Matches empty string.
    LITERAL, // Matches runes[] sequence
    CHAR_CLASS, // Matches Runes interpreted as range pair list
    ANY_CHAR_NOT_NL, // Matches any character except '\n'
    ANY_CHAR, // Matches any character
    BEGIN_LINE, // Matches empty string at end of line
    END_LINE, // Matches empty string at end of line
    BEGIN_TEXT, // Matches empty string at beginning of text
    END_TEXT, // Matches empty string at end of text
    WORD_BOUNDARY, // Matches word boundary `\b`
    NO_WORD_BOUNDARY, // Matches word non-boundary `\B`
    CAPTURE, // Capturing subexpr with index cap, optional name name
    STAR, // Matches subs[0] zero or more times.
    PLUS, // Matches subs[0] one or more times.
    QUEST, // Matches subs[0] zero or one times.
    REPEAT, // Matches subs[0] [min, max] times; max=-1 => no limit.
    CONCAT, // Matches concatenation of subs[]
    ALTERNATE, // Matches union of subs[]

    // Pseudo ops, used internally by Parser for parsing stack:
    LEFT_PAREN,
    VERTICAL_BAR;

    boolean isPseudo() {
      return ordinal() >= LEFT_PAREN.ordinal();
    }
  }

  static final Regexp[] EMPTY_SUBS = {};

  Op op; // operator
  int flags; // bitmap of parse flags
  Regexp[] subs; // subexpressions, if any.  Never null.
  // subs[0] is used as the freelist.
  int[] runes; // matched runes, for LITERAL, CHAR_CLASS
  int min, max; // min, max for REPEAT
  int cap; // capturing index, for CAPTURE
  String name; // capturing name, for CAPTURE
  Map<String, Integer> namedGroups = Collections.emptyMap(); // map of group name -> capturing index
  // Do update copy ctor when adding new fields!

  Regexp(Op op) {
    this.op = op;
  }

  // Shallow copy constructor.
  Regexp(Regexp that) {
    this.op = that.op;
    this.flags = that.flags;
    this.subs = that.subs;
    this.runes = that.runes;
    this.min = that.min;
    this.max = that.max;
    this.cap = that.cap;
    this.name = that.name;
    this.namedGroups = that.namedGroups;
  }

  void reinit() {
    this.flags = 0;
    subs = EMPTY_SUBS;
    runes = null;
    cap = min = max = 0;
    name = null;
    this.namedGroups = Collections.emptyMap();
  }

  @Override
  public String toString() {
    StringBuilder out = new StringBuilder();
    appendTo(out);
    return out.toString();
  }

  private static void quoteIfHyphen(StringBuilder out, int rune) {
    if (rune == '-') {
      out.append('\\');
    }
  }

  // appendTo() appends the Perl syntax for |this| regular expression to |out|.
  private void appendTo(StringBuilder out) {
    switch (op) {
      case NO_MATCH:
        out.append("[^\\x00-\\x{10FFFF}]");
        break;
      case EMPTY_MATCH:
        out.append("(?:)");
        break;
      case STAR:
      case PLUS:
      case QUEST:
      case REPEAT:
        {
          Regexp sub = subs[0];
          if (sub.op.ordinal() > Op.CAPTURE.ordinal()
              || (sub.op == Op.LITERAL && sub.runes.length > 1)) {
            out.append("(?:");
            sub.appendTo(out);
            out.append(')');
          } else {
            sub.appendTo(out);
          }
          switch (op) {
            case STAR:
              out.append('*');
              break;
            case PLUS:
              out.append('+');
              break;
            case QUEST:
              out.append('?');
              break;
            case REPEAT:
              out.append('{').append(min);
              if (min != max) {
                out.append(',');
                if (max >= 0) {
                  out.append(max);
                }
              }
              out.append('}');
              break;
          }
          if ((flags & RE2.NON_GREEDY) != 0) {
            out.append('?');
          }
          break;
        }
      case CONCAT:
        for (Regexp sub : subs) {
          if (sub.op == Op.ALTERNATE) {
            out.append("(?:");
            sub.appendTo(out);
            out.append(')');
          } else {
            sub.appendTo(out);
          }
        }
        break;
      case ALTERNATE:
        {
          String sep = "";
          for (Regexp sub : subs) {
            out.append(sep);
            sep = "|";
            sub.appendTo(out);
          }
          break;
        }
      case LITERAL:
        if ((flags & RE2.FOLD_CASE) != 0) {
          out.append("(?i:");
        }
        for (int rune : runes) {
          Utils.escapeRune(out, rune);
        }
        if ((flags & RE2.FOLD_CASE) != 0) {
          out.append(')');
        }
        break;
      case ANY_CHAR_NOT_NL:
        out.append("(?-s:.)");
        break;
      case ANY_CHAR:
        out.append("(?s:.)");
        break;
      case CAPTURE:
        if (name == null || name.isEmpty()) {
          out.append('(');
        } else {
          out.append("(?P<");
          out.append(name);
          out.append(">");
        }
        if (subs[0].op != Op.EMPTY_MATCH) {
          subs[0].appendTo(out);
        }
        out.append(')');
        break;
      case BEGIN_TEXT:
        out.append("\\A");
        break;
      case END_TEXT:
        if ((flags & RE2.WAS_DOLLAR) != 0) {
          out.append("(?-m:$)");
        } else {
          out.append("\\z");
        }
        break;
      case BEGIN_LINE:
        out.append('^');
        break;
      case END_LINE:
        out.append('$');
        break;
      case WORD_BOUNDARY:
        out.append("\\b");
        break;
      case NO_WORD_BOUNDARY:
        out.append("\\B");
        break;
      case CHAR_CLASS:
        if (runes.length % 2 != 0) {
          out.append("[invalid char class]");
          break;
        }
        out.append('[');
        if (runes.length == 0) {
          out.append("^\\x00-\\x{10FFFF}");
        } else if (runes[0] == 0 && runes[runes.length - 1] == Unicode.MAX_RUNE) {
          // Contains 0 and MAX_RUNE.  Probably a negated class.
          // Print the gaps.
          out.append('^');
          for (int i = 1; i < runes.length - 1; i += 2) {
            int lo = runes[i] + 1;
            int hi = runes[i + 1] - 1;
            quoteIfHyphen(out, lo);
            Utils.escapeRune(out, lo);
            if (lo != hi) {
              out.append('-');
              quoteIfHyphen(out, hi);
              Utils.escapeRune(out, hi);
            }
          }
        } else {
          for (int i = 0; i < runes.length; i += 2) {
            int lo = runes[i];
            int hi = runes[i + 1];
            quoteIfHyphen(out, lo);
            Utils.escapeRune(out, lo);
            if (lo != hi) {
              out.append('-');
              quoteIfHyphen(out, hi);
              Utils.escapeRune(out, hi);
            }
          }
        }
        out.append(']');
        break;
      default: // incl. pseudos
        out.append(op);
        break;
    }
  }

  // maxCap() walks the regexp to find the maximum capture index.
  int maxCap() {
    int m = 0;
    if (op == Op.CAPTURE) {
      m = cap;
    }
    if (subs != null) {
      for (Regexp sub : subs) {
        int n = sub.maxCap();
        if (m < n) {
          m = n;
        }
      }
    }
    return m;
  }

  @Override
  public int hashCode() {
    int hashcode = op.hashCode();
    switch (op) {
      case END_TEXT:
        hashcode += 31 * (flags & RE2.WAS_DOLLAR);
        break;
      case LITERAL:
      case CHAR_CLASS:
        hashcode += 31 * Arrays.hashCode(runes);
        break;
      case ALTERNATE:
      case CONCAT:
        hashcode += 31 * Arrays.deepHashCode(subs);
        break;
      case STAR:
      case PLUS:
      case QUEST:
        hashcode += 31 * (flags & RE2.NON_GREEDY) + 31 * subs[0].hashCode();
        break;
      case REPEAT:
        hashcode += 31 * min + 31 * max + 31 * subs[0].hashCode();
        break;
      case CAPTURE:
        hashcode += 31 * cap + 31 * (name != null ? name.hashCode() : 0) + 31 * subs[0].hashCode();
        break;
    }
    return hashcode;
  }

  // equals() returns true if this and that have identical structure.
  @Override
  public boolean equals(Object that) {
    if (!(that instanceof Regexp)) {
      return false;
    }
    Regexp x = this;
    Regexp y = (Regexp) that;
    if (x.op != y.op) {
      return false;
    }
    switch (x.op) {
      case END_TEXT:
        // The parse flags remember whether this is \z or \Z.
        if ((x.flags & RE2.WAS_DOLLAR) != (y.flags & RE2.WAS_DOLLAR)) {
          return false;
        }
        break;
      case LITERAL:
      case CHAR_CLASS:
        if (!Arrays.equals(x.runes, y.runes)) {
          return false;
        }
        break;
      case ALTERNATE:
      case CONCAT:
        if (x.subs.length != y.subs.length) {
          return false;
        }
        for (int i = 0; i < x.subs.length; ++i) {
          if (!x.subs[i].equals(y.subs[i])) {
            return false;
          }
        }
        break;
      case STAR:
      case PLUS:
      case QUEST:
        if ((x.flags & RE2.NON_GREEDY) != (y.flags & RE2.NON_GREEDY)
            || !x.subs[0].equals(y.subs[0])) {
          return false;
        }
        break;
      case REPEAT:
        if ((x.flags & RE2.NON_GREEDY) != (y.flags & RE2.NON_GREEDY)
            || x.min != y.min
            || x.max != y.max
            || !x.subs[0].equals(y.subs[0])) {
          return false;
        }
        break;
      case CAPTURE:
        if (x.cap != y.cap
            || (x.name == null ? y.name != null : !x.name.equals(y.name))
            || !x.subs[0].equals(y.subs[0])) {
          return false;
        }
        break;
    }
    return true;
  }
}
