/*
 * Copyright (c) 2020 The Go Authors. All rights reserved.
 *
 * Use of this source code is governed by a BSD-style
 * license that can be found in the LICENSE file.
 */
// Original Go source here:
// http://code.google.com/p/go/source/browse/src/pkg/regexp/syntax/parse.go

// TODO(adonovan):
// - Eliminate allocations (new int[], new Regexp[], new ArrayList) by
//   recycling old arrays on a freelist.

package com.google.re2j;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * A parser of regular expression patterns.
 *
 * The only public entry point is {@link #parse(String pattern, int flags)}.
 */
class Parser {

  // Unexpected error
  private static final String ERR_INTERNAL_ERROR = "regexp/syntax: internal error";

  // Parse errors
  private static final String ERR_INVALID_CHAR_RANGE = "invalid character class range";
  private static final String ERR_INVALID_ESCAPE = "invalid escape sequence";
  private static final String ERR_INVALID_NAMED_CAPTURE = "invalid named capture";
  private static final String ERR_INVALID_PERL_OP = "invalid or unsupported Perl syntax";
  private static final String ERR_INVALID_REPEAT_OP = "invalid nested repetition operator";
  private static final String ERR_INVALID_REPEAT_SIZE = "invalid repeat count";
  private static final String ERR_MISSING_BRACKET = "missing closing ]";
  private static final String ERR_MISSING_PAREN = "missing closing )";
  private static final String ERR_MISSING_REPEAT_ARGUMENT =
      "missing argument to repetition operator";
  private static final String ERR_TRAILING_BACKSLASH = "trailing backslash at end of expression";
  private static final String ERR_DUPLICATE_NAMED_CAPTURE = "duplicate capture group name";

  // Hack to expose ArrayList.removeRange().
  private static class Stack extends ArrayList<Regexp> {
    @Override
    public void removeRange(int fromIndex, int toIndex) {
      super.removeRange(fromIndex, toIndex);
    }
  }

  private final String wholeRegexp;
  // Flags control the behavior of the parser and record information about
  // regexp context.
  private int flags; // parse mode flags

  // Stack of parsed expressions.
  private final Stack stack = new Stack();
  private Regexp free;
  private int numCap = 0; // number of capturing groups seen
  private final Map<String, Integer> namedGroups = new HashMap<String, Integer>();

  Parser(String wholeRegexp, int flags) {
    this.wholeRegexp = wholeRegexp;
    this.flags = flags;
  }

  // Allocate a Regexp, from the free list if possible.
  private Regexp newRegexp(Regexp.Op op) {
    Regexp re = free;
    if (re != null && re.subs != null && re.subs.length > 0) {
      free = re.subs[0];
      re.reinit();
      re.op = op;
    } else {
      re = new Regexp(op);
    }
    return re;
  }

  private void reuse(Regexp re) {
    if (re.subs != null && re.subs.length > 0) {
      re.subs[0] = free;
    }
    free = re;
  }

  // Parse stack manipulation.

  private Regexp pop() {
    return stack.remove(stack.size() - 1);
  }

  private Regexp[] popToPseudo() {
    int n = stack.size(), i = n;
    while (i > 0 && !stack.get(i - 1).op.isPseudo()) {
      i--;
    }
    Regexp[] r = stack.subList(i, n).toArray(new Regexp[n - i]);
    stack.removeRange(i, n);
    return r;
  }

  // push pushes the regexp re onto the parse stack and returns the regexp.
  // Returns null for a CHAR_CLASS that can be merged with the top-of-stack.
  private Regexp push(Regexp re) {
    if (re.op == Regexp.Op.CHAR_CLASS && re.runes.length == 2 && re.runes[0] == re.runes[1]) {
      // Collapse range [x-x] -> single rune x.
      if (maybeConcat(re.runes[0], flags & ~RE2.FOLD_CASE)) {
        return null;
      }
      re.op = Regexp.Op.LITERAL;
      re.runes = new int[] {re.runes[0]};
      re.flags = flags & ~RE2.FOLD_CASE;
    } else if ((re.op == Regexp.Op.CHAR_CLASS
            && re.runes.length == 4
            && re.runes[0] == re.runes[1]
            && re.runes[2] == re.runes[3]
            && Unicode.simpleFold(re.runes[0]) == re.runes[2]
            && Unicode.simpleFold(re.runes[2]) == re.runes[0])
        || (re.op == Regexp.Op.CHAR_CLASS
            && re.runes.length == 2
            && re.runes[0] + 1 == re.runes[1]
            && Unicode.simpleFold(re.runes[0]) == re.runes[1]
            && Unicode.simpleFold(re.runes[1]) == re.runes[0])) {
      // Case-insensitive rune like [Aa] or [Δδ].
      if (maybeConcat(re.runes[0], flags | RE2.FOLD_CASE)) {
        return null;
      }

      // Rewrite as (case-insensitive) literal.
      re.op = Regexp.Op.LITERAL;
      re.runes = new int[] {re.runes[0]};
      re.flags = flags | RE2.FOLD_CASE;
    } else {
      // Incremental concatenation.
      maybeConcat(-1, 0);
    }

    stack.add(re);
    return re;
  }

  // maybeConcat implements incremental concatenation
  // of literal runes into string nodes.  The parser calls this
  // before each push, so only the top fragment of the stack
  // might need processing.  Since this is called before a push,
  // the topmost literal is no longer subject to operators like *
  // (Otherwise ab* would turn into (ab)*.)
  // If (r >= 0 and there's a node left over, maybeConcat uses it
  // to push r with the given flags.
  // maybeConcat reports whether r was pushed.
  private boolean maybeConcat(int r, int flags) {
    int n = stack.size();
    if (n < 2) {
      return false;
    }
    Regexp re1 = stack.get(n - 1);
    Regexp re2 = stack.get(n - 2);
    if (re1.op != Regexp.Op.LITERAL
        || re2.op != Regexp.Op.LITERAL
        || (re1.flags & RE2.FOLD_CASE) != (re2.flags & RE2.FOLD_CASE)) {
      return false;
    }

    // Push re1 into re2.
    re2.runes = concatRunes(re2.runes, re1.runes);

    // Reuse re1 if possible.
    if (r >= 0) {
      re1.runes = new int[] {r};
      re1.flags = flags;
      return true;
    }

    pop();
    reuse(re1);
    return false; // did not push r
  }

  // newLiteral returns a new LITERAL Regexp with the given flags
  private Regexp newLiteral(int r, int flags) {
    Regexp re = newRegexp(Regexp.Op.LITERAL);
    re.flags = flags;
    if ((flags & RE2.FOLD_CASE) != 0) {
      r = minFoldRune(r);
    }
    re.runes = new int[] {r};
    return re;
  }

  // minFoldRune returns the minimum rune fold-equivalent to r.
  private static int minFoldRune(int r) {
    if (r < Unicode.MIN_FOLD || r > Unicode.MAX_FOLD) {
      return r;
    }
    int min = r;
    int r0 = r;
    for (r = Unicode.simpleFold(r); r != r0; r = Unicode.simpleFold(r)) {
      if (min > r) {
        min = r;
      }
    }
    return min;
  }

  // literal pushes a literal regexp for the rune r on the stack
  // and returns that regexp.
  private void literal(int r) {
    push(newLiteral(r, flags));
  }

  // op pushes a regexp with the given op onto the stack
  // and returns that regexp.
  private Regexp op(Regexp.Op op) {
    Regexp re = newRegexp(op);
    re.flags = flags;
    return push(re);
  }

  // repeat replaces the top stack element with itself repeated according to
  // op, min, max.  beforePos is the start position of the repetition operator.
  // Pre: t is positioned after the initial repetition operator.
  // Post: t advances past an optional perl-mode '?', or stays put.
  //       Or, it fails with PatternSyntaxException.
  private void repeat(
      Regexp.Op op, int min, int max, int beforePos, StringIterator t, int lastRepeatPos)
      throws PatternSyntaxException {
    int flags = this.flags;
    if ((flags & RE2.PERL_X) != 0) {
      if (t.more() && t.lookingAt('?')) {
        t.skip(1); // '?'
        flags ^= RE2.NON_GREEDY;
      }
      if (lastRepeatPos != -1) {
        // In Perl it is not allowed to stack repetition operators:
        // a** is a syntax error, not a doubled star, and a++ means
        // something else entirely, which we don't support!
        throw new PatternSyntaxException(ERR_INVALID_REPEAT_OP, t.from(lastRepeatPos));
      }
    }
    int n = stack.size();
    if (n == 0) {
      throw new PatternSyntaxException(ERR_MISSING_REPEAT_ARGUMENT, t.from(beforePos));
    }
    Regexp sub = stack.get(n - 1);
    if (sub.op.isPseudo()) {
      throw new PatternSyntaxException(ERR_MISSING_REPEAT_ARGUMENT, t.from(beforePos));
    }
    Regexp re = newRegexp(op);
    re.min = min;
    re.max = max;
    re.flags = flags;
    re.subs = new Regexp[] {sub};
    stack.set(n - 1, re);
  }

  // concat replaces the top of the stack (above the topmost '|' or '(') with
  // its concatenation.
  private Regexp concat() {
    maybeConcat(-1, 0);

    // Scan down to find pseudo-operator | or (.
    Regexp[] subs = popToPseudo();

    // Empty concatenation is special case.
    if (subs.length == 0) {
      return push(newRegexp(Regexp.Op.EMPTY_MATCH));
    }

    return push(collapse(subs, Regexp.Op.CONCAT));
  }

  // alternate replaces the top of the stack (above the topmost '(') with its
  // alternation.
  private Regexp alternate() {
    // Scan down to find pseudo-operator (.
    // There are no | above (.
    Regexp[] subs = popToPseudo();

    // Make sure top class is clean.
    // All the others already are (see swapVerticalBar).
    if (subs.length > 0) {
      cleanAlt(subs[subs.length - 1]);
    }

    // Empty alternate is special case
    // (shouldn't happen but easy to handle).
    if (subs.length == 0) {
      return push(newRegexp(Regexp.Op.NO_MATCH));
    }

    return push(collapse(subs, Regexp.Op.ALTERNATE));
  }

  // cleanAlt cleans re for eventual inclusion in an alternation.
  private void cleanAlt(Regexp re) {
    if (re.op == Regexp.Op.CHAR_CLASS) {
      re.runes = new CharClass(re.runes).cleanClass().toArray();
      if (re.runes.length == 2 && re.runes[0] == 0 && re.runes[1] == Unicode.MAX_RUNE) {
        re.runes = null;
        re.op = Regexp.Op.ANY_CHAR;
      } else if (re.runes.length == 4
          && re.runes[0] == 0
          && re.runes[1] == '\n' - 1
          && re.runes[2] == '\n' + 1
          && re.runes[3] == Unicode.MAX_RUNE) {
        re.runes = null;
        re.op = Regexp.Op.ANY_CHAR_NOT_NL;
      }
    }
  }

  // collapse returns the result of applying op to subs[start:end].
  // If (sub contains op nodes, they all get hoisted up
  // so that there is never a concat of a concat or an
  // alternate of an alternate.
  private Regexp collapse(Regexp[] subs, Regexp.Op op) {
    if (subs.length == 1) {
      return subs[0];
    }
    // Concatenate subs iff op is same.
    // Compute length in first pass.
    int len = 0;
    for (Regexp sub : subs) {
      len += (sub.op == op) ? sub.subs.length : 1;
    }
    Regexp[] newsubs = new Regexp[len];
    int i = 0;
    for (Regexp sub : subs) {
      if (sub.op == op) {
        System.arraycopy(sub.subs, 0, newsubs, i, sub.subs.length);
        i += sub.subs.length;
        reuse(sub);
      } else {
        newsubs[i++] = sub;
      }
    }
    Regexp re = newRegexp(op);
    re.subs = newsubs;

    if (op == Regexp.Op.ALTERNATE) {
      re.subs = factor(re.subs, re.flags);
      if (re.subs.length == 1) {
        Regexp old = re;
        re = re.subs[0];
        reuse(old);
      }
    }
    return re;
  }

  // factor factors common prefixes from the alternation list sub.  It
  // returns a replacement list that reuses the same storage and frees
  // (passes to p.reuse) any removed *Regexps.
  //
  // For example,
  //     ABC|ABD|AEF|BCX|BCY
  // simplifies by literal prefix extraction to
  //     A(B(C|D)|EF)|BC(X|Y)
  // which simplifies by character class introduction to
  //     A(B[CD]|EF)|BC[XY]
  //
  private Regexp[] factor(Regexp[] array, int flags) {
    if (array.length < 2) {
      return array;
    }

    // The following code is subtle, because it's a literal Java
    // translation of code that makes clever use of Go "slices".
    // A slice is a triple (array, offset, length), and the Go
    // implementation uses two slices, |sub| and |out| backed by the
    // same array.  In Java, we have to be explicit about all of these
    // variables, so:
    //
    // Go    Java
    // sub   (array, s, lensub)
    // out   (array, 0, lenout)   // (always a prefix of |array|)
    //
    // In the comments we'll use the logical notation of go slices, e.g. sub[i]
    // even though the Java code will read array[s + i].

    int s = 0; // offset of first |sub| within array.
    int lensub = array.length; // = len(sub)
    int lenout = 0; // = len(out)

    // Round 1: Factor out common literal prefixes.
    // Note: (str, strlen) and (istr, istrlen) are like Go slices
    // onto a prefix of some Regexp's runes array (hence offset=0).
    int[] str = null;
    int strlen = 0;
    int strflags = 0;
    int start = 0;
    for (int i = 0; i <= lensub; i++) {
      // Invariant: the Regexps that were in sub[0:start] have been
      // used or marked for reuse, and the slice space has been reused
      // for out (len <= start).
      //
      // Invariant: sub[start:i] consists of regexps that all begin
      // with str as modified by strflags.
      int[] istr = null;
      int istrlen = 0;
      int iflags = 0;
      if (i < lensub) {
        // NB, we inlined Go's leadingString() since Java has no pair return.
        Regexp re = array[s + i];
        if (re.op == Regexp.Op.CONCAT && re.subs.length > 0) {
          re = re.subs[0];
        }
        if (re.op == Regexp.Op.LITERAL) {
          istr = re.runes;
          istrlen = re.runes.length;
          iflags = re.flags & RE2.FOLD_CASE;
        }
        // istr is the leading literal string that re begins with.
        // The string refers to storage in re or its children.

        if (iflags == strflags) {
          int same = 0;
          while (same < strlen && same < istrlen && str[same] == istr[same]) {
            same++;
          }
          if (same > 0) {
            // Matches at least one rune in current range.
            // Keep going around.
            strlen = same;
            continue;
          }
        }
      }

      // Found end of a run with common leading literal string:
      // sub[start:i] all begin with str[0:strlen], but sub[i]
      // does not even begin with str[0].
      //
      // Factor out common string and append factored expression to out.
      if (i == start) {
        // Nothing to do - run of length 0.
      } else if (i == start + 1) {
        // Just one: don't bother factoring.
        array[lenout++] = array[s + start];
      } else {
        // Construct factored form: prefix(suffix1|suffix2|...)
        Regexp prefix = newRegexp(Regexp.Op.LITERAL);
        prefix.flags = strflags;
        prefix.runes = Utils.subarray(str, 0, strlen);

        for (int j = start; j < i; j++) {
          array[s + j] = removeLeadingString(array[s + j], strlen);
        }
        // Recurse.
        Regexp suffix = collapse(subarray(array, s + start, s + i), Regexp.Op.ALTERNATE);
        Regexp re = newRegexp(Regexp.Op.CONCAT);
        re.subs = new Regexp[] {prefix, suffix};
        array[lenout++] = re;
      }

      // Prepare for next iteration.
      start = i;
      str = istr;
      strlen = istrlen;
      strflags = iflags;
    }
    // In Go: sub = out
    lensub = lenout;
    s = 0;

    // Round 2: Factor out common complex prefixes,
    // just the first piece of each concatenation,
    // whatever it is.  This is good enough a lot of the time.
    start = 0;
    lenout = 0;
    Regexp first = null;
    for (int i = 0; i <= lensub; i++) {
      // Invariant: the Regexps that were in sub[0:start] have been
      // used or marked for reuse, and the slice space has been reused
      // for out (lenout <= start).
      //
      // Invariant: sub[start:i] consists of regexps that all begin with
      // ifirst.
      Regexp ifirst = null;
      if (i < lensub) {
        ifirst = leadingRegexp(array[s + i]);
        if (first != null
            && first.equals(ifirst)
            && (isCharClass(first)
                || (first.op == Regexp.Op.REPEAT
                    && first.min == first.max
                    && isCharClass(first.subs[0])))) {
          continue;
        }
      }

      // Found end of a run with common leading regexp:
      // sub[start:i] all begin with first but sub[i] does not.
      //
      // Factor out common regexp and append factored expression to out.
      if (i == start) {
        // Nothing to do - run of length 0.
      } else if (i == start + 1) {
        // Just one: don't bother factoring.
        array[lenout++] = array[s + start];
      } else {
        // Construct factored form: prefix(suffix1|suffix2|...)
        Regexp prefix = first;
        for (int j = start; j < i; j++) {
          boolean reuse = j != start; // prefix came from sub[start]
          array[s + j] = removeLeadingRegexp(array[s + j], reuse);
        }
        // recurse
        Regexp suffix = collapse(subarray(array, s + start, s + i), Regexp.Op.ALTERNATE);
        Regexp re = newRegexp(Regexp.Op.CONCAT);
        re.subs = new Regexp[] {prefix, suffix};
        array[lenout++] = re;
      }

      // Prepare for next iteration.
      start = i;
      first = ifirst;
    }
    // In Go: sub = out
    lensub = lenout;
    s = 0;

    // Round 3: Collapse runs of single literals into character classes.
    start = 0;
    lenout = 0;
    for (int i = 0; i <= lensub; i++) {
      // Invariant: the Regexps that were in sub[0:start] have been
      // used or marked for reuse, and the slice space has been reused
      // for out (lenout <= start).
      //
      // Invariant: sub[start:i] consists of regexps that are either
      // literal runes or character classes.
      if (i < lensub && isCharClass(array[s + i])) {
        continue;
      }

      // sub[i] is not a char or char class;
      // emit char class for sub[start:i]...
      if (i == start) {
        // Nothing to do - run of length 0.
      } else if (i == start + 1) {
        array[lenout++] = array[s + start];
      } else {
        // Make new char class.
        // Start with most complex regexp in sub[start].
        int max = start;
        for (int j = start + 1; j < i; j++) {
          Regexp subMax = array[s + max], subJ = array[s + j];
          if (subMax.op.ordinal() < subJ.op.ordinal()
              || (subMax.op == subJ.op
                  && (subMax.runes != null ? subMax.runes.length : 0)
                      < (subJ.runes != null ? subJ.runes.length : 0))) {
            max = j;
          }
        }
        // swap sub[start], sub[max].
        Regexp tmp = array[s + start];
        array[s + start] = array[s + max];
        array[s + max] = tmp;

        for (int j = start + 1; j < i; j++) {
          mergeCharClass(array[s + start], array[s + j]);
          reuse(array[s + j]);
        }
        cleanAlt(array[s + start]);
        array[lenout++] = array[s + start];
      }

      // ... and then emit sub[i].
      if (i < lensub) {
        array[lenout++] = array[s + i];
      }
      start = i + 1;
    }
    // In Go: sub = out
    lensub = lenout;
    s = 0;

    // Round 4: Collapse runs of empty matches into a single empty match.
    start = 0;
    lenout = 0;
    for (int i = 0; i < lensub; ++i) {
      if (i + 1 < lensub
          && array[s + i].op == Regexp.Op.EMPTY_MATCH
          && array[s + i + 1].op == Regexp.Op.EMPTY_MATCH) {
        continue;
      }
      array[lenout++] = array[s + i];
    }
    // In Go: sub = out
    lensub = lenout;
    s = 0;

    return subarray(array, s, lensub);
  }

  // removeLeadingString removes the first n leading runes
  // from the beginning of re.  It returns the replacement for re.
  private Regexp removeLeadingString(Regexp re, int n) {
    if (re.op == Regexp.Op.CONCAT && re.subs.length > 0) {
      // Removing a leading string in a concatenation
      // might simplify the concatenation.
      Regexp sub = removeLeadingString(re.subs[0], n);
      re.subs[0] = sub;
      if (sub.op == Regexp.Op.EMPTY_MATCH) {
        reuse(sub);
        switch (re.subs.length) {
          case 0:
          case 1:
            // Impossible but handle.
            re.op = Regexp.Op.EMPTY_MATCH;
            re.subs = null;
            break;
          case 2:
            {
              Regexp old = re;
              re = re.subs[1];
              reuse(old);
              break;
            }
          default:
            re.subs = subarray(re.subs, 1, re.subs.length);
            break;
        }
      }
      return re;
    }

    if (re.op == Regexp.Op.LITERAL) {
      re.runes = Utils.subarray(re.runes, n, re.runes.length);
      if (re.runes.length == 0) {
        re.op = Regexp.Op.EMPTY_MATCH;
      }
    }
    return re;
  }

  // leadingRegexp returns the leading regexp that re begins with.
  // The regexp refers to storage in re or its children.
  private static Regexp leadingRegexp(Regexp re) {
    if (re.op == Regexp.Op.EMPTY_MATCH) {
      return null;
    }
    if (re.op == Regexp.Op.CONCAT && re.subs.length > 0) {
      Regexp sub = re.subs[0];
      if (sub.op == Regexp.Op.EMPTY_MATCH) {
        return null;
      }
      return sub;
    }
    return re;
  }

  // removeLeadingRegexp removes the leading regexp in re.
  // It returns the replacement for re.
  // If reuse is true, it passes the removed regexp (if no longer needed) to
  // reuse.
  private Regexp removeLeadingRegexp(Regexp re, boolean reuse) {
    if (re.op == Regexp.Op.CONCAT && re.subs.length > 0) {
      if (reuse) {
        reuse(re.subs[0]);
      }
      re.subs = subarray(re.subs, 1, re.subs.length);
      switch (re.subs.length) {
        case 0:
          re.op = Regexp.Op.EMPTY_MATCH;
          re.subs = Regexp.EMPTY_SUBS;
          break;
        case 1:
          Regexp old = re;
          re = re.subs[0];
          reuse(old);
          break;
      }
      return re;
    }
    if (reuse) {
      reuse(re);
    }
    return newRegexp(Regexp.Op.EMPTY_MATCH);
  }

  private static Regexp literalRegexp(String s, int flags) {
    Regexp re = new Regexp(Regexp.Op.LITERAL);
    re.flags = flags;
    re.runes = Utils.stringToRunes(s);
    return re;
  }

  // Parsing.

  // StringIterator: a stream of runes with an opaque cursor, permitting
  // rewinding.  The units of the cursor are not specified beyond the
  // fact that ASCII characters are single width.  (Cursor positions
  // could be UTF-8 byte indices, UTF-16 code indices or rune indices.)
  //
  // In particular, be careful with:
  // - skip(int): only use this to advance over ASCII characters
  //   since these always have a width of 1.
  // - skip(String): only use this to advance over strings which are
  //   known to be at the current position, e.g. due to prior call to
  //   lookingAt().
  // Only use pop() to advance over possibly non-ASCII runes.
  private static class StringIterator {
    private final String str; // a stream of UTF-16 codes
    private int pos = 0; // current position in UTF-16 string

    StringIterator(String str) {
      this.str = str;
    }

    // Returns the cursor position.  Do not interpret the result!
    int pos() {
      return pos;
    }

    // Resets the cursor position to a previous value returned by pos().
    void rewindTo(int pos) {
      this.pos = pos;
    }

    // Returns true unless the stream is exhausted.
    boolean more() {
      return pos < str.length();
    }

    // Returns the rune at the cursor position.
    // Precondition: |more()|.
    int peek() {
      return str.codePointAt(pos);
    }

    // Advances the cursor by |n| positions, which must be ASCII runes.
    //
    // (In practise, this is only ever used to skip over regexp
    // metacharacters that are ASCII, so there is no numeric difference
    // between indices into  UTF-8 bytes, UTF-16 codes and runes.)
    void skip(int n) {
      pos += n;
    }

    // Advances the cursor by the number of cursor positions in |s|.
    void skipString(String s) {
      pos += s.length();
    }

    // Returns the rune at the cursor position, and advances the cursor
    // past it.  Precondition: |more()|.
    int pop() {
      int r = str.codePointAt(pos);
      pos += Character.charCount(r);
      return r;
    }

    // Equivalent to both peek() == c but more efficient because we
    // don't support surrogates.  Precondition: |more()|.
    boolean lookingAt(char c) {
      return str.charAt(pos) == c;
    }

    // Equivalent to rest().startsWith(s).
    boolean lookingAt(String s) {
      return rest().startsWith(s);
    }

    // Returns the rest of the pattern as a Java UTF-16 string.
    String rest() {
      return str.substring(pos);
    }

    // Returns the substring from |beforePos| to the current position.
    // |beforePos| must have been previously returned by |pos()|.
    String from(int beforePos) {
      return str.substring(beforePos, pos);
    }

    @Override
    public String toString() {
      return rest();
    }
  }

  /**
   * Parse regular expression pattern {@code pattern} with mode flags {@code flags}.
   */
  static Regexp parse(String pattern, int flags) throws PatternSyntaxException {
    return new Parser(pattern, flags).parseInternal();
  }

  private Regexp parseInternal() throws PatternSyntaxException {
    if ((flags & RE2.LITERAL) != 0) {
      // Trivial parser for literal string.
      return literalRegexp(wholeRegexp, flags);
    }

    // Otherwise, must do real work.
    int lastRepeatPos = -1, min = -1, max = -1;
    StringIterator t = new StringIterator(wholeRegexp);
    while (t.more()) {
      int repeatPos = -1;
      bigswitch:
      switch (t.peek()) {
        default:
          literal(t.pop());
          break;

        case '(':
          if ((flags & RE2.PERL_X) != 0 && t.lookingAt("(?")) {
            // Flag changes and non-capturing groups.
            parsePerlFlags(t);
            break;
          }
          op(Regexp.Op.LEFT_PAREN).cap = ++numCap;
          t.skip(1); // '('
          break;

        case '|':
          parseVerticalBar();
          t.skip(1); // '|'
          break;

        case ')':
          parseRightParen();
          t.skip(1); // ')'
          break;

        case '^':
          if ((flags & RE2.ONE_LINE) != 0) {
            op(Regexp.Op.BEGIN_TEXT);
          } else {
            op(Regexp.Op.BEGIN_LINE);
          }
          t.skip(1); // '^'
          break;

        case '$':
          if ((flags & RE2.ONE_LINE) != 0) {
            op(Regexp.Op.END_TEXT).flags |= RE2.WAS_DOLLAR;
          } else {
            op(Regexp.Op.END_LINE);
          }
          t.skip(1); // '$'
          break;

        case '.':
          if ((flags & RE2.DOT_NL) != 0) {
            op(Regexp.Op.ANY_CHAR);
          } else {
            op(Regexp.Op.ANY_CHAR_NOT_NL);
          }
          t.skip(1); // '.'
          break;

        case '[':
          parseClass(t);
          break;

        case '*':
        case '+':
        case '?':
          {
            repeatPos = t.pos();
            Regexp.Op op = null;
            switch (t.pop()) {
              case '*':
                op = Regexp.Op.STAR;
                break;
              case '+':
                op = Regexp.Op.PLUS;
                break;
              case '?':
                op = Regexp.Op.QUEST;
                break;
            }
            repeat(op, min, max, repeatPos, t, lastRepeatPos);
            // (min and max are now dead.)
            break;
          }
        case '{':
          {
            repeatPos = t.pos();
            int minMax = parseRepeat(t);
            if (minMax < 0) {
              // If the repeat cannot be parsed, { is a literal.
              t.rewindTo(repeatPos);
              literal(t.pop()); // '{'
              break;
            }
            min = minMax >> 16;
            max = (short) (minMax & 0xffff); // sign extend
            repeat(Regexp.Op.REPEAT, min, max, repeatPos, t, lastRepeatPos);
            break;
          }

        case '\\':
          {
            int savedPos = t.pos();
            t.skip(1); // '\\'
            if ((flags & RE2.PERL_X) != 0 && t.more()) {
              int c = t.pop();
              switch (c) {
                case 'A':
                  op(Regexp.Op.BEGIN_TEXT);
                  break bigswitch;
                case 'b':
                  op(Regexp.Op.WORD_BOUNDARY);
                  break bigswitch;
                case 'B':
                  op(Regexp.Op.NO_WORD_BOUNDARY);
                  break bigswitch;
                case 'C':
                  // any byte; not supported
                  throw new PatternSyntaxException(ERR_INVALID_ESCAPE, "\\C");
                case 'Q':
                  {
                    // \Q ... \E: the ... is always literals
                    String lit = t.rest();
                    int i = lit.indexOf("\\E");
                    if (i >= 0) {
                      lit = lit.substring(0, i);
                    }
                    t.skipString(lit);
                    t.skipString("\\E");
                    for (int j = 0; j < lit.length(); ) {
                      int codepoint = lit.codePointAt(j);
                      literal(codepoint);
                      j += Character.charCount(codepoint);
                    }
                    break bigswitch;
                  }
                case 'z':
                  op(Regexp.Op.END_TEXT);
                  break bigswitch;
                default:
                  t.rewindTo(savedPos);
                  break;
              }
            }

            Regexp re = newRegexp(Regexp.Op.CHAR_CLASS);
            re.flags = flags;

            // Look for Unicode character group like \p{Han}
            if (t.lookingAt("\\p") || t.lookingAt("\\P")) {
              CharClass cc = new CharClass();
              if (parseUnicodeClass(t, cc)) {
                re.runes = cc.toArray();
                push(re);
                break bigswitch;
              }
            }

            // Perl character class escape.
            CharClass cc = new CharClass();
            if (parsePerlClassEscape(t, cc)) {
              re.runes = cc.toArray();
              push(re);
              break bigswitch;
            }

            t.rewindTo(savedPos);
            reuse(re);

            // Ordinary single-character escape.
            literal(parseEscape(t));
            break;
          }
      }
      lastRepeatPos = repeatPos;
    }

    concat();
    if (swapVerticalBar()) {
      pop(); // pop vertical bar
    }
    alternate();

    int n = stack.size();
    if (n != 1) {
      throw new PatternSyntaxException(ERR_MISSING_PAREN, wholeRegexp);
    }
    stack.get(0).namedGroups = namedGroups;
    return stack.get(0);
  }

  // parseRepeat parses {min} (max=min) or {min,} (max=-1) or {min,max}.
  // If |t| is not of that form, it returns -1.
  // If |t| has the right form but the values are negative or too big,
  // it returns -2.
  // On success, returns a nonnegative number encoding min/max in the
  // high/low signed halfwords of the result.  (Note: min >= 0; max may
  // be -1.)
  //
  // On success, advances |t| beyond the repeat; otherwise |t.pos()| is
  // undefined.
  private static int parseRepeat(StringIterator t) throws PatternSyntaxException {
    int start = t.pos();
    if (!t.more() || !t.lookingAt('{')) {
      return -1;
    }
    t.skip(1); // '{'
    int min = parseInt(t); // (can be -2)
    if (min == -1) {
      return -1;
    }
    if (!t.more()) {
      return -1;
    }
    int max;
    if (!t.lookingAt(',')) {
      max = min;
    } else {
      t.skip(1); // ','
      if (!t.more()) {
        return -1;
      }
      if (t.lookingAt('}')) {
        max = -1;
      } else if ((max = parseInt(t)) == -1) { // (can be -2)
        return -1;
      }
    }
    if (!t.more() || !t.lookingAt('}')) {
      return -1;
    }
    t.skip(1); // '}'
    if (min < 0 || min > 1000 || max == -2 || max > 1000 || (max >= 0 && min > max)) {
      // Numbers were negative or too big, or max is present and min > max.
      throw new PatternSyntaxException(ERR_INVALID_REPEAT_SIZE, t.from(start));
    }
    return (min << 16) | (max & 0xffff); // success
  }

  // parsePerlFlags parses a Perl flag setting or non-capturing group or both,
  // like (?i) or (?: or (?i:.
  // Pre: t at "(?".  Post: t after ")".
  // Sets numCap.
  private void parsePerlFlags(StringIterator t) throws PatternSyntaxException {
    int startPos = t.pos();

    // Check for named captures, first introduced in Python's regexp library.
    // As usual, there are three slightly different syntaxes:
    //
    //   (?P<name>expr)   the original, introduced by Python
    //   (?<name>expr)    the .NET alteration, adopted by Perl 5.10
    //   (?'name'expr)    another .NET alteration, adopted by Perl 5.10
    //
    // Perl 5.10 gave in and implemented the Python version too,
    // but they claim that the last two are the preferred forms.
    // PCRE and languages based on it (specifically, PHP and Ruby)
    // support all three as well.  EcmaScript 4 uses only the Python form.
    //
    // In both the open source world (via Code Search) and the
    // Google source tree, (?P<name>expr) and (?<name>expr) are the
    // dominant forms of named captures and both are supported.
    String s = t.rest();
    if (s.startsWith("(?P<") || s.startsWith("(?<")) {
      // Pull out name.
      int begin = s.charAt(2) == 'P' ? 4 : 3;
      int end = s.indexOf('>', begin);
      if (end < 0) {
        throw new PatternSyntaxException(ERR_INVALID_NAMED_CAPTURE, s);
      }
      String name = s.substring(begin, end); // "name"
      t.skipString(name);
      t.skip(begin + 1); // "(?P<>" or "(?<>"
      if (!isValidCaptureName(name)) {
        throw new PatternSyntaxException(
            ERR_INVALID_NAMED_CAPTURE, s.substring(0, end + 1)); // "(?P<name>" or "(?<name>"
      }
      // Like ordinary capture, but named.
      Regexp re = op(Regexp.Op.LEFT_PAREN);
      re.cap = ++numCap;
      if (namedGroups.put(name, numCap) != null) {
        throw new PatternSyntaxException(ERR_DUPLICATE_NAMED_CAPTURE, name);
      }
      re.name = name;
      return;
    }

    // Non-capturing group.  Might also twiddle Perl flags.
    t.skip(2); // "(?"
    int flags = this.flags;
    int sign = +1;
    boolean sawFlag = false;
    loop:
    while (t.more()) {
      int c = t.pop();
      switch (c) {
        default:
          break loop;

          // Flags.
        case 'i':
          flags |= RE2.FOLD_CASE;
          sawFlag = true;
          break;
        case 'm':
          flags &= ~RE2.ONE_LINE;
          sawFlag = true;
          break;
        case 's':
          flags |= RE2.DOT_NL;
          sawFlag = true;
          break;
        case 'U':
          flags |= RE2.NON_GREEDY;
          sawFlag = true;
          break;

          // Switch to negation.
        case '-':
          if (sign < 0) {
            break loop;
          }
          sign = -1;
          // Invert flags so that | above turn into &~ and vice versa.
          // We'll invert flags again before using it below.
          flags = ~flags;
          sawFlag = false;
          break;

          // End of flags, starting group or not.
        case ':':
        case ')':
          if (sign < 0) {
            if (!sawFlag) {
              break loop;
            }
            flags = ~flags;
          }
          if (c == ':') {
            // Open new group
            op(Regexp.Op.LEFT_PAREN);
          }
          this.flags = flags;
          return;
      }
    }

    throw new PatternSyntaxException(ERR_INVALID_PERL_OP, t.from(startPos));
  }

  // isValidCaptureName reports whether name
  // is a valid capture name: [A-Za-z0-9_]+.
  // PCRE limits names to 32 bytes.
  // Python rejects names starting with digits.
  // We don't enforce either of those.
  private static boolean isValidCaptureName(String name) {
    if (name.isEmpty()) {
      return false;
    }
    for (int i = 0; i < name.length(); ++i) {
      char c = name.charAt(i);
      if (c != '_' && !Utils.isalnum(c)) {
        return false;
      }
    }
    return true;
  }

  // parseInt parses a nonnegative decimal integer.
  // -1 => bad format.  -2 => format ok, but integer overflow.
  private static int parseInt(StringIterator t) {
    int start = t.pos();
    int c;
    while (t.more() && (c = t.peek()) >= '0' && c <= '9') {
      t.skip(1); // digit
    }
    String n = t.from(start);
    if (n.isEmpty() || (n.length() > 1 && n.charAt(0) == '0')) { // disallow leading zeros
      return -1; // bad format
    }
    if (n.length() > 8) {
      return -2; // overflow
    }
    return Integer.valueOf(n, 10); // can't fail
  }

  // can this be represented as a character class?
  // single-rune literal string, char class, ., and .|\n.
  private static boolean isCharClass(Regexp re) {
    return ((re.op == Regexp.Op.LITERAL && re.runes.length == 1)
        || re.op == Regexp.Op.CHAR_CLASS
        || re.op == Regexp.Op.ANY_CHAR_NOT_NL
        || re.op == Regexp.Op.ANY_CHAR);
  }

  // does re match r?
  private static boolean matchRune(Regexp re, int r) {
    switch (re.op) {
      case LITERAL:
        return re.runes.length == 1 && re.runes[0] == r;
      case CHAR_CLASS:
        for (int i = 0; i < re.runes.length; i += 2) {
          if (re.runes[i] <= r && r <= re.runes[i + 1]) {
            return true;
          }
        }
        return false;
      case ANY_CHAR_NOT_NL:
        return r != '\n';
      case ANY_CHAR:
        return true;
    }
    return false;
  }

  // parseVerticalBar handles a | in the input.
  private void parseVerticalBar() {
    concat();

    // The concatenation we just parsed is on top of the stack.
    // If it sits above an opVerticalBar, swap it below
    // (things below an opVerticalBar become an alternation).
    // Otherwise, push a new vertical bar.
    if (!swapVerticalBar()) {
      op(Regexp.Op.VERTICAL_BAR);
    }
  }

  // mergeCharClass makes dst = dst|src.
  // The caller must ensure that dst.Op >= src.Op,
  // to reduce the amount of copying.
  private static void mergeCharClass(Regexp dst, Regexp src) {
    switch (dst.op) {
      case ANY_CHAR:
        // src doesn't add anything.
        break;
      case ANY_CHAR_NOT_NL:
        // src might add \n
        if (matchRune(src, '\n')) {
          dst.op = Regexp.Op.ANY_CHAR;
        }
        break;
      case CHAR_CLASS:
        // src is simpler, so either literal or char class
        if (src.op == Regexp.Op.LITERAL) {
          dst.runes = new CharClass(dst.runes).appendLiteral(src.runes[0], src.flags).toArray();
        } else {
          dst.runes = new CharClass(dst.runes).appendClass(src.runes).toArray();
        }
        break;
      case LITERAL:
        // both literal
        if (src.runes[0] == dst.runes[0] && src.flags == dst.flags) {
          break;
        }
        dst.op = Regexp.Op.CHAR_CLASS;
        dst.runes =
            new CharClass()
                .appendLiteral(dst.runes[0], dst.flags)
                .appendLiteral(src.runes[0], src.flags)
                .toArray();
        break;
    }
  }

  // If the top of the stack is an element followed by an opVerticalBar
  // swapVerticalBar swaps the two and returns true.
  // Otherwise it returns false.
  private boolean swapVerticalBar() {
    // If above and below vertical bar are literal or char class,
    // can merge into a single char class.
    int n = stack.size();
    if (n >= 3
        && stack.get(n - 2).op == Regexp.Op.VERTICAL_BAR
        && isCharClass(stack.get(n - 1))
        && isCharClass(stack.get(n - 3))) {
      Regexp re1 = stack.get(n - 1);
      Regexp re3 = stack.get(n - 3);
      // Make re3 the more complex of the two.
      if (re1.op.ordinal() > re3.op.ordinal()) {
        Regexp tmp = re3;
        re3 = re1;
        re1 = tmp;
        stack.set(n - 3, re3);
      }
      mergeCharClass(re3, re1);
      reuse(re1);
      pop();
      return true;
    }

    if (n >= 2) {
      Regexp re1 = stack.get(n - 1);
      Regexp re2 = stack.get(n - 2);
      if (re2.op == Regexp.Op.VERTICAL_BAR) {
        if (n >= 3) {
          // Now out of reach.
          // Clean opportunistically.
          cleanAlt(stack.get(n - 3));
        }
        stack.set(n - 2, re1);
        stack.set(n - 1, re2);
        return true;
      }
    }
    return false;
  }

  // parseRightParen handles a ')' in the input.
  private void parseRightParen() throws PatternSyntaxException {
    concat();
    if (swapVerticalBar()) {
      pop(); // pop vertical bar
    }
    alternate();

    int n = stack.size();
    if (n < 2) {
      throw new PatternSyntaxException(ERR_INTERNAL_ERROR, "stack underflow");
    }
    Regexp re1 = pop();
    Regexp re2 = pop();
    if (re2.op != Regexp.Op.LEFT_PAREN) {
      throw new PatternSyntaxException(ERR_MISSING_PAREN, wholeRegexp);
    }
    // Restore flags at time of paren.
    this.flags = re2.flags;
    if (re2.cap == 0) {
      // Just for grouping.
      push(re1);
    } else {
      re2.op = Regexp.Op.CAPTURE;
      re2.subs = new Regexp[] {re1};
      push(re2);
    }
  }

  // parseEscape parses an escape sequence at the beginning of s
  // and returns the rune.
  // Pre: t at '\\'.  Post: after escape.
  @SuppressWarnings("fallthrough") // disables *all* fallthru checking. Lame.
  private static int parseEscape(StringIterator t) throws PatternSyntaxException {
    int startPos = t.pos();
    t.skip(1); // '\\'
    if (!t.more()) {
      throw new PatternSyntaxException(ERR_TRAILING_BACKSLASH);
    }
    int c = t.pop();
    bigswitch:
    switch (c) {
      default:
        if (!Utils.isalnum(c)) {
          // Escaped non-word characters are always themselves.
          // PCRE is not quite so rigorous: it accepts things like
          // \q, but we don't.  We once rejected \_, but too many
          // programs and people insist on using it, so allow \_.
          return c;
        }
        break;

        // Octal escapes.
      case '1':
      case '2':
      case '3':
      case '4':
      case '5':
      case '6':
      case '7':
        // Single non-zero digit is a backreference; not supported
        if (!t.more() || t.peek() < '0' || t.peek() > '7') {
          break;
        }
        /* fallthrough */
      case '0':
        // Consume up to three octal digits; already have one.
        int r = c - '0';
        for (int i = 1; i < 3; i++) {
          if (!t.more() || t.peek() < '0' || t.peek() > '7') {
            break;
          }
          r = r * 8 + t.peek() - '0';
          t.skip(1); // digit
        }
        return r;

        // Hexadecimal escapes.
      case 'x':
        if (!t.more()) {
          break;
        }
        c = t.pop();
        if (c == '{') {
          // Any number of digits in braces.
          // Perl accepts any text at all; it ignores all text
          // after the first non-hex digit.  We require only hex digits,
          // and at least one.
          int nhex = 0;
          r = 0;
          for (; ; ) {
            if (!t.more()) {
              break bigswitch;
            }
            c = t.pop();
            if (c == '}') {
              break;
            }
            int v = Utils.unhex(c);
            if (v < 0) {
              break bigswitch;
            }
            r = r * 16 + v;
            if (r > Unicode.MAX_RUNE) {
              break bigswitch;
            }
            nhex++;
          }
          if (nhex == 0) {
            break bigswitch;
          }
          return r;
        }

        // Easy case: two hex digits.
        int x = Utils.unhex(c);
        if (!t.more()) {
          break;
        }
        c = t.pop();
        int y = Utils.unhex(c);
        if (x < 0 || y < 0) {
          break;
        }
        return x * 16 + y;

        // C escapes.  There is no case 'b', to avoid misparsing
        // the Perl word-boundary \b as the C backspace \b
        // when in POSIX mode.  In Perl, /\b/ means word-boundary
        // but /[\b]/ means backspace.  We don't support that.
        // If you want a backspace, embed a literal backspace
        // character or use \x08.
      case 'a':
        return 7; // No \a in Java
      case 'f':
        return '\f';
      case 'n':
        return '\n';
      case 'r':
        return '\r';
      case 't':
        return '\t';
      case 'v':
        return 11; // No \v in Java
    }
    throw new PatternSyntaxException(ERR_INVALID_ESCAPE, t.from(startPos));
  }

  // parseClassChar parses a character class character and returns it.
  // wholeClassPos is the position of the start of the entire class "[...".
  // Pre: t at class char; Post: t after it.
  private static int parseClassChar(StringIterator t, int wholeClassPos)
      throws PatternSyntaxException {
    if (!t.more()) {
      throw new PatternSyntaxException(ERR_MISSING_BRACKET, t.from(wholeClassPos));
    }

    // Allow regular escape sequences even though
    // many need not be escaped in this context.
    if (t.lookingAt('\\')) {
      return parseEscape(t);
    }

    return t.pop();
  }

  // parsePerlClassEscape parses a leading Perl character class escape like \d
  // from the beginning of |t|.  If one is present, it appends the characters
  // to cc and returns true.  The iterator is advanced past the escape
  // on success, undefined on failure, in which case false is returned.
  private boolean parsePerlClassEscape(StringIterator t, CharClass cc) {
    int beforePos = t.pos();
    if ((flags & RE2.PERL_X) == 0
        || !t.more()
        || t.pop() != '\\'
        || // consume '\\'
        !t.more()) {
      return false;
    }
    t.pop(); // e.g. advance past 'd' in "\\d"
    CharGroup g = CharGroup.PERL_GROUPS.get(t.from(beforePos));
    if (g == null) {
      return false;
    }
    cc.appendGroup(g, (flags & RE2.FOLD_CASE) != 0);
    return true;
  }

  // parseNamedClass parses a leading POSIX named character class like
  // [:alnum:] from the beginning of t.  If one is present, it appends the
  // characters to cc, advances the iterator, and returns true.
  // Pre: t at "[:".  Post: t after ":]".
  // On failure (no class of than name), throws PatternSyntaxException.
  // On misparse, returns false; t.pos() is undefined.
  private boolean parseNamedClass(StringIterator t, CharClass cc) throws PatternSyntaxException {
    // (Go precondition check deleted.)
    String cls = t.rest();
    int i = cls.indexOf(":]");
    if (i < 0) {
      return false;
    }
    String name = cls.substring(0, i + 2); // "[:alnum:]"
    t.skipString(name);
    CharGroup g = CharGroup.POSIX_GROUPS.get(name);
    if (g == null) {
      throw new PatternSyntaxException(ERR_INVALID_CHAR_RANGE, name);
    }
    cc.appendGroup(g, (flags & RE2.FOLD_CASE) != 0);
    return true;
  }

  // RangeTables are represented as int[][], a list of triples (start, end,
  // stride).
  private static final int[][] ANY_TABLE = {
    {0, Unicode.MAX_RUNE, 1},
  };

  // unicodeTable() returns the Unicode RangeTable identified by name
  // and the table of additional fold-equivalent code points.
  // Returns null if |name| does not identify a Unicode character range.
  private static Pair<int[][], int[][]> unicodeTable(String name) {
    // Special case: "Any" means any.
    if (name.equals("Any")) {
      return Pair.of(ANY_TABLE, ANY_TABLE);
    }
    int[][] table = UnicodeTables.CATEGORIES.get(name);
    if (table != null) {
      return Pair.of(table, UnicodeTables.FOLD_CATEGORIES.get(name));
    }
    table = UnicodeTables.SCRIPTS.get(name);
    if (table != null) {
      return Pair.of(table, UnicodeTables.FOLD_SCRIPT.get(name));
    }
    return null;
  }

  // parseUnicodeClass() parses a leading Unicode character class like \p{Han}
  // from the beginning of t.  If one is present, it appends the characters to
  // to |cc|, advances |t| and returns true.
  //
  // Returns false if such a pattern is not present or UNICODE_GROUPS
  // flag is not enabled; |t.pos()| is not advanced in this case.
  // Indicates error by throwing PatternSyntaxException.
  private boolean parseUnicodeClass(StringIterator t, CharClass cc) throws PatternSyntaxException {
    int startPos = t.pos();
    if ((flags & RE2.UNICODE_GROUPS) == 0 || (!t.lookingAt("\\p") && !t.lookingAt("\\P"))) {
      return false;
    }
    t.skip(1); // '\\'
    // Committed to parse or throw exception.
    int sign = +1;
    int c = t.pop(); // 'p' or 'P'
    if (c == 'P') {
      sign = -1;
    }
    if (!t.more()) {
      t.rewindTo(startPos);
      throw new PatternSyntaxException(ERR_INVALID_CHAR_RANGE, t.rest());
    }
    c = t.pop();
    String name;
    if (c != '{') {
      // Single-letter name.
      name = Utils.runeToString(c);
    } else {
      // Name is in braces.
      String rest = t.rest();
      int end = rest.indexOf('}');
      if (end < 0) {
        t.rewindTo(startPos);
        throw new PatternSyntaxException(ERR_INVALID_CHAR_RANGE, t.rest());
      }
      name = rest.substring(0, end); // e.g. "Han"
      t.skipString(name);
      t.skip(1); // '}'
      // Don't use skip(end) because it assumes UTF-16 coding, and
      // StringIterator doesn't guarantee that.
    }

    // Group can have leading negation too.
    //  \p{^Han} == \P{Han}, \P{^Han} == \p{Han}.
    if (!name.isEmpty() && name.charAt(0) == '^') {
      sign = -sign;
      name = name.substring(1);
    }

    Pair<int[][], int[][]> pair = unicodeTable(name);
    if (pair == null) {
      throw new PatternSyntaxException(ERR_INVALID_CHAR_RANGE, t.from(startPos));
    }
    int[][] tab = pair.first;
    int[][] fold = pair.second; // fold-equivalent table

    // Variation of CharClass.appendGroup() for tables.
    if ((flags & RE2.FOLD_CASE) == 0 || fold == null) {
      cc.appendTableWithSign(tab, sign);
    } else {
      // Merge and clean tab and fold in a temporary buffer.
      // This is necessary for the negative case and just tidy
      // for the positive case.
      int[] tmp = new CharClass().appendTable(tab).appendTable(fold).cleanClass().toArray();
      cc.appendClassWithSign(tmp, sign);
    }
    return true;
  }

  // parseClass parses a character class and pushes it onto the parse stack.
  //
  // NOTES:
  // Pre: at '['; Post: after ']'.
  // Mutates stack.  Advances iterator.  May throw.
  private void parseClass(StringIterator t) throws PatternSyntaxException {
    int startPos = t.pos();
    t.skip(1); // '['
    Regexp re = newRegexp(Regexp.Op.CHAR_CLASS);
    re.flags = flags;
    CharClass cc = new CharClass();

    int sign = +1;
    if (t.more() && t.lookingAt('^')) {
      sign = -1;
      t.skip(1); // '^'

      // If character class does not match \n, add it here,
      // so that negation later will do the right thing.
      if ((flags & RE2.CLASS_NL) == 0) {
        cc.appendRange('\n', '\n');
      }
    }

    boolean first = true; // ']' and '-' are okay as first char in class
    while (!t.more() || t.peek() != ']' || first) {
      // POSIX: - is only okay unescaped as first or last in class.
      // Perl: - is okay anywhere.
      if (t.more() && t.lookingAt('-') && (flags & RE2.PERL_X) == 0 && !first) {
        String s = t.rest();
        if (s.equals("-") || !s.startsWith("-]")) {
          t.rewindTo(startPos);
          throw new PatternSyntaxException(ERR_INVALID_CHAR_RANGE, t.rest());
        }
      }
      first = false;

      int beforePos = t.pos();

      // Look for POSIX [:alnum:] etc.
      if (t.lookingAt("[:")) {
        if (parseNamedClass(t, cc)) {
          continue;
        }
        t.rewindTo(beforePos);
      }

      // Look for Unicode character group like \p{Han}.
      if (parseUnicodeClass(t, cc)) {
        continue;
      }

      // Look for Perl character class symbols (extension).
      if (parsePerlClassEscape(t, cc)) {
        continue;
      }
      t.rewindTo(beforePos);

      // Single character or simple range.
      int lo = parseClassChar(t, startPos);
      int hi = lo;
      if (t.more() && t.lookingAt('-')) {
        t.skip(1); // '-'
        if (t.more() && t.lookingAt(']')) {
          // [a-] means (a|-) so check for final ].
          t.skip(-1);
        } else {
          hi = parseClassChar(t, startPos);
          if (hi < lo) {
            throw new PatternSyntaxException(ERR_INVALID_CHAR_RANGE, t.from(beforePos));
          }
        }
      }
      if ((flags & RE2.FOLD_CASE) == 0) {
        cc.appendRange(lo, hi);
      } else {
        cc.appendFoldedRange(lo, hi);
      }
    }
    t.skip(1); // ']'

    cc.cleanClass();
    if (sign < 0) {
      cc.negateClass();
    }
    re.runes = cc.toArray();
    push(re);
  }

  //// Utilities

  // Returns a new copy of the specified subarray.
  static Regexp[] subarray(Regexp[] array, int start, int end) {
    Regexp[] r = new Regexp[end - start];
    for (int i = start; i < end; ++i) {
      r[i - start] = array[i];
    }
    return r;
  }

  private static class Pair<F, S> {
    final F first;
    final S second;

    Pair(F first, S second) {
      this.first = first;
      this.second = second;
    }

    static <F, S> Pair<F, S> of(F first, S second) {
      return new Pair<F, S>(first, second);
    }
  }

  private static int[] concatRunes(int[] x, int[] y) {
    int[] z = new int[x.length + y.length];
    System.arraycopy(x, 0, z, 0, x.length);
    System.arraycopy(y, 0, z, x.length, y.length);
    return z;
  }
}
