/*
 * Copyright (c) 2020 The Go Authors. All rights reserved.
 *
 * Use of this source code is governed by a BSD-style
 * license that can be found in the LICENSE file.
 */
// Original Go source here:
// http://code.google.com/p/go/source/browse/src/pkg/regexp/regexp.go

// Beware, submatch results may pin a large underlying String into
// memory.  Consider creating explicit string copies if submatches are
// long-lived and inputs are large.
//
// The JDK API supports incremental processing of the input without
// necessarily consuming it all; we do not attempt to do so.

// The Java API emphasises UTF-16 Strings, not UTF-8 byte[] as in Go, as
// the primary input datatype, and the method names have been changed to
// reflect this.

package com.google.re2j;

import com.google.re2j.MatcherInput.Encoding;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * An RE2 class instance is a compiled representation of an RE2 regular expression, independent of
 * the public Java-like Pattern/Matcher API.
 *
 * <p>
 * This class also contains various implementation helpers for RE2 regular expressions.
 *
 * <p>
 * Use the {@link #quoteMeta(String)} utility function to quote all regular expression
 * metacharacters in an arbitrary string.
 *
 * <p>
 * See the {@code Matcher} and {@code Pattern} classes for the public API, and the <a
 * href='package.html'>package-level documentation</a> for an overview of how to use this API.
 */
class RE2 {

  // (In the Go implementation this structure is just called "Regexp".)

  //// Parser flags.

  // Fold case during matching (case-insensitive).
  static final int FOLD_CASE = 0x01;

  // Treat pattern as a literal string instead of a regexp.
  static final int LITERAL = 0x02;

  // Allow character classes like [^a-z] and [[:space:]] to match newline.
  static final int CLASS_NL = 0x04;

  // Allow '.' to match newline.
  static final int DOT_NL = 0x08;

  // Treat ^ and $ as only matching at beginning and end of text, not
  // around embedded newlines.  (Perl's default).
  static final int ONE_LINE = 0x10;

  // Make repetition operators default to non-greedy.
  static final int NON_GREEDY = 0x20;

  // allow Perl extensions:
  //   non-capturing parens - (?: )
  //   non-greedy operators - *? +? ?? {}?
  //   flag edits - (?i) (?-i) (?i: )
  //     i - FoldCase
  //     m - !OneLine
  //     s - DotNL
  //     U - NonGreedy
  //   line ends: \A \z
  //   \Q and \E to disable/enable metacharacters
  //   (?P<name>expr) for named captures
  // \C (any byte) is not supported.
  static final int PERL_X = 0x40;

  // Allow \p{Han}, \P{Han} for Unicode group and negation.
  static final int UNICODE_GROUPS = 0x80;

  // Regexp END_TEXT was $, not \z.  Internal use only.
  static final int WAS_DOLLAR = 0x100;

  static final int MATCH_NL = CLASS_NL | DOT_NL;

  // As close to Perl as possible.
  static final int PERL = CLASS_NL | ONE_LINE | PERL_X | UNICODE_GROUPS;

  // POSIX syntax.
  static final int POSIX = 0;

  //// Anchors
  static final int UNANCHORED = 0;
  static final int ANCHOR_START = 1;
  static final int ANCHOR_BOTH = 2;

  //// RE2 instance members.

  final String expr; // as passed to Compile
  final Prog prog; // compiled program
  final int cond; // EMPTY_* bitmask: empty-width conditions
  // required at start of match
  final int numSubexp;
  boolean longest;

  String prefix; // required UTF-16 prefix in unanchored matches
  byte[] prefixUTF8; // required UTF-8 prefix in unanchored matches
  boolean prefixComplete; // true iff prefix is the entire regexp
  int prefixRune; // first rune in prefix

  // Cache of machines for running regexp. Forms a Treiber stack.
  private final AtomicReference<Machine> pooled = new AtomicReference<Machine>();

  public Map<String, Integer> namedGroups;

  // This is visible for testing.
  RE2(String expr) {
    RE2 re2 = RE2.compile(expr);
    // Copy everything.
    this.expr = re2.expr;
    this.prog = re2.prog;
    this.cond = re2.cond;
    this.numSubexp = re2.numSubexp;
    this.longest = re2.longest;
    this.prefix = re2.prefix;
    this.prefixUTF8 = re2.prefixUTF8;
    this.prefixComplete = re2.prefixComplete;
    this.prefixRune = re2.prefixRune;
  }

  private RE2(String expr, Prog prog, int numSubexp, boolean longest) {
    this.expr = expr;
    this.prog = prog;
    this.numSubexp = numSubexp;
    this.cond = prog.startCond();
    this.longest = longest;
  }

  /**
   * Parses a regular expression and returns, if successful, an {@code RE2} instance that can be
   * used to match against text.
   *
   * <p>
   * When matching against text, the regexp returns a match that begins as early as possible in the
   * input (leftmost), and among those it chooses the one that a backtracking search would have
   * found first. This so-called leftmost-first matching is the same semantics that Perl, Python,
   * and other implementations use, although this package implements it without the expense of
   * backtracking. For POSIX leftmost-longest matching, see {@link #compilePOSIX}.
   */
  static RE2 compile(String expr) throws PatternSyntaxException {
    return compileImpl(expr, PERL, /*longest=*/ false);
  }

  /**
   * {@code compilePOSIX} is like {@link #compile} but restricts the regular expression to POSIX ERE
   * (egrep) syntax and changes the match semantics to leftmost-longest.
   *
   * <p>
   * That is, when matching against text, the regexp returns a match that begins as early as
   * possible in the input (leftmost), and among those it chooses a match that is as long as
   * possible. This so-called leftmost-longest matching is the same semantics that early regular
   * expression implementations used and that POSIX specifies.
   *
   * <p>
   * However, there can be multiple leftmost-longest matches, with different submatch choices, and
   * here this package diverges from POSIX. Among the possible leftmost-longest matches, this
   * package chooses the one that a backtracking search would have found first, while POSIX
   * specifies that the match be chosen to maximize the length of the first subexpression, then the
   * second, and so on from left to right. The POSIX rule is computationally prohibitive and not
   * even well-defined. See http://swtch.com/~rsc/regexp/regexp2.html#posix
   */
  static RE2 compilePOSIX(String expr) throws PatternSyntaxException {
    return compileImpl(expr, POSIX, /*longest=*/ true);
  }

  // Exposed to ExecTests.
  static RE2 compileImpl(String expr, int mode, boolean longest) throws PatternSyntaxException {
    Regexp re = Parser.parse(expr, mode);
    int maxCap = re.maxCap(); // (may shrink during simplify)
    re = Simplify.simplify(re);
    Prog prog = Compiler.compileRegexp(re);
    RE2 re2 = new RE2(expr, prog, maxCap, longest);
    StringBuilder prefixBuilder = new StringBuilder();
    re2.prefixComplete = prog.prefix(prefixBuilder);
    re2.prefix = prefixBuilder.toString();
    try {
      re2.prefixUTF8 = re2.prefix.getBytes("UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw new IllegalStateException("can't happen");
    }
    if (!re2.prefix.isEmpty()) {
      re2.prefixRune = re2.prefix.codePointAt(0);
    }
    re2.namedGroups = re.namedGroups;
    return re2;
  }

  /**
   * Returns the number of parenthesized subexpressions in this regular expression.
   */
  int numberOfCapturingGroups() {
    return numSubexp;
  }

  /**
   * Returns the number of instructions in this compiled regular expression program.
   */
  int numberOfInstructions() {
    return prog.numInst();
  }

  // get() returns a machine to use for matching |this|.  It uses |this|'s
  // machine cache if possible, to avoid unnecessary allocation.
  Machine get() {
    // Pop a machine off the stack if available.
    Machine head;
    do {
      head = pooled.get();
    } while (head != null && !pooled.compareAndSet(head, head.next));
    return head;
  }

  // Clears the memory associated with this machine.
  void reset() {
    pooled.set(null);
  }

  // put() returns a machine to |this|'s machine cache.  There is no attempt to
  // limit the size of the cache, so it will grow to the maximum number of
  // simultaneous matches run using |this|.  (The cache empties when |this|
  // gets garbage collected or reset is called.)
  @SuppressWarnings("MakeAlwaysEqual") // for ErrorProne, see below
  void put(Machine m, boolean isNew) {
    // To avoid allocation in the single-thread or uncontended case, reuse a node only if
    // it was the only element in the stack when it was popped, and it's the only element
    // in the stack when it's pushed back after use.
    Machine head;
    do {
      head = pooled.get();
      if (!isNew && head != null) {
        // If an element had a null next pointer and it was previously in the stack, another thread
        // might be trying to pop it out right now, and if it sees the same node now in the
        // stack the pop will succeed, but the new top of the stack will be the stale (null) value
        // of next. Allocate a new Machine so that the CAS will not succeed if this node has been
        // popped and re-pushed.
        m = new Machine(m);
        isNew = true;
      }

      // Without this comparison, TSAN will complain about a race condition:
      // Thread A, B, and C all attempt to do a match on the same pattern.
      //
      // A: Allocates Machine 1; executes match; put machine 1. State is now:
      //
      // pooled -> machine 1 -> null
      //
      // B reads pooled, sees machine 1
      //
      // C reads pooled, sees machine 1
      //
      // B successfully CASes pooled to null
      //
      // B executes match; put machine 1, which involves setting machine1.next to
      // null (even though it's already null); preempted before CAS
      //
      // C resumes, and reads machine1.next in order to execute cas(head, head.next)
      //
      // There is no happens-before relationship between B's redundant null write
      // and C's read, thus triggering TSAN.
      //
      // A future release of ErrorProne may want to make the assignment unconditionally. The
      // @SuppressWarning("MakeAlwaysEqual") on this method is intended to prevent that from happening.
      if (m.next != head) {
        m.next = head;
      }
    } while (!pooled.compareAndSet(head, m));
  }

  @Override
  public String toString() {
    return expr;
  }

  // doExecute() finds the leftmost match in the input and returns
  // the position of its subexpressions.
  // Derived from exec.go.
  private int[] doExecute(MachineInput in, int pos, int anchor, int ncap) {
    Machine m = get();
    // The Treiber stack cannot reuse nodes, unless the node to be reused has only ever been at
    // the bottom of the stack (i.e., next == null).
    boolean isNew = false;
    if (m == null) {
      m = new Machine(this);
      isNew = true;
    } else if (m.next != null) {
      m = new Machine(m);
      isNew = true;
    }

    m.init(ncap);
    int[] cap = m.match(in, pos, anchor) ? m.submatches() : null;
    put(m, isNew);
    return cap;
  }

  /**
   * Returns true iff this regexp matches the string {@code s}.
   */
  boolean match(CharSequence s) {
    return doExecute(MachineInput.fromUTF16(s), 0, UNANCHORED, 0) != null;
  }

  boolean match(CharSequence input, int start, int end, int anchor, int[] group, int ngroup) {
    return match(MatcherInput.utf16(input), start, end, anchor, group, ngroup);
  }

  /**
   * Matches the regular expression against input starting at position start and ending at position
   * end, with the given anchoring. Records the submatch boundaries in group, which is [start, end)
   * pairs of byte offsets. The number of boundaries needed is inferred from the size of the group
   * array. It is most efficient not to ask for submatch boundaries.
   *
   * @param input the input byte array
   * @param start the beginning position in the input
   * @param end the end position in the input
   * @param anchor the anchoring flag (UNANCHORED, ANCHOR_START, ANCHOR_BOTH)
   * @param group the array to fill with submatch positions
   * @param ngroup the number of array pairs to fill in
   * @return true if a match was found
   */
  boolean match(MatcherInput input, int start, int end, int anchor, int[] group, int ngroup) {
    if (start > end) {
      return false;
    }
    // TODO(afrozm): We suspect that the correct code should look something
    // like the following:
    // doExecute(MachineInput.fromUTF16(input), start, anchor, 2*ngroup);
    //
    // In Russ' own words:
    // That is, I believe doExecute needs to know the bounds of the whole input
    // as well as the bounds of the subpiece that is being searched.
    MachineInput machineInput =
        input.getEncoding() == Encoding.UTF_16
            ? MachineInput.fromUTF16(input.asCharSequence(), 0, end)
            : MachineInput.fromUTF8(input.asBytes(), 0, end);
    int[] groupMatch = doExecute(machineInput, start, anchor, 2 * ngroup);

    if (groupMatch == null) {
      return false;
    }

    if (group != null) {
      System.arraycopy(groupMatch, 0, group, 0, groupMatch.length);
    }
    return true;
  }

  /**
   * Returns true iff this regexp matches the UTF-8 byte array {@code b}.
   */
  // This is visible for testing.
  boolean matchUTF8(byte[] b) {
    return doExecute(MachineInput.fromUTF8(b), 0, UNANCHORED, 0) != null;
  }

  /**
   * Returns true iff textual regular expression {@code pattern} matches string {@code s}.
   *
   * <p>
   * More complicated queries need to use {@link #compile} and the full {@code RE2} interface.
   */
  // This is visible for testing.
  static boolean match(String pattern, CharSequence s) throws PatternSyntaxException {
    return compile(pattern).match(s);
  }

  // This is visible for testing.
  interface ReplaceFunc {
    String replace(String orig);
  }

  /**
   * Returns a copy of {@code src} in which all matches for this regexp have been replaced by
   * {@code repl}. No support is provided for expressions (e.g. {@code \1} or {@code $1}) in the
   * replacement string.
   */
  // This is visible for testing.
  String replaceAll(String src, final String repl) {
    return replaceAllFunc(
        src,
        new ReplaceFunc() {
          @Override
          public String replace(String orig) {
            return repl;
          }
        },
        2 * src.length() + 1);
    // TODO(afrozm): Is the reasoning correct, there can be at the most 2*len +1
    // replacements. Basically [a-z]*? abc x will be xaxbcx. So should it be
    // len + 1 or 2*len + 1.
  }

  /**
   * Returns a copy of {@code src} in which only the first match for this regexp has been replaced
   * by {@code repl}. No support is provided for expressions (e.g. {@code \1} or {@code $1}) in the
   * replacement string.
   */
  // This is visible for testing.
  String replaceFirst(String src, final String repl) {
    return replaceAllFunc(
        src,
        new ReplaceFunc() {
          @Override
          public String replace(String orig) {
            return repl;
          }
        },
        1);
  }

  /**
   * Returns a copy of {@code src} in which at most {@code maxReplaces} matches for this regexp have
   * been replaced by the return value of of function {@code repl} (whose first argument is the
   * matched string). No support is provided for expressions (e.g. {@code \1} or {@code $1}) in the
   * replacement string.
   */
  // This is visible for testing.
  String replaceAllFunc(String src, ReplaceFunc repl, int maxReplaces) {
    int lastMatchEnd = 0; // end position of the most recent match
    int searchPos = 0; // position where we next look for a match
    StringBuilder buf = new StringBuilder();
    MachineInput input = MachineInput.fromUTF16(src);
    int numReplaces = 0;
    while (searchPos <= src.length()) {
      int[] a = doExecute(input, searchPos, UNANCHORED, 2);
      if (a == null || a.length == 0) {
        break; // no more matches
      }

      // Copy the unmatched characters before this match.
      buf.append(src.substring(lastMatchEnd, a[0]));

      // Now insert a copy of the replacement string, but not for a
      // match of the empty string immediately after another match.
      // (Otherwise, we get double replacement for patterns that
      // match both empty and nonempty strings.)
      // FIXME(adonovan), FIXME(afrozm) - JDK seems to be doing exactly this
      // put a replacement for a pattern that also matches empty and non-empty
      // strings. The fix would not just be a[1] >= lastMatchEnd, there are a
      // few corner cases in that as well, and there are tests which will fail
      // when that case is touched (happens only at the end of the input string
      // though).
      if (a[1] > lastMatchEnd || a[0] == 0) {
        buf.append(repl.replace(src.substring(a[0], a[1])));
        // Increment the replace count.
        ++numReplaces;
      }
      lastMatchEnd = a[1];

      // Advance past this match; always advance at least one character.
      int width = input.step(searchPos) & 0x7;
      if (searchPos + width > a[1]) {
        searchPos += width;
      } else if (searchPos + 1 > a[1]) {
        // This clause is only needed at the end of the input
        // string.  In that case, DecodeRuneInString returns width=0.
        searchPos++;
      } else {
        searchPos = a[1];
      }
      if (numReplaces >= maxReplaces) {
        // Should never be greater though.
        break;
      }
    }

    // Copy the unmatched characters after the last match.
    buf.append(src.substring(lastMatchEnd));

    return buf.toString();
  }

  /**
   * Returns a string that quotes all regular expression metacharacters inside the argument text;
   * the returned string is a regular expression matching the literal text. For example,
   * {@code quoteMeta("[foo]").equals("\\[foo\\]")}.
   */
  static String quoteMeta(String s) {
    StringBuilder b = new StringBuilder(2 * s.length());
    // A char loop is correct because all metacharacters fit in one UTF-16 code.
    for (int i = 0, len = s.length(); i < len; i++) {
      char c = s.charAt(i);
      if ("\\.+*?()|[]{}^$".indexOf(c) >= 0) {
        b.append('\\');
      }
      b.append(c);
    }
    return b.toString();
  }

  // The number of capture values in the program may correspond
  // to fewer capturing expressions than are in the regexp.
  // For example, "(a){0}" turns into an empty program, so the
  // maximum capture in the program is 0 but we need to return
  // an expression for \1.  Pad returns a with -1s appended as needed;
  // the result may alias a.
  private int[] pad(int[] a) {
    if (a == null) {
      return null; // No match.
    }
    int n = (1 + numSubexp) * 2;
    if (a.length < n) {
      int[] a2 = new int[n];
      System.arraycopy(a, 0, a2, 0, a.length);
      Arrays.fill(a2, a.length, n, -1);
      a = a2;
    }
    return a;
  }

  private interface DeliverFunc {
    // Called iteratively with a list of submatch indices in the same
    // unit as the MachineInput cursor.
    void deliver(int[] x);
  }

  // Find matches in input.
  private void allMatches(MachineInput input, int n, DeliverFunc deliver) {
    int end = input.endPos();
    if (n < 0) {
      n = end + 1;
    }
    for (int pos = 0, i = 0, prevMatchEnd = -1; i < n && pos <= end; ) {
      int[] matches = doExecute(input, pos, UNANCHORED, prog.numCap);
      if (matches == null || matches.length == 0) {
        break;
      }

      boolean accept = true;
      if (matches[1] == pos) {
        // We've found an empty match.
        if (matches[0] == prevMatchEnd) {
          // We don't allow an empty match right
          // after a previous match, so ignore it.
          accept = false;
        }
        int r = input.step(pos);
        if (r < 0) { // EOF
          pos = end + 1;
        } else {
          pos += r & 0x7;
        }
      } else {
        pos = matches[1];
      }
      prevMatchEnd = matches[1];

      if (accept) {
        deliver.deliver(pad(matches));
        i++;
      }
    }
  }

  // Legacy Go-style interface; preserved (package-private) for better
  // test coverage.
  //
  // There are 16 methods of RE2 that match a regular expression and
  // identify the matched text.  Their names are matched by this regular
  // expression:
  //
  //    find(All)?(UTF8)?(Submatch)?(Index)?
  //
  // If 'All' is present, the routine matches successive non-overlapping
  // matches of the entire expression.  Empty matches abutting a
  // preceding match are ignored.  The return value is an array
  // containing the successive return values of the corresponding
  // non-All routine.  These routines take an extra integer argument, n;
  // if n >= 0, the function returns at most n matches/submatches.
  //
  // If 'UTF8' is present, the argument is a UTF-8 encoded byte[] array;
  // otherwise it is a UTF-16 encoded java.lang.String; return values
  // are adjusted as appropriate.
  //
  // If 'Submatch' is present, the return value is an list identifying
  // the successive submatches of the expression.  Submatches are
  // matches of parenthesized subexpressions within the regular
  // expression, numbered from left to right in order of opening
  // parenthesis.  Submatch 0 is the match of the entire expression,
  // submatch 1 the match of the first parenthesized subexpression, and
  // so on.
  //
  // If 'Index' is present, matches and submatches are identified by
  // byte index pairs within the input string: result[2*n:2*n+1]
  // identifies the indexes of the nth submatch.  The pair for n==0
  // identifies the match of the entire expression.  If 'Index' is not
  // present, the match is identified by the text of the match/submatch.
  // If an index is negative, it means that subexpression did not match
  // any string in the input.

  /**
   * Returns an array holding the text of the leftmost match in {@code b} of this regular
   * expression.
   *
   * <p>
   * A return value of null indicates no match.
   */
  // This is visible for testing.
  byte[] findUTF8(byte[] b) {
    int[] a = doExecute(MachineInput.fromUTF8(b), 0, UNANCHORED, 2);
    if (a == null) {
      return null;
    }
    return Utils.subarray(b, a[0], a[1]);
  }

  /**
   * Returns a two-element array of integers defining the location of the leftmost match in
   * {@code b} of this regular expression. The match itself is at {@code b[loc[0]...loc[1]]}.
   *
   * <p>
   * A return value of null indicates no match.
   */
  // This is visible for testing.
  int[] findUTF8Index(byte[] b) {
    int[] a = doExecute(MachineInput.fromUTF8(b), 0, UNANCHORED, 2);
    if (a == null) {
      return null;
    }
    return Utils.subarray(a, 0, 2);
  }

  /**
   * Returns a string holding the text of the leftmost match in {@code s} of this regular
   * expression.
   *
   * <p>
   * If there is no match, the return value is an empty string, but it will also be empty if the
   * regular expression successfully matches an empty string. Use {@link #findIndex} or
   * {@link #findSubmatch} if it is necessary to distinguish these cases.
   */
  // This is visible for testing.
  String find(String s) {
    int[] a = doExecute(MachineInput.fromUTF16(s), 0, UNANCHORED, 2);
    if (a == null) {
      return "";
    }
    return s.substring(a[0], a[1]);
  }

  /**
   * Returns a two-element array of integers defining the location of the leftmost match in
   * {@code s} of this regular expression. The match itself is at
   * {@code s.substring(loc[0], loc[1])}.
   *
   * <p>
   * A return value of null indicates no match.
   */
  // This is visible for testing.
  int[] findIndex(String s) {
    return doExecute(MachineInput.fromUTF16(s), 0, UNANCHORED, 2);
  }

  /**
   * Returns an array of arrays the text of the leftmost match of the regular expression in
   * {@code b} and the matches, if any, of its subexpressions, as defined by the <a
   * href='#submatch'>Submatch</a> description above.
   *
   * <p>
   * A return value of null indicates no match.
   */
  // This is visible for testing.
  byte[][] findUTF8Submatch(byte[] b) {
    int[] a = doExecute(MachineInput.fromUTF8(b), 0, UNANCHORED, prog.numCap);
    if (a == null) {
      return null;
    }
    byte[][] ret = new byte[1 + numSubexp][];
    for (int i = 0; i < ret.length; i++) {
      if (2 * i < a.length && a[2 * i] >= 0) {
        ret[i] = Utils.subarray(b, a[2 * i], a[2 * i + 1]);
      }
    }
    return ret;
  }

  /**
   * Returns an array holding the index pairs identifying the leftmost match of this regular
   * expression in {@code b} and the matches, if any, of its subexpressions, as defined by the the
   * <a href='#submatch'>Submatch</a> and <a href='#index'>Index</a> descriptions above.
   *
   * <p>
   * A return value of null indicates no match.
   */
  // This is visible for testing.
  int[] findUTF8SubmatchIndex(byte[] b) {
    return pad(doExecute(MachineInput.fromUTF8(b), 0, UNANCHORED, prog.numCap));
  }

  /**
   * Returns an array of strings holding the text of the leftmost match of the regular expression in
   * {@code s} and the matches, if any, of its subexpressions, as defined by the <a
   * href='#submatch'>Submatch</a> description above.
   *
   * <p>
   * A return value of null indicates no match.
   */
  // This is visible for testing.
  String[] findSubmatch(String s) {
    int[] a = doExecute(MachineInput.fromUTF16(s), 0, UNANCHORED, prog.numCap);
    if (a == null) {
      return null;
    }
    String[] ret = new String[1 + numSubexp];
    for (int i = 0; i < ret.length; i++) {
      if (2 * i < a.length && a[2 * i] >= 0) {
        ret[i] = s.substring(a[2 * i], a[2 * i + 1]);
      }
    }
    return ret;
  }

  /**
   * Returns an array holding the index pairs identifying the leftmost match of this regular
   * expression in {@code s} and the matches, if any, of its subexpressions, as defined by the <a
   * href='#submatch'>Submatch</a> description above.
   *
   * <p>
   * A return value of null indicates no match.
   */
  // This is visible for testing.
  int[] findSubmatchIndex(String s) {
    return pad(doExecute(MachineInput.fromUTF16(s), 0, UNANCHORED, prog.numCap));
  }

  /**
   * {@code findAllUTF8()} is the <a href='#all'>All</a> version of {@link #findUTF8}; it returns a
   * list of up to {@code n} successive matches of the expression, as defined by the <a
   * href='#all'>All</a> description above.
   *
   * <p>
   * A return value of null indicates no match.
   *
   * TODO(adonovan): think about defining a byte slice view class, like a read-only Go slice backed
   * by |b|.
   */
  // This is visible for testing.
  List<byte[]> findAllUTF8(final byte[] b, int n) {
    final List<byte[]> result = new ArrayList<byte[]>();
    allMatches(
        MachineInput.fromUTF8(b),
        n,
        new DeliverFunc() {
          @Override
          public void deliver(int[] match) {
            result.add(Utils.subarray(b, match[0], match[1]));
          }
        });
    if (result.isEmpty()) {
      return null;
    }
    return result;
  }

  /**
   * {@code findAllUTF8Index} is the <a href='#all'>All</a> version of {@link #findUTF8Index}; it
   * returns a list of up to {@code n} successive matches of the expression, as defined by the <a
   * href='#all'>All</a> description above.
   *
   * <p>
   * A return value of null indicates no match.
   */
  // This is visible for testing.
  List<int[]> findAllUTF8Index(final byte[] b, int n) {
    final List<int[]> result = new ArrayList<int[]>();
    allMatches(
        MachineInput.fromUTF8(b),
        n,
        new DeliverFunc() {
          @Override
          public void deliver(int[] match) {
            result.add(Utils.subarray(match, 0, 2));
          }
        });
    if (result.isEmpty()) {
      return null;
    }
    return result;
  }

  /**
   * {@code findAll} is the <a href='#all'>All</a> version of {@link #find}; it returns a list of up
   * to {@code n} successive matches of the expression, as defined by the <a href='#all'>All</a>
   * description above.
   *
   * <p>
   * A return value of null indicates no match.
   */
  // This is visible for testing.
  List<String> findAll(final String s, int n) {
    final List<String> result = new ArrayList<String>();
    allMatches(
        MachineInput.fromUTF16(s),
        n,
        new DeliverFunc() {
          @Override
          public void deliver(int[] match) {
            result.add(s.substring(match[0], match[1]));
          }
        });
    if (result.isEmpty()) {
      return null;
    }
    return result;
  }

  /**
   * {@code findAllIndex} is the <a href='#all'>All</a> version of {@link #findIndex}; it returns a
   * list of up to {@code n} successive matches of the expression, as defined by the <a
   * href='#all'>All</a> description above.
   *
   * <p>
   * A return value of null indicates no match.
   */
  // This is visible for testing.
  List<int[]> findAllIndex(String s, int n) {
    final List<int[]> result = new ArrayList<int[]>();
    allMatches(
        MachineInput.fromUTF16(s),
        n,
        new DeliverFunc() {
          @Override
          public void deliver(int[] match) {
            result.add(Utils.subarray(match, 0, 2));
          }
        });
    if (result.isEmpty()) {
      return null;
    }
    return result;
  }

  /**
   * {@code findAllUTF8Submatch} is the <a href='#all'>All</a> version of {@link #findUTF8Submatch};
   * it returns a list of up to {@code n} successive matches of the expression, as defined by the <a
   * href='#all'>All</a> description above.
   *
   * <p>
   * A return value of null indicates no match.
   */
  // This is visible for testing.
  List<byte[][]> findAllUTF8Submatch(final byte[] b, int n) {
    final List<byte[][]> result = new ArrayList<byte[][]>();
    allMatches(
        MachineInput.fromUTF8(b),
        n,
        new DeliverFunc() {
          @Override
          public void deliver(int[] match) {
            byte[][] slice = new byte[match.length / 2][];
            for (int j = 0; j < slice.length; ++j) {
              if (match[2 * j] >= 0) {
                slice[j] = Utils.subarray(b, match[2 * j], match[2 * j + 1]);
              }
            }
            result.add(slice);
          }
        });
    if (result.isEmpty()) {
      return null;
    }
    return result;
  }

  /**
   * {@code findAllUTF8SubmatchIndex} is the <a href='#all'>All</a> version of
   * {@link #findUTF8SubmatchIndex}; it returns a list of up to {@code n} successive matches of the
   * expression, as defined by the <a href='#all'>All</a> description above.
   *
   * <p>
   * A return value of null indicates no match.
   */
  // This is visible for testing.
  List<int[]> findAllUTF8SubmatchIndex(byte[] b, int n) {
    final List<int[]> result = new ArrayList<int[]>();
    allMatches(
        MachineInput.fromUTF8(b),
        n,
        new DeliverFunc() {
          @Override
          public void deliver(int[] match) {
            result.add(match);
          }
        });
    if (result.isEmpty()) {
      return null;
    }
    return result;
  }

  /**
   * {@code findAllSubmatch} is the <a href='#all'>All</a> version of {@link #findSubmatch}; it
   * returns a list of up to {@code n} successive matches of the expression, as defined by the <a
   * href='#all'>All</a> description above.
   *
   * <p>
   * A return value of null indicates no match.
   */
  // This is visible for testing.
  List<String[]> findAllSubmatch(final String s, int n) {
    final List<String[]> result = new ArrayList<String[]>();
    allMatches(
        MachineInput.fromUTF16(s),
        n,
        new DeliverFunc() {
          @Override
          public void deliver(int[] match) {
            String[] slice = new String[match.length / 2];
            for (int j = 0; j < slice.length; ++j) {
              if (match[2 * j] >= 0) {
                slice[j] = s.substring(match[2 * j], match[2 * j + 1]);
              }
            }
            result.add(slice);
          }
        });
    if (result.isEmpty()) {
      return null;
    }
    return result;
  }

  /**
   * {@code findAllSubmatchIndex} is the <a href='#all'>All</a> version of
   * {@link #findSubmatchIndex}; it returns a list of up to {@code n} successive matches of the
   * expression, as defined by the <a href='#all'>All</a> description above.
   *
   * <p>
   * A return value of null indicates no match.
   */
  // This is visible for testing.
  List<int[]> findAllSubmatchIndex(String s, int n) {
    final List<int[]> result = new ArrayList<int[]>();
    allMatches(
        MachineInput.fromUTF16(s),
        n,
        new DeliverFunc() {
          @Override
          public void deliver(int[] match) {
            result.add(match);
          }
        });
    if (result.isEmpty()) {
      return null;
    }
    return result;
  }
}
