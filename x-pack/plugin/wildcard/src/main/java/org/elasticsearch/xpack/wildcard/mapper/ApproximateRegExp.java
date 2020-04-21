/*
 * dk.brics.automaton
 * 
 * Copyright (c) 2001-2009 Anders Moeller
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 * 3. The name of the author may not be used to endorse or promote products
 *    derived from this software without specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE AUTHOR ``AS IS'' AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
 * IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
 * NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF
 * THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package org.elasticsearch.xpack.wildcard.mapper;

import org.apache.lucene.util.automaton.Automata;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.AutomatonProvider;
import org.apache.lucene.util.automaton.MinimizationOperations;
import org.apache.lucene.util.automaton.Operations;
import org.apache.lucene.util.automaton.TooComplexToDeterminizeException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 
 * This class is a fork of Lucene's RegExp class and is used to create a very simplified
 * form of automaton that is used to extract ngrams used in accelerating searches on
 * the wildcard field.
 * Any non-concrete expressions in the regex e.g. ".*" are converted into a special marker character
 * used in {@link NGramAutomaton} to denote sections of the automaton that are unparseable 
 * 
 *
 */
public class ApproximateRegExp {
  
  enum Kind {
    REGEXP_UNION, REGEXP_CONCATENATION, REGEXP_INTERSECTION, REGEXP_OPTIONAL, REGEXP_REPEAT, REGEXP_REPEAT_MIN, 
    REGEXP_REPEAT_MINMAX, REGEXP_COMPLEMENT, REGEXP_CHAR, REGEXP_CHAR_RANGE, REGEXP_ANYCHAR, REGEXP_EMPTY, 
    REGEXP_STRING, REGEXP_ANYSTRING, REGEXP_AUTOMATON, REGEXP_INTERVAL
  }
  
  /**
   * Syntax flag, enables intersection (<tt>&amp;</tt>).
   */
  public static final int INTERSECTION = 0x0001;
  
  /**
   * Syntax flag, enables complement (<tt>~</tt>).
   */
  public static final int COMPLEMENT = 0x0002;
  
  /**
   * Syntax flag, enables empty language (<tt>#</tt>).
   */
  public static final int EMPTY = 0x0004;
  
  /**
   * Syntax flag, enables anystring (<tt>@</tt>).
   */
  public static final int ANYSTRING = 0x0008;
  
  /**
   * Syntax flag, enables named automata (<tt>&lt;</tt>identifier<tt>&gt;</tt>).
   */
  public static final int AUTOMATON = 0x0010;
  
  /**
   * Syntax flag, enables numerical intervals (
   * <tt>&lt;<i>n</i>-<i>m</i>&gt;</tt>).
   */
  public static final int INTERVAL = 0x0020;
  
  /**
   * Syntax flag, enables all optional regexp syntax.
   */
  public static final int ALL = 0xffff;
  
  /**
   * Syntax flag, enables no optional regexp syntax.
   */
  public static final int NONE = 0x0000;

  private final String originalString;
  Kind kind;
  ApproximateRegExp exp1, exp2;
  String s;
  int c;
  int min, max, digits;
  int from, to;
  
  int flags;
  int pos;
  
  ApproximateRegExp() {
    this.originalString = null;
  }
  
  /**
   * Constructs new <code>RegExp</code> from a string. Same as
   * <code>RegExp(s, ALL)</code>.
   * 
   * @param s regexp string
   * @exception IllegalArgumentException if an error occurred while parsing the
   *              regular expression
   */
  public ApproximateRegExp(String s) throws IllegalArgumentException {
    this(s, ALL);
  }
  
  /**
   * Constructs new <code>RegExp</code> from a string.
   * 
   * @param s regexp string
   * @param syntax_flags boolean 'or' of optional syntax constructs to be
   *          enabled
   * @exception IllegalArgumentException if an error occurred while parsing the
   *              regular expression
   */
  public ApproximateRegExp(String s, int syntax_flags) throws IllegalArgumentException {
    originalString = s;
    flags = syntax_flags;
    ApproximateRegExp e;
    if (s.length() == 0) e = makeString("");
    else {
      e = parseUnionExp();
      if (pos < originalString.length()) throw new IllegalArgumentException(
          "end-of-string expected at position " + pos);
    }
    kind = e.kind;
    exp1 = e.exp1;
    exp2 = e.exp2;
    this.s = e.s;
    c = e.c;
    min = e.min;
    max = e.max;
    digits = e.digits;
    from = e.from;
    to = e.to;
  }

  /**
   * Constructs new <code>Automaton</code> from this <code>RegExp</code>. Same
   * as <code>toAutomaton(null)</code> (empty automaton map).
   */
  public Automaton toAutomaton() {
    return toAutomaton(null, null, Operations.DEFAULT_MAX_DETERMINIZED_STATES);
  }

  /**
   * Constructs new <code>Automaton</code> from this <code>RegExp</code>. The
   * constructed automaton is minimal and deterministic and has no transitions
   * to dead states.
   * 
   * @param maxDeterminizedStates maximum number of states in the resulting
   *   automata.  If the automata would need more than this many states
   *   TooComplextToDeterminizeException is thrown.  Higher number require more
   *   space but can process more complex regexes.
   * @exception IllegalArgumentException if this regular expression uses a named
   *              identifier that is not available from the automaton provider
   * @exception TooComplexToDeterminizeException if determinizing this regexp
   *   requires more than maxDeterminizedStates states
   */
  public Automaton toAutomaton(int maxDeterminizedStates)
      throws IllegalArgumentException, TooComplexToDeterminizeException {
    return toAutomaton(null, null, maxDeterminizedStates);
  }

  
  private Automaton toAutomaton(Map<String,Automaton> automata,
      AutomatonProvider automaton_provider, int maxDeterminizedStates)
      throws IllegalArgumentException, TooComplexToDeterminizeException {
      return toAutomatonInternal(automata, automaton_provider,
        maxDeterminizedStates);
  }
  
  private Automaton toAutomatonInternal(Map<String,Automaton> automata,
      AutomatonProvider automaton_provider, int maxDeterminizedStates)
      throws IllegalArgumentException {
    List<Automaton> list;
    Automaton a = null;
    switch (kind) {
      case REGEXP_UNION:
        list = new ArrayList<>();
        findLeaves(exp1, Kind.REGEXP_UNION, list, automata, automaton_provider,
          maxDeterminizedStates);
        findLeaves(exp2, Kind.REGEXP_UNION, list, automata, automaton_provider,
          maxDeterminizedStates);
        a = Operations.union(list);
        a = MinimizationOperations.minimize(a, maxDeterminizedStates);
        break;
      case REGEXP_CONCATENATION:
        list = new ArrayList<>();
        findLeaves(exp1, Kind.REGEXP_CONCATENATION, list, automata,
            automaton_provider, maxDeterminizedStates);
        findLeaves(exp2, Kind.REGEXP_CONCATENATION, list, automata,
            automaton_provider, maxDeterminizedStates);
        a = Operations.concatenate(list);
        a = MinimizationOperations.minimize(a, maxDeterminizedStates);
        break;
      case REGEXP_INTERSECTION:
        a = Operations.intersection(
            exp1.toAutomatonInternal(
              automata, automaton_provider, maxDeterminizedStates),
            exp2.toAutomatonInternal(
              automata, automaton_provider, maxDeterminizedStates));
        a = MinimizationOperations.minimize(a, maxDeterminizedStates);
        break;
      case REGEXP_OPTIONAL:
      case REGEXP_REPEAT:
      case REGEXP_REPEAT_MIN:
      case REGEXP_REPEAT_MINMAX:
      case REGEXP_COMPLEMENT:
      case REGEXP_CHAR_RANGE:
      case REGEXP_ANYSTRING:
      case REGEXP_ANYCHAR:
      case REGEXP_INTERVAL:
        a = Automata.makeString(NGramAutomaton.INVALID_CHAR_STRING);
        break;
      case REGEXP_EMPTY:
        a = Automata.makeEmpty();
        break;
      case REGEXP_STRING:
        a = Automata.makeString(s);
        break;
      case REGEXP_CHAR:
          a = Automata.makeChar(c);
          break;
      case REGEXP_AUTOMATON:
        Automaton aa = null;
        if (automata != null) {
          aa = automata.get(s);
        }
        if (aa == null && automaton_provider != null) {
          try {
            aa = automaton_provider.getAutomaton(s);
          } catch (IOException e) {
            throw new IllegalArgumentException(e);
          }
        }
        if (aa == null) {
          throw new IllegalArgumentException("'" + s + "' not found");
        }
        a = aa;
        break;
    }
    return a;
  }
  
  private void findLeaves(ApproximateRegExp exp, Kind kind, List<Automaton> list,
      Map<String,Automaton> automata, AutomatonProvider automaton_provider,
      int maxDeterminizedStates) {
    if (exp.kind == kind) {
      findLeaves(exp.exp1, kind, list, automata, automaton_provider,
        maxDeterminizedStates);
      findLeaves(exp.exp2, kind, list, automata, automaton_provider,
        maxDeterminizedStates);
    } else {
      list.add(exp.toAutomatonInternal(automata, automaton_provider, 
        maxDeterminizedStates));
    }
  }

  /**
   * The string that was used to construct the regex.  Compare to toString.
   */
  public String getOriginalString() {
    return originalString;
  }

  static ApproximateRegExp makeUnion(ApproximateRegExp exp1, ApproximateRegExp exp2) {
    ApproximateRegExp r = new ApproximateRegExp();
    r.kind = Kind.REGEXP_UNION;
    r.exp1 = exp1;
    r.exp2 = exp2;
    return r;
  }
  
  static ApproximateRegExp makeConcatenation(ApproximateRegExp exp1, ApproximateRegExp exp2) {
    if ((exp1.kind == Kind.REGEXP_CHAR || exp1.kind == Kind.REGEXP_STRING)
        && (exp2.kind == Kind.REGEXP_CHAR || exp2.kind == Kind.REGEXP_STRING)) return makeString(
        exp1, exp2);
    ApproximateRegExp r = new ApproximateRegExp();
    r.kind = Kind.REGEXP_CONCATENATION;
    if (exp1.kind == Kind.REGEXP_CONCATENATION
        && (exp1.exp2.kind == Kind.REGEXP_CHAR || exp1.exp2.kind == Kind.REGEXP_STRING)
        && (exp2.kind == Kind.REGEXP_CHAR || exp2.kind == Kind.REGEXP_STRING)) {
      r.exp1 = exp1.exp1;
      r.exp2 = makeString(exp1.exp2, exp2);
    } else if ((exp1.kind == Kind.REGEXP_CHAR || exp1.kind == Kind.REGEXP_STRING)
        && exp2.kind == Kind.REGEXP_CONCATENATION
        && (exp2.exp1.kind == Kind.REGEXP_CHAR || exp2.exp1.kind == Kind.REGEXP_STRING)) {
      r.exp1 = makeString(exp1, exp2.exp1);
      r.exp2 = exp2.exp2;
    } else {
      r.exp1 = exp1;
      r.exp2 = exp2;
    }
    return r;
  }
  
  static private ApproximateRegExp makeString(ApproximateRegExp exp1, ApproximateRegExp exp2) {
    StringBuilder b = new StringBuilder();
    if (exp1.kind == Kind.REGEXP_STRING) b.append(exp1.s);
    else b.appendCodePoint(exp1.c);
    if (exp2.kind == Kind.REGEXP_STRING) b.append(exp2.s);
    else b.appendCodePoint(exp2.c);
    return makeString(b.toString());
  }
  
  static ApproximateRegExp makeIntersection(ApproximateRegExp exp1, ApproximateRegExp exp2) {
    ApproximateRegExp r = new ApproximateRegExp();
    r.kind = Kind.REGEXP_INTERSECTION;
    r.exp1 = exp1;
    r.exp2 = exp2;
    return r;
  }
  
  static ApproximateRegExp makeOptional(ApproximateRegExp exp) {
    ApproximateRegExp r = new ApproximateRegExp();
    r.kind = Kind.REGEXP_OPTIONAL;
    r.exp1 = exp;
    return r;
  }
  
  static ApproximateRegExp makeRepeat(ApproximateRegExp exp) {
    ApproximateRegExp r = new ApproximateRegExp();
    r.kind = Kind.REGEXP_REPEAT;
    r.exp1 = exp;
    return r;
  }
  
  static ApproximateRegExp makeRepeat(ApproximateRegExp exp, int min) {
    ApproximateRegExp r = new ApproximateRegExp();
    r.kind = Kind.REGEXP_REPEAT_MIN;
    r.exp1 = exp;
    r.min = min;
    return r;
  }
  
  static ApproximateRegExp makeRepeat(ApproximateRegExp exp, int min, int max) {
    ApproximateRegExp r = new ApproximateRegExp();
    r.kind = Kind.REGEXP_REPEAT_MINMAX;
    r.exp1 = exp;
    r.min = min;
    r.max = max;
    return r;
  }
  
  static ApproximateRegExp makeComplement(ApproximateRegExp exp) {
    ApproximateRegExp r = new ApproximateRegExp();
    r.kind = Kind.REGEXP_COMPLEMENT;
    r.exp1 = exp;
    return r;
  }
  
  static ApproximateRegExp makeChar(int c) {
    ApproximateRegExp r = new ApproximateRegExp();
    r.kind = Kind.REGEXP_CHAR;
    r.c = c;
    return r;
  }
  
  static ApproximateRegExp makeCharRange(int from, int to) {
    if (from > to) 
      throw new IllegalArgumentException("invalid range: from (" + from + ") cannot be > to (" + to + ")");
    ApproximateRegExp r = new ApproximateRegExp();
    r.kind = Kind.REGEXP_CHAR_RANGE;
    r.from = from;
    r.to = to;
    return r;
  }
  
  static ApproximateRegExp makeAnyChar() {
    ApproximateRegExp r = new ApproximateRegExp();
    r.kind = Kind.REGEXP_ANYCHAR;
    return r;
  }
  
  static ApproximateRegExp makeEmpty() {
    ApproximateRegExp r = new ApproximateRegExp();
    r.kind = Kind.REGEXP_EMPTY;
    return r;
  }
  
  static ApproximateRegExp makeString(String s) {
    ApproximateRegExp r = new ApproximateRegExp();
    r.kind = Kind.REGEXP_STRING;
    r.s = s;
    return r;
  }
  
  static ApproximateRegExp makeAnyString() {
    ApproximateRegExp r = new ApproximateRegExp();
    r.kind = Kind.REGEXP_ANYSTRING;
    return r;
  }
  
  static ApproximateRegExp makeAutomaton(String s) {
    ApproximateRegExp r = new ApproximateRegExp();
    r.kind = Kind.REGEXP_AUTOMATON;
    r.s = s;
    return r;
  }
  
  static ApproximateRegExp makeInterval(int min, int max, int digits) {
    ApproximateRegExp r = new ApproximateRegExp();
    r.kind = Kind.REGEXP_INTERVAL;
    r.min = min;
    r.max = max;
    r.digits = digits;
    return r;
  }
  
  private boolean peek(String s) {
    return more() && s.indexOf(originalString.codePointAt(pos)) != -1;
  }
  
  private boolean match(int c) {
    if (pos >= originalString.length()) return false;
    if (originalString.codePointAt(pos) == c) {
      pos += Character.charCount(c);
      return true;
    }
    return false;
  }
  
  private boolean more() {
    return pos < originalString.length();
  }
  
  private int next() throws IllegalArgumentException {
    if (!more()) throw new IllegalArgumentException("unexpected end-of-string");
    int ch = originalString.codePointAt(pos);
    pos += Character.charCount(ch);
    return ch;
  }
  
  private boolean check(int flag) {
    return (flags & flag) != 0;
  }
  
  final ApproximateRegExp parseUnionExp() throws IllegalArgumentException {
    ApproximateRegExp e = parseInterExp();
    if (match('|')) e = makeUnion(e, parseUnionExp());
    return e;
  }
  
  final ApproximateRegExp parseInterExp() throws IllegalArgumentException {
    ApproximateRegExp e = parseConcatExp();
    if (check(INTERSECTION) && match('&')) e = makeIntersection(e,
        parseInterExp());
    return e;
  }
  
  final ApproximateRegExp parseConcatExp() throws IllegalArgumentException {
    ApproximateRegExp e = parseRepeatExp();
    if (more() && !peek(")|") && (!check(INTERSECTION) || !peek("&"))) e = makeConcatenation(
        e, parseConcatExp());
    return e;
  }
  
  final ApproximateRegExp parseRepeatExp() throws IllegalArgumentException {
    ApproximateRegExp e = parseComplExp();
    while (peek("?*+{")) {
      if (match('?')) e = makeOptional(e);
      else if (match('*')) e = makeRepeat(e);
      else if (match('+')) e = makeRepeat(e, 1);
      else if (match('{')) {
        int start = pos;
        while (peek("0123456789"))
          next();
        if (start == pos) throw new IllegalArgumentException(
            "integer expected at position " + pos);
        int n = Integer.parseInt(originalString.substring(start, pos));
        int m = -1;
        if (match(',')) {
          start = pos;
          while (peek("0123456789"))
            next();
          if (start != pos) m = Integer.parseInt(
            originalString.substring(start, pos));
        } else m = n;
        if (!match('}')) throw new IllegalArgumentException(
            "expected '}' at position " + pos);
        if (m == -1) e = makeRepeat(e, n);
        else e = makeRepeat(e, n, m);
      }
    }
    return e;
  }
  
  final ApproximateRegExp parseComplExp() throws IllegalArgumentException {
    if (check(COMPLEMENT) && match('~')) return makeComplement(parseComplExp());
    else return parseCharClassExp();
  }
  
  final ApproximateRegExp parseCharClassExp() throws IllegalArgumentException {
    if (match('[')) {
      boolean negate = false;
      if (match('^')) negate = true;
      ApproximateRegExp e = parseCharClasses();
      if (negate) e = makeIntersection(makeAnyChar(), makeComplement(e));
      if (!match(']')) throw new IllegalArgumentException(
          "expected ']' at position " + pos);
      return e;
    } else return parseSimpleExp();
  }
  
  final ApproximateRegExp parseCharClasses() throws IllegalArgumentException {
    ApproximateRegExp e = parseCharClass();
    while (more() && !peek("]"))
      e = makeUnion(e, parseCharClass());
    return e;
  }
  
  final ApproximateRegExp parseCharClass() throws IllegalArgumentException {
    int c = parseCharExp();
    if (match('-')) return makeCharRange(c, parseCharExp());
    else return makeChar(c);
  }
  
  final ApproximateRegExp parseSimpleExp() throws IllegalArgumentException {
    if (match('.')) return makeAnyChar();
    else if (check(EMPTY) && match('#')) return makeEmpty();
    else if (check(ANYSTRING) && match('@')) return makeAnyString();
    else if (match('"')) {
      int start = pos;
      while (more() && !peek("\""))
        next();
      if (!match('"')) throw new IllegalArgumentException(
          "expected '\"' at position " + pos);
      return makeString(originalString.substring(start, pos - 1));
    } else if (match('(')) {
      if (match(')')) return makeString("");
      ApproximateRegExp e = parseUnionExp();
      if (!match(')')) throw new IllegalArgumentException(
          "expected ')' at position " + pos);
      return e;
    } else if ((check(AUTOMATON) || check(INTERVAL)) && match('<')) {
      int start = pos;
      while (more() && !peek(">"))
        next();
      if (!match('>')) throw new IllegalArgumentException(
          "expected '>' at position " + pos);
      String s = originalString.substring(start, pos - 1);
      int i = s.indexOf('-');
      if (i == -1) {
        if (!check(AUTOMATON)) throw new IllegalArgumentException(
            "interval syntax error at position " + (pos - 1));
        return makeAutomaton(s);
      } else {
        if (!check(INTERVAL)) throw new IllegalArgumentException(
            "illegal identifier at position " + (pos - 1));
        try {
          if (i == 0 || i == s.length() - 1 || i != s.lastIndexOf('-')) throw new NumberFormatException();
          String smin = s.substring(0, i);
          String smax = s.substring(i + 1, s.length());
          int imin = Integer.parseInt(smin);
          int imax = Integer.parseInt(smax);
          int digits;
          if (smin.length() == smax.length()) digits = smin.length();
          else digits = 0;
          if (imin > imax) {
            int t = imin;
            imin = imax;
            imax = t;
          }
          return makeInterval(imin, imax, digits);
        } catch (NumberFormatException e) {
          throw new IllegalArgumentException(
              "interval syntax error at position " + (pos - 1));
        }
      }
    } else return makeChar(parseCharExp());
  }
  
  final int parseCharExp() throws IllegalArgumentException {
    match('\\');
    return next();
  }
}
