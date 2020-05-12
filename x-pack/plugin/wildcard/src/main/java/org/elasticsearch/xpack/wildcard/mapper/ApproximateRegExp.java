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

import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

/**
 * 
 * This class is a fork of Lucene's RegExp class and is used to create a simplified
 * Query for use in accelerating searches to find those documents likely to match the regex. 
 *
 */
public class ApproximateRegExp {

    enum Kind {
        REGEXP_UNION,
        REGEXP_CONCATENATION,
        REGEXP_INTERSECTION,
        REGEXP_OPTIONAL,
        REGEXP_REPEAT,
        REGEXP_REPEAT_MIN,
        REGEXP_REPEAT_MINMAX,
        REGEXP_COMPLEMENT,
        REGEXP_CHAR,
        REGEXP_CHAR_RANGE,
        REGEXP_ANYCHAR,
        REGEXP_EMPTY,
        REGEXP_STRING,
        REGEXP_ANYSTRING,
        REGEXP_AUTOMATON,
        REGEXP_INTERVAL
    }

    /**
     * Syntax flag, enables intersection (<code>&amp;</code>).
     */
    public static final int INTERSECTION = 0x0001;

    /**
     * Syntax flag, enables complement (<code>~</code>).
     */
    public static final int COMPLEMENT = 0x0002;

    /**
     * Syntax flag, enables empty language (<code>#</code>).
     */
    public static final int EMPTY = 0x0004;

    /**
     * Syntax flag, enables anystring (<code>@</code>).
     */
    public static final int ANYSTRING = 0x0008;

    /**
     * Syntax flag, enables named automata (<code>&lt;</code>identifier<code>&gt;</code>).
     */
    public static final int AUTOMATON = 0x0010;

    /**
     * Syntax flag, enables numerical intervals (
     * <code>&lt;<i>n</i>-<i>m</i>&gt;</code>).
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
    
    // The string content in between operators can be normalised using this hook
    public interface StringNormalizer {
        String normalize(String s);
    }

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
            if (pos < originalString.length()) throw new IllegalArgumentException("end-of-string expected at position " + pos);
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

    // Convert a regular expression to a simplified query consisting of BooleanQuery and TermQuery objects
    // which captures as much of the logic as possible. Query can produce some false positives but shouldn't
    // produce any false negatives.
    // In addition to Term and BooleanQuery clauses there are MatchAllDocsQuery objects (e.g for .*) and
    // an equivalent MatchAllButRequireVerificationQuery. 
    // *  If an expression resolves to a single MatchAllDocsQuery eg .* then a match all shortcut is possible with 
    //    no verification needed.     
    // * If an expression resolves to a MatchAllButRequireVerificationQuery eg ?? then only the verification 
    //   query is run.
    // * Anything else is a concrete query that should be run on the ngram index.
    public Query toApproximationQuery(StringNormalizer normalizer) throws IllegalArgumentException {
        Query result = null;
        switch (kind) {
            case REGEXP_UNION:
                result = createUnionQuery(normalizer);
                break;
            case REGEXP_CONCATENATION:
                result = createConcatenationQuery(normalizer);
                break;
            case REGEXP_STRING:
                String normalizedString = normalizer == null ? s : normalizer.normalize(s);
                result = new TermQuery(new Term("", normalizedString));
                break;
            case REGEXP_CHAR:
                String cs = Character.toString(c);
                String normalizedChar = normalizer == null ? cs : normalizer.normalize(cs);
                result = new TermQuery(new Term("", normalizedChar));
                break;
            case REGEXP_REPEAT:
                // Repeat is zero or more times so zero matches = match all
                result = new MatchAllDocsQuery();
                break;
                
            case REGEXP_REPEAT_MIN:
            case REGEXP_REPEAT_MINMAX:
                if (min > 0) {
                    result = exp1.toApproximationQuery(normalizer);
                    if(result instanceof TermQuery) {
                        // Wrap the repeating expression so that it is not concatenated by a parent which concatenates
                        // plain TermQuery objects together. Boolean queries are interpreted as a black box and not
                        // concatenated.
                        BooleanQuery.Builder wrapper = new BooleanQuery.Builder();
                        wrapper.add(result, Occur.MUST);
                        result = wrapper.build();
                    }
                } else {
                    // Expressions like (a){0,3} match empty string or up to 3 a's.
                    // No lucene query equivalent for this.
                    result = new MatchAllButRequireVerificationQuery();
                }
                break;
            case REGEXP_ANYSTRING:
                // optimisation for .* queries - match all and no verification stage required.
                result = new MatchAllDocsQuery();
                break;
            // All other kinds of expression cannot be represented as a boolean or term query so return an object 
            // that indicates verification is required
            case REGEXP_OPTIONAL:
            case REGEXP_INTERSECTION:
            case REGEXP_COMPLEMENT:
            case REGEXP_CHAR_RANGE:
            case REGEXP_ANYCHAR:
            case REGEXP_INTERVAL:
            case REGEXP_EMPTY: 
            case REGEXP_AUTOMATON:
                result = new MatchAllButRequireVerificationQuery();
                break;
        }
        assert result != null; // All regex types are understood and translated to a query.
        return result;
    }
    
    private Query createConcatenationQuery(StringNormalizer normalizer) {
        // Create ANDs of expressions plus collapse consecutive TermQuerys into single longer ones
        ArrayList<Query> queries = new ArrayList<>();
        findLeaves(exp1, Kind.REGEXP_CONCATENATION, queries, normalizer);
        findLeaves(exp2, Kind.REGEXP_CONCATENATION, queries, normalizer);
        BooleanQuery.Builder bAnd = new BooleanQuery.Builder();
        StringBuilder sequence = new StringBuilder();
        for (Query query : queries) {
            if (query instanceof TermQuery) {
                TermQuery tq = (TermQuery) query;
                sequence.append(tq.getTerm().text());
            } else {
                if (sequence.length() > 0) {
                    bAnd.add(new TermQuery(new Term("", sequence.toString())), Occur.MUST);
                    sequence = new StringBuilder();
                }
                bAnd.add(query, Occur.MUST);                    
            }
        }
        if (sequence.length() > 0) {
            bAnd.add(new TermQuery(new Term("", sequence.toString())), Occur.MUST);
        }
        BooleanQuery combined = bAnd.build();
        if (combined.clauses().size() > 0) {
            return combined;
        }
        // There's something in the regex we couldn't represent as a query - resort to a match all with verification 
        return new MatchAllButRequireVerificationQuery();
    }

    private Query createUnionQuery(StringNormalizer normalizer) {
        // Create an OR of clauses
        ArrayList<Query> queries = new ArrayList<>();
        findLeaves(exp1, Kind.REGEXP_UNION, queries, normalizer);
        findLeaves(exp2, Kind.REGEXP_UNION, queries, normalizer);
        BooleanQuery.Builder bOr = new BooleanQuery.Builder();
        HashSet<Query> uniqueClauses = new HashSet<>();
        for (Query query : queries) {
            if (uniqueClauses.add(query)) {
                bOr.add(query, Occur.SHOULD);
            }
        }
        if (uniqueClauses.size() > 0) {
            if (uniqueClauses.size() == 1) {
                // Fully-understood ORs that collapse to a single term should be returned minus
                // the BooleanQuery wrapper so that they might be concatenated.
                // Helps turn [Pp][Oo][Ww][Ee][Rr][Ss][Hh][Ee][Ll][Ll] into "powershell"
                // Each char pair eg (P OR p) can be normalized to (p) which can be a single term
                return uniqueClauses.iterator().next();
            } else {
                return bOr.build();
            }
        }
        // There's something in the regex we couldn't represent as a query - resort to a match all with verification 
        return new MatchAllButRequireVerificationQuery();
    }

    private void findLeaves(ApproximateRegExp exp, Kind kind, List<Query> queries, StringNormalizer normalizer) {
        if (exp.kind == kind) {
            findLeaves(exp.exp1, kind, queries, normalizer);
            findLeaves(exp.exp2, kind, queries, normalizer);
        } else {
            queries.add(exp.toApproximationQuery(normalizer));
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
        if ((exp1.kind == Kind.REGEXP_CHAR || exp1.kind == Kind.REGEXP_STRING) && (exp2.kind == Kind.REGEXP_CHAR
            || exp2.kind == Kind.REGEXP_STRING)) return makeString(exp1, exp2);
        ApproximateRegExp r = new ApproximateRegExp();
        r.kind = Kind.REGEXP_CONCATENATION;
        if (exp1.kind == Kind.REGEXP_CONCATENATION && (exp1.exp2.kind == Kind.REGEXP_CHAR || exp1.exp2.kind == Kind.REGEXP_STRING)
            && (exp2.kind == Kind.REGEXP_CHAR || exp2.kind == Kind.REGEXP_STRING)) {
            r.exp1 = exp1.exp1;
            r.exp2 = makeString(exp1.exp2, exp2);
        } else if ((exp1.kind == Kind.REGEXP_CHAR || exp1.kind == Kind.REGEXP_STRING) && exp2.kind == Kind.REGEXP_CONCATENATION
            && (exp2.exp1.kind == Kind.REGEXP_CHAR || exp2.exp1.kind == Kind.REGEXP_STRING)) {
                r.exp1 = makeString(exp1, exp2.exp1);
                r.exp2 = exp2.exp2;
            } else {
                r.exp1 = exp1;
                r.exp2 = exp2;
            }
        return r;
    }

    private static ApproximateRegExp makeString(ApproximateRegExp exp1, ApproximateRegExp exp2) {
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
        if (from > to) throw new IllegalArgumentException("invalid range: from (" + from + ") cannot be > to (" + to + ")");
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
        if (check(INTERSECTION) && match('&')) e = makeIntersection(e, parseInterExp());
        return e;
    }

    final ApproximateRegExp parseConcatExp() throws IllegalArgumentException {
        ApproximateRegExp e = parseRepeatExp();
        if (more() && !peek(")|") && (!check(INTERSECTION) || !peek("&"))) e = makeConcatenation(e, parseConcatExp());
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
                if (start == pos) throw new IllegalArgumentException("integer expected at position " + pos);
                int n = Integer.parseInt(originalString.substring(start, pos));
                int m = -1;
                if (match(',')) {
                    start = pos;
                    while (peek("0123456789"))
                        next();
                    if (start != pos) m = Integer.parseInt(originalString.substring(start, pos));
                } else m = n;
                if (!match('}')) throw new IllegalArgumentException("expected '}' at position " + pos);
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
            if (!match(']')) throw new IllegalArgumentException("expected ']' at position " + pos);
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
            if (!match('"')) throw new IllegalArgumentException("expected '\"' at position " + pos);
            return makeString(originalString.substring(start, pos - 1));
        } else if (match('(')) {
            if (match(')')) return makeString("");
            ApproximateRegExp e = parseUnionExp();
            if (!match(')')) throw new IllegalArgumentException("expected ')' at position " + pos);
            return e;
        } else if ((check(AUTOMATON) || check(INTERVAL)) && match('<')) {
            int start = pos;
            while (more() && !peek(">"))
                next();
            if (!match('>')) throw new IllegalArgumentException("expected '>' at position " + pos);
            String s = originalString.substring(start, pos - 1);
            int i = s.indexOf('-');
            if (i == -1) {
                if (!check(AUTOMATON)) throw new IllegalArgumentException("interval syntax error at position " + (pos - 1));
                return makeAutomaton(s);
            } else {
                if (!check(INTERVAL)) throw new IllegalArgumentException("illegal identifier at position " + (pos - 1));
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
                    throw new IllegalArgumentException("interval syntax error at position " + (pos - 1));
                }
            }
        } else return makeChar(parseCharExp());
    }

    final int parseCharExp() throws IllegalArgumentException {
        match('\\');
        return next();
    }
}
