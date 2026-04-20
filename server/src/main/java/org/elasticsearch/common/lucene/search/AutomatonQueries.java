/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.lucene.search;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.AutomatonQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.Automata;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.MinimizationOperations;
import org.apache.lucene.util.automaton.Operations;
import org.apache.lucene.util.automaton.RegExp;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.lucene.util.automaton.CircuitBreakingOperations;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

/**
 * Helper functions for creating various forms of {@link AutomatonQuery}
 */
public class AutomatonQueries {

    /** Build an automaton query accepting all terms with the specified prefix, ASCII case insensitive. */
    public static Automaton caseInsensitivePrefix(String s) {
        List<Automaton> list = new ArrayList<>();
        Iterator<Integer> iter = s.codePoints().iterator();
        while (iter.hasNext()) {
            list.add(toCaseInsensitiveChar(iter.next()));
        }
        list.add(Automata.makeAnyString());

        Automaton a = Operations.concatenate(list);
        // since all elements in the list should be deterministic already, the concatenation also is, so no need to determinized
        assert a.isDeterministic();
        a = MinimizationOperations.minimize(a, 0);
        assert a.isDeterministic();
        return a;
    }

    /** Build an automaton query accepting all terms with the specified prefix, ASCII case insensitive. */
    public static AutomatonQuery caseInsensitivePrefixQuery(Term prefix) {
        return new CaseInsensitivePrefixQuery(prefix);
    }

    /** Build an automaton accepting all terms ASCII case insensitive. */
    public static AutomatonQuery caseInsensitiveTermQuery(Term term) {
        return new CaseInsensitiveTermQuery(term);
    }

    /** Build an automaton matching a wildcard pattern, ASCII case insensitive. */
    public static AutomatonQuery caseInsensitiveWildcardQuery(Term wildcardquery) {
        return new CaseInsensitiveWildcardQuery(wildcardquery);
    }

    /** String equality with support for wildcards */
    public static final char WILDCARD_STRING = '*';

    /** Char equality with support for wildcards */
    public static final char WILDCARD_CHAR = '?';

    /** Escape character */
    public static final char WILDCARD_ESCAPE = '\\';

    /**
     * Convert Lucene wildcard syntax into an automaton.
     */
    public static Automaton toCaseInsensitiveWildcardAutomaton(Term wildcardquery) {
        Automaton nfa = toCaseInsensitiveWildcardNFA(wildcardquery);
        return Operations.determinize(nfa, Operations.DEFAULT_DETERMINIZE_WORK_LIMIT);
    }

    /**
     * Convert Lucene wildcard syntax into an automaton, checking a circuit breaker
     * during determinization to prevent OOM from huge automatons.
     */
    public static Automaton toCaseInsensitiveWildcardAutomaton(Term wildcardquery, CircuitBreaker circuitBreaker) {
        Automaton nfa = toCaseInsensitiveWildcardNFA(wildcardquery);
        return CircuitBreakingOperations.determinize(
            nfa,
            Operations.DEFAULT_DETERMINIZE_WORK_LIMIT,
            circuitBreaker,
            "wildcard-ci:" + wildcardquery.field()
        );
    }

    /**
     * Convert Lucene wildcard syntax into a case-sensitive automaton, checking a circuit breaker
     * during determinization to prevent OOM from huge automatons.
     */
    public static Automaton toWildcardAutomaton(Term wildcardquery, CircuitBreaker circuitBreaker) {
        Automaton nfa = toWildcardNFA(wildcardquery);
        return CircuitBreakingOperations.determinize(
            nfa,
            Operations.DEFAULT_DETERMINIZE_WORK_LIMIT,
            circuitBreaker,
            "wildcard:" + wildcardquery.field()
        );
    }

    /**
     * Build a deterministic automaton from a regular expression, checking a circuit breaker
     * during determinization to prevent OOM from huge automatons.
     */
    public static Automaton toRegexpAutomaton(
        Term term,
        int syntaxFlags,
        int matchFlags,
        int maxDeterminizedStates,
        CircuitBreaker circuitBreaker
    ) {
        Automaton nfa = new RegExp(term.text(), syntaxFlags, matchFlags).toAutomaton();
        return CircuitBreakingOperations.determinize(nfa, maxDeterminizedStates, circuitBreaker, "regexp:" + term.field());
    }

    /**
     * Build the NFA for a case-insensitive wildcard pattern without determinizing.
     */
    @SuppressWarnings("fallthrough")
    static Automaton toCaseInsensitiveWildcardNFA(Term wildcardquery) {
        List<Automaton> automata = new ArrayList<>();

        String wildcardText = wildcardquery.text();

        for (int i = 0; i < wildcardText.length();) {
            final int c = wildcardText.codePointAt(i);
            int length = Character.charCount(c);
            switch (c) {
                case WILDCARD_STRING:
                    automata.add(Automata.makeAnyString());
                    break;
                case WILDCARD_CHAR:
                    automata.add(Automata.makeAnyChar());
                    break;
                case WILDCARD_ESCAPE:
                    // add the next codepoint instead, if it exists
                    if (i + length < wildcardText.length()) {
                        final int nextChar = wildcardText.codePointAt(i + length);
                        length += Character.charCount(nextChar);
                        automata.add(Automata.makeChar(nextChar));
                        break;
                    } // else fallthru, lenient parsing with a trailing \
                default:
                    automata.add(toCaseInsensitiveChar(c));
            }
            i += length;
        }

        return Operations.concatenate(automata);
    }

    /**
     * Build the NFA for a case-sensitive wildcard pattern without determinizing.
     */
    @SuppressWarnings("fallthrough")
    public static Automaton toWildcardNFA(Term wildcardquery) {
        List<Automaton> automata = new ArrayList<>();

        String wildcardText = wildcardquery.text();

        for (int i = 0; i < wildcardText.length();) {
            final int c = wildcardText.codePointAt(i);
            int length = Character.charCount(c);
            switch (c) {
                case WILDCARD_STRING:
                    automata.add(Automata.makeAnyString());
                    break;
                case WILDCARD_CHAR:
                    automata.add(Automata.makeAnyChar());
                    break;
                case WILDCARD_ESCAPE:
                    // add the next codepoint instead, if it exists
                    if (i + length < wildcardText.length()) {
                        final int nextChar = wildcardText.codePointAt(i + length);
                        length += Character.charCount(nextChar);
                        automata.add(Automata.makeChar(nextChar));
                        break;
                    } // else fallthru, lenient parsing with a trailing \
                default:
                    automata.add(Automata.makeChar(c));
            }
            i += length;
        }

        return Operations.concatenate(automata);
    }

    protected static Automaton toCaseInsensitiveString(BytesRef br) {
        return toCaseInsensitiveString(br.utf8ToString());
    }

    public static Automaton toCaseInsensitiveString(String s) {
        List<Automaton> list = new ArrayList<>();
        Iterator<Integer> iter = s.codePoints().iterator();
        while (iter.hasNext()) {
            list.add(toCaseInsensitiveChar(iter.next()));
        }

        Automaton a = Operations.concatenate(list);
        // concatenating deterministic automata should result in a deterministic automaton. No need to determinize here.
        assert a.isDeterministic();
        a = MinimizationOperations.minimize(a, 0);
        return a;
    }

    /**
     * Collapses consecutive repetition operators ({@code +}, {@code *}, {@code ?}) in a Lucene regex
     * pattern down to a single, language-equivalent operator. Stacking quantifiers is always
     * semantically redundant (e.g. {@code x+++} = {@code x+}, {@code x+?} = {@code x*}) and causes
     * exponential NFA state growth in {@link org.apache.lucene.util.automaton.RegExp#toAutomaton()},
     * leading to OOM.
     * <p>
     * The scan respects escape sequences ({@code \+}), character classes ({@code [+*?]}), and
     * Lucene quoted strings ({@code "+++"}) where these characters are literals.
     *
     * @param pattern the raw regex pattern string
     * @return the pattern with redundant consecutive quantifiers collapsed
     * @throws NullPointerException if {@code pattern} is {@code null}
     */
    public static String collapseConsecutiveQuantifiers(String pattern) {
        Objects.requireNonNull(pattern, "pattern must not be null");
        final int length = pattern.length();
        StringBuilder sb = new StringBuilder(length);
        boolean inCharClass = false;
        boolean inQuotedString = false;
        boolean prevWasQuantifier = false;
        for (int i = 0; i < length; i++) {
            char c = pattern.charAt(i);
            if (c == '\\' && i + 1 < length) {
                sb.append(c);
                sb.append(pattern.charAt(i + 1));
                i++;
                prevWasQuantifier = false;
            } else if (inQuotedString) {
                sb.append(c);
                if (c == '"') {
                    inQuotedString = false;
                }
                prevWasQuantifier = false;
            } else if (inCharClass) {
                sb.append(c);
                if (c == ']') {
                    inCharClass = false;
                }
                prevWasQuantifier = false;
            } else if (c == '"') {
                sb.append(c);
                inQuotedString = true;
                prevWasQuantifier = false;
            } else if (c == '[') {
                sb.append(c);
                inCharClass = true;
                prevWasQuantifier = false;
            } else if (c == '+' || c == '*' || c == '?') {
                if (prevWasQuantifier == false) {
                    sb.append(c);
                    prevWasQuantifier = true;
                } else {
                    int previousQuantifierIndex = sb.length() - 1;
                    sb.setCharAt(previousQuantifierIndex, collapseConsecutiveQuantifierPair(sb.charAt(previousQuantifierIndex), c));
                }
            } else {
                sb.append(c);
                prevWasQuantifier = false;
            }
        }
        return sb.toString();
    }

    private static char collapseConsecutiveQuantifierPair(char existing, char incoming) {
        assert isRepetitionOperator(existing) && isRepetitionOperator(incoming)
            : "expected repetition operators but got [" + existing + "] and [" + incoming + "]";
        if (existing == incoming) {
            return existing;
        }
        return '*';
    }

    private static boolean isRepetitionOperator(char c) {
        return c == '+' || c == '*' || c == '?';
    }

    public static Automaton toCaseInsensitiveChar(int codepoint) {
        Automaton case1 = Automata.makeChar(codepoint);
        if (codepoint > 128) {
            return case1;
        }
        int altCase = Character.isLowerCase(codepoint) ? Character.toUpperCase(codepoint) : Character.toLowerCase(codepoint);
        Automaton result;
        if (altCase != codepoint) {
            result = Operations.union(case1, Automata.makeChar(altCase));
            // this automaton should always be deterministic, no need to determinize
            result = MinimizationOperations.minimize(result, 0);
            assert result.isDeterministic();
        } else {
            result = case1;
        }
        return result;
    }
}
