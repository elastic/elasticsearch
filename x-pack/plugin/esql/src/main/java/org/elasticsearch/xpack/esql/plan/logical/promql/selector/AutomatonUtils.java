/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.promql.selector;

import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.Operations;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.AbstractStringPattern;

import java.util.ArrayList;
import java.util.List;

/**
 * Utility class for analyzing Lucene Automaton patterns to extract optimization opportunities.
 *
 * This class provides methods to detect common patterns in regex automatons that can be
 * optimized into simpler query predicates like equality or IN clauses.
 */
public final class AutomatonUtils {

    /**
     * Maximum number of values to extract for IN clause optimization.
     * Beyond this threshold, fall back to regex matching.
     */
    private static final int MAX_IN_VALUES = 256;

    /**
     * Maximum pattern length for string analysis.
     * Very long patterns are kept as-is to avoid excessive processing.
     */
    private static final int MAX_PATTERN_LENGTH = 1000;

    private AutomatonUtils() {
        // Utility class
    }

    /**
     * Checks if the automaton matches all possible strings.
     *
     * @param automaton the automaton to check
     * @return true if it matches everything
     */
    public static boolean matchesAll(Automaton automaton) {
        return Operations.isTotal(automaton);
    }

    /**
     * Checks if the automaton matches no strings.
     *
     * @param automaton the automaton to check
     * @return true if it matches nothing
     */
    public static boolean matchesNone(Automaton automaton) {
        return Operations.isEmpty(automaton);
    }

    /**
     * Checks if the automaton matches the empty string.
     *
     * @param automaton the automaton to check
     * @return true if it matches the empty string
     */
    public static boolean matchesEmpty(Automaton automaton) {
        return Operations.run(automaton, "");
    }

    /**
     * Extracts an exact match if the automaton matches only a single string.
     *
     * @param automaton the automaton to analyze
     * @return the single matched string, or null if it matches zero or multiple strings, or one no keyword can hold
     * @see AbstractStringPattern#exactMatch(Automaton)
     */
    public static String matchesExact(Automaton automaton) {
        return AbstractStringPattern.exactMatch(automaton);
    }

    /**
     * Checks if a string is a literal (no regex metacharacters).
     *
     * @param s the string to check
     * @return true if the string contains no regex metacharacters
     */
    private static boolean isLiteral(String s) {
        if (s == null || s.isEmpty()) {
            return true;
        }

        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '.':
                case '*':
                case '+':
                case '?':
                case '[':
                case ']':
                case '{':
                case '}':
                case '(':
                case ')':
                case '|':
                case '^':
                case '$':
                case '\\':
                    return false;
            }
        }
        return true;
    }

    /**
     * The alternatives of a regex that is a list of literals, such as {@code api|web|}, which match as a set, or null if
     * any alternative has a metacharacter. Prometheus splits a regex only in this case and reads any other through its
     * parse tree, so the split never needs to know where a class or a quote ends; such a regex is matched as a whole.
     * An empty alternative matches the empty value, so it is kept.
     */
    public static List<String> literalAlternatives(String pattern) {
        if (pattern == null || pattern.length() > MAX_PATTERN_LENGTH) {
            return null;
        }
        List<String> values = new ArrayList<>();
        int start = 0;
        for (int end = pattern.indexOf('|'); end >= 0; end = pattern.indexOf('|', start)) {
            values.add(pattern.substring(start, end));
            start = end + 1;
        }
        values.add(pattern.substring(start));
        if (values.size() > MAX_IN_VALUES) {
            return null;
        }
        for (String value : values) {
            if (isLiteral(value) == false) {
                return null;
            }
        }
        return values;
    }
}
