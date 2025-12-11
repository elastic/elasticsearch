/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical.promql;

import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.UnicodeUtil;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.Operations;

import java.util.ArrayList;
import java.util.List;

/**
 * Utility class for analyzing Lucene Automaton patterns to extract optimization opportunities.
 *
 * This class provides methods to detect common patterns in regex automatons that can be
 * optimized into simpler query predicates like equality, prefix matching, or IN clauses.
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
     * @return the single matched string, or null if it matches zero or multiple strings
     */
    public static String matchesExact(Automaton automaton) {
        if (automaton.getNumStates() == 0) {
            return null; // Empty automaton
        }
        IntsRef singleton = Operations.getSingleton(automaton);
        if (singleton == null) {
            return null;
        }
        return UnicodeUtil.newString(singleton.ints, singleton.offset, singleton.length);
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

    private static String removeStartingAnchor(String normalized) {
        return normalized.startsWith("^") ? normalized.substring(1) : normalized;
    }

    private static String removeEndingAnchor(String normalized) {
        return normalized.endsWith("$") ? normalized.substring(0, normalized.length() - 1) : normalized;
    }

    /**
     * Represents a pattern fragment that can be optimized.
     */
    public static class PatternFragment {
        public enum Type {
            EXACT,         // Exact literal match
            PREFIX,        // Starts with literal
            PROPER_PREFIX, // Starts with literal but not the literal itself
            SUFFIX,        // Ends with literal
            PROPER_SUFFIX, // Ends with literal but not the literal itself
            REGEX          // Complex regex pattern
        }

        private final Type type;
        private final String value;

        public PatternFragment(Type type, String value) {
            this.type = type;
            this.value = value;
        }

        public Type type() {
            return type;
        }

        public String value() {
            return value;
        }
    }

    /**
     * Analyzes a single alternation part and classifies it into a pattern fragment.
     *
     * @param part the alternation part to analyze
     * @return PatternFragment with appropriate type (EXACT, PREFIX, SUFFIX, or REGEX)
     */
    private static PatternFragment classifyPart(String part) {
        String trimmed = removeStartingAnchor(removeEndingAnchor(part.trim()));

        // Empty pattern
        if (trimmed.isEmpty()) {
            return new PatternFragment(PatternFragment.Type.EXACT, "");
        }

        // Check for contains pattern: .*substring.*
        boolean startsWithWildcard = trimmed.startsWith(".*") || trimmed.startsWith(".+");
        boolean endsWithWildcard = trimmed.endsWith(".*") || trimmed.endsWith(".+");

        if (startsWithWildcard && endsWithWildcard) {
            // Contains pattern - fallback to REGEX
            return new PatternFragment(PatternFragment.Type.REGEX, part.trim());
        }

        if (startsWithWildcard) {
            // Suffix pattern: .*suffix
            String suffix = trimmed.substring(2);
            if (isLiteral(suffix)) {
                return new PatternFragment(
                    trimmed.startsWith(".*") ? PatternFragment.Type.SUFFIX : PatternFragment.Type.PROPER_SUFFIX,
                    suffix
                );
            }
            // Complex suffix pattern - fallback to REGEX
            return new PatternFragment(PatternFragment.Type.REGEX, part.trim());
        }

        if (endsWithWildcard) {
            // Prefix pattern: prefix.*
            String prefix = trimmed.substring(0, trimmed.length() - 2);
            if (isLiteral(prefix)) {
                return new PatternFragment(
                    trimmed.endsWith(".*") ? PatternFragment.Type.PREFIX : PatternFragment.Type.PROPER_PREFIX,
                    prefix
                );
            }
            // Complex prefix pattern - fallback to REGEX
            return new PatternFragment(PatternFragment.Type.REGEX, part.trim());
        }

        // Exact literal match
        if (isLiteral(trimmed)) {
            return new PatternFragment(PatternFragment.Type.EXACT, trimmed);
        }

        // Complex pattern - fallback to REGEX
        return new PatternFragment(PatternFragment.Type.REGEX, part.trim());
    }

    /**
     * Extracts potentially mixed disjoint pattern within the given regex.
     * Handles disjunctions if specified with | operator.
     *
     * This handles mixed patterns like:
     * - prod-.*|staging|.*-dev|.*test.*  - mix of prefix, exact, suffix, contains
     * - http://.*|https://.*|ftp          - mix of prefixes and exact
     * - *.txt|*.csv|readme                - mix of suffixes and exact
     *
     * Each part is classified and can be optimized independently, then combined with OR.
     *
     * @param pattern the regex pattern string
     * @return list of pattern fragments, or null if any part cannot be optimized
     */
    public static List<PatternFragment> extractFragments(String pattern) {
        if (pattern == null || pattern.length() > MAX_PATTERN_LENGTH) {
            return null;
        }

        String normalized = pattern;

        // Remove anchors
        normalized = removeStartingAnchor(normalized);
        normalized = removeEndingAnchor(normalized);

        // Check for alternation pattern

        // Avoid nested groups
        if (normalized.contains("(") || normalized.contains(")")) {
            return null;
        }

        // Split by | (watch for escaped pipes)
        // this string is NOT a regex pattern and thus looks for \|
        if (normalized.contains("\\|")) {
            return null; // Contains escaped pipe, too complex
        }

        // same string IS a regex pattern and thus search for |
        // which gets optimized internally by the JVM
        String[] parts = normalized.split("\\|");
        if (parts.length > MAX_IN_VALUES) {
            return null; // too many parts
        }

        // Classify each part
        List<PatternFragment> fragments = new ArrayList<>(parts.length);
        for (String part : parts) {
            PatternFragment fragment = classifyPart(part);
            fragments.add(fragment);
        }

        return fragments;
    }
}
