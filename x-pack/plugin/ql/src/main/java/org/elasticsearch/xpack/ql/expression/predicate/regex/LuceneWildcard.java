/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ql.expression.predicate.regex;

/**
 * Answers what the optimizer asks of a Lucene wildcard pattern by reading its text, as {@code WildcardQuery.toAutomaton}
 * reads it, instead of building its automaton: a run of {@code *} is quadratic to build, and the answers do not need it.
 */
final class LuceneWildcard {

    private LuceneWildcard() {}

    /** Only a pattern made of nothing but {@code *} matches every string. */
    static boolean matchesAll(String wildcard) {
        if (wildcard.isEmpty()) {
            return false;
        }
        for (int i = 0; i < wildcard.length(); i++) {
            if (wildcard.charAt(i) != '*') {
                return false;
            }
        }
        return true;
    }

    /** The one string a non-empty pattern without {@code *} or {@code ?} matches, with its escapes removed; else {@code null}. */
    static String exactMatch(String wildcard) {
        // Lucene's automaton for the empty wildcard matches nothing, not the empty string
        if (wildcard.isEmpty()) {
            return null;
        }
        StringBuilder match = new StringBuilder(wildcard.length());
        for (int i = 0; i < wildcard.length(); i++) {
            char c = wildcard.charAt(i);
            if (c == '*' || c == '?') {
                return null;
            }
            // a trailing backslash is a literal one
            if (c == '\\' && i + 1 < wildcard.length()) {
                c = wildcard.charAt(++i);
            }
            match.append(c);
        }
        return match.toString();
    }
}
