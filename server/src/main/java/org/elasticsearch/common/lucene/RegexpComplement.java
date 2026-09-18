/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.lucene;

import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.Operations;
import org.apache.lucene.util.automaton.RegExp;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Compiles a Lucene regexp string, restoring {@code ~(X)} as language complement.
 * <p>
 * Lucene 11 dropped {@code RegExp.DEPRECATED_COMPLEMENT}, so {@code ~} is a literal tilde.
 * Elasticsearch still documents and ships {@code ~(X)} for security {@code /regex/} index
 * privileges and system-index patterns. Named-automaton placeholders ({@code <id>}) are
 * substituted for each complement so the rest of the pattern can go through Lucene's parser.
 * <p>
 * TODO: LUCENE11 stop-gap for #113465. Drop this class once reserved roles, {@code _xpack},
 * and system-index patterns no longer use {@code ~}, and the public {@code /regex/} / regexp
 * docs no longer promise complement. Search {@code regexp} / query_string still parse {@code ~}
 * as a literal.
 *
 * @see <a href="https://github.com/elastic/elasticsearch/issues/113465">#113465</a>
 */
public final class RegexpComplement {

    private RegexpComplement() {}

    /**
     * @param regex Lucene regexp <em>without</em> the surrounding {@code /…/} of a security pattern
     * @param determinizeWorkLimit work limit passed to {@link Operations#complement(Automaton, int)}
     */
    public static Automaton toAutomaton(String regex, int determinizeWorkLimit) {
        Rewrite rewrite = rewriteComplements(regex, determinizeWorkLimit);
        if (rewrite.automata.isEmpty()) {
            return new RegExp(rewrite.pattern, RegExp.ALL).toAutomaton();
        }
        return new RegExp(rewrite.pattern, RegExp.ALL | RegExp.AUTOMATON).toAutomaton(rewrite.automata);
    }

    private static Rewrite rewriteComplements(String regex, int determinizeWorkLimit) {
        if (regex.indexOf('~') < 0) {
            return new Rewrite(regex, Map.of());
        }
        StringBuilder out = new StringBuilder(regex.length());
        Map<String, Automaton> automata = new LinkedHashMap<>();
        boolean inClass = false;
        for (int i = 0; i < regex.length(); i++) {
            char c = regex.charAt(i);
            if (c == '\\') {
                out.append(c);
                if (i + 1 < regex.length()) {
                    out.append(regex.charAt(++i));
                }
                continue;
            }
            if (inClass) {
                out.append(c);
                if (c == ']') {
                    inClass = false;
                }
                continue;
            }
            if (c == '[') {
                inClass = true;
                out.append(c);
                continue;
            }
            if (c == '~' && i + 1 < regex.length() && regex.charAt(i + 1) == '(') {
                int close = findMatchingParen(regex, i + 1);
                String inner = regex.substring(i + 2, close);
                Automaton innerAutomaton = toAutomaton(inner, determinizeWorkLimit);
                String id = "c" + automata.size();
                automata.put(id, Operations.complement(innerAutomaton, determinizeWorkLimit));
                out.append('<').append(id).append('>');
                i = close;
                continue;
            }
            out.append(c);
        }
        return new Rewrite(out.toString(), automata);
    }

    private static int findMatchingParen(String regex, int openIdx) {
        int depth = 1;
        boolean inClass = false;
        for (int i = openIdx + 1; i < regex.length(); i++) {
            char c = regex.charAt(i);
            if (c == '\\') {
                i++;
                continue;
            }
            if (inClass) {
                if (c == ']') {
                    inClass = false;
                }
                continue;
            }
            if (c == '[') {
                inClass = true;
            } else if (c == '(') {
                depth++;
            } else if (c == ')') {
                depth--;
                if (depth == 0) {
                    return i;
                }
            }
        }
        throw new IllegalArgumentException("unbalanced complement group in regexp [" + regex + "]");
    }

    private record Rewrite(String pattern, Map<String, Automaton> automata) {}
}
