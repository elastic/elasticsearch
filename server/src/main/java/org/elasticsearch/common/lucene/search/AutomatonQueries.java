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
import org.apache.lucene.util.automaton.Operations;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

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
    @SuppressWarnings("fallthrough")
    public static Automaton toCaseInsensitiveWildcardAutomaton(Term wildcardquery) {
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

        return Operations.determinize(Operations.concatenate(automata), Operations.DEFAULT_DETERMINIZE_WORK_LIMIT);
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
        return a;
    }

    public static Automaton toCaseInsensitiveChar(int codepoint) {
        Automaton case1 = Automata.makeChar(codepoint);
        // For now we only work with ASCII characters
        if (codepoint > 128) {
            return case1;
        }
        int altCase = Character.isLowerCase(codepoint) ? Character.toUpperCase(codepoint) : Character.toLowerCase(codepoint);
        Automaton result;
        if (altCase != codepoint) {
            result = Operations.union(case1, Automata.makeChar(altCase));
            // this automaton should always be deterministic, no need to determinize
            assert result.isDeterministic();
        } else {
            result = case1;
        }
        return result;
    }
}
