/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.xcontent.support;

import org.apache.lucene.util.automaton.Automata;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.CharacterRunAutomaton;
import org.apache.lucene.util.automaton.Operations;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class XContentParserFilter {

    private static final int MAX_DETERMINIZED_STATES = 50_000;

    public static Map<String, Object> filter(XContentParser parser, String[] includes) {
        return filter(includes).apply(parser);
    }

    /**
     * Returns a function that filters a document map based on the given include and exclude rules.
     * @see #filter(XContentParser, String[]) for details
     */
    public static Function<XContentParser, Map<String, Object>> filter(String[] includes) {
        CharacterRunAutomaton matchAllAutomaton = new CharacterRunAutomaton(Automata.makeAnyString());
        CharacterRunAutomaton include = compileAutomaton(includes, matchAllAutomaton);

        // NOTE: We cannot use Operations.minus because of the special case that
        // we want all sub properties to match as soon as an object matches

        return (parser) -> {
            XContentParser.Token startObjectToken;
            try {
                startObjectToken = parser.nextToken();
                assert startObjectToken == XContentParser.Token.START_OBJECT;
                return filter(parser, include, 0, matchAllAutomaton);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        };
    }

    public static CharacterRunAutomaton compileAutomaton(String[] patterns, CharacterRunAutomaton defaultValue) {
        if (patterns == null || patterns.length == 0) {
            return defaultValue;
        }
        var aut = Regex.simpleMatchToAutomaton(patterns);
        aut = Operations.determinize(makeMatchDotsInFieldNames(aut), MAX_DETERMINIZED_STATES);
        return new CharacterRunAutomaton(aut);
    }

    /** Make matches on objects also match dots in field names.
     *  For instance, if the original simple regex is `foo`, this will translate
     *  it into `foo` OR `foo.*`. */
    private static Automaton makeMatchDotsInFieldNames(Automaton automaton) {
        /*
         * We presume `automaton` is quite large compared to the mechanisms
         * to match the trailing `.*` bits so we duplicate it only once.
         */
        Automaton tail = Operations.union(
            Automata.makeEmptyString(),
            Operations.concatenate(Automata.makeChar('.'), Automata.makeAnyString())
        );
        return Operations.concatenate(automaton, tail);
    }

    private static int step(CharacterRunAutomaton automaton, String key, int state) {
        for (int i = 0; state != -1 && i < key.length(); ++i) {
            state = automaton.step(state, key.charAt(i));
        }
        return state;
    }

    private static Map<String, Object> filter(
        XContentParser parser,
        CharacterRunAutomaton includeAutomaton,
        int initialIncludeState,
        CharacterRunAutomaton matchAllAutomaton
    ) throws IOException {
        Map<String, Object> filtered = new HashMap<>();
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {

            assert token == XContentParser.Token.FIELD_NAME;
            String key = parser.currentName();
            // Now value token
            token = parser.nextToken();
            int includeState = step(includeAutomaton, key, initialIncludeState);
            if (includeState == -1) {
                parser.skipChildren();
                continue;
            }

            CharacterRunAutomaton subIncludeAutomaton = includeAutomaton;
            int subIncludeState = includeState;
            // if (includeAutomaton.isAccept(includeState)) {
            // while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            // assert token == XContentParser.Token.FIELD_NAME;
            // String currentName = parser.currentName();
            // parser.nextToken();
            // filtered.put(currentName, parser.objectBytes());
            // }
            // continue;
            // }

            if (token == XContentParser.Token.START_OBJECT) {
                subIncludeState = subIncludeAutomaton.step(subIncludeState, '.');
                if (subIncludeState == -1) {
                    parser.skipChildren();
                    continue;
                }
                Map<String, Object> filteredValue = filter(parser, subIncludeAutomaton, subIncludeState, matchAllAutomaton);
                if (includeAutomaton.isAccept(includeState) || filteredValue.isEmpty() == false) {
                    filtered.put(key, filteredValue);
                }

            } else if (token == XContentParser.Token.START_ARRAY) {
                List<Object> filteredValue = filterArray(parser, subIncludeAutomaton, subIncludeState, matchAllAutomaton);
                if (includeAutomaton.isAccept(includeState) || filteredValue.isEmpty() == false) {
                    filtered.put(key, filteredValue);
                }

            } else {
                // leaf property
                if (includeAutomaton.isAccept(includeState)) {
                    filtered.put(key, parser.objectText());
                }
            }
        }
        return filtered;
    }

    private static List<Object> filterArray(
        XContentParser parser,
        CharacterRunAutomaton includeAutomaton,
        int initialIncludeState,
        CharacterRunAutomaton matchAllAutomaton
    ) throws IOException {
        List<Object> filtered = new ArrayList<>();
        boolean isInclude = includeAutomaton.isAccept(initialIncludeState);
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
            if (token == XContentParser.Token.START_OBJECT) {
                int includeState = includeAutomaton.step(initialIncludeState, '.');
                @SuppressWarnings("unchecked")
                Map<String, Object> filteredValue = filter(parser, includeAutomaton, includeState, matchAllAutomaton);
                if (filteredValue.isEmpty() == false) {
                    filtered.add(filteredValue);
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                List<Object> filteredValue = filterArray(parser, includeAutomaton, initialIncludeState, matchAllAutomaton);
                if (filteredValue.isEmpty() == false) {
                    filtered.add(filteredValue);
                }
            } else if (isInclude) {
                // #22557: only accept this array value if the key we are on is accepted:
                filtered.add(parser.objectBytes());
            }
        }
        return filtered;
    }
}
