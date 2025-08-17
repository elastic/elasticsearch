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
import org.elasticsearch.ingest.ESONIndexed;
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

    public static Map<String, Object> filter(Map<String, Object> map, String[] includes, String[] excludes) {
        return filter(includes, excludes).apply(map);
    }

    /**
     * Returns a function that filters a document map based on the given include and exclude rules.
     * @see #filter(Map, String[], String[]) for details
     */
    public static Function<Map<String, Object>, Map<String, Object>> filter(String[] includes, String[] excludes) {
        CharacterRunAutomaton matchAllAutomaton = new CharacterRunAutomaton(Automata.makeAnyString());
        CharacterRunAutomaton include = compileAutomaton(includes, matchAllAutomaton);
        CharacterRunAutomaton exclude = compileAutomaton(excludes, new CharacterRunAutomaton(Automata.makeEmpty()));

        // NOTE: We cannot use Operations.minus because of the special case that
        // we want all sub properties to match as soon as an object matches

        return (map) -> filter((XContentParser) null, include, 0, exclude, 0, matchAllAutomaton);
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
        CharacterRunAutomaton excludeAutomaton,
        int initialExcludeState,
        CharacterRunAutomaton matchAllAutomaton
    ) {
        Map<String, Object> filtered = new HashMap<>();
        try {
            XContentParser.Token startObjectToken = parser.nextToken();
            assert startObjectToken == XContentParser.Token.START_OBJECT;
            XContentParser.Token token = parser.nextToken();
            while (true) {
                if (token == XContentParser.Token.END_OBJECT) {
                    return filtered;
                }

                token = parser.nextToken();
                assert token == XContentParser.Token.FIELD_NAME;
                String key = parser.currentName();
                token = parser.nextToken();
                int includeState = step(includeAutomaton, key, initialIncludeState);
                if (includeState == -1) {
                    parser.skipChildren();
                    continue;
                }

                break;
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        ESONIndexed.ESONObject map = null;
        for (Map.Entry<String, ?> entry : map.entrySet()) {
            String key = entry.getKey();

            int includeState = step(includeAutomaton, key, initialIncludeState);
            if (includeState == -1) {
                continue;
            }

            int excludeState = step(excludeAutomaton, key, initialExcludeState);
            if (excludeState != -1 && excludeAutomaton.isAccept(excludeState)) {
                continue;
            }

            Object value;
            if (entry instanceof ESONIndexed.ESONObject.LazyEntry lazyEntry && lazyEntry.isUTF8Bytes()) {
                value = lazyEntry.utf8Bytes();
            } else {
                value = entry.getValue();
            }

            CharacterRunAutomaton subIncludeAutomaton = includeAutomaton;
            int subIncludeState = includeState;
            if (includeAutomaton.isAccept(includeState)) {
                if (excludeState == -1 || excludeAutomaton.step(excludeState, '.') == -1) {
                    // the exclude has no chances to match inner properties
                    filtered.put(key, value);
                    continue;
                } else {
                    // the object matched, so consider that the include matches every inner property
                    // we only care about excludes now
                    subIncludeAutomaton = matchAllAutomaton;
                    subIncludeState = 0;
                }
            }

            if (value instanceof Map) {

                subIncludeState = subIncludeAutomaton.step(subIncludeState, '.');
                if (subIncludeState == -1) {
                    continue;
                }
                if (excludeState != -1) {
                    excludeState = excludeAutomaton.step(excludeState, '.');
                }

                @SuppressWarnings("unchecked")
                Map<String, Object> valueAsMap = (Map<String, Object>) value;
                Map<String, Object> filteredValue = filter(
                    (XContentParser) null,
//                    valueAsMap,
                    subIncludeAutomaton,
                    subIncludeState,
                    excludeAutomaton,
                    excludeState,
                    matchAllAutomaton
                );
                if (includeAutomaton.isAccept(includeState) || filteredValue.isEmpty() == false) {
                    filtered.put(key, filteredValue);
                }

            } else if (value instanceof Iterable) {

                List<Object> filteredValue = filter(
                    (Iterable<?>) value,
                    subIncludeAutomaton,
                    subIncludeState,
                    excludeAutomaton,
                    excludeState,
                    matchAllAutomaton
                );
                if (includeAutomaton.isAccept(includeState) || filteredValue.isEmpty() == false) {
                    filtered.put(key, filteredValue);
                }

            } else {

                // leaf property
                if (includeAutomaton.isAccept(includeState) && (excludeState == -1 || excludeAutomaton.isAccept(excludeState) == false)) {
                    filtered.put(key, value);
                }

            }

        }
        return filtered;
    }

    private static List<Object> filter(
        Iterable<?> iterable,
        CharacterRunAutomaton includeAutomaton,
        int initialIncludeState,
        CharacterRunAutomaton excludeAutomaton,
        int initialExcludeState,
        CharacterRunAutomaton matchAllAutomaton
    ) {
        List<Object> filtered = new ArrayList<>();
        boolean isInclude = includeAutomaton.isAccept(initialIncludeState);
        for (Object value : iterable) {
            if (value instanceof Map) {
                int includeState = includeAutomaton.step(initialIncludeState, '.');
                int excludeState = initialExcludeState;
                if (excludeState != -1) {
                    excludeState = excludeAutomaton.step(excludeState, '.');
                }
                @SuppressWarnings("unchecked")
                Map<String, Object> filteredValue = filter(
                    (XContentParser) null,
//                    (Map<String, ?>) value,
                    includeAutomaton,
                    includeState,
                    excludeAutomaton,
                    excludeState,
                    matchAllAutomaton
                );
                if (filteredValue.isEmpty() == false) {
                    filtered.add(filteredValue);
                }
            } else if (value instanceof Iterable) {
                List<Object> filteredValue = filter(
                    (Iterable<?>) value,
                    includeAutomaton,
                    initialIncludeState,
                    excludeAutomaton,
                    initialExcludeState,
                    matchAllAutomaton
                );
                if (filteredValue.isEmpty() == false) {
                    filtered.add(filteredValue);
                }
            } else if (isInclude) {
                // #22557: only accept this array value if the key we are on is accepted:
                filtered.add(value);
            }
        }
        return filtered;
    }
}
