/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.fetch.subphase;

import org.apache.lucene.util.automaton.Automata;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.CharacterRunAutomaton;
import org.apache.lucene.util.automaton.Operations;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.search.lookup.Source;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Class to fetch all unmapped fields from a Source that match a set of patterns
 *
 * Takes a set of mapped fields to ignore when matching, which should include
 * any nested mappers.
 */
public class UnmappedFieldFetcher {

    /**
     * Default maximum number of states in the automaton that looks up unmapped fields.
     */
    private static final int AUTOMATON_MAX_DETERMINIZED_STATES = 100000;

    private final CharacterRunAutomaton unmappedFieldsFetchAutomaton;
    private final List<String> unmappedConcreteFields = new ArrayList<>();
    private final Set<String> mappedFields = new HashSet<>();

    public static final UnmappedFieldFetcher EMPTY = new UnmappedFieldFetcher(Set.of(), List.of(), List.of());

    /**
     * Builds an UnmappedFieldFetcher
     * @param mappedFields              a set of fields to ignore when iterating through the map
     * @param unmappedFetchPatterns     a set of patterns to match unmapped fields in the source against
     */
    public UnmappedFieldFetcher(Set<String> mappedFields, List<String> nestedChildren, List<String> unmappedFetchPatterns) {
        List<String> unmappedWildcardPatterns = new ArrayList<>();
        CharacterRunAutomaton nestedChildrenAutomaton = nestedChildrenAutomaton(nestedChildren);
        // We separate the "include_unmapped" field patters with wildcards from the rest in order to use less
        // space in the lookup automaton
        for (String pattern : unmappedFetchPatterns) {
            if (Regex.isSimpleMatchPattern(pattern)) {
                unmappedWildcardPatterns.add(pattern);
            } else {
                // Exclude any concrete field patterns that would match within a nested
                // child - these will be handled by UnmappedFieldFetchers living within
                // the NestedValueFetchers
                if (nestedChildrenAutomaton.run(pattern) == false) {
                    unmappedConcreteFields.add(pattern);
                }
            }
        }
        this.unmappedFieldsFetchAutomaton = buildUnmappedFieldPatternAutomaton(unmappedWildcardPatterns);
        this.mappedFields.addAll(mappedFields);
        this.mappedFields.addAll(nestedChildren);
    }

    // Builds an automaton that will match any field that lives under a nested child mapper
    private static CharacterRunAutomaton nestedChildrenAutomaton(List<String> nestedChildren) {
        List<Automaton> automata = new ArrayList<>();
        for (String child : nestedChildren) {
            automata.add(Operations.concatenate(Automata.makeString(child + "."), Automata.makeAnyString()));
        }
        return new CharacterRunAutomaton(Operations.determinize(Operations.union(automata), AUTOMATON_MAX_DETERMINIZED_STATES));
    }

    // Builds an automaton that will match any field that conforms to one of the input patterns
    private static CharacterRunAutomaton buildUnmappedFieldPatternAutomaton(List<String> patterns) {
        if (patterns.isEmpty()) {
            return null;
        }
        Automaton a = Operations.determinize(
            Regex.simpleMatchToAutomaton(patterns.toArray(String[]::new)),
            AUTOMATON_MAX_DETERMINIZED_STATES
        );
        return new CharacterRunAutomaton(a);
    }

    /**
     * Collect unmapped fields from a Source
     * @param documentFields    a map to receive unmapped field values as DocumentFields
     * @param source            the Source
     */
    public void collectUnmapped(Map<String, DocumentField> documentFields, Source source) {
        if (this.unmappedFieldsFetchAutomaton == null && this.unmappedConcreteFields.isEmpty()) {
            return;
        }
        collectUnmapped(documentFields, source.source(), "", 0);
    }

    private void collectUnmapped(Map<String, DocumentField> documentFields, Map<String, Object> source, String parentPath, int lastState) {
        // lookup field patterns containing wildcards
        if (this.unmappedFieldsFetchAutomaton != null) {
            for (String key : source.keySet()) {
                Object value = source.get(key);
                String currentPath = parentPath + key;
                if (this.mappedFields.contains(currentPath)) {
                    continue;
                }
                int currentState = step(this.unmappedFieldsFetchAutomaton, key, lastState);
                if (currentState == -1) {
                    // current path doesn't match any fields pattern
                    continue;
                }
                if (value instanceof Map) {
                    // one step deeper into source tree
                    @SuppressWarnings("unchecked")
                    Map<String, Object> objectMap = (Map<String, Object>) value;
                    collectUnmapped(
                        documentFields,
                        objectMap,
                        currentPath + ".",
                        step(this.unmappedFieldsFetchAutomaton, ".", currentState)
                    );
                } else if (value instanceof List) {
                    // iterate through list values
                    collectUnmappedList(documentFields, (List<?>) value, currentPath, currentState);
                } else {
                    // we have a leaf value
                    if (this.unmappedFieldsFetchAutomaton.isAccept(currentState)) {
                        if (value != null) {
                            DocumentField currentEntry = documentFields.get(currentPath);
                            if (currentEntry == null) {
                                List<Object> list = new ArrayList<>();
                                list.add(value);
                                documentFields.put(currentPath, new DocumentField(currentPath, list));
                            } else {
                                currentEntry.getValues().add(value);
                            }
                        }
                    }
                }
            }
        }

        // lookup concrete fields
        for (String path : unmappedConcreteFields) {
            if (this.mappedFields.contains(parentPath + path)) {
                continue; // this is actually a mapped field
            }
            List<Object> values = XContentMapValues.extractRawValues(path, source);
            if (values.isEmpty() == false) {
                documentFields.put(path, new DocumentField(path, values));
            }
        }
    }

    private void collectUnmappedList(Map<String, DocumentField> documentFields, Iterable<?> iterable, String parentPath, int lastState) {
        List<Object> list = new ArrayList<>();
        for (Object value : iterable) {
            if (value instanceof Map) {
                @SuppressWarnings("unchecked")
                final Map<String, Object> objectMap = (Map<String, Object>) value;
                collectUnmapped(documentFields, objectMap, parentPath + ".", step(this.unmappedFieldsFetchAutomaton, ".", lastState));
            } else if (value instanceof List) {
                // weird case, but can happen for objects with "enabled" : "false"
                collectUnmappedList(documentFields, (List<?>) value, parentPath, lastState);
            } else if (this.unmappedFieldsFetchAutomaton.isAccept(lastState) && this.mappedFields.contains(parentPath) == false) {
                list.add(value);
            }
        }
        if (list.isEmpty() == false) {
            DocumentField currentEntry = documentFields.get(parentPath);
            if (currentEntry == null) {
                documentFields.put(parentPath, new DocumentField(parentPath, list));
            } else {
                currentEntry.getValues().addAll(list);
            }
        }
    }

    private static int step(CharacterRunAutomaton automaton, String key, int state) {
        for (int i = 0; state != -1 && i < key.length(); ++i) {
            state = automaton.step(state, key.charAt(i));
        }
        return state;
    }

}
