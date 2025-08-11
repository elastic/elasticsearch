/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.fetch.subphase;

import org.apache.lucene.util.automaton.CharacterRunAutomaton;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.search.lookup.SourceLookup;

import java.util.ArrayList;
import java.util.Collections;
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
    private final Set<String> mappedFields;

    public static final UnmappedFieldFetcher EMPTY = new UnmappedFieldFetcher(Collections.emptySet(), Collections.emptyList());

    /**
     * Builds an UnmappedFieldFetcher
     * @param mappedFields              a set of fields to ignore when iterating through the map
     * @param unmappedFetchPatterns     a set of patterns to match unmapped fields in the source against
     */
    public UnmappedFieldFetcher(Set<String> mappedFields, List<String> unmappedFetchPatterns) {
        List<String> unmappedWildcardPatterns = new ArrayList<>();
        // We separate the "include_unmapped" field patters with wildcards from the rest in order to use less
        // space in the lookup automaton
        for (String pattern : unmappedFetchPatterns) {
            if (Regex.isSimpleMatchPattern(pattern)) {
                unmappedWildcardPatterns.add(pattern);
            } else {
                unmappedConcreteFields.add(pattern);
            }
        }
        this.unmappedFieldsFetchAutomaton = buildAutomaton(unmappedWildcardPatterns);
        this.mappedFields = mappedFields;
    }

    private static CharacterRunAutomaton buildAutomaton(List<String> patterns) {
        if (patterns.isEmpty()) {
            return null;
        }
        return new CharacterRunAutomaton(Regex.simpleMatchToAutomaton(patterns.toArray(new String[0])), AUTOMATON_MAX_DETERMINIZED_STATES);
    }

    /**
     * Collect unmapped fields from a Source
     * @param documentFields    a map to receive unmapped field values as DocumentFields
     * @param source            the Source
     */
    public void collectUnmapped(Map<String, DocumentField> documentFields, SourceLookup source) {
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
        if (this.unmappedConcreteFields != null) {
            for (String path : unmappedConcreteFields) {
                if (this.mappedFields.contains(path)) {
                    continue; // this is actually a mapped field
                }
                List<Object> values = XContentMapValues.extractRawValues(path, source);
                if (values.isEmpty() == false) {
                    documentFields.put(path, new DocumentField(path, values));
                }
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
