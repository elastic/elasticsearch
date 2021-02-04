/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.fetch.subphase;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.automaton.Automata;
import org.apache.lucene.util.automaton.CharacterRunAutomaton;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.ValueFetcher;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.search.lookup.SourceLookup;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A helper class to {@link FetchFieldsPhase} that's initialized with a list of field patterns to fetch.
 * Then given a specific document, it can retrieve the corresponding fields from the document's source.
 */
public class FieldFetcher {
    public static FieldFetcher create(SearchExecutionContext context,
                                      SearchLookup searchLookup,
                                      Collection<FieldAndFormat> fieldAndFormats) {

        // Using a LinkedHashMap so fields are returned in the order requested.
        // We won't formally guarantee this but but its good for readability of the response
        Map<String, FieldContext> fieldContexts = new LinkedHashMap<>();
        List<String> unmappedFetchPattern = new ArrayList<>();
        boolean includeUnmapped = false;

        for (FieldAndFormat fieldAndFormat : fieldAndFormats) {
            String fieldPattern = fieldAndFormat.field;
            if (fieldAndFormat.includeUnmapped != null && fieldAndFormat.includeUnmapped) {
                unmappedFetchPattern.add(fieldAndFormat.field);
                includeUnmapped = true;
            }
            String format = fieldAndFormat.format;

            Collection<String> concreteFields = context.simpleMatchToIndexNames(fieldPattern);
            for (String field : concreteFields) {
                MappedFieldType ft = context.getFieldType(field);
                if (ft == null || context.isMetadataField(field)) {
                    continue;
                }
                ValueFetcher valueFetcher = ft.valueFetcher(context, format);
                fieldContexts.put(field, new FieldContext(field, valueFetcher));
            }
        }
        CharacterRunAutomaton unmappedFetchAutomaton = new CharacterRunAutomaton(Automata.makeEmpty());
        if (unmappedFetchPattern.isEmpty() == false) {
            unmappedFetchAutomaton = new CharacterRunAutomaton(
                Regex.simpleMatchToAutomaton(unmappedFetchPattern.toArray(new String[unmappedFetchPattern.size()]))
            );
        }
        return new FieldFetcher(fieldContexts, unmappedFetchAutomaton, includeUnmapped);
    }

    private final Map<String, FieldContext> fieldContexts;
    private final CharacterRunAutomaton unmappedFetchAutomaton;
    private final boolean includeUnmapped;

    private FieldFetcher(
        Map<String, FieldContext> fieldContexts,
        CharacterRunAutomaton unmappedFetchAutomaton,
        boolean includeUnmapped
    ) {
        this.fieldContexts = fieldContexts;
        this.unmappedFetchAutomaton = unmappedFetchAutomaton;
        this.includeUnmapped = includeUnmapped;
    }

    public Map<String, DocumentField> fetch(SourceLookup sourceLookup, Set<String> ignoredFields) throws IOException {
        Map<String, DocumentField> documentFields = new HashMap<>();
        for (FieldContext context : fieldContexts.values()) {
            String field = context.fieldName;
            if (ignoredFields.contains(field)) {
                continue;
            }

            ValueFetcher valueFetcher = context.valueFetcher;
            List<Object> parsedValues = valueFetcher.fetchValues(sourceLookup);

            if (parsedValues.isEmpty() == false) {
                documentFields.put(field, new DocumentField(field, parsedValues));
            }
        }
        if (this.includeUnmapped) {
            collectUnmapped(documentFields, sourceLookup.source(), "", 0);
        }
        return documentFields;
    }

    private void collectUnmapped(Map<String, DocumentField> documentFields, Map<String, Object> source, String parentPath, int lastState) {
        for (String key : source.keySet()) {
            Object value = source.get(key);
            String currentPath = parentPath + key;
            int currentState = step(this.unmappedFetchAutomaton, key, lastState);
            if (currentState == -1) {
                // current path doesn't match any fields pattern
                continue;
            }
            if (value instanceof Map) {
                // one step deeper into source tree
                collectUnmapped(
                    documentFields,
                    (Map<String, Object>) value,
                    currentPath + ".",
                    step(this.unmappedFetchAutomaton, ".", currentState)
                );
            } else if (value instanceof List) {
                // iterate through list values
                collectUnmappedList(documentFields, (List<?>) value, currentPath, currentState);
            } else {
                // we have a leaf value
                if (this.unmappedFetchAutomaton.isAccept(currentState) && this.fieldContexts.containsKey(currentPath) == false) {
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

    private void collectUnmappedList(Map<String, DocumentField> documentFields, Iterable<?> iterable, String parentPath, int lastState) {
        List<Object> list = new ArrayList<>();
        for (Object value : iterable) {
            if (value instanceof Map) {
                collectUnmapped(
                    documentFields,
                    (Map<String, Object>) value,
                    parentPath + ".",
                    step(this.unmappedFetchAutomaton, ".", lastState)
                );
            } else if (value instanceof List) {
                // weird case, but can happen for objects with "enabled" : "false"
                collectUnmappedList(documentFields, (List<?>) value, parentPath, lastState);
            } else if (this.unmappedFetchAutomaton.isAccept(lastState) && this.fieldContexts.containsKey(parentPath) == false) {
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

    public void setNextReader(LeafReaderContext readerContext) {
        for (FieldContext field : fieldContexts.values()) {
            field.valueFetcher.setNextReader(readerContext);
        }
    }

    private static class FieldContext {
        final String fieldName;
        final ValueFetcher valueFetcher;

        FieldContext(String fieldName,
                     ValueFetcher valueFetcher) {
            this.fieldName = fieldName;
            this.valueFetcher = valueFetcher;
        }
    }
}
