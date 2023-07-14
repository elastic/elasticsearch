/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.fetch.subphase;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.automaton.CharacterRunAutomaton;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NestedValueFetcher;
import org.elasticsearch.index.mapper.ValueFetcher;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.NestedUtils;
import org.elasticsearch.search.fetch.StoredFieldsSpec;
import org.elasticsearch.search.lookup.Source;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * A helper class to {@link FetchFieldsPhase} that's initialized with a list of field patterns to fetch.
 * Then given a specific document, it can retrieve the corresponding fields through their corresponding {@link ValueFetcher}s.
 */
public class FieldFetcher {

    /**
     * Default maximum number of states in the automaton that looks up unmapped fields.
     */
    private static final int AUTOMATON_MAX_DETERMINIZED_STATES = 100000;

    private record ResolvedField(String field, String matchingPattern, MappedFieldType ft, String format) {}

    public static FieldFetcher create(SearchExecutionContext context, Collection<FieldAndFormat> fieldAndFormats) {

        List<String> unmappedFetchPattern = new ArrayList<>();
        List<ResolvedField> resolvedFields = new ArrayList<>();

        for (FieldAndFormat fieldAndFormat : fieldAndFormats) {
            String fieldPattern = fieldAndFormat.field;
            String matchingPattern = Regex.isSimpleMatchPattern(fieldPattern) ? fieldPattern : null;
            if (fieldAndFormat.includeUnmapped != null && fieldAndFormat.includeUnmapped) {
                unmappedFetchPattern.add(fieldAndFormat.field);
            }

            for (String field : context.getMatchingFieldNames(fieldPattern)) {
                MappedFieldType ft = context.getFieldType(field);
                // we want to skip metadata fields if we have a wildcard pattern
                if (context.isMetadataField(field) && matchingPattern != null) {
                    continue;
                }
                resolvedFields.add(new ResolvedField(field, matchingPattern, ft, fieldAndFormat.format));
            }
        }

        // Using a LinkedHashMap so fields are returned in the order requested.
        // We won't formally guarantee this, but but it's good for readability of the response
        Map<String, FieldContext> fieldContexts = new LinkedHashMap<>(buildFieldContexts(context, "", resolvedFields));

        CharacterRunAutomaton unmappedFieldsFetchAutomaton = null;
        // We separate the "include_unmapped" field patters with wildcards from the rest in order to use less
        // space in the lookup automaton
        Map<Boolean, List<String>> partitions = unmappedFetchPattern.stream()
            .collect(Collectors.partitioningBy((Regex::isSimpleMatchPattern)));
        List<String> unmappedWildcardPattern = partitions.get(true);
        List<String> unmappedConcreteFields = partitions.get(false);
        if (unmappedWildcardPattern.isEmpty() == false) {
            unmappedFieldsFetchAutomaton = new CharacterRunAutomaton(
                Regex.simpleMatchToAutomaton(unmappedWildcardPattern.toArray(String[]::new)),
                AUTOMATON_MAX_DETERMINIZED_STATES
            );
        }
        return new FieldFetcher(fieldContexts, unmappedFieldsFetchAutomaton, unmappedConcreteFields);
    }

    private static ValueFetcher buildValueFetcher(SearchExecutionContext context, ResolvedField fieldAndFormat) {
        try {
            return fieldAndFormat.ft.valueFetcher(context, fieldAndFormat.format);
        } catch (IllegalArgumentException e) {
            StringBuilder error = new StringBuilder("error fetching [").append(fieldAndFormat.field).append(']');
            if (fieldAndFormat.matchingPattern != null) {
                error.append(" which matched [").append(fieldAndFormat.matchingPattern).append(']');
            }
            error.append(": ").append(e.getMessage());
            throw new IllegalArgumentException(error.toString(), e);
        }
    }

    private static Map<String, FieldContext> buildFieldContexts(
        SearchExecutionContext context,
        String nestedScope,
        List<ResolvedField> fields
    ) {

        List<String> nestedMappers = context.nestedLookup().getImmediateChildMappers(nestedScope);
        Map<String, List<ResolvedField>> fieldsByNestedMapper = NestedUtils.partitionByChildren(
            nestedScope,
            nestedMappers,
            fields,
            f -> f.ft.name()
        );

        Map<String, FieldContext> output = new HashMap<>();
        for (String scope : fieldsByNestedMapper.keySet()) {
            if (nestedScope.equals(scope)) {
                // These are fields in the current scope, so add them directly to the output map
                for (ResolvedField ff : fieldsByNestedMapper.get(nestedScope)) {
                    output.put(ff.field, new FieldContext(ff.field, buildValueFetcher(context, ff)));
                }
            } else {
                // These fields are in a child scope, so build a nested mapper for them
                Map<String, FieldContext> scopedFields = buildFieldContexts(context, scope, fieldsByNestedMapper.get(scope));
                NestedValueFetcher nvf = new NestedValueFetcher(scope, new FieldFetcher(scopedFields, null, List.of()));
                output.put(scope, new FieldContext(scope, nvf));
            }
        }
        return output;
    }

    private final Map<String, FieldContext> fieldContexts;
    private final CharacterRunAutomaton unmappedFieldsFetchAutomaton;
    private final List<String> unmappedConcreteFields;
    private final StoredFieldsSpec storedFieldsSpec;

    private FieldFetcher(
        Map<String, FieldContext> fieldContexts,
        @Nullable CharacterRunAutomaton unmappedFieldsFetchAutomaton,
        @Nullable List<String> unmappedConcreteFields
    ) {
        this.fieldContexts = fieldContexts;
        this.unmappedFieldsFetchAutomaton = unmappedFieldsFetchAutomaton;
        this.unmappedConcreteFields = unmappedConcreteFields;
        this.storedFieldsSpec = StoredFieldsSpec.build(fieldContexts.values(), fc -> fc.valueFetcher.storedFieldsSpec());
    }

    public StoredFieldsSpec storedFieldsSpec() {
        return storedFieldsSpec;
    }

    public Map<String, DocumentField> fetch(Source source, int doc) throws IOException {
        Map<String, DocumentField> documentFields = new HashMap<>();
        for (FieldContext context : fieldContexts.values()) {
            String field = context.fieldName;
            ValueFetcher valueFetcher = context.valueFetcher;
            final DocumentField docField = valueFetcher.fetchDocumentField(field, source, doc);
            if (docField != null) {
                documentFields.put(field, docField);
            }
        }
        collectUnmapped(documentFields, source::source, "", 0);
        return documentFields;
    }

    private void collectUnmapped(
        Map<String, DocumentField> documentFields,
        Supplier<Map<String, Object>> source,
        String parentPath,
        int lastState
    ) {
        // lookup field patterns containing wildcards
        if (this.unmappedFieldsFetchAutomaton != null) {
            for (String key : source.get().keySet()) {
                Object value = source.get().get(key);
                String currentPath = parentPath + key;
                if (this.fieldContexts.containsKey(currentPath)) {
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
                        () -> objectMap,
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
                if (this.fieldContexts.containsKey(path)) {
                    continue; // this is actually a mapped field
                }
                List<Object> values = XContentMapValues.extractRawValues(path, source.get());
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
                collectUnmapped(documentFields, () -> objectMap, parentPath + ".", step(this.unmappedFieldsFetchAutomaton, ".", lastState));
            } else if (value instanceof List) {
                // weird case, but can happen for objects with "enabled" : "false"
                collectUnmappedList(documentFields, (List<?>) value, parentPath, lastState);
            } else if (this.unmappedFieldsFetchAutomaton.isAccept(lastState) && this.fieldContexts.containsKey(parentPath) == false) {
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

    private static Set<String> getParentPaths(Set<String> nestedPathsInScope, SearchExecutionContext context) {
        Set<String> parentPaths = new HashSet<>();
        for (String candidate : nestedPathsInScope) {
            String nestedParent = context.nestedLookup().getNestedParent(candidate);
            // if the candidate has no nested parent itself, its a minimal parent path
            // if the candidate has a parent which is out of scope this means it minimal itself
            if (nestedParent == null || nestedPathsInScope.contains(nestedParent) == false) {
                parentPaths.add(candidate);
            }
        }
        return parentPaths;
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

    private record FieldContext(String fieldName, ValueFetcher valueFetcher) {}
}
