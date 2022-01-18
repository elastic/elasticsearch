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
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.LookupRuntimeFieldType;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NestedValueFetcher;
import org.elasticsearch.index.mapper.ObjectMapper;
import org.elasticsearch.index.mapper.ValueFetcher;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.lookup.SourceLookup;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.search.SearchService.ALLOW_EXPENSIVE_QUERIES;

/**
 * A helper class to {@link FetchFieldsPhase} that's initialized with a list of field patterns to fetch.
 * Then given a specific document, it can retrieve the corresponding fields from the document's source.
 */
public class FieldFetcher {

    /**
     * Default maximum number of states in the automaton that looks up unmapped fields.
     */
    private static final int AUTOMATON_MAX_DETERMINIZED_STATES = 100000;

    public static FieldFetcher create(SearchExecutionContext context, Collection<FieldAndFormat> fieldAndFormats) {
        Set<String> nestedMappingPaths = context.hasNested()
            ? context.nestedMappings().stream().map(ObjectMapper::name).collect(Collectors.toSet())
            : Collections.emptySet();
        return create(context, fieldAndFormats, nestedMappingPaths, "");
    }

    private static FieldFetcher create(
        SearchExecutionContext context,
        Collection<FieldAndFormat> fieldAndFormats,
        Set<String> nestedMappingsInScope,
        String nestedScopePath
    ) {
        // here we only need the nested paths that are closes to the root, e.g. only "foo" if also "foo.bar" is present.
        // the remaining nested field paths are handled recursively
        Set<String> nestedParentPaths = getParentPaths(nestedMappingsInScope, context);

        // Using a LinkedHashMap so fields are returned in the order requested.
        // We won't formally guarantee this but but its good for readability of the response
        Map<String, FieldContext> fieldContexts = new LinkedHashMap<>();
        List<String> unmappedFetchPattern = new ArrayList<>();
        List<LookupRuntimeFieldType> lookupFields = new ArrayList<>();

        for (FieldAndFormat fieldAndFormat : fieldAndFormats) {
            String fieldPattern = fieldAndFormat.field;
            boolean isWildcardPattern = Regex.isSimpleMatchPattern(fieldPattern);
            if (fieldAndFormat.includeUnmapped != null && fieldAndFormat.includeUnmapped) {
                unmappedFetchPattern.add(fieldAndFormat.field);
            }

            for (String field : context.getMatchingFieldNames(fieldPattern)) {
                MappedFieldType ft = context.getFieldType(field);
                if (ft instanceof LookupRuntimeFieldType lookupField) {
                    lookupFields.add(lookupField);
                    continue;
                }
                // we want to skip metadata fields if we have a wildcard pattern
                if (context.isMetadataField(field) && isWildcardPattern) {
                    continue;
                }
                if (field.startsWith(nestedScopePath) == false) {
                    // this field is out of scope for this FieldFetcher (its likely nested) so ignore
                    continue;
                }
                String nestedParentPath = null;
                if (nestedParentPaths.isEmpty() == false) {
                    // try to find the shortest nested parent path for this field
                    for (String nestedFieldPath : nestedParentPaths) {
                        if (field.startsWith(nestedFieldPath)) {
                            nestedParentPath = nestedFieldPath;
                            break;
                        }
                    }
                }
                // only add concrete fields if they are not beneath a known nested field
                if (nestedParentPath == null) {
                    ValueFetcher valueFetcher;
                    try {
                        valueFetcher = ft.valueFetcher(context, fieldAndFormat.format);
                    } catch (IllegalArgumentException e) {
                        StringBuilder error = new StringBuilder("error fetching [").append(field).append(']');
                        if (isWildcardPattern) {
                            error.append(" which matched [").append(fieldAndFormat.field).append(']');
                        }
                        error.append(": ").append(e.getMessage());
                        throw new IllegalArgumentException(error.toString(), e);
                    }
                    fieldContexts.put(field, new FieldContext(field, valueFetcher));
                }
            }
        }

        // create a new nested value fetcher for patterns under nested field
        for (String nestedFieldPath : nestedParentPaths) {
            // We construct a field fetcher that narrows the allowed lookup scope to everything beneath its nested field path.
            // We also need to remove this nested field path and everything beneath it from the list of available nested fields before
            // creating this internal field fetcher to avoid infinite loops on this recursion
            Set<String> narrowedScopeNestedMappings = nestedMappingsInScope.stream()
                .filter(s -> nestedParentPaths.contains(s) == false)
                .collect(Collectors.toSet());

            FieldFetcher nestedSubFieldFetcher = FieldFetcher.create(
                context,
                fieldAndFormats,
                narrowedScopeNestedMappings,
                nestedFieldPath
            );

            // add a special ValueFetcher that filters source and collects its subfields
            fieldContexts.put(
                nestedFieldPath,
                new FieldContext(nestedFieldPath, new NestedValueFetcher(nestedFieldPath, nestedSubFieldFetcher))
            );
        }

        CharacterRunAutomaton unmappedFieldsFetchAutomaton = null;
        // We separate the "include_unmapped" field patters with wildcards from the rest in order to use less
        // space in the lookup automaton
        Map<Boolean, List<String>> partitions = unmappedFetchPattern.stream()
            .collect(Collectors.partitioningBy((s -> Regex.isSimpleMatchPattern(s))));
        List<String> unmappedWildcardPattern = partitions.get(true);
        List<String> unmappedConcreteFields = partitions.get(false);
        if (unmappedWildcardPattern.isEmpty() == false) {
            unmappedFieldsFetchAutomaton = new CharacterRunAutomaton(
                Regex.simpleMatchToAutomaton(unmappedWildcardPattern.toArray(new String[unmappedWildcardPattern.size()])),
                AUTOMATON_MAX_DETERMINIZED_STATES
            );
        }
        List<LookupFieldAndIdFetcher> lookupFieldAndIdFetchers = lookupFields.stream().map(ft -> {
            final ValueFetcher idFetcher;
            if (fieldContexts.containsKey(ft.getIdField())) {
                idFetcher = null; // already fetched
            } else {
                if (context.isFieldMapped(ft.getIdField())) {
                    idFetcher = context.getFieldType(ft.getIdField()).valueFetcher(context, null);
                } else {
                    idFetcher = null;
                }
            }
            return new LookupFieldAndIdFetcher(ft, idFetcher);
        }).toList();
        return new FieldFetcher(
            fieldContexts,
            unmappedFieldsFetchAutomaton,
            unmappedConcreteFields,
            lookupFieldAndIdFetchers,
            context.allowExpensiveQueries()
        );
    }

    private final Map<String, FieldContext> fieldContexts;
    private final CharacterRunAutomaton unmappedFieldsFetchAutomaton;
    private final List<String> unmappedConcreteFields;
    private final List<LookupFieldAndIdFetcher> lookupFieldAndIdFetchers;
    private final boolean allowExpensiveQueries;

    private FieldFetcher(
        Map<String, FieldContext> fieldContexts,
        @Nullable CharacterRunAutomaton unmappedFieldsFetchAutomaton,
        @Nullable List<String> unmappedConcreteFields,
        List<LookupFieldAndIdFetcher> lookupFieldAndIdFetchers,
        boolean allowExpensiveQueries
    ) {
        this.fieldContexts = fieldContexts;
        this.unmappedFieldsFetchAutomaton = unmappedFieldsFetchAutomaton;
        this.unmappedConcreteFields = unmappedConcreteFields;
        this.lookupFieldAndIdFetchers = lookupFieldAndIdFetchers;
        this.allowExpensiveQueries = allowExpensiveQueries;
    }

    public Map<String, DocumentField> fetch(SourceLookup sourceLookup) throws IOException {
        Map<String, DocumentField> documentFields = new HashMap<>();
        for (FieldContext context : fieldContexts.values()) {
            String field = context.fieldName;

            ValueFetcher valueFetcher = context.valueFetcher;
            List<Object> ignoredValues = new ArrayList<>();
            List<Object> parsedValues = valueFetcher.fetchValues(sourceLookup, ignoredValues);
            if (parsedValues.isEmpty() == false || ignoredValues.isEmpty() == false) {
                documentFields.put(field, new DocumentField(field, parsedValues, ignoredValues));
            }
        }
        collectUnmapped(documentFields, sourceLookup.source(), "", 0);
        collectLookupFields(documentFields, sourceLookup);
        return documentFields;
    }

    private void collectLookupFields(Map<String, DocumentField> documentFields, SourceLookup sourceLookup) throws IOException {
        for (LookupFieldAndIdFetcher lookupFieldAndIdFetcher : lookupFieldAndIdFetchers) {
            final LookupRuntimeFieldType ft = lookupFieldAndIdFetcher.field;
            final List<Object> idValues;
            if (lookupFieldAndIdFetcher.idFetcher != null) {
                idValues = lookupFieldAndIdFetcher.idFetcher.fetchValues(sourceLookup, new ArrayList<>());
            } else {
                final DocumentField idDocField = documentFields.get(ft.getIdField());
                if (idDocField != null) {
                    idValues = idDocField.getValues();
                } else {
                    idValues = null;
                }
            }
            if (idValues != null && idValues.isEmpty() == false) {
                final List<LookupField> lookupFields = idValues.stream()
                    .map(id -> new LookupField(ft.getLookupIndex(), id.toString(), ft.getLookupFields()))
                    .toList();
                if (allowExpensiveQueries == false) {
                    final ElasticsearchException failure = new ElasticsearchException(
                        "cannot be executed against lookup fields while [" + ALLOW_EXPENSIVE_QUERIES.getKey() + "] is set to [false]."
                    );
                    lookupFields.forEach(field -> field.setFailure(failure));
                }
                documentFields.compute(ft.name(), (name, curr) -> {
                    if (curr == null) {
                        return new DocumentField(name, List.of(), List.of(), lookupFields);
                    } else {
                        return new DocumentField(name, curr.getValues(), curr.getIgnoredValues(), lookupFields);
                    }
                });
            }
        }
    }

    private void collectUnmapped(Map<String, DocumentField> documentFields, Map<String, Object> source, String parentPath, int lastState) {
        // lookup field patterns containing wildcards
        if (this.unmappedFieldsFetchAutomaton != null) {
            for (String key : source.keySet()) {
                Object value = source.get(key);
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
                if (this.fieldContexts.containsKey(path)) {
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
            String nestedParent = context.getNestedParent(candidate);
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

    private static class FieldContext {
        final String fieldName;
        final ValueFetcher valueFetcher;

        FieldContext(String fieldName, ValueFetcher valueFetcher) {
            this.fieldName = fieldName;
            this.valueFetcher = valueFetcher;
        }
    }

    private record LookupFieldAndIdFetcher(LookupRuntimeFieldType field, ValueFetcher idFetcher) {}
}
