/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.fetch.subphase;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.automaton.CharacterRunAutomaton;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.ValueFetcher;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.search.lookup.SourceLookup;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A helper class to {@link FetchFieldsPhase} that's initialized with a list of field patterns to fetch.
 * Then given a specific document, it can retrieve the corresponding fields from the document's source.
 */
public class FieldFetcher {
    public static FieldFetcher create(QueryShardContext context,
                                      SearchLookup searchLookup,
                                      Collection<FieldAndFormat> fieldAndFormats,
                                      boolean includeUnmapped) {

        List<FieldContext> fieldContexts = new ArrayList<>();
        String[] originalPattern = new String[fieldAndFormats.size()];
        Set<String> mappedToExclude = new HashSet<>();
        int i = 0;

        for (FieldAndFormat fieldAndFormat : fieldAndFormats) {
            String fieldPattern = fieldAndFormat.field;
            originalPattern[i] = fieldAndFormat.field;
            i++;
            String format = fieldAndFormat.format;

            Collection<String> concreteFields = context.simpleMatchToIndexNames(fieldPattern);
            for (String field : concreteFields) {
                MappedFieldType ft = context.getFieldType(field);
                if (ft == null || context.isMetadataField(field)) {
                    continue;
                }
                ValueFetcher valueFetcher = ft.valueFetcher(context, searchLookup, format);
                mappedToExclude.add(field);
                fieldContexts.add(new FieldContext(field, valueFetcher));
            }
        }
        CharacterRunAutomaton pathAutomaton = new CharacterRunAutomaton(Regex.simpleMatchToAutomaton(originalPattern));
        return new FieldFetcher(fieldContexts, includeUnmapped, pathAutomaton, mappedToExclude);
    }

    private final List<FieldContext> fieldContexts;
    private final boolean includeUnmapped;
    private final CharacterRunAutomaton pathAutomaton;
    private final Set<String> mappedToExclude;

    private FieldFetcher(
        List<FieldContext> fieldContexts,
        boolean includeUnmapped,
        CharacterRunAutomaton pathAutomaton,
        Set<String> mappedToExclude
    ) {
        this.fieldContexts = fieldContexts;
        this.includeUnmapped = includeUnmapped;
        this.pathAutomaton = pathAutomaton;
        this.mappedToExclude = mappedToExclude;
    }

    public Map<String, DocumentField> fetch(SourceLookup sourceLookup, Set<String> ignoredFields) throws IOException {
        Map<String, DocumentField> documentFields = new HashMap<>();
        for (FieldContext context : fieldContexts) {
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
        if (includeUnmapped) {
            collect(documentFields, sourceLookup.loadSourceIfNeeded(), "", 0);
        }
        return documentFields;
    }

    private void collect(Map<String, DocumentField> documentFields, Map<String, Object> source, String parentPath, int lastState) {
        for (String key : source.keySet()) {
            Object value = source.get(key);
            String currentPath = parentPath + key;
            int currentState = step(this.pathAutomaton, key, lastState);
            if (currentState == -1) {
                // path doesn't match any fields pattern
                continue;
            }
            if (value instanceof Map) {
                // one step deeper into source tree
                collect(documentFields, (Map<String, Object>) value, currentPath + ".", step(this.pathAutomaton, ".", currentState));
            } else if (value instanceof List) {
                // iterate through list values
                List<Object> list = collectList(documentFields, (List<?>) value, currentPath, currentState);
                if (list.isEmpty() == false) {
                    documentFields.put(currentPath, new DocumentField(currentPath, list));
                }
            } else {
                // we have a leaf value
                if (this.pathAutomaton.isAccept(currentState) && this.mappedToExclude.contains(currentPath) == false) {
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

    private List<Object> collectList(Map<String, DocumentField> documentFields, Iterable<?> iterable, String parentPath, int lastState) {
        List<Object> list = new ArrayList<>();
        for (Object value : iterable) {
            if (value instanceof Map) {
                collect(documentFields, (Map<String, Object>) value, parentPath + ".", step(this.pathAutomaton, ".", lastState));
            } else if (value instanceof List) {
                // weird case, but can happen for objects with "enabled" : "false"
                list.add(collectList(documentFields, (List<?>) value, parentPath, lastState));
            } else if (this.pathAutomaton.isAccept(lastState)) {
                list.add(value);
            }
        }
        return list;
    }

    private static int step(CharacterRunAutomaton automaton, String key, int state) {
        for (int i = 0; state != -1 && i < key.length(); ++i) {
            state = automaton.step(state, key.charAt(i));
        }
        return state;
    }

    public void setNextReader(LeafReaderContext readerContext) {
        for (FieldContext field : fieldContexts) {
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
