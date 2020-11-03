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
import java.util.Collections;
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
        List<String> originalPattern = new ArrayList<>();
        List<String> mappedFields = new ArrayList<>();

        for (FieldAndFormat fieldAndFormat : fieldAndFormats) {
            String fieldPattern = fieldAndFormat.field;
            String format = fieldAndFormat.format;

            Collection<String> concreteFields = context.simpleMatchToIndexNames(fieldPattern);
            originalPattern.add(fieldAndFormat.field);
            for (String field : concreteFields) {
                MappedFieldType ft = context.getFieldType(field);
                if (ft == null || context.isMetadataField(field)) {
                    continue;
                }
                ValueFetcher valueFetcher = ft.valueFetcher(context, searchLookup, format);
                mappedFields.add(field);
                fieldContexts.add(new FieldContext(field, valueFetcher));
            }
        }

        return new FieldFetcher(fieldContexts, originalPattern, mappedFields, includeUnmapped);
    }

    private final List<FieldContext> fieldContexts;
    private final List<String> originalPattern;
    private final List<String> mappedFields;
    private final boolean includeUnmapped;

    private FieldFetcher(
        List<FieldContext> fieldContexts,
        List<String> originalPattern,
        List<String> mappedFields,
        boolean includeUnmapped
    ) {
        this.fieldContexts = fieldContexts;
        this.originalPattern = originalPattern;
        this.mappedFields = mappedFields;
        this.includeUnmapped = includeUnmapped;
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
            // also look up unmapped fields from source
            Set<String> allSourcePaths = extractAllLeafPaths(sourceLookup.loadSourceIfNeeded());
            for (String fetchFieldPattern : originalPattern) {
                if (Regex.isSimpleMatchPattern(fetchFieldPattern) == false) {
                    // if pattern has no wildcard, simply look up field if not already present
                    if (allSourcePaths.contains(fetchFieldPattern)) {
                        addValueFromSource(sourceLookup, fetchFieldPattern, documentFields);
                    }
                } else {
                    for (String singlePath : allSourcePaths) {
                        if (Regex.simpleMatch(fetchFieldPattern, singlePath)) {
                            addValueFromSource(sourceLookup, singlePath, documentFields);
                        }
                    }
                }
            }
        }
        return documentFields;
    }

    private void addValueFromSource(SourceLookup sourceLookup, String path, Map<String, DocumentField> documentFields) {
        // checking mapped fields here again to avoid adding from _source where some value was already added or ignored on purpose before
        if (mappedFields.contains(path) == false) {
            Object object = sourceLookup.extractValue(path, null);
            DocumentField f;
            if (object instanceof List) {
                f = new DocumentField(path, (List) object);
            } else {
                f = new DocumentField(path, Collections.singletonList(object));
            }
            if (f.getValue() != null) {
                documentFields.put(path, f);
            }
        }
    }

    static Set<String> extractAllLeafPaths(Map<String, Object> map) {
        Set<String> allPaths = new HashSet<>();
        collectAllPaths(allPaths, "", map);
        return allPaths;
    }

    private static void collectAllPaths(Set<String> allPaths, String prefix, Map<String, Object> source) {
        for (String key : source.keySet()) {
            Object value = source.get(key);
            if (value instanceof Map) {
                collectAllPaths(allPaths, prefix + key + ".", (Map<String, Object>) value);
            } else {
                allPaths.add(prefix + key);
            }
        }
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
