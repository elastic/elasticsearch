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
import org.elasticsearch.common.xcontent.support.XContentMapValues;
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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

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
        List<String> excludeForUnmappedFetch = new ArrayList<>();

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
                excludeForUnmappedFetch.add(field);
                fieldContexts.add(new FieldContext(field, valueFetcher));
            }
            if (fieldPattern.charAt(fieldPattern.length() - 1) != '*') {
                // not a prefix pattern, exclude potential sub-fields when fetching unmapped fields
                excludeForUnmappedFetch.add(fieldPattern + ".*");
            }
        }
        Function<Map<String, ?>, Map<String, Object>> filter = XContentMapValues.filter(
            originalPattern.toArray(new String[originalPattern.size()]),
            excludeForUnmappedFetch.toArray(new String[excludeForUnmappedFetch.size()])
        );

        return new FieldFetcher(fieldContexts, includeUnmapped, filter);
    }

    private final List<FieldContext> fieldContexts;
    private final boolean includeUnmapped;
    private final Function<Map<String, ?>, Map<String, Object>> filter;

    private FieldFetcher(
        List<FieldContext> fieldContexts,
        boolean includeUnmapped,
        Function<Map<String, ?>, Map<String, Object>> filter
    ) {
        this.fieldContexts = fieldContexts;
        this.includeUnmapped = includeUnmapped;
        this.filter = filter;
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
            Map<String, Object> unmappedFieldsToAdd = filter.apply(sourceLookup.loadSourceIfNeeded());
            collectLeafValues(unmappedFieldsToAdd, documentFields);
        }
        return documentFields;
    }

    static void collectLeafValues(Map<String, Object> map, Map<String, DocumentField> documentFields) {
        collectAllPaths("", map, documentFields);
    }

    private static void collectAllPaths(String prefix, Map<String, Object> source, Map<String, DocumentField> documentFields) {
        for (String key : source.keySet()) {
            Object value = source.get(key);
            String currentPath = prefix + key;
            if (value instanceof Map) {
                collectAllPaths(currentPath + ".", (Map<String, Object>) value, documentFields);
            } else {
                DocumentField f;
                if (value instanceof List) {
                    f = new DocumentField(currentPath, (List) value);
                } else {
                    f = new DocumentField(currentPath, Collections.singletonList(value));
                }
                if (f.getValue() != null) {
                    documentFields.put(currentPath, f);
                }
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
