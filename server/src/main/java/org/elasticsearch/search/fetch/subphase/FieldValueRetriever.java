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

import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.search.lookup.SourceLookup;

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
public class FieldValueRetriever {
    private final List<FieldContext> fieldContexts;
    private final Set<String> sourcePaths;

    public static FieldValueRetriever create(MapperService mapperService,
                                             Collection<String> fieldPatterns) {
        List<FieldContext> fields = new ArrayList<>();
        Set<String> sourcePaths = new HashSet<>();

        for (String fieldPattern : fieldPatterns) {
            Collection<String> concreteFields = mapperService.simpleMatchToFullName(fieldPattern);
            for (String field : concreteFields) {
                MappedFieldType fieldType = mapperService.fieldType(field);

                if (fieldType != null) {
                    Set<String> sourcePath = mapperService.sourcePath(field);
                    fields.add(new FieldContext(field, sourcePath));
                    sourcePaths.addAll(sourcePath);
                }
            }
        }

        return new FieldValueRetriever(fields, sourcePaths);
    }

    private FieldValueRetriever(List<FieldContext> fieldContexts, Set<String> sourcePaths) {
        this.fieldContexts = fieldContexts;
        this.sourcePaths = sourcePaths;
    }

    @SuppressWarnings("unchecked")
    public Map<String, DocumentField> retrieve(SourceLookup sourceLookup) {
        Map<String, DocumentField> result = new HashMap<>();
        Map<String, Object> sourceValues = extractValues(sourceLookup, sourcePaths);

        for (FieldContext fieldContext : fieldContexts) {
            String field = fieldContext.fieldName;
            Set<String> sourcePath = fieldContext.sourcePath;

            List<Object> values = new ArrayList<>();
            for (String path : sourcePath) {
                Object value = sourceValues.get(path);
                if (value != null) {
                    if (value instanceof List) {
                        values.addAll((List<Object>) value);
                    } else {
                        values.add(value);
                    }
                }
            }
            result.put(field, new DocumentField(field, values));
        }
        return result;
    }

    /**
     * For each of the provided paths, return its value in the source. Note that in contrast with
     * {@link SourceLookup#extractRawValues}, array and object values can be returned.
     */
    private static Map<String, Object> extractValues(SourceLookup sourceLookup, Set<String> paths) {
        Map<String, Object> result = new HashMap<>(paths.size());
        for (String path : paths) {
            Object value = XContentMapValues.extractValue(path, sourceLookup);
            if (value != null) {
                result.put(path, value);
            }
        }
        return result;
    }

    private static class FieldContext {
        final String fieldName;
        final Set<String> sourcePath;

        FieldContext(String fieldName, Set<String> sourcePath) {
            this.fieldName = fieldName;
            this.sourcePath = sourcePath;
        }
    }
}
