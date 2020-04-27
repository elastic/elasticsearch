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
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.search.lookup.SourceLookup;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class FieldValueRetriever {
    private final Set<String> fields;

    public static FieldValueRetriever create(MapperService mapperService,
                                             Collection<String> fieldPatterns) {
        Set<String> fields = new HashSet<>();
        DocumentMapper documentMapper = mapperService.documentMapper();

        for (String fieldPattern : fieldPatterns) {
            if (documentMapper.objectMappers().containsKey(fieldPattern)) {
                continue;
            }
            Collection<String> concreteFields = mapperService.simpleMatchToFullName(fieldPattern);
            fields.addAll(concreteFields);
        }
        return new FieldValueRetriever(fields);
    }

    private FieldValueRetriever(Set<String> fields) {
        this.fields = fields;
    }

    @SuppressWarnings("unchecked")
    public Map<String, DocumentField> retrieve(SourceLookup sourceLookup) {
        Map<String, DocumentField> result = new HashMap<>();
        Map<String, Object> sourceValues = extractValues(sourceLookup, this.fields);

        for (Map.Entry<String, Object> entry : sourceValues.entrySet()) {
            String field = entry.getKey();
            Object value = entry.getValue();
            List<Object> values = value instanceof List
                ? (List<Object>) value
                : List.of(value);
            result.put(field, new DocumentField(field, values));
        }
        return result;
    }

    /**
     * For each of the provided paths, return its value in the source. Note that in contrast with
     * {@link SourceLookup#extractRawValues}, array and object values can be returned.
     */
    private static Map<String, Object> extractValues(SourceLookup sourceLookup, Collection<String> paths) {
        Map<String, Object> result = new HashMap<>(paths.size());
        for (String path : paths) {
            Object value = XContentMapValues.extractValue(path, sourceLookup);
            if (value != null) {
                result.put(path, value);
            }
        }
        return result;
    }
}
