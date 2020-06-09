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

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.index.mapper.DocumentFieldMappers;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.search.lookup.SourceLookup;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A helper class to {@link FetchFieldsPhase} that's initialized with a list of field patterns to fetch.
 * Then given a specific document, it can retrieve the corresponding fields from the document's source.
 */
public class FieldValueRetriever {
    private final DocumentFieldMappers fieldMappers;
    private final List<FieldContext> fieldContexts;

    public static FieldValueRetriever create(MapperService mapperService,
                                             Collection<FieldAndFormat> fieldAndFormats) {
        DocumentFieldMappers fieldMappers = mapperService.documentMapper().mappers();
        List<FieldContext> fields = new ArrayList<>();

        for (FieldAndFormat fieldAndFormat : fieldAndFormats) {
            String fieldPattern = fieldAndFormat.field;
            String format = fieldAndFormat.format;

            Collection<String> concreteFields = mapperService.simpleMatchToFullName(fieldPattern);
            for (String field : concreteFields) {
                if (fieldMappers.getMapper(field) != null) {
                    Set<String> sourcePath = mapperService.sourcePath(field);
                    fields.add(new FieldContext(field, sourcePath, format));
                }
            }
        }

        return new FieldValueRetriever(fieldMappers, fields);
    }

    private FieldValueRetriever(DocumentFieldMappers fieldMappers,
                                List<FieldContext> fieldContexts) {
        this.fieldMappers = fieldMappers;
        this.fieldContexts = fieldContexts;
    }

    public Map<String, DocumentField> retrieve(SourceLookup sourceLookup, Set<String> ignoredFields) {
        Map<String, DocumentField> documentFields = new HashMap<>();
        for (FieldContext context : fieldContexts) {
            String field = context.fieldName;
            if (ignoredFields.contains(field)) {
                continue;
            }

            List<Object> parsedValues = new ArrayList<>();
            for (String path : context.sourcePath) {
                FieldMapper fieldMapper = (FieldMapper) fieldMappers.getMapper(path);
                List<?> values = fieldMapper.lookupValues(sourceLookup, context.format);
                parsedValues.addAll(values);
            }

            if (parsedValues.isEmpty() == false) {
                documentFields.put(field, new DocumentField(field, parsedValues));
            }
        }
        return documentFields;
    }

    private static class FieldContext {
        final String fieldName;
        final Set<String> sourcePath;
        final @Nullable String format;

        FieldContext(String fieldName,
                     Set<String> sourcePath,
                     @Nullable String format) {
            this.fieldName = fieldName;
            this.sourcePath = sourcePath;
            this.format = format;
        }
    }
}
