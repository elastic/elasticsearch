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
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MappingLookup;
import org.elasticsearch.index.mapper.ValueFetcher;
import org.elasticsearch.index.mapper.ValueFetcher.LeafValueFetcher;
import org.elasticsearch.search.fetch.FetchSubPhase.HitContext;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.stream.Collectors.toList;

/**
 * A helper class to {@link FetchFieldsPhase} that's initialized with a list of field patterns to fetch.
 * Then given a specific document, it can retrieve the corresponding fields from the document's source.
 */
public class FieldValueRetriever {
    public static FieldValueRetriever create(MapperService mapperService,
                                             Collection<FieldAndFormat> fieldAndFormats) {
        MappingLookup fieldMappers = mapperService.documentMapper().mappers();
        List<FieldContext> fields = new ArrayList<>();

        for (FieldAndFormat fieldAndFormat : fieldAndFormats) {
            String fieldPattern = fieldAndFormat.field;
            String format = fieldAndFormat.format;

            Collection<String> concreteFields = mapperService.simpleMatchToFullName(fieldPattern);
            for (String field : concreteFields) {
                if (fieldMappers.getMapper(field) != null && mapperService.isMetadataField(field) == false) {
                    Set<String> sourcePaths = mapperService.sourcePath(field);
                    List<FieldMapper> mappers = sourcePaths.stream()
                        .map(path -> (FieldMapper) fieldMappers.getMapper(path))
                        .collect(toList());
                    fields.add(new FieldContext(field, mappers, format));
                }
            }
        }

        return new FieldValueRetriever(fields);
    }

    private final List<FieldContext> fieldContexts;
    private LeafReaderContext lastLeaf;

    FieldValueRetriever(List<FieldContext> fieldContexts) {
        this.fieldContexts = fieldContexts;
    }

    public void prepare(SearchLookup lookup) throws IOException {
        fieldContexts.stream().forEach(field -> field.buildFetchers(lookup));
    }

    public Map<String, DocumentField> retrieve(HitContext hitContext, Set<String> ignoredFields) throws IOException {
        /*
         * The fields fetch APIs are leaf by leaf but Fetch sub phases don't
         * work that way so we have to track the last leaf. This should be
         * fairly safe because this is called in sorted order which sorts
         * by leafs. 
         */
        if (lastLeaf != hitContext.readerContext()) {
            lastLeaf = hitContext.readerContext();
            for (FieldContext fieldContext : fieldContexts) {
                fieldContext.prepareForLeaf(hitContext.readerContext());
            }
        }
        Map<String, DocumentField> documentFields = new HashMap<>();
        for (FieldContext context : fieldContexts) {
            String field = context.fieldName;
            if (ignoredFields.contains(field)) {
                continue;
            }

            List<Object> parsedValues = new ArrayList<>();
            for (LeafValueFetcher fetcher : context.leafFetchers) {
                parsedValues.addAll(fetcher.fetch(hitContext.docId()));
            }
            if (parsedValues.isEmpty() == false) {
                documentFields.put(field, new DocumentField(field, parsedValues));
            }
        }
        return documentFields;
    }

    static class FieldContext {
        final String fieldName;
        final List<FieldMapper> mappers;
        final @Nullable String format;
        final List<ValueFetcher> fetchers;
        final List<ValueFetcher.LeafValueFetcher> leafFetchers;

        FieldContext(String fieldName,
                     List<FieldMapper> mappers,
                     @Nullable String format) {
            this.fieldName = fieldName;
            this.mappers = mappers;
            this.format = format;
            fetchers = new ArrayList<>(mappers.size());
            leafFetchers = new ArrayList<>(mappers.size());
        }

        private void buildFetchers(SearchLookup lookup) {
            for (FieldMapper mapper : mappers) {
                fetchers.add(mapper.valueFetcher(lookup, format));
            };
        }

        private void prepareForLeaf(LeafReaderContext context) throws IOException {
            leafFetchers.clear();
            for (ValueFetcher fetcher : fetchers) {
                leafFetchers.add(fetcher.leaf(context));
            }
        }
    }
}
