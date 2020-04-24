/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.lucene.index.ReaderUtil;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.lookup.SourceLookup;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * A fetch sub-phase for high-level field retrieval. Given a list of fields, it
 * retrieves the field values from _source and returns them as document fields.
 */
public final class FetchFieldsPhase implements FetchSubPhase {

    @Override
    public void hitsExecute(SearchContext context, SearchHit[] hits) {
        hitsExecute(context, hit -> getSourceLookup(context, hit), hits);
    }

    // Visible for testing.
    @SuppressWarnings("unchecked")
    void hitsExecute(SearchContext context,
                     Function<SearchHit, SourceLookup> sourceProvider,
                     SearchHit[] hits) {
        FetchFieldsContext fetchFieldsContext = context.fetchFieldsContext();
        if (fetchFieldsContext == null || fetchFieldsContext.fields().isEmpty()) {
            return;
        }

        DocumentMapper documentMapper = context.mapperService().documentMapper();
        if (documentMapper.sourceMapper().enabled() == false) {
            throw new IllegalArgumentException("Unable to retrieve the requested [fields] since _source is " +
                "disabled in the mappings for index [" + context.indexShard().shardId().getIndexName() + "]");
        }

        Set<String> fields = new HashSet<>();
        for (String fieldPattern : context.fetchFieldsContext().fields()) {
            if (documentMapper.objectMappers().containsKey(fieldPattern)) {
                continue;
            }
            Collection<String> concreteFields = context.mapperService().simpleMatchToFullName(fieldPattern);
            fields.addAll(concreteFields);
        }

        for (SearchHit hit : hits) {
            SourceLookup sourceLookup = sourceProvider.apply(hit);
            Map<String, Object> valuesByField = extractValues(sourceLookup, fields);

            for (Map.Entry<String, Object> entry : valuesByField.entrySet()) {
                String field = entry.getKey();
                Object value = entry.getValue();
                List<Object> values = value instanceof List
                    ? (List<Object>) value
                    : List.of(value);

                DocumentField documentField = new DocumentField(field, values);
                hit.setField(field, documentField);
            }
        }
    }

    private SourceLookup getSourceLookup(SearchContext context, SearchHit hit) {
        SourceLookup sourceLookup = context.lookup().source();
        int readerIndex = ReaderUtil.subIndex(hit.docId(), context.searcher().getIndexReader().leaves());
        LeafReaderContext readerContext = context.searcher().getIndexReader().leaves().get(readerIndex);
        sourceLookup.setSegmentAndDocument(readerContext, hit.docId());
        return sourceLookup;
    }

    /**
     * For each of the provided paths, return its value in the source. Note that in contrast with
     * {@link SourceLookup#extractRawValues}, array and object values can be returned.
     */
    private Map<String, Object> extractValues(SourceLookup sourceLookup, Collection<String> paths) {
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
