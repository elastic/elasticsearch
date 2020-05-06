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
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.IgnoredFieldMapper;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.lookup.SourceLookup;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * A fetch sub-phase for high-level field retrieval. Given a list of fields, it
 * retrieves the field values from _source and returns them as document fields.
 */
public final class FetchFieldsPhase implements FetchSubPhase {

    @Override
    public void hitsExecute(SearchContext context, SearchHit[] hits) {
        FetchFieldsContext fetchFieldsContext = context.fetchFieldsContext();
        if (fetchFieldsContext == null || fetchFieldsContext.fields().isEmpty()) {
            return;
        }

        DocumentMapper documentMapper = context.mapperService().documentMapper();
        if (documentMapper.sourceMapper().enabled() == false) {
            throw new IllegalArgumentException("Unable to retrieve the requested [fields] since _source is " +
                "disabled in the mappings for index [" + context.indexShard().shardId().getIndexName() + "]");
        }

        SourceLookup sourceLookup = context.lookup().source();
        FieldValueRetriever fieldValueRetriever = FieldValueRetriever.create(
            context.mapperService(),
            fetchFieldsContext.fields());

        for (SearchHit hit : hits) {
            int readerIndex = ReaderUtil.subIndex(hit.docId(), context.searcher().getIndexReader().leaves());
            LeafReaderContext readerContext = context.searcher().getIndexReader().leaves().get(readerIndex);
            sourceLookup.setSegmentAndDocument(readerContext, hit.docId());

            Set<String> ignoredFields = getIgnoredFields(hit);
            Map<String, DocumentField> fieldValues = fieldValueRetriever.retrieve(sourceLookup, ignoredFields);
            hit.fields(fieldValues);
        }
    }

    private Set<String> getIgnoredFields(SearchHit hit) {
        DocumentField field = hit.field(IgnoredFieldMapper.NAME);
        if (field == null) {
            return Set.of();
        }

        Set<String> ignoredFields = new HashSet<>();
        for (Object value : field.getValues()) {
            ignoredFields.add((String) value);
        }
        return ignoredFields;
    }
}
