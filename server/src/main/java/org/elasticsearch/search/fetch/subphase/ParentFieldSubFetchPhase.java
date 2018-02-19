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

import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.ParentFieldMapper;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public final class ParentFieldSubFetchPhase implements FetchSubPhase {
    @Override
    public void hitsExecute(SearchContext context, SearchHit[] hits) throws IOException {
        if (context.storedFieldsContext() != null && context.storedFieldsContext().fetchFields() == false) {
            return ;
        }

        hits = hits.clone(); // don't modify the incoming hits
        Arrays.sort(hits, Comparator.comparingInt(SearchHit::docId));

        MapperService mapperService = context.mapperService();
        Set<String> parentFields = new HashSet<>();
        for (SearchHit hit : hits) {
            ParentFieldMapper parentFieldMapper = mapperService.documentMapper(hit.getType()).parentFieldMapper();
            if (parentFieldMapper.active()) {
                parentFields.add(parentFieldMapper.name());
            }
        }

        int lastReaderId = -1;
        Map<String, SortedDocValues> docValuesMap = new HashMap<>();
        for (SearchHit hit : hits) {
            ParentFieldMapper parentFieldMapper = mapperService.documentMapper(hit.getType()).parentFieldMapper();
            if (parentFieldMapper.active() == false) {
                continue;
            }
            int readerId = ReaderUtil.subIndex(hit.docId(), context.searcher().getIndexReader().leaves());
            LeafReaderContext subReaderContext = context.searcher().getIndexReader().leaves().get(readerId);
            if (lastReaderId != readerId) {
                docValuesMap.clear();
                for (String field : parentFields) {
                    docValuesMap.put(field, subReaderContext.reader().getSortedDocValues(field));
                }
                lastReaderId = readerId;
            }
            int docId = hit.docId() - subReaderContext.docBase;
            SortedDocValues values = docValuesMap.get(parentFieldMapper.name());
            if (values != null && values.advanceExact(docId)) {
                BytesRef binaryValue = values.binaryValue();
                String value = binaryValue.length > 0 ? binaryValue.utf8ToString() : null;
                if (value == null) {
                    // hit has no _parent field. Can happen for nested inner hits if parent hit is a p/c document.
                    continue;
                }
                Map<String, DocumentField> fields = hit.fieldsOrNull();
                if (fields == null) {
                    fields = new HashMap<>();
                    hit.fields(fields);
                }
                fields.put(ParentFieldMapper.NAME, new DocumentField(ParentFieldMapper.NAME, Collections.singletonList(value)));
            }
        }
    }

    public static String getParentId(ParentFieldMapper fieldMapper, LeafReader reader, int docId) {
        try {
            SortedDocValues docValues = reader.getSortedDocValues(fieldMapper.name());
            if (docValues == null || docValues.advanceExact(docId) == false) {
                // hit has no _parent field.
                return null;
            }
            BytesRef parentId = docValues.binaryValue();
            return parentId.length > 0 ? parentId.utf8ToString() : null;
        } catch (IOException e) {
            throw ExceptionsHelper.convertToElastic(e);
        }
    }
}
