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

package org.elasticsearch.join.fetch;

import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.join.mapper.ParentIdFieldMapper;
import org.elasticsearch.join.mapper.ParentJoinFieldMapper;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * A sub fetch phase that retrieves the join name and the parent id for each document containing
 * a {@link ParentJoinFieldMapper} field.
 */
public final class ParentJoinFieldSubFetchPhase implements FetchSubPhase {
    @Override
    public void hitExecute(SearchContext context, HitContext hitContext) {
        if (context.storedFieldsContext() != null && context.storedFieldsContext().fetchFields() == false) {
            return;
        }
        ParentJoinFieldMapper mapper = ParentJoinFieldMapper.getMapper(context.mapperService());
        if (mapper == null) {
            // hit has no join field.
            return;
        }
        String joinName = getSortedDocValue(mapper.name(), hitContext.reader(), hitContext.docId());
        if (joinName == null) {
            return;
        }

        // if the hit is a children we extract the parentId (if it's a parent we can use the _id field directly)
        ParentIdFieldMapper parentMapper = mapper.getParentIdFieldMapper(joinName, false);
        String parentId = null;
        if (parentMapper != null) {
            parentId = getSortedDocValue(parentMapper.name(), hitContext.reader(), hitContext.docId());
        }

        Map<String, SearchHitField> fields = hitContext.hit().fieldsOrNull();
        if (fields == null) {
            fields = new HashMap<>();
            hitContext.hit().fields(fields);
        }
        fields.put(mapper.name(), new SearchHitField(mapper.name(), Collections.singletonList(joinName)));
        if (parentId != null) {
            fields.put(parentMapper.name(), new SearchHitField(parentMapper.name(), Collections.singletonList(parentId)));
        }
    }

    private String getSortedDocValue(String field, LeafReader reader, int docId) {
        try {
            SortedDocValues docValues = reader.getSortedDocValues(field);
            if (docValues == null || docValues.advanceExact(docId) == false) {
                return null;
            }
            int ord = docValues.ordValue();
            BytesRef joinName = docValues.lookupOrd(ord);
            return joinName.utf8ToString();
        } catch (IOException e) {
            throw ExceptionsHelper.convertToElastic(e);
        }
    }
}
