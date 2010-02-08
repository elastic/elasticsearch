/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.search.fetch;

import com.google.common.collect.ImmutableMap;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Fieldable;
import org.elasticsearch.index.mapper.*;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.SearchParseElement;
import org.elasticsearch.search.SearchPhase;
import org.elasticsearch.search.internal.InternalSearchHit;
import org.elasticsearch.search.internal.InternalSearchHitField;
import org.elasticsearch.search.internal.InternalSearchHits;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * @author kimchy (Shay Banon)
 */
public class FetchPhase implements SearchPhase {

    @Override public Map<String, ? extends SearchParseElement> parseElements() {
        return ImmutableMap.of("explain", new ExplainParseElement(), "fields", new FieldsParseElement());
    }

    public void execute(SearchContext context) {
        FieldMappersFieldSelector fieldSelector = buildFieldSelectors(context);

        SearchHit[] hits = new SearchHit[context.docIdsToLoad().length];
        int index = 0;
        for (int docId : context.docIdsToLoad()) {
            Document doc = loadDocument(context, fieldSelector, docId);
            Uid uid = extractUid(context, doc);

            DocumentMapper documentMapper = context.mapperService().type(uid.type());

            String source = extractSource(doc, documentMapper);

            InternalSearchHit searchHit = new InternalSearchHit(uid.id(), uid.type(), source, null);
            hits[index] = searchHit;

            for (Object oField : doc.getFields()) {
                Fieldable field = (Fieldable) oField;
                String name = field.name();
                Object value = null;
                FieldMappers fieldMappers = documentMapper.mappers().indexName(field.name());
                if (fieldMappers != null) {
                    FieldMapper mapper = fieldMappers.mapper();
                    if (mapper != null) {
                        name = mapper.name();
                        value = mapper.valueForSearch(field);
                    }
                }
                if (value == null) {
                    if (field.isBinary()) {
                        value = field.getBinaryValue();
                    } else {
                        value = field.stringValue();
                    }
                }

                if (searchHit.fields() == null) {
                    searchHit.fields(new HashMap<String, SearchHitField>(2));
                }

                SearchHitField hitField = searchHit.fields().get(name);
                if (hitField == null) {
                    hitField = new InternalSearchHitField(name, new ArrayList<Object>(2));
                    searchHit.fields().put(name, hitField);
                }
                hitField.values().add(value);
            }
            doExplanation(context, docId, searchHit);

            index++;
        }
        context.fetchResult().hits(new InternalSearchHits(hits, context.queryResult().topDocs().totalHits));
    }

    private void doExplanation(SearchContext context, int docId, InternalSearchHit searchHit) {
        if (context.explain()) {
            try {
                searchHit.explanation(context.searcher().explain(context.query(), docId));
            } catch (IOException e) {
                throw new FetchPhaseExecutionException(context, "Failed to explain doc [" + docId + "]", e);
            }
        }
    }

    private String extractSource(Document doc, DocumentMapper documentMapper) {
        String source = null;
        Fieldable sourceField = doc.getFieldable(documentMapper.sourceMapper().indexName());
        if (sourceField != null) {
            source = documentMapper.sourceMapper().valueAsString(sourceField);
            doc.removeField(documentMapper.sourceMapper().indexName());
        }
        return source;
    }

    private Uid extractUid(SearchContext context, Document doc) {
        Uid uid = null;
        for (FieldMapper fieldMapper : context.mapperService().uidFieldMappers()) {
            String sUid = doc.get(fieldMapper.indexName());
            if (sUid != null) {
                uid = Uid.createUid(sUid);
                doc.removeField(fieldMapper.indexName());
                break;
            }
        }
        if (uid == null) {
            // no type, nothing to do (should not really happen
            throw new FetchPhaseExecutionException(context, "Failed to load uid from the index");
        }
        return uid;
    }

    private Document loadDocument(SearchContext context, FieldMappersFieldSelector fieldSelector, int docId) {
        Document doc;
        try {
            doc = context.searcher().doc(docId, fieldSelector);
        } catch (IOException e) {
            throw new FetchPhaseExecutionException(context, "Failed to fetch doc id [" + docId + "]", e);
        }
        return doc;
    }

    private FieldMappersFieldSelector buildFieldSelectors(SearchContext context) {
        FieldMappersFieldSelector fieldSelector = new FieldMappersFieldSelector();
        if (context.fieldNames() != null) {
            for (String fieldName : context.fieldNames()) {
                FieldMappers x = context.mapperService().smartNameFieldMappers(fieldName);
                if (x == null) {
                    throw new FetchPhaseExecutionException(context, "No mapping for field [" + fieldName + "]");
                }
                fieldSelector.add(x);
            }
        } else {
            fieldSelector.add(context.mapperService().sourceFieldMappers());
        }
        // add the uids by default, so we can return the id/type
        fieldSelector.add(context.mapperService().uidFieldMappers());
        return fieldSelector;
    }
}
