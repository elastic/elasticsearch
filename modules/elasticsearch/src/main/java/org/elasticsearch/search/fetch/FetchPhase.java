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

import org.apache.lucene.document.Document;
import org.apache.lucene.document.FieldSelector;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.Filter;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.docset.DocSet;
import org.elasticsearch.common.thread.ThreadLocals;
import org.elasticsearch.index.mapper.*;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.SearchParseElement;
import org.elasticsearch.search.SearchPhase;
import org.elasticsearch.search.fetch.script.ScriptFieldsContext;
import org.elasticsearch.search.fetch.script.ScriptFieldsParseElement;
import org.elasticsearch.search.highlight.HighlightPhase;
import org.elasticsearch.search.internal.InternalSearchHit;
import org.elasticsearch.search.internal.InternalSearchHitField;
import org.elasticsearch.search.internal.InternalSearchHits;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author kimchy (shay.banon)
 */
public class FetchPhase implements SearchPhase {

    private static ThreadLocal<ThreadLocals.CleanableValue<Map<String, Object>>> cachedSameDocScriptCache = new ThreadLocal<ThreadLocals.CleanableValue<Map<String, Object>>>() {
        @Override protected ThreadLocals.CleanableValue<Map<String, Object>> initialValue() {
            return new ThreadLocals.CleanableValue<java.util.Map<java.lang.String, java.lang.Object>>(new HashMap<String, Object>());
        }
    };

    private final HighlightPhase highlightPhase;

    @Inject public FetchPhase(HighlightPhase highlightPhase) {
        this.highlightPhase = highlightPhase;
    }

    @Override public Map<String, ? extends SearchParseElement> parseElements() {
        ImmutableMap.Builder<String, SearchParseElement> parseElements = ImmutableMap.builder();
        parseElements.put("explain", new ExplainParseElement())
                .put("fields", new FieldsParseElement())
                .put("script_fields", new ScriptFieldsParseElement())
                .put("scriptFields", new ScriptFieldsParseElement())
                .putAll(highlightPhase.parseElements());
        return parseElements.build();
    }

    @Override public void preProcess(SearchContext context) {
        highlightPhase.preProcess(context);
    }

    public void execute(SearchContext context) {
        FieldSelector fieldSelector = buildFieldSelectors(context);

        Map<String, Object> sameDocCache = cachedSameDocScriptCache.get().get();

        InternalSearchHit[] hits = new InternalSearchHit[context.docIdsToLoadSize()];
        for (int index = 0; index < context.docIdsToLoadSize(); index++) {
            int docId = context.docIdsToLoad()[context.docIdsToLoadFrom() + index];
            Document doc = loadDocument(context, fieldSelector, docId);
            Uid uid = extractUid(context, doc);

            DocumentMapper documentMapper = context.mapperService().type(uid.type());

            byte[] source = extractSource(doc, documentMapper);

            InternalSearchHit searchHit = new InternalSearchHit(docId, uid.id(), uid.type(), source, null);
            hits[index] = searchHit;

            for (Object oField : doc.getFields()) {
                Fieldable field = (Fieldable) oField;
                String name = field.name();

                // ignore UID, we handled it above
                if (name.equals(UidFieldMapper.NAME)) {
                    continue;
                }

                // ignore source, we handled it above
                if (name.equals(SourceFieldMapper.NAME)) {
                    continue;
                }

                Object value = null;
                FieldMappers fieldMappers = documentMapper.mappers().indexName(field.name());
                if (fieldMappers != null) {
                    FieldMapper mapper = fieldMappers.mapper();
                    if (mapper != null) {
                        name = mapper.names().fullName();
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

            if (context.hasScriptFields()) {
                sameDocCache.clear();
                int readerIndex = context.searcher().readerIndex(docId);
                IndexReader subReader = context.searcher().subReaders()[readerIndex];
                int subDoc = docId - context.searcher().docStarts()[readerIndex];
                for (ScriptFieldsContext.ScriptField scriptField : context.scriptFields().fields()) {
                    scriptField.scriptFieldsFunction().setNextReader(subReader);

                    Object value = scriptField.scriptFieldsFunction().execute(subDoc, scriptField.params(), sameDocCache);

                    if (searchHit.fields() == null) {
                        searchHit.fields(new HashMap<String, SearchHitField>(2));
                    }

                    SearchHitField hitField = searchHit.fields().get(scriptField.name());
                    if (hitField == null) {
                        hitField = new InternalSearchHitField(scriptField.name(), new ArrayList<Object>(2));
                        searchHit.fields().put(scriptField.name(), hitField);
                    }
                    hitField.values().add(value);
                }
                sameDocCache.clear();
            }

            if (!context.parsedQuery().namedFilters().isEmpty()) {
                int readerIndex = context.searcher().readerIndex(docId);
                IndexReader subReader = context.searcher().subReaders()[readerIndex];
                int subDoc = docId - context.searcher().docStarts()[readerIndex];
                List<String> matchedFilters = Lists.newArrayListWithCapacity(2);
                for (Map.Entry<String, Filter> entry : context.parsedQuery().namedFilters().entrySet()) {
                    String name = entry.getKey();
                    Filter filter = entry.getValue();
                    filter = context.filterCache().cache(filter);
                    try {
                        DocIdSet docIdSet = filter.getDocIdSet(subReader);
                        if (docIdSet instanceof DocSet && ((DocSet) docIdSet).get(subDoc)) {
                            matchedFilters.add(name);
                        }
                    } catch (IOException e) {
                        // ignore
                    }
                }
                searchHit.matchedFilters(matchedFilters.toArray(new String[matchedFilters.size()]));
            }

            doExplanation(context, docId, searchHit);
        }
        context.fetchResult().hits(new InternalSearchHits(hits, context.queryResult().topDocs().totalHits, context.queryResult().topDocs().getMaxScore()));

        highlightPhase.execute(context);
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

    private byte[] extractSource(Document doc, DocumentMapper documentMapper) {
        Fieldable sourceField = doc.getFieldable(SourceFieldMapper.NAME);
        if (sourceField != null) {
            return documentMapper.sourceMapper().nativeValue(sourceField);
        }
        return null;
    }

    private Uid extractUid(SearchContext context, Document doc) {
        // TODO we might want to use FieldData here to speed things up, so we don't have to load it at all...
        String sUid = doc.get(UidFieldMapper.NAME);
        if (sUid != null) {
            return Uid.createUid(sUid);
        }
        // no type, nothing to do (should not really happen
        throw new FetchPhaseExecutionException(context, "Failed to load uid from the index");
    }

    private Document loadDocument(SearchContext context, FieldSelector fieldSelector, int docId) {
        try {
            return context.searcher().doc(docId, fieldSelector);
        } catch (IOException e) {
            throw new FetchPhaseExecutionException(context, "Failed to fetch doc id [" + docId + "]", e);
        }
    }

    private FieldSelector buildFieldSelectors(SearchContext context) {
        if (context.hasScriptFields() && !context.hasFieldNames()) {
            // we ask for script fields, and no field names, don't load the source
            return UidFieldSelector.INSTANCE;
        }

        if (!context.hasFieldNames()) {
            return new UidAndSourceFieldSelector();
        }

        if (context.fieldNames().isEmpty()) {
            return UidFieldSelector.INSTANCE;
        }

        // asked for all stored fields, just return null so all of them will be loaded
        if (context.fieldNames().get(0).equals("*")) {
            return null;
        }

        FieldMappersFieldSelector fieldSelector = new FieldMappersFieldSelector();
        for (String fieldName : context.fieldNames()) {
            FieldMappers x = context.mapperService().smartNameFieldMappers(fieldName);
            if (x == null) {
                throw new FetchPhaseExecutionException(context, "No mapping for field [" + fieldName + "]");
            }
            fieldSelector.add(x);
        }
        fieldSelector.add(context.mapperService().uidFieldMappers());
        return fieldSelector;
    }
}
