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

package org.elasticsearch.search.fetch;

import com.google.common.collect.ImmutableMap;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.text.StringAndBytesText;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.index.fieldvisitor.*;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.FieldMappers;
import org.elasticsearch.index.mapper.internal.SourceFieldMapper;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.SearchParseElement;
import org.elasticsearch.search.SearchPhase;
import org.elasticsearch.search.fetch.explain.ExplainFetchSubPhase;
import org.elasticsearch.search.fetch.fielddata.FieldDataFieldsFetchSubPhase;
import org.elasticsearch.search.fetch.matchedqueries.MatchedQueriesFetchSubPhase;
import org.elasticsearch.search.fetch.partial.PartialFieldsFetchSubPhase;
import org.elasticsearch.search.fetch.script.ScriptFieldsFetchSubPhase;
import org.elasticsearch.search.fetch.source.FetchSourceContext;
import org.elasticsearch.search.fetch.source.FetchSourceSubPhase;
import org.elasticsearch.search.fetch.version.VersionFetchSubPhase;
import org.elasticsearch.search.highlight.HighlightPhase;
import org.elasticsearch.search.internal.InternalSearchHit;
import org.elasticsearch.search.internal.InternalSearchHitField;
import org.elasticsearch.search.internal.InternalSearchHits;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.*;

import static com.google.common.collect.Lists.newArrayList;

/**
 *
 */
public class FetchPhase implements SearchPhase {

    private final FetchSubPhase[] fetchSubPhases;

    @Inject
    public FetchPhase(HighlightPhase highlightPhase, ScriptFieldsFetchSubPhase scriptFieldsPhase, PartialFieldsFetchSubPhase partialFieldsPhase,
                      MatchedQueriesFetchSubPhase matchedQueriesPhase, ExplainFetchSubPhase explainPhase, VersionFetchSubPhase versionPhase,
                      FetchSourceSubPhase fetchSourceSubPhase, FieldDataFieldsFetchSubPhase fieldDataFieldsFetchSubPhase) {
        this.fetchSubPhases = new FetchSubPhase[]{scriptFieldsPhase, partialFieldsPhase, matchedQueriesPhase, explainPhase, highlightPhase,
                fetchSourceSubPhase, versionPhase, fieldDataFieldsFetchSubPhase};
    }

    @Override
    public Map<String, ? extends SearchParseElement> parseElements() {
        ImmutableMap.Builder<String, SearchParseElement> parseElements = ImmutableMap.builder();
        parseElements.put("fields", new FieldsParseElement());
        for (FetchSubPhase fetchSubPhase : fetchSubPhases) {
            parseElements.putAll(fetchSubPhase.parseElements());
        }
        return parseElements.build();
    }

    @Override
    public void preProcess(SearchContext context) {
    }

    public void execute(SearchContext context) {
        FieldsVisitor fieldsVisitor;
        List<String> extractFieldNames = null;

        if (!context.hasFieldNames()) {
            if (context.hasPartialFields()) {
                // partial fields need the source, so fetch it
                fieldsVisitor = new UidAndSourceFieldsVisitor();
            } else {
                // no fields specified, default to return source if no explicit indication
                if (!context.hasScriptFields() && !context.hasFetchSourceContext()) {
                    context.fetchSourceContext(new FetchSourceContext(true));
                }
                fieldsVisitor = context.sourceRequested() ? new UidAndSourceFieldsVisitor() : new JustUidFieldsVisitor();
            }
        } else if (context.fieldNames().isEmpty()) {
            if (context.sourceRequested()) {
                fieldsVisitor = new UidAndSourceFieldsVisitor();
            } else {
                fieldsVisitor = new JustUidFieldsVisitor();
            }
        } else {
            boolean loadAllStored = false;
            Set<String> fieldNames = null;
            for (String fieldName : context.fieldNames()) {
                if (fieldName.equals("*")) {
                    loadAllStored = true;
                    continue;
                }
                if (fieldName.equals(SourceFieldMapper.NAME)) {
                    if (context.hasFetchSourceContext()) {
                        context.fetchSourceContext().fetchSource(true);
                    } else {
                        context.fetchSourceContext(new FetchSourceContext(true));
                    }
                    continue;
                }
                FieldMappers x = context.smartNameFieldMappers(fieldName);
                if (x == null) {
                    // Only fail if we know it is a object field, missing paths / fields shouldn't fail.
                    if (context.smartNameObjectMapper(fieldName) != null) {
                        throw new ElasticsearchIllegalArgumentException("field [" + fieldName + "] isn't a leaf field");
                    }
                } else if (x.mapper().fieldType().stored()) {
                    if (fieldNames == null) {
                        fieldNames = new HashSet<>();
                    }
                    fieldNames.add(x.mapper().names().indexName());
                } else {
                    if (extractFieldNames == null) {
                        extractFieldNames = newArrayList();
                    }
                    extractFieldNames.add(fieldName);
                }
            }
            if (loadAllStored) {
                fieldsVisitor = new AllFieldsVisitor(); // load everything, including _source
            } else if (fieldNames != null) {
                boolean loadSource = extractFieldNames != null || context.sourceRequested();
                fieldsVisitor = new CustomFieldsVisitor(fieldNames, loadSource);
            } else if (extractFieldNames != null || context.sourceRequested()) {
                fieldsVisitor = new UidAndSourceFieldsVisitor();
            } else {
                fieldsVisitor = new JustUidFieldsVisitor();
            }
        }

        InternalSearchHit[] hits = new InternalSearchHit[context.docIdsToLoadSize()];
        FetchSubPhase.HitContext hitContext = new FetchSubPhase.HitContext();
        for (int index = 0; index < context.docIdsToLoadSize(); index++) {
            int docId = context.docIdsToLoad()[context.docIdsToLoadFrom() + index];

            loadStoredFields(context, fieldsVisitor, docId);
            fieldsVisitor.postProcess(context.mapperService());

            Map<String, SearchHitField> searchFields = null;
            if (!fieldsVisitor.fields().isEmpty()) {
                searchFields = new HashMap<>(fieldsVisitor.fields().size());
                for (Map.Entry<String, List<Object>> entry : fieldsVisitor.fields().entrySet()) {
                    searchFields.put(entry.getKey(), new InternalSearchHitField(entry.getKey(), entry.getValue()));
                }
            }

            DocumentMapper documentMapper = context.mapperService().documentMapper(fieldsVisitor.uid().type());
            Text typeText;
            if (documentMapper == null) {
                typeText = new StringAndBytesText(fieldsVisitor.uid().type());
            } else {
                typeText = documentMapper.typeText();
            }
            InternalSearchHit searchHit = new InternalSearchHit(docId, fieldsVisitor.uid().id(), typeText, searchFields);

            hits[index] = searchHit;

            int readerIndex = ReaderUtil.subIndex(docId, context.searcher().getIndexReader().leaves());
            AtomicReaderContext subReaderContext = context.searcher().getIndexReader().leaves().get(readerIndex);
            int subDoc = docId - subReaderContext.docBase;

            // go over and extract fields that are not mapped / stored
            context.lookup().setNextReader(subReaderContext);
            context.lookup().setNextDocId(subDoc);
            if (fieldsVisitor.source() != null) {
                context.lookup().source().setNextSource(fieldsVisitor.source());
            }
            if (extractFieldNames != null) {
                for (String extractFieldName : extractFieldNames) {
                    List<Object> values = context.lookup().source().extractRawValues(extractFieldName);
                    if (!values.isEmpty()) {
                        if (searchHit.fieldsOrNull() == null) {
                            searchHit.fields(new HashMap<String, SearchHitField>(2));
                        }

                        SearchHitField hitField = searchHit.fields().get(extractFieldName);
                        if (hitField == null) {
                            hitField = new InternalSearchHitField(extractFieldName, new ArrayList<>(2));
                            searchHit.fields().put(extractFieldName, hitField);
                        }
                        for (Object value : values) {
                            hitField.values().add(value);
                        }
                    }
                }
            }

            hitContext.reset(searchHit, subReaderContext, subDoc, context.searcher().getIndexReader(), docId, fieldsVisitor);
            for (FetchSubPhase fetchSubPhase : fetchSubPhases) {
                if (fetchSubPhase.hitExecutionNeeded(context)) {
                    fetchSubPhase.hitExecute(context, hitContext);
                }
            }
        }

        for (FetchSubPhase fetchSubPhase : fetchSubPhases) {
            if (fetchSubPhase.hitsExecutionNeeded(context)) {
                fetchSubPhase.hitsExecute(context, hits);
            }
        }

        context.fetchResult().hits(new InternalSearchHits(hits, context.queryResult().topDocs().totalHits, context.queryResult().topDocs().getMaxScore()));
    }

    private void loadStoredFields(SearchContext context, FieldsVisitor fieldVisitor, int docId) {
        fieldVisitor.reset();
        try {
            context.searcher().doc(docId, fieldVisitor);
        } catch (IOException e) {
            throw new FetchPhaseExecutionException(context, "Failed to fetch doc id [" + docId + "]", e);
        }
    }
}
