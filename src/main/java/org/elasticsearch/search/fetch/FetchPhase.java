/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
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
import com.google.common.collect.Lists;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.index.IndexReader;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.document.ResetFieldSelector;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.FieldMappers;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.mapper.internal.SourceFieldMapper;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;
import org.elasticsearch.index.mapper.selector.AllButSourceFieldSelector;
import org.elasticsearch.index.mapper.selector.FieldMappersFieldSelector;
import org.elasticsearch.index.mapper.selector.UidAndSourceFieldSelector;
import org.elasticsearch.index.mapper.selector.UidFieldSelector;
import org.elasticsearch.indices.TypeMissingException;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.SearchParseElement;
import org.elasticsearch.search.SearchPhase;
import org.elasticsearch.search.fetch.explain.ExplainFetchSubPhase;
import org.elasticsearch.search.fetch.matchedfilters.MatchedFiltersFetchSubPhase;
import org.elasticsearch.search.fetch.partial.PartialFieldsFetchSubPhase;
import org.elasticsearch.search.fetch.script.ScriptFieldsFetchSubPhase;
import org.elasticsearch.search.fetch.version.VersionFetchSubPhase;
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
 *
 */
public class FetchPhase implements SearchPhase {

    private final FetchSubPhase[] fetchSubPhases;

    @Inject
    public FetchPhase(HighlightPhase highlightPhase, ScriptFieldsFetchSubPhase scriptFieldsPhase, PartialFieldsFetchSubPhase partialFieldsPhase,
                      MatchedFiltersFetchSubPhase matchFiltersPhase, ExplainFetchSubPhase explainPhase, VersionFetchSubPhase versionPhase) {
        this.fetchSubPhases = new FetchSubPhase[]{scriptFieldsPhase, partialFieldsPhase, matchFiltersPhase, explainPhase, highlightPhase, versionPhase};
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
        ResetFieldSelector fieldSelector;
        List<String> extractFieldNames = null;
        boolean sourceRequested = false;
        if (!context.hasFieldNames()) {
            if (context.hasPartialFields()) {
                // partial fields need the source, so fetch it, but don't return it
                fieldSelector = new UidAndSourceFieldSelector();
                sourceRequested = false;
            } else if (context.hasScriptFields()) {
                // we ask for script fields, and no field names, don't load the source
                fieldSelector = UidFieldSelector.INSTANCE;
                sourceRequested = false;
            } else {
                fieldSelector = new UidAndSourceFieldSelector();
                sourceRequested = true;
            }
        } else if (context.fieldNames().isEmpty()) {
            fieldSelector = UidFieldSelector.INSTANCE;
            sourceRequested = false;
        } else {
            boolean loadAllStored = false;
            FieldMappersFieldSelector fieldSelectorMapper = null;
            for (String fieldName : context.fieldNames()) {
                if (fieldName.equals("*")) {
                    loadAllStored = true;
                    continue;
                }
                if (fieldName.equals(SourceFieldMapper.NAME)) {
                    sourceRequested = true;
                    continue;
                }
                FieldMappers x = context.smartNameFieldMappers(fieldName);
                if (x != null && x.mapper().stored()) {
                    if (fieldSelectorMapper == null) {
                        fieldSelectorMapper = new FieldMappersFieldSelector();
                    }
                    fieldSelectorMapper.add(x);
                } else {
                    if (extractFieldNames == null) {
                        extractFieldNames = Lists.newArrayList();
                    }
                    extractFieldNames.add(fieldName);
                }
            }

            if (loadAllStored) {
                if (sourceRequested || extractFieldNames != null) {
                    fieldSelector = null; // load everything, including _source
                } else {
                    fieldSelector = AllButSourceFieldSelector.INSTANCE;
                }
            } else if (fieldSelectorMapper != null) {
                // we are asking specific stored fields, just add the UID and be done
                fieldSelectorMapper.add(UidFieldMapper.NAME);
                if (extractFieldNames != null || sourceRequested) {
                    fieldSelectorMapper.add(SourceFieldMapper.NAME);
                }
                fieldSelector = fieldSelectorMapper;
            } else if (extractFieldNames != null || sourceRequested) {
                fieldSelector = new UidAndSourceFieldSelector();
            } else {
                fieldSelector = UidFieldSelector.INSTANCE;
            }
        }

        InternalSearchHit[] hits = new InternalSearchHit[context.docIdsToLoadSize()];
        for (int index = 0; index < context.docIdsToLoadSize(); index++) {
            int docId = context.docIdsToLoad()[context.docIdsToLoadFrom() + index];
            Document doc = loadDocument(context, fieldSelector, docId);
            Uid uid = extractUid(context, doc, fieldSelector);

            DocumentMapper documentMapper = context.mapperService().documentMapper(uid.type());

            if (documentMapper == null) {
                throw new TypeMissingException(new Index(context.shardTarget().index()), uid.type(), "failed to find type loaded for doc [" + uid.id() + "]");
            }

            byte[] source = extractSource(doc, documentMapper);

            // get the version

            InternalSearchHit searchHit = new InternalSearchHit(docId, uid.id(), uid.type(), sourceRequested ? source : null, null);
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
                        value = new BytesArray(field.getBinaryValue(), field.getBinaryOffset(), field.getBinaryLength());
                    } else {
                        value = field.stringValue();
                    }
                }

                if (searchHit.fieldsOrNull() == null) {
                    searchHit.fields(new HashMap<String, SearchHitField>(2));
                }

                SearchHitField hitField = searchHit.fields().get(name);
                if (hitField == null) {
                    hitField = new InternalSearchHitField(name, new ArrayList<Object>(2));
                    searchHit.fields().put(name, hitField);
                }
                hitField.values().add(value);
            }

            int readerIndex = context.searcher().readerIndex(docId);
            IndexReader subReader = context.searcher().subReaders()[readerIndex];
            int subDoc = docId - context.searcher().docStarts()[readerIndex];

            // go over and extract fields that are not mapped / stored
            context.lookup().setNextReader(subReader);
            context.lookup().setNextDocId(subDoc);
            if (source != null) {
                context.lookup().source().setNextSource(new BytesArray(source));
            }
            if (extractFieldNames != null) {
                for (String extractFieldName : extractFieldNames) {
                    Object value = context.lookup().source().extractValue(extractFieldName);
                    if (value != null) {
                        if (searchHit.fieldsOrNull() == null) {
                            searchHit.fields(new HashMap<String, SearchHitField>(2));
                        }

                        SearchHitField hitField = searchHit.fields().get(extractFieldName);
                        if (hitField == null) {
                            hitField = new InternalSearchHitField(extractFieldName, new ArrayList<Object>(2));
                            searchHit.fields().put(extractFieldName, hitField);
                        }
                        hitField.values().add(value);
                    }
                }
            }

            for (FetchSubPhase fetchSubPhase : fetchSubPhases) {
                FetchSubPhase.HitContext hitContext = new FetchSubPhase.HitContext();
                if (fetchSubPhase.hitExecutionNeeded(context)) {
                    hitContext.reset(searchHit, subReader, subDoc, context.searcher().getIndexReader(), docId, doc);
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

    private byte[] extractSource(Document doc, DocumentMapper documentMapper) {
        Fieldable sourceField = doc.getFieldable(SourceFieldMapper.NAME);
        if (sourceField != null) {
            return documentMapper.sourceMapper().nativeValue(sourceField);
        }
        return null;
    }

    private Uid extractUid(SearchContext context, Document doc, @Nullable ResetFieldSelector fieldSelector) {
        String sUid = doc.get(UidFieldMapper.NAME);
        if (sUid != null) {
            return Uid.createUid(sUid);
        }
        // no type, nothing to do (should not really happen)
        List<String> fieldNames = new ArrayList<String>();
        for (Fieldable field : doc.getFields()) {
            fieldNames.add(field.name());
        }
        throw new FetchPhaseExecutionException(context, "Failed to load uid from the index, missing internal _uid field, current fields in the doc [" + fieldNames + "], selector [" + fieldSelector + "]");
    }

    private Document loadDocument(SearchContext context, @Nullable ResetFieldSelector fieldSelector, int docId) {
        try {
            if (fieldSelector != null) fieldSelector.reset();
            return context.searcher().doc(docId, fieldSelector);
        } catch (IOException e) {
            throw new FetchPhaseExecutionException(context, "Failed to fetch doc id [" + docId + "]", e);
        }
    }
}
