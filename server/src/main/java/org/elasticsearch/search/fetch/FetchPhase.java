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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.BitSet;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.fieldvisitor.CustomFieldsVisitor;
import org.elasticsearch.index.fieldvisitor.FieldsVisitor;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.ObjectMapper;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchPhase;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.fetch.subphase.InnerHitsContext;
import org.elasticsearch.search.fetch.subphase.InnerHitsPhase;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.lookup.SourceLookup;
import org.elasticsearch.tasks.TaskCancelledException;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.emptyMap;

/**
 * Fetch phase of a search request, used to fetch the actual top matching documents to be returned to the client, identified
 * after reducing all of the matches returned by the query phase
 */
public class FetchPhase implements SearchPhase {
    private static final Logger LOGGER = LogManager.getLogger(FetchPhase.class);

    private final FetchSubPhase[] fetchSubPhases;

    public FetchPhase(List<FetchSubPhase> fetchSubPhases) {
        this.fetchSubPhases = fetchSubPhases.toArray(new FetchSubPhase[fetchSubPhases.size() + 1]);
        this.fetchSubPhases[fetchSubPhases.size()] = new InnerHitsPhase(this);
    }

    @Override
    public void preProcess(SearchContext context) {
    }

    @Override
    public void execute(SearchContext context) {

        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("{}", new SearchContextSourcePrinter(context));
        }

        final FieldsVisitor fieldsVisitor;
        Map<String, Set<String>> storedToRequestedFields = new HashMap<>();
        StoredFieldsContext storedFieldsContext = context.storedFieldsContext();

        if (storedFieldsContext == null) {
            // no fields specified, default to return source if no explicit indication
            if (!context.hasScriptFields() && !context.hasFetchSourceContext()) {
                context.fetchSourceContext(new FetchSourceContext(true));
            }
            boolean loadSource = context.sourceRequested() || context.fetchFieldsContext() != null;
            fieldsVisitor = new FieldsVisitor(loadSource);
        } else if (storedFieldsContext.fetchFields() == false) {
            // disable stored fields entirely
            fieldsVisitor = null;
        } else {
            for (String fieldNameOrPattern : context.storedFieldsContext().fieldNames()) {
                if (fieldNameOrPattern.equals(SourceFieldMapper.NAME)) {
                    FetchSourceContext fetchSourceContext = context.hasFetchSourceContext() ? context.fetchSourceContext()
                        : FetchSourceContext.FETCH_SOURCE;
                    context.fetchSourceContext(new FetchSourceContext(true, fetchSourceContext.includes(), fetchSourceContext.excludes()));
                    continue;
                }

                Collection<String> fieldNames = context.mapperService().simpleMatchToFullName(fieldNameOrPattern);
                for (String fieldName : fieldNames) {
                    MappedFieldType fieldType = context.fieldType(fieldName);
                    if (fieldType == null) {
                        // Only fail if we know it is a object field, missing paths / fields shouldn't fail.
                        if (context.getObjectMapper(fieldName) != null) {
                            throw new IllegalArgumentException("field [" + fieldName + "] isn't a leaf field");
                        }
                    } else {
                        String storedField = fieldType.name();
                        Set<String> requestedFields = storedToRequestedFields.computeIfAbsent(
                            storedField, key -> new HashSet<>());
                        requestedFields.add(fieldName);
                    }
                }
            }
            boolean loadSource = context.sourceRequested() || context.fetchFieldsContext() != null;
            if (storedToRequestedFields.isEmpty()) {
                // empty list specified, default to disable _source if no explicit indication
                fieldsVisitor = new FieldsVisitor(loadSource);
            } else {
                fieldsVisitor = new CustomFieldsVisitor(storedToRequestedFields.keySet(), loadSource);
            }
        }

        try {
            DocIdToIndex[] docs = new DocIdToIndex[context.docIdsToLoadSize()];
            for (int index = 0; index < context.docIdsToLoadSize(); index++) {
                docs[index] = new DocIdToIndex(context.docIdsToLoad()[context.docIdsToLoadFrom() + index], index);
            }
            Arrays.sort(docs);

            SearchHit[] hits = new SearchHit[context.docIdsToLoadSize()];
            SearchHit[] sortedHits = new SearchHit[context.docIdsToLoadSize()];
            FetchSubPhase.HitContext hitContext = new FetchSubPhase.HitContext();
            for (int index = 0; index < context.docIdsToLoadSize(); index++) {
                if (context.isCancelled()) {
                    throw new TaskCancelledException("cancelled");
                }
                int docId = docs[index].docId;
                int readerIndex = ReaderUtil.subIndex(docId, context.searcher().getIndexReader().leaves());
                LeafReaderContext subReaderContext = context.searcher().getIndexReader().leaves().get(readerIndex);
                int subDocId = docId - subReaderContext.docBase;

                final SearchHit searchHit;
                int rootDocId = findRootDocumentIfNested(context, subReaderContext, subDocId);
                if (rootDocId != -1) {
                    searchHit = createNestedSearchHit(context, docId, subDocId, rootDocId,
                        storedToRequestedFields, subReaderContext);
                } else {
                    searchHit = createSearchHit(context, fieldsVisitor, docId, subDocId,
                        storedToRequestedFields, subReaderContext);
                }

                sortedHits[index] = searchHit;
                hits[docs[index].index] = searchHit;
                hitContext.reset(searchHit, subReaderContext, subDocId, context.searcher());
                for (FetchSubPhase fetchSubPhase : fetchSubPhases) {
                    fetchSubPhase.hitExecute(context, hitContext);
                }
            }
            if (context.isCancelled()) {
                throw new TaskCancelledException("cancelled");
            }

            for (FetchSubPhase fetchSubPhase : fetchSubPhases) {
                fetchSubPhase.hitsExecute(context, sortedHits);
                if (context.isCancelled()) {
                    throw new TaskCancelledException("cancelled");
                }
            }

            TotalHits totalHits = context.queryResult().getTotalHits();
            context.fetchResult().hits(new SearchHits(hits, totalHits, context.queryResult().getMaxScore()));
        } catch (IOException e) {
            throw ExceptionsHelper.convertToElastic(e);
        }
    }

    static class DocIdToIndex implements Comparable<DocIdToIndex> {
        final int docId;
        final int index;

        DocIdToIndex(int docId, int index) {
            this.docId = docId;
            this.index = index;
        }

        @Override
        public int compareTo(DocIdToIndex o) {
            return Integer.compare(docId, o.docId);
        }
    }

    private int findRootDocumentIfNested(SearchContext context, LeafReaderContext subReaderContext, int subDocId) throws IOException {
        if (context.mapperService().hasNested()) {
            BitSet bits = context.bitsetFilterCache()
                .getBitSetProducer(Queries.newNonNestedFilter())
                .getBitSet(subReaderContext);
            if (!bits.get(subDocId)) {
                return bits.nextSetBit(subDocId);
            }
        }
        return -1;
    }

    private SearchHit createSearchHit(SearchContext context,
                                      FieldsVisitor fieldsVisitor,
                                      int docId,
                                      int subDocId,
                                      Map<String, Set<String>> storedToRequestedFields,
                                      LeafReaderContext subReaderContext) {
        if (fieldsVisitor == null) {
            return new SearchHit(docId, null, null, null);
        }
        loadStoredFields(context.shardTarget(), subReaderContext, fieldsVisitor, subDocId);
        fieldsVisitor.postProcess(context.mapperService());
        SearchHit searchHit;
        if (fieldsVisitor.fields().isEmpty() == false) {
            Map<String, DocumentField> docFields = new HashMap<>();
            Map<String, DocumentField> metaFields = new HashMap<>();
            fillDocAndMetaFields(context, fieldsVisitor, storedToRequestedFields, docFields, metaFields);
            searchHit = new SearchHit(docId, fieldsVisitor.id(), docFields, metaFields);
        } else {
            searchHit = new SearchHit(docId, fieldsVisitor.id(), emptyMap(), emptyMap());
        }
        // Set _source if requested.
        SourceLookup sourceLookup = context.lookup().source();
        sourceLookup.setSegmentAndDocument(subReaderContext, subDocId);
        if (fieldsVisitor.source() != null) {
            sourceLookup.setSource(fieldsVisitor.source());
        }
        return searchHit;
    }

    @SuppressWarnings("unchecked")
    private SearchHit createNestedSearchHit(SearchContext context,
                                            int nestedTopDocId,
                                            int nestedSubDocId,
                                            int rootSubDocId,
                                            Map<String, Set<String>> storedToRequestedFields,
                                            LeafReaderContext subReaderContext) throws IOException {
        // Also if highlighting is requested on nested documents we need to fetch the _source from the root document,
        // otherwise highlighting will attempt to fetch the _source from the nested doc, which will fail,
        // because the entire _source is only stored with the root document.
        final String id;
        final BytesReference source;
        final boolean needSource = context.sourceRequested() || context.highlight() != null;
        if (needSource || (context instanceof InnerHitsContext.InnerHitSubContext == false)) {
            FieldsVisitor rootFieldsVisitor = new FieldsVisitor(needSource);
            loadStoredFields(context.shardTarget(), subReaderContext, rootFieldsVisitor, rootSubDocId);
            rootFieldsVisitor.postProcess(context.mapperService());
            id = rootFieldsVisitor.id();
            source = rootFieldsVisitor.source();
        } else {
            // In case of nested inner hits we already know the uid, so no need to fetch it from stored fields again!
            id = ((InnerHitsContext.InnerHitSubContext) context).getId();
            source = null;
        }

        Map<String, DocumentField> docFields = emptyMap();
        Map<String, DocumentField> metaFields = emptyMap();
        if (context.hasStoredFields() && !context.storedFieldsContext().fieldNames().isEmpty()) {
            FieldsVisitor nestedFieldsVisitor = new CustomFieldsVisitor(storedToRequestedFields.keySet(), false);
            loadStoredFields(context.shardTarget(), subReaderContext, nestedFieldsVisitor, nestedSubDocId);
            nestedFieldsVisitor.postProcess(context.mapperService());
            if (nestedFieldsVisitor.fields().isEmpty() == false) {
                docFields = new HashMap<>();
                metaFields = new HashMap<>();
                fillDocAndMetaFields(context, nestedFieldsVisitor, storedToRequestedFields, docFields, metaFields);
            }
        }

        DocumentMapper documentMapper = context.mapperService().documentMapper();
        SourceLookup sourceLookup = context.lookup().source();
        sourceLookup.setSegmentAndDocument(subReaderContext, nestedSubDocId);

        ObjectMapper nestedObjectMapper = documentMapper.findNestedObjectMapper(nestedSubDocId, context, subReaderContext);
        assert nestedObjectMapper != null;
        SearchHit.NestedIdentity nestedIdentity =
                getInternalNestedIdentity(context, nestedSubDocId, subReaderContext, context.mapperService(), nestedObjectMapper);

        if (source != null) {
            Tuple<XContentType, Map<String, Object>> tuple = XContentHelper.convertToMap(source, true);
            Map<String, Object> sourceAsMap = tuple.v2();

            // Isolate the nested json array object that matches with nested hit and wrap it back into the same json
            // structure with the nested json array object being the actual content. The latter is important, so that
            // features like source filtering and highlighting work consistent regardless of whether the field points
            // to a json object array for consistency reasons on how we refer to fields
            Map<String, Object> nestedSourceAsMap = new HashMap<>();
            Map<String, Object> current = nestedSourceAsMap;
            for (SearchHit.NestedIdentity nested = nestedIdentity; nested != null; nested = nested.getChild()) {
                String nestedPath = nested.getField().string();
                current.put(nestedPath, new HashMap<>());
                Object extractedValue = XContentMapValues.extractValue(nestedPath, sourceAsMap);
                List<?> nestedParsedSource;
                if (extractedValue instanceof List) {
                    // nested field has an array value in the _source
                    nestedParsedSource = (List<?>) extractedValue;
                } else if (extractedValue instanceof Map) {
                    // nested field has an object value in the _source. This just means the nested field has just one inner object,
                    // which is valid, but uncommon.
                    nestedParsedSource = Collections.singletonList(extractedValue);
                } else {
                    throw new IllegalStateException("extracted source isn't an object or an array");
                }
                if ((nestedParsedSource.get(0) instanceof Map) == false &&
                    nestedObjectMapper.parentObjectMapperAreNested(context.mapperService()) == false) {
                    // When one of the parent objects are not nested then XContentMapValues.extractValue(...) extracts the values
                    // from two or more layers resulting in a list of list being returned. This is because nestedPath
                    // encapsulates two or more object layers in the _source.
                    //
                    // This is why only the first element of nestedParsedSource needs to be checked.
                    throw new IllegalArgumentException("Cannot execute inner hits. One or more parent object fields of nested field [" +
                        nestedObjectMapper.name() + "] are not nested. All parent fields need to be nested fields too");
                }
                sourceAsMap = (Map<String, Object>) nestedParsedSource.get(nested.getOffset());
                if (nested.getChild() == null) {
                    current.put(nestedPath, sourceAsMap);
                } else {
                    Map<String, Object> next = new HashMap<>();
                    current.put(nestedPath, next);
                    current = next;
                }
            }
            context.lookup().source().setSource(nestedSourceAsMap);
            XContentType contentType = tuple.v1();
            context.lookup().source().setSourceContentType(contentType);
        }
        return new SearchHit(nestedTopDocId, id, nestedIdentity, docFields, metaFields);
    }

    private SearchHit.NestedIdentity getInternalNestedIdentity(SearchContext context, int nestedSubDocId,
                                                               LeafReaderContext subReaderContext,
                                                               MapperService mapperService,
                                                               ObjectMapper nestedObjectMapper) throws IOException {
        int currentParent = nestedSubDocId;
        ObjectMapper nestedParentObjectMapper;
        ObjectMapper current = nestedObjectMapper;
        String originalName = nestedObjectMapper.name();
        SearchHit.NestedIdentity nestedIdentity = null;
        do {
            Query parentFilter;
            nestedParentObjectMapper = current.getParentObjectMapper(mapperService);
            if (nestedParentObjectMapper != null) {
                if (nestedParentObjectMapper.nested().isNested() == false) {
                    current = nestedParentObjectMapper;
                    continue;
                }
                parentFilter = nestedParentObjectMapper.nestedTypeFilter();
            } else {
                parentFilter = Queries.newNonNestedFilter();
            }

            Query childFilter = nestedObjectMapper.nestedTypeFilter();
            if (childFilter == null) {
                current = nestedParentObjectMapper;
                continue;
            }
            final Weight childWeight = context.searcher()
                .createWeight(context.searcher().rewrite(childFilter), ScoreMode.COMPLETE_NO_SCORES, 1f);
            Scorer childScorer = childWeight.scorer(subReaderContext);
            if (childScorer == null) {
                current = nestedParentObjectMapper;
                continue;
            }
            DocIdSetIterator childIter = childScorer.iterator();

            BitSet parentBits = context.bitsetFilterCache().getBitSetProducer(parentFilter).getBitSet(subReaderContext);

            int offset = 0;
            /*
             * Starts from the previous parent and finds the offset of the
             * <code>nestedSubDocID</code> within the nested children. Nested documents
             * are indexed in the same order than in the source array so the offset
             * of the nested child is the number of nested document with the same parent
             * that appear before him.
             */
            int previousParent = parentBits.prevSetBit(currentParent);
            for (int docId = childIter.advance(previousParent + 1); docId < nestedSubDocId && docId != DocIdSetIterator.NO_MORE_DOCS;
                 docId = childIter.nextDoc()) {
                offset++;
            }
            currentParent = nestedSubDocId;
            current = nestedObjectMapper = nestedParentObjectMapper;
            int currentPrefix = current == null ? 0 : current.name().length() + 1;
            nestedIdentity = new SearchHit.NestedIdentity(originalName.substring(currentPrefix), offset, nestedIdentity);
            if (current != null) {
                originalName = current.name();
            }
        } while (current != null);
        return nestedIdentity;
    }

    private void loadStoredFields(SearchShardTarget shardTarget, LeafReaderContext readerContext, FieldsVisitor fieldVisitor, int docId) {
        fieldVisitor.reset();
        try {
            readerContext.reader().document(docId, fieldVisitor);
        } catch (IOException e) {
            throw new FetchPhaseExecutionException(shardTarget, "Failed to fetch doc id [" + docId + "]", e);
        }
    }

    private static void fillDocAndMetaFields(SearchContext context, FieldsVisitor fieldsVisitor,
            Map<String, Set<String>> storedToRequestedFields,
            Map<String, DocumentField> docFields, Map<String, DocumentField> metaFields) {
        for (Map.Entry<String, List<Object>> entry : fieldsVisitor.fields().entrySet()) {
            String storedField = entry.getKey();
            List<Object> storedValues = entry.getValue();
            if (storedToRequestedFields.containsKey(storedField)) {
                for (String requestedField : storedToRequestedFields.get(storedField)) {
                    if (context.mapperService().isMetadataField(requestedField)) {
                        metaFields.put(requestedField, new DocumentField(requestedField, storedValues));
                    } else {
                        docFields.put(requestedField, new DocumentField(requestedField, storedValues));
                    }
                }
            } else {
                if (context.mapperService().isMetadataField(storedField)) {
                    metaFields.put(storedField, new DocumentField(storedField, storedValues));
                } else {
                    docFields.put(storedField, new DocumentField(storedField, storedValues));
                }
            }
        }
    }
}
