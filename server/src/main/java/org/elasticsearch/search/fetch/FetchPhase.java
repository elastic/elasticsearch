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
import org.elasticsearch.search.SearchContextSourcePrinter;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.fetch.FetchSubPhase.HitContext;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.fetch.subphase.InnerHitsContext;
import org.elasticsearch.search.fetch.subphase.InnerHitsPhase;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.lookup.SourceLookup;
import org.elasticsearch.tasks.TaskCancelledException;

import java.io.IOException;
import java.util.ArrayList;
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
public class FetchPhase {
    private static final Logger LOGGER = LogManager.getLogger(FetchPhase.class);

    private final FetchSubPhase[] fetchSubPhases;

    public FetchPhase(List<FetchSubPhase> fetchSubPhases) {
        this.fetchSubPhases = fetchSubPhases.toArray(new FetchSubPhase[fetchSubPhases.size() + 1]);
        this.fetchSubPhases[fetchSubPhases.size()] = new InnerHitsPhase(this);
    }

    public void execute(SearchContext context) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("{}", new SearchContextSourcePrinter(context));
        }

        Map<String, Set<String>> storedToRequestedFields = new HashMap<>();
        FieldsVisitor fieldsVisitor = createStoredFieldsVisitor(context, storedToRequestedFields);

        try {
            DocIdToIndex[] docs = new DocIdToIndex[context.docIdsToLoadSize()];
            for (int index = 0; index < context.docIdsToLoadSize(); index++) {
                docs[index] = new DocIdToIndex(context.docIdsToLoad()[context.docIdsToLoadFrom() + index], index);
            }
            Arrays.sort(docs);

            SearchHit[] hits = new SearchHit[context.docIdsToLoadSize()];
            Map<String, Object> sharedCache = new HashMap<>();

            List<FetchSubPhaseProcessor> processors = new ArrayList<>();
            for (FetchSubPhase fsp : fetchSubPhases) {
                FetchSubPhaseProcessor processor = fsp.getProcessor(context);
                if (processor != null) {
                    processors.add(processor);
                }
            }

            int currentReaderIndex = -1;
            LeafReaderContext currentReaderContext = null;
            for (int index = 0; index < context.docIdsToLoadSize(); index++) {
                if (context.isCancelled()) {
                    throw new TaskCancelledException("cancelled");
                }
                int docId = docs[index].docId;
                int readerIndex = ReaderUtil.subIndex(docId, context.searcher().getIndexReader().leaves());
                if (currentReaderIndex != readerIndex) {
                    currentReaderContext = context.searcher().getIndexReader().leaves().get(readerIndex);
                    currentReaderIndex = readerIndex;
                    for (FetchSubPhaseProcessor processor : processors) {
                        processor.setNextReader(currentReaderContext);
                    }
                }
                assert currentReaderContext != null;

                HitContext hit
                    = prepareHitContext(context, fieldsVisitor, docId, storedToRequestedFields, currentReaderContext, sharedCache);
                for (FetchSubPhaseProcessor processor : processors) {
                    processor.process(hit);
                }
                hits[docs[index].index] = hit.hit();
            }
            if (context.isCancelled()) {
                throw new TaskCancelledException("cancelled");
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

    private FieldsVisitor createStoredFieldsVisitor(SearchContext context, Map<String, Set<String>> storedToRequestedFields) {
        StoredFieldsContext storedFieldsContext = context.storedFieldsContext();

        if (storedFieldsContext == null) {
            // no fields specified, default to return source if no explicit indication
            if (!context.hasScriptFields() && !context.hasFetchSourceContext()) {
                context.fetchSourceContext(new FetchSourceContext(true));
            }
            boolean loadSource = sourceRequired(context);
            return new FieldsVisitor(loadSource);
        } else if (storedFieldsContext.fetchFields() == false) {
            // disable stored fields entirely
            return null;
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
            boolean loadSource = sourceRequired(context);
            if (storedToRequestedFields.isEmpty()) {
                // empty list specified, default to disable _source if no explicit indication
                return new FieldsVisitor(loadSource);
            } else {
                return new CustomFieldsVisitor(storedToRequestedFields.keySet(), loadSource);
            }
        }
    }

    private boolean sourceRequired(SearchContext context) {
        return context.sourceRequested() || context.fetchFieldsContext() != null;
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

    private HitContext prepareHitContext(SearchContext context, FieldsVisitor fieldsVisitor, int docId,
                                         Map<String, Set<String>> storedToRequestedFields,
                                         LeafReaderContext subReaderContext, Map<String, Object> sharedCache) throws IOException {
        int rootDocId = findRootDocumentIfNested(context, subReaderContext, docId - subReaderContext.docBase);
        if (rootDocId == -1) {
            return prepareNonNestedHitContext(context, fieldsVisitor, docId, storedToRequestedFields, subReaderContext, sharedCache);
        } else {
            return prepareNestedHitContext(context, docId, rootDocId, storedToRequestedFields, subReaderContext, sharedCache);
        }
    }

    /**
     * Resets the provided {@link HitContext} with information on the current
     * document. This includes the following:
     *   - Adding an initial {@link SearchHit} instance.
     *   - Loading the document source and setting it on {@link SourceLookup}. This allows
     *     fetch subphases that use the hit context to access the preloaded source.
     */
    private HitContext prepareNonNestedHitContext(SearchContext context,
                                   FieldsVisitor fieldsVisitor,
                                   int docId,
                                   Map<String, Set<String>> storedToRequestedFields,
                                   LeafReaderContext subReaderContext,
                                   Map<String, Object> sharedCache) {
        int subDocId = docId - subReaderContext.docBase;
        if (fieldsVisitor == null) {
            SearchHit hit = new SearchHit(docId, null, null, null);
            return new HitContext(hit, subReaderContext, subDocId, context.searcher(), sharedCache);
        } else {
            SearchHit hit;
            loadStoredFields(context.shardTarget(), context.mapperService(), subReaderContext, fieldsVisitor, subDocId);
            if (fieldsVisitor.fields().isEmpty() == false) {
                Map<String, DocumentField> docFields = new HashMap<>();
                Map<String, DocumentField> metaFields = new HashMap<>();
                fillDocAndMetaFields(context, fieldsVisitor, storedToRequestedFields, docFields, metaFields);
                hit = new SearchHit(docId, fieldsVisitor.id(), docFields, metaFields);
            } else {
                hit = new SearchHit(docId, fieldsVisitor.id(), emptyMap(), emptyMap());
            }

            HitContext hitContext = new HitContext(hit, subReaderContext, subDocId, context.searcher(), sharedCache);
            if (fieldsVisitor.source() != null) {
                hitContext.sourceLookup().setSource(fieldsVisitor.source());
            }
            return hitContext;
        }
    }

    /**
     /**
     * Resets the provided {@link HitContext} with information on the current
     * nested document. This includes the following:
     *   - Adding an initial {@link SearchHit} instance.
     *   - Loading the document source, filtering it based on the nested document ID, then
     *     setting it on {@link SourceLookup}. This allows fetch subphases that use the hit
     *     context to access the preloaded source.
     */
    @SuppressWarnings("unchecked")
    private HitContext prepareNestedHitContext(SearchContext context,
                                               int nestedTopDocId,
                                               int rootDocId,
                                               Map<String, Set<String>> storedToRequestedFields,
                                               LeafReaderContext subReaderContext,
                                               Map<String, Object> sharedCache) throws IOException {
        // Also if highlighting is requested on nested documents we need to fetch the _source from the root document,
        // otherwise highlighting will attempt to fetch the _source from the nested doc, which will fail,
        // because the entire _source is only stored with the root document.
        boolean needSource = sourceRequired(context) || context.highlight() != null;

        String rootId;
        Map<String, Object> rootSourceAsMap = null;
        XContentType rootSourceContentType = null;

        int nestedDocId = nestedTopDocId - subReaderContext.docBase;

        if (context instanceof InnerHitsContext.InnerHitSubContext) {
            InnerHitsContext.InnerHitSubContext innerHitsContext = (InnerHitsContext.InnerHitSubContext) context;
            rootId = innerHitsContext.getRootId();

            if (needSource) {
                SourceLookup rootLookup = innerHitsContext.getRootLookup();
                rootSourceAsMap = rootLookup.loadSourceIfNeeded();
                rootSourceContentType = rootLookup.sourceContentType();
            }
        } else {
            FieldsVisitor rootFieldsVisitor = new FieldsVisitor(needSource);
            loadStoredFields(context.shardTarget(), context.mapperService(), subReaderContext, rootFieldsVisitor, rootDocId);
            rootId = rootFieldsVisitor.id();

            if (needSource) {
                BytesReference rootSource = rootFieldsVisitor.source();
                Tuple<XContentType, Map<String, Object>> tuple = XContentHelper.convertToMap(rootSource, false);
                rootSourceAsMap = tuple.v2();
                rootSourceContentType = tuple.v1();
            }
        }

        Map<String, DocumentField> docFields = emptyMap();
        Map<String, DocumentField> metaFields = emptyMap();
        if (context.hasStoredFields() && !context.storedFieldsContext().fieldNames().isEmpty()) {
            FieldsVisitor nestedFieldsVisitor = new CustomFieldsVisitor(storedToRequestedFields.keySet(), false);
            loadStoredFields(context.shardTarget(), context.mapperService(), subReaderContext, nestedFieldsVisitor, nestedDocId);
            if (nestedFieldsVisitor.fields().isEmpty() == false) {
                docFields = new HashMap<>();
                metaFields = new HashMap<>();
                fillDocAndMetaFields(context, nestedFieldsVisitor, storedToRequestedFields, docFields, metaFields);
            }
        }

        DocumentMapper documentMapper = context.mapperService().documentMapper();
        ObjectMapper nestedObjectMapper
            = documentMapper.findNestedObjectMapper(nestedDocId, context, subReaderContext);
        assert nestedObjectMapper != null;
        SearchHit.NestedIdentity nestedIdentity =
                getInternalNestedIdentity(context, nestedDocId, subReaderContext, context.mapperService(), nestedObjectMapper);

        SearchHit hit = new SearchHit(nestedTopDocId, rootId, nestedIdentity, docFields, metaFields);
        HitContext hitContext = new HitContext(hit, subReaderContext, nestedDocId, context.searcher(), sharedCache);

        if (rootSourceAsMap != null) {
            // Isolate the nested json array object that matches with nested hit and wrap it back into the same json
            // structure with the nested json array object being the actual content. The latter is important, so that
            // features like source filtering and highlighting work consistent regardless of whether the field points
            // to a json object array for consistency reasons on how we refer to fields
            Map<String, Object> nestedSourceAsMap = new HashMap<>();
            Map<String, Object> current = nestedSourceAsMap;
            for (SearchHit.NestedIdentity nested = nestedIdentity; nested != null; nested = nested.getChild()) {
                String nestedPath = nested.getField().string();
                current.put(nestedPath, new HashMap<>());
                Object extractedValue = XContentMapValues.extractValue(nestedPath, rootSourceAsMap);
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
                rootSourceAsMap = (Map<String, Object>) nestedParsedSource.get(nested.getOffset());
                if (nested.getChild() == null) {
                    current.put(nestedPath, rootSourceAsMap);
                } else {
                    Map<String, Object> next = new HashMap<>();
                    current.put(nestedPath, next);
                    current = next;
                }
            }

            hitContext.sourceLookup().setSource(nestedSourceAsMap);
            hitContext.sourceLookup().setSourceContentType(rootSourceContentType);
        }
        return hitContext;
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

    private void loadStoredFields(SearchShardTarget shardTarget,
                                  MapperService mapperService,
                                  LeafReaderContext readerContext,
                                  FieldsVisitor fieldVisitor, int docId) {
        fieldVisitor.reset();
        try {
            readerContext.reader().document(docId, fieldVisitor);
        } catch (IOException e) {
            throw new FetchPhaseExecutionException(shardTarget, "Failed to fetch doc id [" + docId + "]", e);
        }
        fieldVisitor.postProcess(mapperService);
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
