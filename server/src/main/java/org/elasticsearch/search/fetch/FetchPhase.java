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
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.lucene.index.SequentialStoredFieldsLeafReader;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.fieldvisitor.CustomFieldsVisitor;
import org.elasticsearch.index.fieldvisitor.FieldsVisitor;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.LeafNestedDocuments;
import org.elasticsearch.search.NestedDocuments;
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
import java.util.function.Function;
import java.util.function.Predicate;

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

        if (context.isCancelled()) {
            throw new TaskCancelledException("cancelled");
        }

        if (context.docIdsToLoadSize() == 0) {
            // no individual hits to process, so we shortcut
            context.fetchResult().hits(new SearchHits(new SearchHit[0], context.queryResult().getTotalHits(),
                context.queryResult().getMaxScore()));
            return;
        }

        DocIdToIndex[] docs = new DocIdToIndex[context.docIdsToLoadSize()];
        for (int index = 0; index < context.docIdsToLoadSize(); index++) {
            docs[index] = new DocIdToIndex(context.docIdsToLoad()[index], index);
        }
        // make sure that we iterate in doc id order
        Arrays.sort(docs);

        Map<String, Set<String>> storedToRequestedFields = new HashMap<>();
        FieldsVisitor fieldsVisitor = createStoredFieldsVisitor(context, storedToRequestedFields);

        FetchContext fetchContext = new FetchContext(context);

        SearchHit[] hits = new SearchHit[context.docIdsToLoadSize()];

        List<FetchSubPhaseProcessor> processors = getProcessors(context.shardTarget(), fetchContext);
        NestedDocuments nestedDocuments = context.getNestedDocuments();

        int currentReaderIndex = -1;
        LeafReaderContext currentReaderContext = null;
        LeafNestedDocuments leafNestedDocuments = null;
        CheckedBiConsumer<Integer, FieldsVisitor, IOException> fieldReader = null;
        boolean hasSequentialDocs = hasSequentialDocs(docs);
        for (int index = 0; index < context.docIdsToLoadSize(); index++) {
            if (context.isCancelled()) {
                throw new TaskCancelledException("cancelled");
            }
            int docId = docs[index].docId;
            try {
                int readerIndex = ReaderUtil.subIndex(docId, context.searcher().getIndexReader().leaves());
                if (currentReaderIndex != readerIndex) {
                    currentReaderContext = context.searcher().getIndexReader().leaves().get(readerIndex);
                    currentReaderIndex = readerIndex;
                    if (currentReaderContext.reader() instanceof SequentialStoredFieldsLeafReader
                            && hasSequentialDocs && docs.length >= 10) {
                        // All the docs to fetch are adjacent but Lucene stored fields are optimized
                        // for random access and don't optimize for sequential access - except for merging.
                        // So we do a little hack here and pretend we're going to do merges in order to
                        // get better sequential access.
                        SequentialStoredFieldsLeafReader lf = (SequentialStoredFieldsLeafReader) currentReaderContext.reader();
                        fieldReader = lf.getSequentialStoredFieldsReader()::visitDocument;
                    } else {
                        fieldReader = currentReaderContext.reader()::document;
                    }
                    for (FetchSubPhaseProcessor processor : processors) {
                        processor.setNextReader(currentReaderContext);
                    }
                    leafNestedDocuments = nestedDocuments.getLeafNestedDocuments(currentReaderContext);
                }
                assert currentReaderContext != null;
                HitContext hit = prepareHitContext(
                    context,
                    leafNestedDocuments,
                    nestedDocuments::hasNonNestedParent,
                    fieldsVisitor,
                    docId,
                    storedToRequestedFields,
                    currentReaderContext,
                    fieldReader);
                for (FetchSubPhaseProcessor processor : processors) {
                    processor.process(hit);
                }
                hits[docs[index].index] = hit.hit();
            } catch (Exception e) {
                throw new FetchPhaseExecutionException(context.shardTarget(), "Error running fetch phase for doc [" + docId + "]", e);
            }
        }
        if (context.isCancelled()) {
            throw new TaskCancelledException("cancelled");
        }

        TotalHits totalHits = context.queryResult().getTotalHits();
        context.fetchResult().hits(new SearchHits(hits, totalHits, context.queryResult().getMaxScore()));

    }

    List<FetchSubPhaseProcessor> getProcessors(SearchShardTarget target, FetchContext context) {
        try {
            List<FetchSubPhaseProcessor> processors = new ArrayList<>();
            for (FetchSubPhase fsp : fetchSubPhases) {
                FetchSubPhaseProcessor processor = fsp.getProcessor(context);
                if (processor != null) {
                    processors.add(processor);
                }
            }
            return processors;
        } catch (Exception e) {
            throw new FetchPhaseExecutionException(target, "Error building fetch sub-phases", e);
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
                QueryShardContext queryShardContext = context.getQueryShardContext();
                Collection<String> fieldNames = queryShardContext.simpleMatchToIndexNames(fieldNameOrPattern);
                for (String fieldName : fieldNames) {
                    MappedFieldType fieldType = queryShardContext.getFieldType(fieldName);
                    if (fieldType == null) {
                        // Only fail if we know it is a object field, missing paths / fields shouldn't fail.
                        if (queryShardContext.getObjectMapper(fieldName) != null) {
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

    private HitContext prepareHitContext(SearchContext context,
                                         LeafNestedDocuments nestedDocuments,
                                         Predicate<String> hasNonNestedParent,
                                         FieldsVisitor fieldsVisitor,
                                         int docId,
                                         Map<String, Set<String>> storedToRequestedFields,
                                         LeafReaderContext subReaderContext,
                                         CheckedBiConsumer<Integer, FieldsVisitor, IOException> storedFieldReader) throws IOException {
        if (nestedDocuments.advance(docId - subReaderContext.docBase) == null) {
            return prepareNonNestedHitContext(
                context, fieldsVisitor, docId, storedToRequestedFields, subReaderContext, storedFieldReader);
        } else {
            return prepareNestedHitContext(context, docId, nestedDocuments, hasNonNestedParent, storedToRequestedFields,
                subReaderContext, storedFieldReader);
        }
    }

    /**
     * Resets the provided {@link HitContext} with information on the current
     * document. This includes the following:
     *   - Adding an initial {@link SearchHit} instance.
     *   - Loading the document source and setting it on {@link HitContext#sourceLookup()}. This
     *     allows fetch subphases that use the hit context to access the preloaded source.
     */
    private HitContext prepareNonNestedHitContext(SearchContext context,
                                                  FieldsVisitor fieldsVisitor,
                                                  int docId,
                                                  Map<String, Set<String>> storedToRequestedFields,
                                                  LeafReaderContext subReaderContext,
                                                  CheckedBiConsumer<Integer, FieldsVisitor, IOException> fieldReader) throws IOException {
        int subDocId = docId - subReaderContext.docBase;
        if (fieldsVisitor == null) {
            SearchHit hit = new SearchHit(docId, null, null, null);
            return new HitContext(hit, subReaderContext, subDocId);
        } else {
            SearchHit hit;
            loadStoredFields(context.getQueryShardContext()::getFieldType, fieldReader, fieldsVisitor, subDocId);
            if (fieldsVisitor.fields().isEmpty() == false) {
                Map<String, DocumentField> docFields = new HashMap<>();
                Map<String, DocumentField> metaFields = new HashMap<>();
                fillDocAndMetaFields(context, fieldsVisitor, storedToRequestedFields, docFields, metaFields);
                hit = new SearchHit(docId, fieldsVisitor.id(), docFields, metaFields);
            } else {
                hit = new SearchHit(docId, fieldsVisitor.id(), emptyMap(), emptyMap());
            }

            HitContext hitContext = new HitContext(hit, subReaderContext, subDocId);
            if (fieldsVisitor.source() != null) {
                // Store the loaded source on the hit context so that fetch subphases can access it.
                // Also make it available to scripts by storing it on the shared SearchLookup instance.
                hitContext.sourceLookup().setSource(fieldsVisitor.source());

                SourceLookup scriptSourceLookup = context.getQueryShardContext().lookup().source();
                scriptSourceLookup.setSegmentAndDocument(subReaderContext, subDocId);
                scriptSourceLookup.setSource(fieldsVisitor.source());
            }
            return hitContext;
        }
    }

    /**
     * Resets the provided {@link HitContext} with information on the current
     * nested document. This includes the following:
     *   - Adding an initial {@link SearchHit} instance.
     *   - Loading the document source, filtering it based on the nested document ID, then
     *     setting it on {@link HitContext#sourceLookup()}. This allows fetch subphases that
     *     use the hit context to access the preloaded source.
     */
    @SuppressWarnings("unchecked")
    private HitContext prepareNestedHitContext(SearchContext context,
                                               int topDocId,
                                               LeafNestedDocuments nestedInfo,
                                               Predicate<String> hasNonNestedParent,
                                               Map<String, Set<String>> storedToRequestedFields,
                                               LeafReaderContext subReaderContext,
                                               CheckedBiConsumer<Integer, FieldsVisitor, IOException> storedFieldReader)
            throws IOException {
        // Also if highlighting is requested on nested documents we need to fetch the _source from the root document,
        // otherwise highlighting will attempt to fetch the _source from the nested doc, which will fail,
        // because the entire _source is only stored with the root document.
        boolean needSource = sourceRequired(context) || context.highlight() != null;

        String rootId;
        Map<String, Object> rootSourceAsMap = null;
        XContentType rootSourceContentType = null;

        QueryShardContext queryShardContext = context.getQueryShardContext();
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
            loadStoredFields(queryShardContext::getFieldType, storedFieldReader, rootFieldsVisitor, nestedInfo.rootDoc());
            rootId = rootFieldsVisitor.id();

            if (needSource) {
                if (rootFieldsVisitor.source() != null) {
                    Tuple<XContentType, Map<String, Object>> tuple = XContentHelper.convertToMap(rootFieldsVisitor.source(), false);
                    rootSourceAsMap = tuple.v2();
                    rootSourceContentType = tuple.v1();
                } else {
                    rootSourceAsMap = Collections.emptyMap();
                }
            }
        }

        Map<String, DocumentField> docFields = emptyMap();
        Map<String, DocumentField> metaFields = emptyMap();
        if (context.hasStoredFields() && !context.storedFieldsContext().fieldNames().isEmpty()) {
            FieldsVisitor nestedFieldsVisitor = new CustomFieldsVisitor(storedToRequestedFields.keySet(), false);
            loadStoredFields(queryShardContext::getFieldType, storedFieldReader, nestedFieldsVisitor, nestedInfo.doc());
            if (nestedFieldsVisitor.fields().isEmpty() == false) {
                docFields = new HashMap<>();
                metaFields = new HashMap<>();
                fillDocAndMetaFields(context, nestedFieldsVisitor, storedToRequestedFields, docFields, metaFields);
            }
        }

        SearchHit.NestedIdentity nestedIdentity = nestedInfo.nestedIdentity();

        SearchHit hit = new SearchHit(topDocId, rootId, nestedIdentity, docFields, metaFields);
        HitContext hitContext = new HitContext(hit, subReaderContext, nestedInfo.doc());

        if (rootSourceAsMap != null && rootSourceAsMap.isEmpty() == false) {
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
                if ((nestedParsedSource.get(0) instanceof Map) == false && hasNonNestedParent.test(nestedPath)) {
                    // When one of the parent objects are not nested then XContentMapValues.extractValue(...) extracts the values
                    // from two or more layers resulting in a list of list being returned. This is because nestedPath
                    // encapsulates two or more object layers in the _source.
                    //
                    // This is why only the first element of nestedParsedSource needs to be checked.
                    throw new IllegalArgumentException("Cannot execute inner hits. One or more parent object fields of nested field [" +
                        nestedPath + "] are not nested. All parent fields need to be nested fields too");
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

    private void loadStoredFields(Function<String, MappedFieldType> fieldTypeLookup,
                                  CheckedBiConsumer<Integer, FieldsVisitor, IOException> fieldReader,
                                  FieldsVisitor fieldVisitor, int docId) throws IOException {
        fieldVisitor.reset();
        fieldReader.accept(docId, fieldVisitor);
        fieldVisitor.postProcess(fieldTypeLookup);
    }

    private static void fillDocAndMetaFields(SearchContext context, FieldsVisitor fieldsVisitor,
            Map<String, Set<String>> storedToRequestedFields,
            Map<String, DocumentField> docFields, Map<String, DocumentField> metaFields) {
        for (Map.Entry<String, List<Object>> entry : fieldsVisitor.fields().entrySet()) {
            String storedField = entry.getKey();
            List<Object> storedValues = entry.getValue();
            if (storedToRequestedFields.containsKey(storedField)) {
                for (String requestedField : storedToRequestedFields.get(storedField)) {
                    if (context.getQueryShardContext().isMetadataField(requestedField)) {
                        metaFields.put(requestedField, new DocumentField(requestedField, storedValues));
                    } else {
                        docFields.put(requestedField, new DocumentField(requestedField, storedValues));
                    }
                }
            } else {
                if (context.getQueryShardContext().isMetadataField(storedField)) {
                    metaFields.put(storedField, new DocumentField(storedField, storedValues));
                } else {
                    docFields.put(storedField, new DocumentField(storedField, storedValues));
                }
            }
        }
    }

    /**
     * Returns <code>true</code> if the provided <code>docs</code> are
     * stored sequentially (Dn = Dn-1 + 1).
     */
    static boolean hasSequentialDocs(DocIdToIndex[] docs) {
        return docs.length > 0 && docs[docs.length-1].docId - docs[0].docId == docs.length - 1;
    }
}
