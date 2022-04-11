/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.fetch;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.lucene.index.SequentialStoredFieldsLeafReader;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.fieldvisitor.CustomFieldsVisitor;
import org.elasticsearch.index.fieldvisitor.FieldsVisitor;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.index.query.SearchExecutionContext;
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
import org.elasticsearch.search.profile.ProfileResult;
import org.elasticsearch.search.profile.Profilers;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.xcontent.XContentType;

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
            SearchHits hits = new SearchHits(new SearchHit[0], context.queryResult().getTotalHits(), context.queryResult().getMaxScore());
            context.fetchResult().shardResult(hits, null);
            return;
        }

        Profiler profiler = context.getProfilers() == null ? Profiler.NOOP : Profilers.startProfilingFetchPhase();
        SearchHits hits = null;
        try {
            hits = buildSearchHits(context, profiler);
        } finally {
            // Always finish profiling
            ProfileResult profileResult = profiler.finish();
            // Only set the shardResults if building search hits was successful
            if (hits != null) {
                context.fetchResult().shardResult(hits, profileResult);
            }
        }
    }

    private SearchHits buildSearchHits(SearchContext context, Profiler profiler) {
        DocIdToIndex[] docs = new DocIdToIndex[context.docIdsToLoadSize()];
        for (int index = 0; index < context.docIdsToLoadSize(); index++) {
            docs[index] = new DocIdToIndex(context.docIdsToLoad()[index], index);
        }
        // make sure that we iterate in doc id order
        Arrays.sort(docs);

        Map<String, Set<String>> storedToRequestedFields = new HashMap<>();
        FieldsVisitor fieldsVisitor = createStoredFieldsVisitor(context, storedToRequestedFields);
        profiler.visitor(fieldsVisitor);

        FetchContext fetchContext = new FetchContext(context);

        SearchHit[] hits = new SearchHit[context.docIdsToLoadSize()];

        List<FetchSubPhaseProcessor> processors = getProcessors(context.shardTarget(), fetchContext, profiler);
        NestedDocuments nestedDocuments = context.getSearchExecutionContext().getNestedDocuments();

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
                    profiler.startNextReader();
                    try {
                        currentReaderContext = context.searcher().getIndexReader().leaves().get(readerIndex);
                        currentReaderIndex = readerIndex;
                        if (currentReaderContext.reader()instanceof SequentialStoredFieldsLeafReader lf
                            && hasSequentialDocs
                            && docs.length >= 10) {
                            // All the docs to fetch are adjacent but Lucene stored fields are optimized
                            // for random access and don't optimize for sequential access - except for merging.
                            // So we do a little hack here and pretend we're going to do merges in order to
                            // get better sequential access.
                            fieldReader = lf.getSequentialStoredFieldsReader()::visitDocument;
                        } else {
                            fieldReader = currentReaderContext.reader()::document;
                        }
                        for (FetchSubPhaseProcessor processor : processors) {
                            processor.setNextReader(currentReaderContext);
                        }
                        leafNestedDocuments = nestedDocuments.getLeafNestedDocuments(currentReaderContext);
                    } finally {
                        profiler.stopNextReader();
                    }
                }
                assert currentReaderContext != null;
                HitContext hit = prepareHitContext(
                    context,
                    profiler,
                    leafNestedDocuments,
                    fieldsVisitor,
                    docId,
                    storedToRequestedFields,
                    currentReaderContext,
                    fieldReader
                );
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
        return new SearchHits(hits, totalHits, context.queryResult().getMaxScore());
    }

    List<FetchSubPhaseProcessor> getProcessors(SearchShardTarget target, FetchContext context, Profiler profiler) {
        try {
            List<FetchSubPhaseProcessor> processors = new ArrayList<>();
            for (FetchSubPhase fsp : fetchSubPhases) {
                FetchSubPhaseProcessor processor = fsp.getProcessor(context);
                if (processor != null) {
                    processors.add(profiler.profile(fsp.getClass().getSimpleName(), "", processor));
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

    private static FieldsVisitor createStoredFieldsVisitor(SearchContext context, Map<String, Set<String>> storedToRequestedFields) {
        StoredFieldsContext storedFieldsContext = context.storedFieldsContext();

        if (storedFieldsContext == null) {
            // no fields specified, default to return source if no explicit indication
            if (context.hasScriptFields() == false && context.hasFetchSourceContext() == false) {
                context.fetchSourceContext(FetchSourceContext.FETCH_SOURCE);
            }
            boolean loadSource = sourceRequired(context);
            return new FieldsVisitor(loadSource);
        } else if (storedFieldsContext.fetchFields() == false) {
            // disable stored fields entirely
            return null;
        } else {
            for (String fieldNameOrPattern : context.storedFieldsContext().fieldNames()) {
                if (fieldNameOrPattern.equals(SourceFieldMapper.NAME)) {
                    FetchSourceContext fetchSourceContext = context.hasFetchSourceContext()
                        ? context.fetchSourceContext()
                        : FetchSourceContext.FETCH_SOURCE;
                    context.fetchSourceContext(FetchSourceContext.of(true, fetchSourceContext.includes(), fetchSourceContext.excludes()));
                    continue;
                }
                SearchExecutionContext searchExecutionContext = context.getSearchExecutionContext();
                Collection<String> fieldNames = searchExecutionContext.getMatchingFieldNames(fieldNameOrPattern);
                for (String fieldName : fieldNames) {
                    MappedFieldType fieldType = searchExecutionContext.getFieldType(fieldName);
                    String storedField = fieldType.name();
                    Set<String> requestedFields = storedToRequestedFields.computeIfAbsent(storedField, key -> new HashSet<>());
                    requestedFields.add(fieldName);
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

    private static boolean sourceRequired(SearchContext context) {
        return context.sourceRequested() || context.fetchFieldsContext() != null;
    }

    private static HitContext prepareHitContext(
        SearchContext context,
        Profiler profiler,
        LeafNestedDocuments nestedDocuments,
        FieldsVisitor fieldsVisitor,
        int docId,
        Map<String, Set<String>> storedToRequestedFields,
        LeafReaderContext subReaderContext,
        CheckedBiConsumer<Integer, FieldsVisitor, IOException> storedFieldReader
    ) throws IOException {
        if (nestedDocuments.advance(docId - subReaderContext.docBase) == null) {
            return prepareNonNestedHitContext(
                context,
                profiler,
                fieldsVisitor,
                docId,
                storedToRequestedFields,
                subReaderContext,
                storedFieldReader
            );
        } else {
            return prepareNestedHitContext(
                context,
                profiler,
                docId,
                nestedDocuments,
                storedToRequestedFields,
                subReaderContext,
                storedFieldReader
            );
        }
    }

    /**
     * Resets the provided {@link HitContext} with information on the current
     * document. This includes the following:
     *   - Adding an initial {@link SearchHit} instance.
     *   - Loading the document source and setting it on {@link HitContext#sourceLookup()}. This
     *     allows fetch subphases that use the hit context to access the preloaded source.
     */
    private static HitContext prepareNonNestedHitContext(
        SearchContext context,
        Profiler profiler,
        FieldsVisitor fieldsVisitor,
        int docId,
        Map<String, Set<String>> storedToRequestedFields,
        LeafReaderContext subReaderContext,
        CheckedBiConsumer<Integer, FieldsVisitor, IOException> fieldReader
    ) throws IOException {
        int subDocId = docId - subReaderContext.docBase;
        if (fieldsVisitor == null) {
            SearchHit hit = new SearchHit(docId, null, null, null);
            return new HitContext(hit, subReaderContext, subDocId);
        } else {
            SearchHit hit;
            loadStoredFields(context.getSearchExecutionContext()::getFieldType, profiler, fieldReader, fieldsVisitor, subDocId);
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

                SourceLookup scriptSourceLookup = context.getSearchExecutionContext().lookup().source();
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
    private static HitContext prepareNestedHitContext(
        SearchContext context,
        Profiler profiler,
        int topDocId,
        LeafNestedDocuments nestedInfo,
        Map<String, Set<String>> storedToRequestedFields,
        LeafReaderContext subReaderContext,
        CheckedBiConsumer<Integer, FieldsVisitor, IOException> storedFieldReader
    ) throws IOException {
        // Also if highlighting is requested on nested documents we need to fetch the _source from the root document,
        // otherwise highlighting will attempt to fetch the _source from the nested doc, which will fail,
        // because the entire _source is only stored with the root document.
        boolean needSource = sourceRequired(context) || context.highlight() != null;

        String rootId;
        Map<String, Object> rootSourceAsMap = null;
        XContentType rootSourceContentType = null;

        SearchExecutionContext searchExecutionContext = context.getSearchExecutionContext();
        if (context instanceof InnerHitsContext.InnerHitSubContext innerHitsContext) {
            rootId = innerHitsContext.getRootId();

            if (needSource) {
                SourceLookup rootLookup = innerHitsContext.getRootLookup();
                rootSourceAsMap = rootLookup.source();
                rootSourceContentType = rootLookup.sourceContentType();
            }
        } else {
            FieldsVisitor rootFieldsVisitor = new FieldsVisitor(needSource);
            loadStoredFields(searchExecutionContext::getFieldType, profiler, storedFieldReader, rootFieldsVisitor, nestedInfo.rootDoc());
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
        if (context.hasStoredFields() && context.storedFieldsContext().fieldNames().isEmpty() == false) {
            FieldsVisitor nestedFieldsVisitor = new CustomFieldsVisitor(storedToRequestedFields.keySet(), false);
            loadStoredFields(searchExecutionContext::getFieldType, profiler, storedFieldReader, nestedFieldsVisitor, nestedInfo.doc());
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
                List<Map<?, ?>> nestedParsedSource = XContentMapValues.extractNestedSources(nestedPath, rootSourceAsMap);
                if (nestedParsedSource == null) {
                    throw new IllegalStateException("Couldn't find nested source for path " + nestedPath);
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

    private static void loadStoredFields(
        Function<String, MappedFieldType> fieldTypeLookup,
        Profiler profileListener,
        CheckedBiConsumer<Integer, FieldsVisitor, IOException> fieldReader,
        FieldsVisitor fieldVisitor,
        int docId
    ) throws IOException {
        try {
            profileListener.startLoadingStoredFields();
            fieldVisitor.reset();
            fieldReader.accept(docId, fieldVisitor);
            fieldVisitor.postProcess(fieldTypeLookup);
        } finally {
            profileListener.stopLoadingStoredFields();
        }
    }

    private static void fillDocAndMetaFields(
        SearchContext context,
        FieldsVisitor fieldsVisitor,
        Map<String, Set<String>> storedToRequestedFields,
        Map<String, DocumentField> docFields,
        Map<String, DocumentField> metaFields
    ) {
        for (Map.Entry<String, List<Object>> entry : fieldsVisitor.fields().entrySet()) {
            String storedField = entry.getKey();
            List<Object> storedValues = entry.getValue();
            if (storedToRequestedFields.containsKey(storedField)) {
                for (String requestedField : storedToRequestedFields.get(storedField)) {
                    if (context.getSearchExecutionContext().isMetadataField(requestedField)) {
                        metaFields.put(requestedField, new DocumentField(requestedField, storedValues));
                    } else {
                        docFields.put(requestedField, new DocumentField(requestedField, storedValues));
                    }
                }
            } else {
                if (context.getSearchExecutionContext().isMetadataField(storedField)) {
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
        return docs.length > 0 && docs[docs.length - 1].docId - docs[0].docId == docs.length - 1;
    }

    interface Profiler {
        ProfileResult finish();

        FetchSubPhaseProcessor profile(String type, String description, FetchSubPhaseProcessor processor);

        void visitor(FieldsVisitor fieldsVisitor);

        void startLoadingStoredFields();

        void stopLoadingStoredFields();

        void startNextReader();

        void stopNextReader();

        Profiler NOOP = new Profiler() {
            @Override
            public ProfileResult finish() {
                return null;
            }

            @Override
            public void visitor(FieldsVisitor fieldsVisitor) {}

            @Override
            public FetchSubPhaseProcessor profile(String type, String description, FetchSubPhaseProcessor processor) {
                return processor;
            }

            @Override
            public void startLoadingStoredFields() {}

            @Override
            public void stopLoadingStoredFields() {}

            @Override
            public void startNextReader() {}

            @Override
            public void stopNextReader() {}

            @Override
            public String toString() {
                return "noop";
            }
        };
    }
}
