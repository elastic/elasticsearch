/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.get;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.IndexSearcher;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.lucene.uid.VersionsAndSeqNoResolver.DocIdAndVersion;
import org.elasticsearch.common.metrics.CounterMetric;
import org.elasticsearch.common.metrics.MeanMetric;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.fieldvisitor.LeafStoredFieldLoader;
import org.elasticsearch.index.fieldvisitor.StoredFieldLoader;
import org.elasticsearch.index.mapper.IgnoredFieldMapper;
import org.elasticsearch.index.mapper.InferenceMetadataFieldsMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperMetrics;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MappingLookup;
import org.elasticsearch.index.mapper.RoutingFieldMapper;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.index.mapper.SourceLoader;
import org.elasticsearch.index.shard.AbstractIndexShardComponent;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.MultiEngineGet;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.lookup.Source;
import org.elasticsearch.search.lookup.SourceFilter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_PRIMARY_TERM;
import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;

public final class ShardGetService extends AbstractIndexShardComponent {
    private final MapperService mapperService;
    private final MeanMetric existsMetric = new MeanMetric();
    private final MeanMetric missingMetric = new MeanMetric();
    private final CounterMetric currentMetric = new CounterMetric();
    private final IndexShard indexShard;
    private final MapperMetrics mapperMetrics;

    public ShardGetService(IndexSettings indexSettings, IndexShard indexShard, MapperService mapperService, MapperMetrics mapperMetrics) {
        super(indexShard.shardId(), indexSettings);
        this.mapperService = mapperService;
        this.indexShard = indexShard;
        this.mapperMetrics = mapperMetrics;
    }

    public GetStats stats() {
        return new GetStats(
            existsMetric.count(),
            TimeUnit.NANOSECONDS.toMillis(existsMetric.sum()),
            missingMetric.count(),
            TimeUnit.NANOSECONDS.toMillis(missingMetric.sum()),
            currentMetric.count()
        );
    }

    public GetResult get(
        String id,
        String[] gFields,
        boolean realtime,
        long version,
        VersionType versionType,
        FetchSourceContext fetchSourceContext,
        boolean forceSyntheticSource
    ) throws IOException {
        return doGet(
            id,
            gFields,
            realtime,
            version,
            versionType,
            UNASSIGNED_SEQ_NO,
            UNASSIGNED_PRIMARY_TERM,
            fetchSourceContext,
            forceSyntheticSource,
            indexShard::get
        );
    }

    public GetResult mget(
        String id,
        String[] gFields,
        boolean realtime,
        long version,
        VersionType versionType,
        FetchSourceContext fetchSourceContext,
        boolean forceSyntheticSource,
        MultiEngineGet mget
    ) throws IOException {
        return doGet(
            id,
            gFields,
            realtime,
            version,
            versionType,
            UNASSIGNED_SEQ_NO,
            UNASSIGNED_PRIMARY_TERM,
            fetchSourceContext,
            forceSyntheticSource,
            mget::get
        );
    }

    private GetResult doGet(
        String id,
        String[] gFields,
        boolean realtime,
        long version,
        VersionType versionType,
        long ifSeqNo,
        long ifPrimaryTerm,
        FetchSourceContext fetchSourceContext,
        boolean forceSyntheticSource,
        Function<Engine.Get, Engine.GetResult> engineGetOperator
    ) throws IOException {
        currentMetric.inc();
        final long now = System.nanoTime();
        try {
            var engineGet = new Engine.Get(realtime, realtime, id).version(version)
                .versionType(versionType)
                .setIfSeqNo(ifSeqNo)
                .setIfPrimaryTerm(ifPrimaryTerm);

            final GetResult getResult;
            try (Engine.GetResult get = engineGetOperator.apply(engineGet)) {
                if (get == null) {
                    getResult = null;
                } else if (get.exists() == false) {
                    getResult = new GetResult(
                        shardId.getIndexName(),
                        id,
                        UNASSIGNED_SEQ_NO,
                        UNASSIGNED_PRIMARY_TERM,
                        -1,
                        false,
                        null,
                        null,
                        null
                    );
                } else {
                    // break between having loaded it from translog (so we only have _source), and having a document to load
                    getResult = innerGetFetch(
                        id,
                        gFields,
                        normalizeFetchSourceContent(fetchSourceContext, gFields),
                        get,
                        forceSyntheticSource
                    );
                }
            }
            if (getResult != null && getResult.isExists()) {
                existsMetric.inc(System.nanoTime() - now);
            } else {
                missingMetric.inc(System.nanoTime() - now);
            }
            return getResult;
        } finally {
            currentMetric.dec();
        }
    }

    public GetResult getFromTranslog(
        String id,
        String[] gFields,
        boolean realtime,
        long version,
        VersionType versionType,
        FetchSourceContext fetchSourceContext,
        boolean forceSyntheticSource
    ) throws IOException {
        return doGet(
            id,
            gFields,
            realtime,
            version,
            versionType,
            UNASSIGNED_SEQ_NO,
            UNASSIGNED_PRIMARY_TERM,
            fetchSourceContext,
            forceSyntheticSource,
            indexShard::getFromTranslog
        );
    }

    public GetResult getForUpdate(String id, long ifSeqNo, long ifPrimaryTerm, String[] gFields) throws IOException {
        return doGet(
            id,
            gFields,
            true,
            Versions.MATCH_ANY,
            VersionType.INTERNAL,
            ifSeqNo,
            ifPrimaryTerm,
            FetchSourceContext.FETCH_SOURCE,
            false,
            indexShard::get
        );
    }

    /**
     * Returns {@link GetResult} based on the specified {@link org.elasticsearch.index.engine.Engine.GetResult} argument.
     * This method basically loads specified fields for the associated document in the engineGetResult.
     * This method load the fields from the Lucene index and not from transaction log and therefore isn't realtime.
     * <p>
     * Note: Call <b>must</b> release engine searcher associated with engineGetResult!
     */
    public GetResult get(Engine.GetResult engineGetResult, String id, String[] fields, FetchSourceContext fetchSourceContext)
        throws IOException {
        if (engineGetResult.exists() == false) {
            return new GetResult(shardId.getIndexName(), id, UNASSIGNED_SEQ_NO, UNASSIGNED_PRIMARY_TERM, -1, false, null, null, null);
        }

        currentMetric.inc();
        try {
            long now = System.nanoTime();
            fetchSourceContext = normalizeFetchSourceContent(fetchSourceContext, fields);
            GetResult getResult = innerGetFetch(id, fields, fetchSourceContext, engineGetResult, false);
            if (getResult.isExists()) {
                existsMetric.inc(System.nanoTime() - now);
            } else {
                missingMetric.inc(System.nanoTime() - now); // This shouldn't happen...
            }
            return getResult;
        } finally {
            currentMetric.dec();
        }
    }

    /**
     * decides what needs to be done based on the request input and always returns a valid non-null FetchSourceContext
     */
    private static FetchSourceContext normalizeFetchSourceContent(@Nullable FetchSourceContext context, @Nullable String[] gFields) {
        if (context != null) {
            return context;
        }
        if (gFields == null) {
            return FetchSourceContext.FETCH_SOURCE;
        }
        for (String field : gFields) {
            if (SourceFieldMapper.NAME.equals(field)) {
                return FetchSourceContext.FETCH_SOURCE;
            }
        }
        return FetchSourceContext.DO_NOT_FETCH_SOURCE;
    }

    private GetResult innerGetFetch(
        String id,
        String[] storedFields,
        FetchSourceContext fetchSourceContext,
        Engine.GetResult get,
        boolean forceSyntheticSource
    ) throws IOException {
        assert get.exists() : "method should only be called if document could be retrieved";
        // check first if stored fields to be loaded don't contain an object field
        MappingLookup mappingLookup = mapperService.mappingLookup();
        final Set<String> storedFieldSet = new HashSet<>();
        boolean hasInferenceMetadataFields = false;
        if (storedFields != null) {
            for (String field : storedFields) {
                if (field.equals(InferenceMetadataFieldsMapper.NAME)
                    && InferenceMetadataFieldsMapper.isEnabled(indexShard.mapperService().mappingLookup())) {
                    hasInferenceMetadataFields = true;
                    continue;
                }
                Mapper fieldMapper = mappingLookup.getMapper(field);
                if (fieldMapper == null) {
                    if (mappingLookup.objectMappers().get(field) != null) {
                        // Only fail if we know it is a object field, missing paths / fields shouldn't fail.
                        throw new IllegalArgumentException("field [" + field + "] isn't a leaf field");
                    }
                }
                storedFieldSet.add(field);
            }
        }

        Map<String, DocumentField> documentFields = null;
        Map<String, DocumentField> metadataFields = null;
        DocIdAndVersion docIdAndVersion = get.docIdAndVersion();
        var sourceFilter = fetchSourceContext.filter();
        SourceLoader loader = forceSyntheticSource
            ? new SourceLoader.Synthetic(
                sourceFilter,
                () -> mappingLookup.getMapping().syntheticFieldLoader(sourceFilter),
                mapperMetrics.sourceFieldMetrics()
            )
            : mappingLookup.newSourceLoader(sourceFilter, mapperMetrics.sourceFieldMetrics());
        StoredFieldLoader storedFieldLoader = buildStoredFieldLoader(storedFieldSet, fetchSourceContext, loader);
        LeafStoredFieldLoader leafStoredFieldLoader = storedFieldLoader.getLoader(docIdAndVersion.reader.getContext(), null);
        try {
            leafStoredFieldLoader.advanceTo(docIdAndVersion.docId);
        } catch (IOException e) {
            throw new ElasticsearchException("Failed to get id [" + id + "]", e);
        }

        final boolean supportDocValuesForIgnoredMetaField = indexSettings.getIndexVersionCreated()
            .onOrAfter(IndexVersions.DOC_VALUES_FOR_IGNORED_META_FIELD);

        // put stored fields into result objects
        if (leafStoredFieldLoader.storedFields().isEmpty() == false) {
            Set<String> needed = new HashSet<>();
            if (storedFields != null) {
                Collections.addAll(needed, storedFields);
            }
            needed.add(RoutingFieldMapper.NAME); // we always return _routing if we see it, even if you don't ask for it.....
            documentFields = new HashMap<>();
            metadataFields = new HashMap<>();
            for (Map.Entry<String, List<Object>> entry : leafStoredFieldLoader.storedFields().entrySet()) {
                if (false == needed.contains(entry.getKey())) {
                    continue;
                }
                if (IgnoredFieldMapper.NAME.equals(entry.getKey()) && supportDocValuesForIgnoredMetaField) {
                    continue;
                }
                MappedFieldType ft = mapperService.fieldType(entry.getKey());
                if (ft == null) {
                    continue;   // user asked for a non-existent field, ignore it
                }
                List<Object> values = entry.getValue().stream().map(ft::valueForDisplay).toList();
                if (mapperService.isMetadataField(entry.getKey())) {
                    metadataFields.put(entry.getKey(), new DocumentField(entry.getKey(), values));
                } else {
                    documentFields.put(entry.getKey(), new DocumentField(entry.getKey(), values));
                }
            }
        }

        // NOTE: when _ignored is requested via `stored_fields` we need to load it from doc values instead of loading it from stored fields.
        // The _ignored field used to be stored, but as a result of supporting aggregations on it, it moved from using a stored field to
        // using doc values.
        if (supportDocValuesForIgnoredMetaField && storedFields != null && Arrays.asList(storedFields).contains(IgnoredFieldMapper.NAME)) {
            final DocumentField ignoredDocumentField = loadIgnoredMetadataField(docIdAndVersion);
            if (ignoredDocumentField != null) {
                if (metadataFields == null) {
                    metadataFields = new HashMap<>();
                }
                metadataFields.put(IgnoredFieldMapper.NAME, ignoredDocumentField);
            }
        }

        BytesReference sourceBytes = null;
        if (mapperService.mappingLookup().isSourceEnabled() && fetchSourceContext.fetchSource()) {
            Source source = loader.leaf(docIdAndVersion.reader, new int[] { docIdAndVersion.docId })
                .source(leafStoredFieldLoader, docIdAndVersion.docId);

            SourceFilter filter = fetchSourceContext.filter();
            if (filter != null) {
                source = source.filter(filter);
            }

            if (hasInferenceMetadataFields) {
                /**
                 * Adds the {@link InferenceMetadataFieldsMapper#NAME} field from the document fields
                 * to the original _source if it has been requested.
                 */
                source = addInferenceMetadataFields(mapperService, docIdAndVersion.reader.getContext(), docIdAndVersion.docId, source);
            }
            sourceBytes = source.internalSourceRef();
        }

        return new GetResult(
            shardId.getIndexName(),
            id,
            get.docIdAndVersion().seqNo,
            get.docIdAndVersion().primaryTerm,
            get.version(),
            get.exists(),
            sourceBytes,
            documentFields,
            metadataFields
        );
    }

    private static DocumentField loadIgnoredMetadataField(final DocIdAndVersion docIdAndVersion) throws IOException {
        final SortedSetDocValues ignoredDocValues = docIdAndVersion.reader.getContext()
            .reader()
            .getSortedSetDocValues(IgnoredFieldMapper.NAME);
        if (ignoredDocValues == null
            || ignoredDocValues.advanceExact(docIdAndVersion.docId) == false
            || ignoredDocValues.docValueCount() <= 0) {
            return null;
        }
        final List<Object> ignoredValues = new ArrayList<>(ignoredDocValues.docValueCount());
        for (int i = 0; i < ignoredDocValues.docValueCount(); i++) {
            ignoredValues.add(ignoredDocValues.lookupOrd(ignoredDocValues.nextOrd()).utf8ToString());
        }
        return new DocumentField(IgnoredFieldMapper.NAME, ignoredValues);
    }

    private static Source addInferenceMetadataFields(MapperService mapperService, LeafReaderContext readerContext, int docId, Source source)
        throws IOException {
        var mappingLookup = mapperService.mappingLookup();
        var inferenceMetadata = (InferenceMetadataFieldsMapper) mappingLookup.getMapping()
            .getMetadataMapperByName(InferenceMetadataFieldsMapper.NAME);
        if (inferenceMetadata == null || mapperService.mappingLookup().inferenceFields().isEmpty()) {
            return source;
        }
        var inferenceLoader = inferenceMetadata.fieldType()
            .valueFetcher(mappingLookup, mapperService.getBitSetProducer(), new IndexSearcher(readerContext.reader()));
        inferenceLoader.setNextReader(readerContext);
        List<Object> values = inferenceLoader.fetchValues(source, docId, List.of());
        if (values.size() == 1) {
            var newSource = source.source();
            newSource.put(InferenceMetadataFieldsMapper.NAME, values.get(0));
            return Source.fromMap(newSource, source.sourceContentType());
        }
        return source;
    }

    private static StoredFieldLoader buildStoredFieldLoader(
        Set<String> fields,
        FetchSourceContext fetchSourceContext,
        SourceLoader loader
    ) {
        if (fetchSourceContext.fetchSource()) {
            fields.addAll(loader.requiredStoredFields());
        } else {
            if (fields.isEmpty()) {
                return StoredFieldLoader.empty();
            }
        }
        return StoredFieldLoader.create(fetchSourceContext.fetchSource(), fields);
    }
}
