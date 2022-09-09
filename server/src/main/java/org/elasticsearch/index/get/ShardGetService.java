/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.get;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.lucene.uid.VersionsAndSeqNoResolver.DocIdAndVersion;
import org.elasticsearch.common.metrics.CounterMetric;
import org.elasticsearch.common.metrics.MeanMetric;
import org.elasticsearch.common.xcontent.XContentFieldFilter;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.fieldvisitor.LeafStoredFieldLoader;
import org.elasticsearch.index.fieldvisitor.StoredFieldLoader;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MappingLookup;
import org.elasticsearch.index.mapper.RoutingFieldMapper;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.index.mapper.SourceLoader;
import org.elasticsearch.index.shard.AbstractIndexShardComponent;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.search.fetch.FetchPhase;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_PRIMARY_TERM;
import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;

public final class ShardGetService extends AbstractIndexShardComponent {
    private final MapperService mapperService;
    private final MeanMetric existsMetric = new MeanMetric();
    private final MeanMetric missingMetric = new MeanMetric();
    private final CounterMetric currentMetric = new CounterMetric();
    private final IndexShard indexShard;

    public ShardGetService(IndexSettings indexSettings, IndexShard indexShard, MapperService mapperService) {
        super(indexShard.shardId(), indexSettings);
        this.mapperService = mapperService;
        this.indexShard = indexShard;
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
        return get(
            id,
            gFields,
            realtime,
            version,
            versionType,
            UNASSIGNED_SEQ_NO,
            UNASSIGNED_PRIMARY_TERM,
            fetchSourceContext,
            forceSyntheticSource
        );
    }

    private GetResult get(
        String id,
        String[] gFields,
        boolean realtime,
        long version,
        VersionType versionType,
        long ifSeqNo,
        long ifPrimaryTerm,
        FetchSourceContext fetchSourceContext,
        boolean forceSyntheticSource
    ) throws IOException {
        currentMetric.inc();
        try {
            long now = System.nanoTime();
            GetResult getResult = innerGet(
                id,
                gFields,
                realtime,
                version,
                versionType,
                ifSeqNo,
                ifPrimaryTerm,
                fetchSourceContext,
                forceSyntheticSource
            );

            if (getResult.isExists()) {
                existsMetric.inc(System.nanoTime() - now);
            } else {
                missingMetric.inc(System.nanoTime() - now);
            }
            return getResult;
        } finally {
            currentMetric.dec();
        }
    }

    public GetResult getForUpdate(String id, long ifSeqNo, long ifPrimaryTerm) throws IOException {
        return get(
            id,
            new String[] { RoutingFieldMapper.NAME },
            true,
            Versions.MATCH_ANY,
            VersionType.INTERNAL,
            ifSeqNo,
            ifPrimaryTerm,
            FetchSourceContext.FETCH_SOURCE,
            false
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

    private GetResult innerGet(
        String id,
        String[] gFields,
        boolean realtime,
        long version,
        VersionType versionType,
        long ifSeqNo,
        long ifPrimaryTerm,
        FetchSourceContext fetchSourceContext,
        boolean forceSyntheticSource
    ) throws IOException {
        fetchSourceContext = normalizeFetchSourceContent(fetchSourceContext, gFields);
        try (
            Engine.GetResult get = indexShard.get(
                new Engine.Get(realtime, realtime, id).version(version)
                    .versionType(versionType)
                    .setIfSeqNo(ifSeqNo)
                    .setIfPrimaryTerm(ifPrimaryTerm)
            )
        ) {
            if (get.exists() == false) {
                return new GetResult(shardId.getIndexName(), id, UNASSIGNED_SEQ_NO, UNASSIGNED_PRIMARY_TERM, -1, false, null, null, null);
            }
            // break between having loaded it from translog (so we only have _source), and having a document to load
            return innerGetFetch(id, gFields, fetchSourceContext, get, forceSyntheticSource);
        }
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
        if (storedFields != null) {
            for (String field : storedFields) {
                Mapper fieldMapper = mappingLookup.getMapper(field);
                if (fieldMapper == null) {
                    if (mappingLookup.objectMappers().get(field) != null) {
                        // Only fail if we know it is a object field, missing paths / fields shouldn't fail.
                        throw new IllegalArgumentException("field [" + field + "] isn't a leaf field");
                    }
                }
            }
        }

        Map<String, DocumentField> documentFields = null;
        Map<String, DocumentField> metadataFields = null;
        BytesReference source;
        DocIdAndVersion docIdAndVersion = get.docIdAndVersion();
        SourceLoader loader = forceSyntheticSource
            ? new SourceLoader.Synthetic(mappingLookup.getMapping())
            : mappingLookup.newSourceLoader();
        StoredFieldLoader storedFieldLoader = buildStoredFieldLoader(storedFields, fetchSourceContext, loader);
        LeafStoredFieldLoader leafStoredFieldLoader = storedFieldLoader.getLoader(docIdAndVersion.reader.getContext(), null);
        try {
            leafStoredFieldLoader.advanceTo(docIdAndVersion.docId);
        } catch (IOException e) {
            throw new ElasticsearchException("Failed to get id [" + id + "]", e);
        }

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
                List<Object> values = FetchPhase.processStoredField(mapperService::fieldType, entry.getKey(), entry.getValue());
                if (mapperService.isMetadataField(entry.getKey())) {
                    metadataFields.put(entry.getKey(), new DocumentField(entry.getKey(), values));
                } else {
                    documentFields.put(entry.getKey(), new DocumentField(entry.getKey(), values));
                }
            }
        }
        source = loader.leaf(docIdAndVersion.reader, new int[] { docIdAndVersion.docId })
            .source(leafStoredFieldLoader, docIdAndVersion.docId);

        if (source != null) {
            // apply request-level source filtering
            if (fetchSourceContext.fetchSource() == false) {
                source = null;
            } else if (fetchSourceContext.includes().length > 0 || fetchSourceContext.excludes().length > 0) {
                try {
                    source = XContentFieldFilter.newFieldFilter(fetchSourceContext.includes(), fetchSourceContext.excludes())
                        .apply(source, null);
                } catch (IOException e) {
                    throw new ElasticsearchException("Failed to get id [" + id + "] with includes/excludes set", e);
                }
            }
        }

        return new GetResult(
            shardId.getIndexName(),
            id,
            get.docIdAndVersion().seqNo,
            get.docIdAndVersion().primaryTerm,
            get.version(),
            get.exists(),
            source,
            documentFields,
            metadataFields
        );
    }

    private static StoredFieldLoader buildStoredFieldLoader(String[] fields, FetchSourceContext fetchSourceContext, SourceLoader loader) {
        Set<String> fieldsToLoad = new HashSet<>();
        if (fields != null && fields.length > 0) {
            Collections.addAll(fieldsToLoad, fields);
        }
        if (fetchSourceContext.fetchSource()) {
            fieldsToLoad.addAll(loader.requiredStoredFields());
        } else {
            if (fieldsToLoad.isEmpty()) {
                return StoredFieldLoader.empty();
            }
        }
        return StoredFieldLoader.create(fetchSourceContext.fetchSource(), fieldsToLoad);
    }
}
