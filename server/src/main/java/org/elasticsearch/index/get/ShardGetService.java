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
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.common.xcontent.XContentFieldFilter;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.fieldvisitor.CustomFieldsVisitor;
import org.elasticsearch.index.fieldvisitor.FieldsVisitor;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MappingLookup;
import org.elasticsearch.index.mapper.RoutingFieldMapper;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.index.shard.AbstractIndexShardComponent;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
        FetchSourceContext fetchSourceContext
    ) {
        return get(new Engine.Get(realtime, realtime, id).version(version).versionType(versionType), gFields, fetchSourceContext);
    }

    private GetResult get(Engine.Get engineGet, String[] gFields, FetchSourceContext fetchSourceContext) {
        final GetAndFetchContext[] requests = new GetAndFetchContext[] { new GetAndFetchContext(engineGet, gFields, fetchSourceContext) };
        final GetResultOrFailure[] resultOrFailures = multiGet(requests);
        assert resultOrFailures.length == 1 && resultOrFailures[0] != null : resultOrFailures;
        return resultOrFailures[0].getResultOrThrow();
    }

    public GetResult getForUpdate(String id, long ifSeqNo, long ifPrimaryTerm) {
        Engine.Get engineGet = new Engine.Get(true, true, id).version(Versions.MATCH_ANY)
            .versionType(VersionType.INTERNAL)
            .setIfSeqNo(ifSeqNo)
            .setIfPrimaryTerm(ifPrimaryTerm);
        return get(engineGet, new String[] { RoutingFieldMapper.NAME }, FetchSourceContext.FETCH_SOURCE);
    }

    /**
     * Returns {@link GetResult} based on the specified {@link org.elasticsearch.index.engine.Engine.GetResult} argument.
     * This method basically loads specified fields for the associated document in the engineGetResult.
     * This method load the fields from the Lucene index and not from transaction log and therefore isn't realtime.
     * <p>
     * Note: Call <b>must</b> release engine searcher associated with engineGetResult!
     */
    public GetResult get(Engine.GetResult engineGetResult, String id, String[] fields, FetchSourceContext fetchSourceContext) {
        if (engineGetResult.exists() == false) {
            return new GetResult(shardId.getIndexName(), id, UNASSIGNED_SEQ_NO, UNASSIGNED_PRIMARY_TERM, -1, false, null, null, null);
        }

        currentMetric.inc();
        try {
            long now = System.nanoTime();
            fetchSourceContext = normalizeFetchSourceContent(fetchSourceContext, fields);
            GetResult getResult = innerGetLoadFromStoredFields(id, fields, fetchSourceContext, engineGetResult);
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

    private static class Lookup implements Releasable {
        private final int index;
        private final Engine.GetResult engineResult;
        private final long lookupTimeInNanos;
        boolean closed = false;

        Lookup(int index, Engine.GetResult engineResult, long lookupTimeInNanos) {
            this.index = index;
            this.engineResult = engineResult;
            this.lookupTimeInNanos = lookupTimeInNanos;
        }

        @Override
        public void close() {
            if (closed == false) {
                closed = true;
                engineResult.close();
            }
        }
    }

    public GetResultOrFailure[] multiGet(GetAndFetchContext[] requests) {
        final int numRequests = requests.length;
        final List<Lookup> lookups = new ArrayList<>(numRequests);
        final GetResultOrFailure[] resultOrFailures = new GetResultOrFailure[numRequests];
        currentMetric.inc(numRequests);
        try {
            // perform all engine gets
            final Function<Engine.Get, Engine.GetResult> getFromEngine = indexShard.getFromEngine(requests.length > 1);
            for (int i = 0; i < numRequests; i++) {
                final GetAndFetchContext request = requests[i];
                final long now = System.nanoTime();
                try {
                    final Engine.GetResult engineResult = getFromEngine.apply(request.engineGet());
                    if (engineResult.exists()) {
                        lookups.add(new Lookup(i, engineResult, System.nanoTime() - now));
                    } else {
                        engineResult.close();
                        resultOrFailures[i] = new GetResultOrFailure(notExist(request.id()));
                        missingMetric.inc(System.nanoTime() - now);
                    }
                } catch (RuntimeException e) {
                    resultOrFailures[i] = new GetResultOrFailure(e);
                }
            }
            // load stored fields
            // TODO: Use sequential stored field reader when possible
            for (Lookup lookup : lookups) {
                final long now = System.nanoTime();
                final GetAndFetchContext request = requests[lookup.index];
                final FetchSourceContext fetchSourceContext = normalizeFetchSourceContent(request.fetchSourceContext(), request.gFields());
                try (lookup) {
                    final GetResult getResult = innerGetLoadFromStoredFields(
                        request.id(),
                        request.gFields(),
                        fetchSourceContext,
                        lookup.engineResult
                    );
                    resultOrFailures[lookup.index] = new GetResultOrFailure(getResult);
                    final long tookTime = lookup.lookupTimeInNanos + System.nanoTime() - now;
                    if (getResult.isExists()) {
                        existsMetric.inc(tookTime);
                    } else {
                        missingMetric.inc(tookTime);
                    }
                } catch (RuntimeException e) {
                    resultOrFailures[lookup.index] = new GetResultOrFailure(e);
                }
            }
        } finally {
            currentMetric.dec(numRequests);
        }
        assert Arrays.stream(resultOrFailures).allMatch(Objects::nonNull) : "some slots are unset";
        return resultOrFailures;
    }

    private GetResult notExist(String id) {
        return new GetResult(shardId.getIndexName(), id, UNASSIGNED_SEQ_NO, UNASSIGNED_PRIMARY_TERM, -1, false, null, null, null);
    }

    /**
     * decides what needs to be done based on the request input and always returns a valid non-null FetchSourceContext
     */
    private FetchSourceContext normalizeFetchSourceContent(@Nullable FetchSourceContext context, @Nullable String[] gFields) {
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

    private GetResult innerGetLoadFromStoredFields(
        String id,
        String[] storedFields,
        FetchSourceContext fetchSourceContext,
        Engine.GetResult get
    ) {
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
        BytesReference source = null;
        DocIdAndVersion docIdAndVersion = get.docIdAndVersion();
        FieldsVisitor fieldVisitor = buildFieldsVisitors(storedFields, fetchSourceContext);
        if (fieldVisitor != null) {
            try {
                docIdAndVersion.reader.document(docIdAndVersion.docId, fieldVisitor);
            } catch (IOException e) {
                throw new ElasticsearchException("Failed to get id [" + id + "]", e);
            }
            source = fieldVisitor.source();

            // put stored fields into result objects
            if (fieldVisitor.fields().isEmpty() == false) {
                fieldVisitor.postProcess(mapperService::fieldType);
                documentFields = new HashMap<>();
                metadataFields = new HashMap<>();
                for (Map.Entry<String, List<Object>> entry : fieldVisitor.fields().entrySet()) {
                    if (mapperService.isMetadataField(entry.getKey())) {
                        metadataFields.put(entry.getKey(), new DocumentField(entry.getKey(), entry.getValue()));
                    } else {
                        documentFields.put(entry.getKey(), new DocumentField(entry.getKey(), entry.getValue()));
                    }
                }
            }
        }

        if (source != null) {
            // apply request-level source filtering
            if (fetchSourceContext.fetchSource() == false) {
                source = null;
            } else if (fetchSourceContext.includes().length > 0 || fetchSourceContext.excludes().length > 0) {
                // TODO: The source might be parsed and available in the sourceLookup but that one uses unordered maps so different.
                // Do we care?
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

    private static FieldsVisitor buildFieldsVisitors(String[] fields, FetchSourceContext fetchSourceContext) {
        if (fields == null || fields.length == 0) {
            return fetchSourceContext.fetchSource() ? new FieldsVisitor(true) : null;
        }

        return new CustomFieldsVisitor(Sets.newHashSet(fields), fetchSourceContext.fetchSource());
    }
}
