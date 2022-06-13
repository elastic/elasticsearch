/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.mapreduce;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.LongObjectPagedHashMap;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorBase;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.xcontent.ToXContent;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

import static org.elasticsearch.xpack.ml.aggs.mapreduce.MapReduceValueSourceRegistry.REGISTRY_KEY;

public abstract class MapReduceAggregator<
    MapContext extends Closeable,
    MapFinalContext extends Writeable,
    ReduceContext extends Closeable,
    Result extends ToXContent & Writeable> extends AggregatorBase {

    private final List<ValuesExtractor> extractors;
    private final List<String> fieldNames;
    private final AbstractMapReducer<MapContext, MapFinalContext, ReduceContext, Result> mapReducer;
    private final BigArrays bigArraysForMapReduce;
    private final LongObjectPagedHashMap<Object> mapReduceContextByBucketOrdinal;
    private final boolean profiling;
    private final DelegatingCircuitBreakerService breakerService;

    protected MapReduceAggregator(
        String name,
        AggregationContext context,
        Aggregator parent,
        Map<String, Object> metadata,
        AbstractMapReducer<MapContext, MapFinalContext, ReduceContext, Result> mapReducer,
        List<ValuesSourceConfig> configs
    ) throws IOException {
        super(name, AggregatorFactories.EMPTY, context, parent, CardinalityUpperBound.NONE, metadata);

        List<ValuesExtractor> extractors = new ArrayList<>();
        List<String> fieldNames = new ArrayList<>();
        int id = 0;
        for (ValuesSourceConfig c : configs) {
            ValuesExtractor e = context.getValuesSourceRegistry().getAggregator(REGISTRY_KEY, c).build(c, id++);
            fieldNames.add(e.getField().getName());
            extractors.add(e);
        }

        this.extractors = Collections.unmodifiableList(extractors);
        this.fieldNames = Collections.unmodifiableList(fieldNames);
        this.mapReducer = mapReducer;
        this.profiling = context.profiling();

        // big arrays used for the map reduce context have a lifespan beyond this aggregator, so they can't use the bigarray from the
        // context. But the non-recycling big array instance does not trip the circuit breaker, so we do a trick here:
        // We use the non-recycling big array but with the circuit breaker of the context and the counter of the aggregation.
        // This ensures that we 1st of all have a circuit breaker _and_ that all bytes that are added to the circuit breaker get deducted
        // later. This is important as otherwise the circuit breaker continues with a wrong count and eventually trips.
        // (if we would use the big array breaker we would not deduct the counter)
        this.breakerService = new DelegatingCircuitBreakerService(context.breaker(), this::addRequestCircuitBreakerBytes);
        this.bigArraysForMapReduce = BigArrays.NON_RECYCLING_INSTANCE.withBreakerService(breakerService).withCircuitBreaking();
        this.mapReduceContextByBucketOrdinal = new LongObjectPagedHashMap<>(1, context.bigArrays());
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalMapReduceAggregation<>(name, metadata(), mapReducer, null, null, fieldNames, profiling);
    }

    @Override
    public final InternalAggregation[] buildAggregations(long[] owningBucketOrds) throws IOException {
        InternalAggregation[] results = new InternalAggregation[owningBucketOrds.length];
        for (int ordIdx = 0; ordIdx < owningBucketOrds.length; ordIdx++) {
            results[ordIdx] = buildAggregation(ordIdx);
        }

        return results;
    }

    @Override
    protected LeafBucketCollector getLeafCollector(LeafReaderContext ctx, LeafBucketCollector sub) throws IOException {
        return new LeafBucketCollectorBase(sub, null) {
            @Override
            public void collect(int doc, long owningBucketOrd) throws IOException {
                SetOnce<IOException> firstException = new SetOnce<>();

                mapReducer.map(extractors.stream().map(extractor -> {
                    try {
                        return extractor.collectValues(ctx, doc);
                    } catch (IOException e) {
                        firstException.trySet(e);
                        // ignored in AbstractMapReducer
                        return null;
                    }
                }), getMapReduceContext(owningBucketOrd));

                if (firstException.get() != null) {
                    throw firstException.get();
                }
            }
        };
    }

    @Override
    public void doPostCollection() {
        for (long ordIdx = 0; ordIdx < mapReduceContextByBucketOrdinal.size(); ordIdx++) {
            MapContext context = getMapReduceContext(ordIdx);
            mapReduceContextByBucketOrdinal.put(ordIdx, mapReducer.mapFinalize(context));
        }
    }

    @Override
    public void collectDebugInfo(BiConsumer<String, Object> add) {
        add.accept("map_reducer", mapReducer.getWriteableName());
        mapReducer.collectDebugInfo(add);
    }

    @Override
    protected void doClose() {
        // disconnect the aggregation context circuit breaker, so big arrays used in results can be passed
        if (breakerService != null) {
            breakerService.disconnect();
        }

        Releasables.close(mapReduceContextByBucketOrdinal);
    }

    private MapContext getMapReduceContext(long bucketOrd) {
        @SuppressWarnings("unchecked")
        MapContext context = (MapContext) mapReduceContextByBucketOrdinal.get(bucketOrd);
        if (context == null) {
            context = mapReducer.mapInit(bigArraysForMapReduce);
            mapReduceContextByBucketOrdinal.put(bucketOrd, context);
        }

        return context;
    }

    private InternalAggregation buildAggregation(long owningBucketOrdinal) throws IOException {
        @SuppressWarnings("unchecked")
        MapFinalContext context = (MapFinalContext) mapReduceContextByBucketOrdinal.get(owningBucketOrdinal);
        if (context == null) {
            return buildEmptyAggregation();
        }

        return new InternalMapReduceAggregation<>(name, metadata(), mapReducer, context, null, fieldNames, profiling);
    }

}
