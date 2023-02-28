/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.frequentitemsets.mr;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.LongObjectPagedHashMap;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.aggregations.AggregationExecutionContext;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorBase;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.bucket.terms.IncludeExclude;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceRegistry;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xpack.ml.aggs.frequentitemsets.mr.ItemSetMapReduceValueSource.Field;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

public abstract class ItemSetMapReduceAggregator<
    MapContext extends Closeable,
    MapFinalContext extends Writeable,
    ReduceContext extends Closeable,
    Result extends ToXContent & Writeable> extends AggregatorBase {

    private final List<ItemSetMapReduceValueSource> valueSources;
    private final Weight weightDocumentFilter;
    private final List<Field> fields;
    private final AbstractItemSetMapReducer<MapContext, MapFinalContext, ReduceContext, Result> mapReducer;
    private final BigArrays bigArraysForMapReduce;
    private final LongObjectPagedHashMap<Object> mapReduceContextByBucketOrdinal;
    private final boolean profiling;
    private final boolean rewriteBasedOnOrdinals;
    private final DelegatingCircuitBreakerService breakerService;

    protected ItemSetMapReduceAggregator(
        String name,
        ValuesSourceRegistry.RegistryKey<ItemSetMapReduceValueSource.ValueSourceSupplier> registryKey,
        AggregationContext context,
        Aggregator parent,
        Map<String, Object> metadata,
        AbstractItemSetMapReducer<MapContext, MapFinalContext, ReduceContext, Result> mapReducer,
        List<Tuple<ValuesSourceConfig, IncludeExclude>> configsAndValueFilters,
        QueryBuilder documentFilter,
        AbstractItemSetMapReducer.OrdinalOptimization ordinalOptimization
    ) throws IOException {
        super(name, AggregatorFactories.EMPTY, context, parent, CardinalityUpperBound.NONE, metadata);

        List<ItemSetMapReduceValueSource> valueSources = new ArrayList<>();
        List<Field> fields = new ArrayList<>();
        IndexSearcher contextSearcher = context.searcher();
        LeafReaderContext ctx = getLeafReaderForOrdinals(context);

        int id = 0;
        this.weightDocumentFilter = documentFilter != null
            ? contextSearcher.createWeight(contextSearcher.rewrite(context.buildQuery(documentFilter)), ScoreMode.COMPLETE_NO_SCORES, 1f)
            : null;

        boolean rewriteBasedOnOrdinals = false;

        for (var c : configsAndValueFilters) {
            ItemSetMapReduceValueSource e = context.getValuesSourceRegistry()
                .getAggregator(registryKey, c.v1())
                .build(c.v1(), id++, c.v2(), ordinalOptimization, ctx);
            if (e.getField().getName() != null) {
                fields.add(e.getField());
                valueSources.add(e);
            }
            rewriteBasedOnOrdinals |= e.usesOrdinals();
        }

        this.rewriteBasedOnOrdinals = rewriteBasedOnOrdinals;
        this.valueSources = Collections.unmodifiableList(valueSources);
        this.fields = Collections.unmodifiableList(fields);
        this.mapReducer = mapReducer;
        this.profiling = context.profiling();

        // big arrays used for the map reduce context have a lifespan beyond this aggregator, so they can't use the bigarray from the
        // context. The {@link DelegatingCircuitBreakerService} workarounds several accounting problems. Please have a look
        // into the description of {@link DelegatingCircuitBreakerService} for more details.
        this.breakerService = new DelegatingCircuitBreakerService(context.breaker(), this::addRequestCircuitBreakerBytes);
        this.bigArraysForMapReduce = BigArrays.NON_RECYCLING_INSTANCE.withBreakerService(breakerService).withCircuitBreaking();
        this.mapReduceContextByBucketOrdinal = new LongObjectPagedHashMap<>(1, context.bigArrays());
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalItemSetMapReduceAggregation<>(name, metadata(), mapReducer, null, null, fields, profiling);
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
    protected LeafBucketCollector getLeafCollector(AggregationExecutionContext ctx, LeafBucketCollector sub) throws IOException {

        final Bits bits = weightDocumentFilter != null
            ? Lucene.asSequentialAccessBits(
                ctx.getLeafReaderContext().reader().maxDoc(),
                weightDocumentFilter.scorerSupplier(ctx.getLeafReaderContext())
            )
            : null;

        List<ItemSetMapReduceValueSource.ValueCollector> valueCollectors = new ArrayList<>(valueSources.size());
        for (ItemSetMapReduceValueSource valueSource : valueSources) {
            valueCollectors.add(valueSource.getValueCollector(ctx.getLeafReaderContext()));
        }

        return new LeafBucketCollectorBase(sub, null) {
            @Override
            public void collect(int doc, long owningBucketOrd) throws IOException {
                SetOnce<IOException> firstException = new SetOnce<>();
                if (bits == null || bits.get(doc)) {
                    mapReducer.map(valueCollectors.stream().map(c -> {
                        try {
                            return c.collect(doc);
                        } catch (IOException e) {
                            firstException.trySet(e);
                            // ignored in AbstractMapReducer
                            return null;
                        }
                    }), getMapReduceContext(owningBucketOrd));
                } else {
                    mapReducer.mapFiltered(getMapReduceContext(owningBucketOrd));
                }

                if (firstException.get() != null) {
                    throw firstException.get();
                }
            }
        };
    }

    @Override
    public void doPostCollection() throws IOException {
        List<AbstractItemSetMapReducer.OrdinalLookupFunction> ordinalLookupFunctions = null;

        // only rewrite ordinals back to real values when required
        if (rewriteBasedOnOrdinals) {
            ordinalLookupFunctions = new ArrayList<>(valueSources.size());
            for (ItemSetMapReduceValueSource valueSource : valueSources) {
                ordinalLookupFunctions.add(valueSource::mapOrdinal);
            }
        }

        for (long ordIdx = 0; ordIdx < mapReduceContextByBucketOrdinal.size(); ordIdx++) {
            MapContext context = getMapReduceContext(ordIdx);
            mapReduceContextByBucketOrdinal.put(ordIdx, mapReducer.mapFinalize(context, ordinalLookupFunctions));
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

        return new InternalItemSetMapReduceAggregation<>(name, metadata(), mapReducer, context, null, fields, profiling);
    }

    private static LeafReaderContext getLeafReaderForOrdinals(AggregationContext context) {
        IndexReader reader = context.searcher().getIndexReader();
        return reader.leaves().get(0);
    }
}
