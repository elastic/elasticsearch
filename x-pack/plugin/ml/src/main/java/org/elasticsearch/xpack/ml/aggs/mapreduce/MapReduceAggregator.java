/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.mapreduce;

import org.apache.lucene.index.LeafReaderContext;
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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.ml.aggs.mapreduce.MapReduceValueSourceRegistry.REGISTRY_KEY;

public abstract class MapReduceAggregator extends AggregatorBase {

    private final List<ValuesExtractor> extractors;
    private final AbstractMapReducer<?, ?, ?> mapReducer;
    private final BigArrays bigArrays;
    private final LongObjectPagedHashMap<Writeable> mapReduceContextByBucketOrdinal;

    private final boolean profiling;

    protected MapReduceAggregator(
        String name,
        AggregationContext context,
        Aggregator parent,
        Map<String, Object> metadata,
        AbstractMapReducer<?, ?, ?> mapReducer,
        List<ValuesSourceConfig> configs
    ) throws IOException {
        super(name, AggregatorFactories.EMPTY, context, parent, CardinalityUpperBound.NONE, metadata);

        List<ValuesExtractor> extractors = configs.stream()
            .map(c -> context.getValuesSourceRegistry().getAggregator(REGISTRY_KEY, c).build(c))
            .collect(Collectors.toList());

        this.extractors = extractors;
        this.mapReducer = mapReducer;
        this.bigArrays = context.bigArrays();
        this.profiling = context.profiling();
        this.mapReduceContextByBucketOrdinal = new LongObjectPagedHashMap<>(1, bigArrays);
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalMapReduceAggregation(name, metadata(), mapReducer, mapReducer.mapInit(bigArrays), profiling);
    }

    @Override
    public final InternalAggregation[] buildAggregations(long[] owningBucketOrds) throws IOException {
        InternalAggregation[] results = new InternalAggregation[owningBucketOrds.length];
        for (int ordIdx = 0; ordIdx < owningBucketOrds.length; ordIdx++) {
            results[ordIdx] = new InternalMapReduceAggregation(name, metadata(), mapReducer, getMapReduceContext(ordIdx), profiling);
        }
        return results;
    }

    @Override
    protected LeafBucketCollector getLeafCollector(LeafReaderContext ctx, LeafBucketCollector sub) throws IOException {
        return new LeafBucketCollectorBase(sub, null) {
            @Override
            public void collect(int doc, long owningBucketOrd) throws IOException {
                mapReducer.map(extractors.stream().map(extractor -> {
                    try {
                        return extractor.collectValues(ctx, doc);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                }), getMapReduceContext(owningBucketOrd));
            }
        };
    }

    @Override
    public void collectDebugInfo(BiConsumer<String, Object> add) {
        add.accept("map_reducer", mapReducer.getWriteableName());
    }

    @Override
    protected void doClose() {
        // TODO: the initialized MapReduceContext objects could be releasables, if something goes wrong, who is cleaning them up??
        Releasables.close(mapReduceContextByBucketOrdinal);
    }

    private Writeable getMapReduceContext(long bucketOrd) {
        Writeable context = mapReduceContextByBucketOrdinal.get(bucketOrd);
        if (context == null) {

            // TODO: as long as we haven't clarified who is closing, we can't use big arrays from the context
            context = mapReducer.mapInit(BigArrays.NON_RECYCLING_INSTANCE);

            // context = mapReducer.mapInit(bigArrays);
            mapReduceContextByBucketOrdinal.put(bucketOrd, context);
        }

        return context;
    }
}
