/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.mapreduce;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.common.Strings;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorBase;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public abstract class MapReduceAggregator extends AggregatorBase {

    public final class MapReduceContext {
        private final List<ValuesExtractor> extractors;
        private final Supplier<MapReducer> mapReduceSupplier;
        private final Map<Long, MapReducer> mapReducerByBucketOrdinal = new HashMap<>();

        public MapReduceContext(List<ValuesExtractor> extractors, Supplier<MapReducer> mapReduceSupplier) {
            this.extractors = extractors;
            this.mapReduceSupplier = mapReduceSupplier;
        }

        public MapReducer getMapReducer(long bucketOrd) {
            // TODO: are bucketOrdinals arbitrary long values or a counter (so we can use a list instead)???
            MapReducer mapReducer = mapReducerByBucketOrdinal.get(bucketOrd);
            if (mapReducer == null) {
                mapReducer = mapReduceSupplier.get();
                mapReducerByBucketOrdinal.put(bucketOrd, mapReducer);
                mapReducer.mapInit();
            }

            return mapReducer;
        }

        public Supplier<MapReducer> getMapReduceSupplier() {
            return mapReduceSupplier;
        }

        public List<ValuesExtractor> getExtractors() {
            return extractors;
        }
    }

    private final MapReduceContext context;

    protected MapReduceAggregator(
        String name,
        AggregationContext context,
        Aggregator parent,
        Map<String, Object> metadata,
        Supplier<MapReducer> mapReducer,
        List<ValuesSourceConfig> configs
    ) throws IOException {
        super(name, AggregatorFactories.EMPTY, context, parent, CardinalityUpperBound.NONE, metadata);

        this.context = new MapReduceContext(configs.stream().map(c -> {
            ValuesSource vs = c.getValuesSource();

            // TODO: make this nicer
            if (CoreValuesSourceType.KEYWORD.equals(c.valueSourceType())) {
                String fieldName = c.fieldContext() != null ? c.fieldContext().field() : null;

                if (Strings.isNullOrEmpty(fieldName)) {
                    throw new IllegalArgumentException("scripts are not supported at the moment");
                }

                return new ValuesExtractor.Keyword(fieldName, vs);
            }
            return (ValuesExtractor) null;
        }).collect(Collectors.toList()), mapReducer);
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        // TODO: handle empty MapReducer???
        return new InternalMapReduceAggregation(name, metadata(), context.getMapReducer(0));
    }

    @Override
    public final InternalAggregation[] buildAggregations(long[] owningBucketOrds) throws IOException {
        InternalAggregation[] results = new InternalAggregation[owningBucketOrds.length];
        for (int ordIdx = 0; ordIdx < owningBucketOrds.length; ordIdx++) {
            results[ordIdx] = new InternalMapReduceAggregation(name, metadata(), context.getMapReducer(ordIdx));
        }
        return results;
    }

    @Override
    protected LeafBucketCollector getLeafCollector(LeafReaderContext ctx, LeafBucketCollector sub) throws IOException {
        return new LeafBucketCollectorBase(sub, context) {
            @Override
            public void collect(int doc, long owningBucketOrd) throws IOException {

                // TODO: partition by owningBucketOrd
                context.getMapReducer(owningBucketOrd).map(context.getExtractors().stream().map(extractor -> {
                    try {
                        return extractor.collectValues(ctx, doc);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                }));
            }
        };
    }

}
