/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.mapreduce;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public abstract class MapReduceAggregator extends AggregatorBase {

    public final class MapReduceContext {
        private final List<ValuesExtractor> extractors;
        private final MapReducer mapReducer;

        public MapReduceContext(List<ValuesExtractor> extractors, MapReducer mapReducer) {
            this.extractors = extractors;
            this.mapReducer = mapReducer;
        }

        public MapReducer getMapReducer() {
            return mapReducer;
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
        MapReducer mapReducer,
        List<ValuesSourceConfig> configs
    ) throws IOException {
        super(name, AggregatorFactories.EMPTY, context, parent, CardinalityUpperBound.NONE, metadata);

        this.context = new MapReduceContext(configs.stream().map(c -> {
            ValuesSource vs = c.getValuesSource();

            // TODO: make this nicer
            if (CoreValuesSourceType.KEYWORD.equals(c.valueSourceType())) {
                return new KeywordValuesExtractor(vs);
            }
            return (ValuesExtractor) null;
        }).collect(Collectors.toList()), mapReducer);
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        // TODO: handle empty MapReducer???
        return new InternalMapReduceAggregation(name, metadata(), context.getMapReducer());
    }

    @Override
    public final InternalAggregation[] buildAggregations(long[] owningBucketOrds) throws IOException {
        InternalAggregation[] results = new InternalAggregation[owningBucketOrds.length];
        for (int ordIdx = 0; ordIdx < owningBucketOrds.length; ordIdx++) {
            results[ordIdx] = new InternalMapReduceAggregation(name, metadata(), context.getMapReducer());
        }
        return results;
    }

    @Override
    protected LeafBucketCollector getLeafCollector(LeafReaderContext ctx, LeafBucketCollector sub) throws IOException {

        return new LeafBucketCollectorBase(sub, context) {

            @Override
            public void collect(int doc, long owningBucketOrd) throws IOException {
                context.getMapReducer().map(context.getExtractors().stream().map(extractor -> {
                    try {
                        return extractor.collectValues(ctx, doc);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                }));
            }
        };
    }

    interface ValuesExtractor {
        List<Object> collectValues(LeafReaderContext ctx, int doc) throws IOException;
    }

    abstract static class ValuesExtractorBase implements ValuesExtractor {
        protected final ValuesSource source;

        ValuesExtractorBase(ValuesSource source) {
            this.source = source;
        }
    }

    static class KeywordValuesExtractor extends ValuesExtractorBase {

        KeywordValuesExtractor(ValuesSource source) {
            super(source);
        }

        @Override
        public List<Object> collectValues(LeafReaderContext ctx, int doc) throws IOException {

            SortedBinaryDocValues values = source.bytesValues(ctx);

            if (values.advanceExact(doc)) {
                int valuesCount = values.docValueCount();
                List<Object> objects = new ArrayList<>(valuesCount);

                for (int i = 0; i < valuesCount; ++i) {
                    objects.add(values.nextValue().utf8ToString());
                }
                return objects;
            }
            return Collections.emptyList();
        }

    }

}
