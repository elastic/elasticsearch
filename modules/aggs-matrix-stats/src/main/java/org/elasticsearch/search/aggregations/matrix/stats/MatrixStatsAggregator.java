/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations.matrix.stats;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.ScoreMode;
import org.elasticsearch.common.util.ObjectArray;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.fielddata.NumericDoubleValues;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.metrics.MetricsAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ArrayValuesSource.NumericArrayValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSource;

import java.io.IOException;
import java.util.Map;

/**
 * Metric Aggregation for computing the pearson product correlation coefficient between multiple fields
 **/
final class MatrixStatsAggregator extends MetricsAggregator {
    /** Multiple ValuesSource with field names */
    private final NumericArrayValuesSource valuesSources;

    /** array of descriptive stats, per shard, needed to compute the correlation */
    ObjectArray<RunningStats> stats;

    MatrixStatsAggregator(
        String name,
        Map<String, ValuesSource.Numeric> valuesSources,
        AggregationContext context,
        Aggregator parent,
        MultiValueMode multiValueMode,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, context, parent, metadata);
        if (valuesSources != null && valuesSources.isEmpty() == false) {
            this.valuesSources = new NumericArrayValuesSource(valuesSources, multiValueMode);
            stats = context.bigArrays().newObjectArray(1);
        } else {
            this.valuesSources = null;
        }
    }

    @Override
    public ScoreMode scoreMode() {
        return (valuesSources != null && valuesSources.needsScores()) ? ScoreMode.COMPLETE : ScoreMode.COMPLETE_NO_SCORES;
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx, final LeafBucketCollector sub) throws IOException {
        if (valuesSources == null) {
            return LeafBucketCollector.NO_OP_COLLECTOR;
        }
        final NumericDoubleValues[] values = new NumericDoubleValues[valuesSources.fieldNames().length];
        for (int i = 0; i < values.length; ++i) {
            values[i] = valuesSources.getField(i, ctx);
        }

        return new LeafBucketCollectorBase(sub, values) {
            final String[] fieldNames = valuesSources.fieldNames();
            final double[] fieldVals = new double[fieldNames.length];

            @Override
            public void collect(int doc, long bucket) throws IOException {
                // get fields
                if (includeDocument(doc)) {
                    stats = bigArrays().grow(stats, bucket + 1);
                    RunningStats stat = stats.get(bucket);
                    // add document fields to correlation stats
                    if (stat == null) {
                        stat = new RunningStats(fieldNames, fieldVals);
                        stats.set(bucket, stat);
                    } else {
                        stat.add(fieldNames, fieldVals);
                    }
                }
            }

            /**
             * return a map of field names and data
             */
            private boolean includeDocument(int doc) throws IOException {
                // loop over fields
                for (int i = 0; i < fieldVals.length; ++i) {
                    final NumericDoubleValues doubleValues = values[i];
                    if (doubleValues.advanceExact(doc)) {
                        final double value = doubleValues.doubleValue();
                        if (value == Double.NEGATIVE_INFINITY) {
                            // TODO: Fix matrix stats to treat neg inf as any other value
                            return false;
                        }
                        fieldVals[i] = value;
                    } else {
                        return false;
                    }
                }
                return true;
            }
        };
    }

    @Override
    public InternalAggregation buildAggregation(long bucket) {
        if (valuesSources == null || bucket >= stats.size()) {
            return buildEmptyAggregation();
        }
        return new InternalMatrixStats(name, stats.size(), stats.get(bucket), null, metadata());
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalMatrixStats(name, 0, null, null, metadata());
    }

    @Override
    public void doClose() {
        Releasables.close(stats);
    }
}
