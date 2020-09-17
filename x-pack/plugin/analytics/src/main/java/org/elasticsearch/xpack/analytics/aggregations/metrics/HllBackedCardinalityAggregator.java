/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.analytics.aggregations.metrics;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.ScoreMode;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.metrics.HyperLogLogPlusPlus;
import org.elasticsearch.search.aggregations.metrics.InternalCardinality;
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregator;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.xpack.analytics.aggregations.support.HllValuesSource;
import org.elasticsearch.xpack.analytics.mapper.fielddata.HllValue;
import org.elasticsearch.xpack.analytics.mapper.fielddata.HllValues;

import java.io.IOException;
import java.util.Map;

/**
 * An aggregator that computes approximate counts of unique values from Hll sketches.
 */
public class HllBackedCardinalityAggregator extends NumericMetricsAggregator.SingleValue {

    private final int precision;
    private final int fieldPrecision;
    private final ValuesSource valuesSource;
    private final HyperLogLog counts;

    public HllBackedCardinalityAggregator(
            String name,
            ValuesSourceConfig valuesSourceConfig,
            int precision,
            int fieldPrecision,
            SearchContext context,
            Aggregator parent,
            Map<String, Object> metadata) throws IOException {
        super(name, context, parent, metadata);
        assert valuesSourceConfig.hasValues(); // should always have doc values
        this.valuesSource = valuesSourceConfig.getValuesSource();
        this.precision = precision;
        this.fieldPrecision = fieldPrecision;
        this.counts = new HyperLogLog(precision, context.bigArrays(), 1);
    }

    @Override
    public ScoreMode scoreMode() {
        return valuesSource.needsScores() ? ScoreMode.COMPLETE : ScoreMode.COMPLETE_NO_SCORES;
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx,
                                               LeafBucketCollector sub) throws IOException {
        HllValuesSource.HllSketch source = (HllValuesSource.HllSketch) valuesSource;
        if (precision == fieldPrecision) {
            return new EqualPrecisionHllCollector(counts, source.getHllValues(ctx));
        } else {
            return new DifferentPrecisionHllCollector(counts, source.getHllValues(ctx), fieldPrecision);
        }
    }

    @Override
    public double metric(long owningBucketOrd) {
        return counts.cardinality(owningBucketOrd);
    }

    @Override
    public InternalAggregation buildAggregation(long owningBucketOrdinal) {
        if (owningBucketOrdinal >= counts.maxBucket() || counts.cardinality(owningBucketOrdinal) == 0) {
            return buildEmptyAggregation();
        }
        // We need to build a copy because the returned Aggregation needs remain usable after
        // this Aggregator (and its HLL++ counters) is released.
        HyperLogLogPlusPlus copy = new HyperLogLogPlusPlus(precision, BigArrays.NON_RECYCLING_INSTANCE, 1);
        copy.merge(0, counts, owningBucketOrdinal);
        return new InternalCardinality(name, copy, metadata());
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalCardinality(name, null, metadata());
    }

    @Override
    protected void doClose() {
        Releasables.close(counts);
    }

    /** Collector used when the target precision is equal to the precision of the hll field */
    private static class EqualPrecisionHllCollector extends LeafBucketCollector {

        private final HllValues values;
        private final HyperLogLog counts;
        private final int m;

        EqualPrecisionHllCollector(HyperLogLog counts, HllValues values) {
            this.counts = counts;
            this.values = values;
            this.m = 1 << counts.precision();
        }

        @Override
        public void collect(int doc, long bucketOrd) throws IOException {
            if (values.advanceExact(doc)) {
                final HllValue value = values.hllValue();
                for (int i = 0; i < m; i++) {
                    value.next();
                    final byte runLen = value.value();
                    counts.addRunLen(bucketOrd, i, runLen);
                }
                assert value.next() == false;
            }
        }
    }

    /** Collector used when the target precision is lower than the precision of the hll field */
    private static class DifferentPrecisionHllCollector extends LeafBucketCollector {

        private final HllValues values;
        private final HyperLogLog counts;
        private final int m;
        private final int precisionDiff;
        private final int registersToMerge;

        DifferentPrecisionHllCollector(HyperLogLog counts,
                                       HllValues values,
                                       int fieldPrecision) {
            this.counts = counts;
            this.values = values;
            this.m = 1 << counts.precision();
            this.precisionDiff = fieldPrecision - counts.precision();
            this.registersToMerge = 1 << precisionDiff;
        }

        @Override
        public void collect(int doc, long bucketOrd) throws IOException {
            if (values.advanceExact(doc)) {
                final HllValue value = values.hllValue();
                for (int i = 0; i < m; i++) {
                    final byte runLen = mergeRegister(value);
                    counts.addRunLen(bucketOrd, i, runLen);
                }
                assert value.next() == false;
            }
        }

        private byte mergeRegister(HllValue value) {
            for (int i = 0; i < registersToMerge; i++) {
                value.next();
                final byte runLen = value.value();
                if (runLen != 0) {
                    value.skip(registersToMerge - i - 1);
                    if (i == 0) {
                        // If the first element is set, then runLen is the current runLen plus the change in precision
                        return (byte) (runLen + precisionDiff);
                    } else {
                        // If any other register is set, the runLen is computed from the register position
                        return (byte) (precisionDiff - (int) (Math.log(i) / Math.log(2)));
                    }
                }
            }
            // No value for this register
            return 0;
        }
    }
}
