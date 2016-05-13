/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.search.aggregations.matrix.stats;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.ObjectArray;
import org.elasticsearch.index.fielddata.NumericDoubleValues;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.metrics.MetricsAggregator;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.MultiValuesSource.NumericMultiValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSource;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Metric Aggregation for computing the pearson product correlation coefficient between multiple fields
 **/
public class MatrixStatsAggregator extends MetricsAggregator {
    /** Multiple ValuesSource with field names */
    final NumericMultiValuesSource valuesSources;

    /** array of descriptive stats, per shard, needed to compute the correlation */
    ObjectArray<RunningStats> stats;

    public MatrixStatsAggregator(String name, Map<String, ValuesSource.Numeric> valuesSources, AggregationContext context,
                                 Aggregator parent, MultiValueMode multiValueMode, List<PipelineAggregator> pipelineAggregators,
                                 Map<String,Object> metaData) throws IOException {
        super(name, context, parent, pipelineAggregators, metaData);
        if (valuesSources != null && !valuesSources.isEmpty()) {
            this.valuesSources = new NumericMultiValuesSource(valuesSources, multiValueMode);
            stats = context.bigArrays().newObjectArray(1);
        } else {
            this.valuesSources = null;
        }
    }

    @Override
    public boolean needsScores() {
        return (valuesSources == null) ? false : valuesSources.needsScores();
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx,
                                                final LeafBucketCollector sub) throws IOException {
        if (valuesSources == null) {
            return LeafBucketCollector.NO_OP_COLLECTOR;
        }
        final BigArrays bigArrays = context.bigArrays();
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
                if (includeDocument(doc) == true) {
                    stats = bigArrays.grow(stats, bucket + 1);
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
            private boolean includeDocument(int doc) {
                // loop over fields
                for (int i = 0; i < fieldVals.length; ++i) {
                    final NumericDoubleValues doubleValues = values[i];
                    final double value = doubleValues.get(doc);
                    // skip if value is missing
                    if (value == Double.NEGATIVE_INFINITY) {
                        return false;
                    }
                    fieldVals[i] = value;
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
        return new InternalMatrixStats(name, stats.size(), stats.get(bucket), null, pipelineAggregators(), metaData());
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalMatrixStats(name, 0, null, null, pipelineAggregators(), metaData());
    }

    @Override
    public void doClose() {
        Releasables.close(stats);
    }
}
