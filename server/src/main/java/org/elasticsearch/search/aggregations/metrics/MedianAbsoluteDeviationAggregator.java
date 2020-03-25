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

package org.elasticsearch.search.aggregations.metrics;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.ScoreMode;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.ObjectArray;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.search.aggregations.metrics.InternalMedianAbsoluteDeviation.computeMedianAbsoluteDeviation;

public class MedianAbsoluteDeviationAggregator extends NumericMetricsAggregator.SingleValue {

    private final ValuesSource.Numeric valuesSource;
    private final DocValueFormat format;

    private final double compression;

    private ObjectArray<TDigestState> valueSketches;

    MedianAbsoluteDeviationAggregator(String name,
                                             SearchContext context,
                                             Aggregator parent,
                                             List<PipelineAggregator> pipelineAggregators,
                                             Map<String, Object> metaData,
                                             @Nullable ValuesSource.Numeric valuesSource,
                                             DocValueFormat format,
                                             double compression) throws IOException {

        super(name, context, parent, pipelineAggregators, metaData);

        this.valuesSource = valuesSource;
        this.format = Objects.requireNonNull(format);
        this.compression = compression;
        this.valueSketches = context.bigArrays().newObjectArray(1);
    }

    private boolean hasDataForBucket(long bucketOrd) {
        return bucketOrd < valueSketches.size() && valueSketches.get(bucketOrd) != null;
    }

    @Override
    public double metric(long owningBucketOrd) {
        if (hasDataForBucket(owningBucketOrd)) {
            return computeMedianAbsoluteDeviation(valueSketches.get(owningBucketOrd));
        } else {
            return Double.NaN;
        }
    }

    @Override
    public ScoreMode scoreMode() {
        if (valuesSource != null && valuesSource.needsScores()) {
            return ScoreMode.COMPLETE;
        } else {
            return ScoreMode.COMPLETE_NO_SCORES;
        }
    }

    @Override
    protected LeafBucketCollector getLeafCollector(LeafReaderContext ctx, LeafBucketCollector sub) throws IOException {
        if (valuesSource == null) {
            return LeafBucketCollector.NO_OP_COLLECTOR;
        }

        final BigArrays bigArrays = context.bigArrays();
        final SortedNumericDoubleValues values = valuesSource.doubleValues(ctx);

        return new LeafBucketCollectorBase(sub, values) {
            @Override
            public void collect(int doc, long bucket) throws IOException {

                valueSketches = bigArrays.grow(valueSketches, bucket + 1);

                TDigestState valueSketch = valueSketches.get(bucket);
                if (valueSketch == null) {
                    valueSketch = new TDigestState(compression);
                    valueSketches.set(bucket, valueSketch);
                }

                if (values.advanceExact(doc)) {
                    final int valueCount = values.docValueCount();
                    for (int i = 0; i < valueCount; i++) {
                        final double value = values.nextValue();
                        valueSketch.add(value);
                    }
                }
            }
        };
    }

    @Override
    public InternalAggregation buildAggregation(long bucket) throws IOException {
        if (hasDataForBucket(bucket)) {
            final TDigestState valueSketch = valueSketches.get(bucket);
            return new InternalMedianAbsoluteDeviation(name, pipelineAggregators(), metaData(), format, valueSketch);
        } else {
            return buildEmptyAggregation();
        }
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalMedianAbsoluteDeviation(name, pipelineAggregators(), metaData(), format, new TDigestState(compression));
    }

    @Override
    public void doClose() {
        Releasables.close(valueSketches);
    }

}
