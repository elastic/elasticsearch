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

package org.elasticsearch.search.aggregations.transformer.derivative;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationStreams;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.bucket.histogram.InternalHistogram;
import org.elasticsearch.search.aggregations.metrics.InternalNumericMetricsAggregation;
import org.elasticsearch.search.aggregations.support.format.ValueFormatter;
import org.elasticsearch.search.aggregations.transformer.InternalSimpleValue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class InternalDerivative<B extends InternalHistogram.Bucket> extends InternalHistogram<B> implements Derivative {

    final static Type TYPE = new Type("derivative", "deriv");

    private final static AggregationStreams.Stream STREAM = new AggregationStreams.Stream() {
        @Override
        public InternalDerivative readResult(StreamInput in) throws IOException {
            InternalDerivative histogram = new InternalDerivative();
            histogram.readFrom(in);
            return histogram;
        }
    };

    public static void registerStream() {
        AggregationStreams.registerStream(STREAM, TYPE.stream());
    }

    private GapPolicy gapPolicy;
    private InternalAggregations aggregations;

    InternalDerivative() {
        super();
    }

    public InternalDerivative(String name, boolean keyed, @Nullable ValueFormatter formatter, GapPolicy gapPolicy,
            InternalAggregations subAggregations, Map<String, Object> metaData) {
        super(name, Collections.EMPTY_LIST, null, 1, null, formatter, keyed, metaData);
        this.gapPolicy = gapPolicy;
        this.aggregations = subAggregations;
    }

    public InternalDerivative(String name, List<B> buckets, @Nullable ValueFormatter formatter, boolean keyed, GapPolicy gapPolicy,
            Map<String, Object> metaData) {
        super(name, buckets, null, 1, null, formatter, keyed, metaData);
        this.gapPolicy = gapPolicy;
    }

    @Override
    public Type type() {
        return TYPE;
    }

    @Override
    @SuppressWarnings("unchecked")
    public InternalAggregation reduce(ReduceContext reduceContext) {
        List<InternalAggregation> aggregations = reduceContext.aggregations();
        List<InternalAggregations> subAggregationsList = new ArrayList<>(aggregations.size());
        for (InternalAggregation aggregation : aggregations) {
            assert aggregation.getName().equals(getName());
            subAggregationsList.add(((InternalDerivative<B>) aggregation).aggregations);
        }
        final InternalAggregations aggs = InternalAggregations.reduce(subAggregationsList, reduceContext);
        InternalHistogram histo = (InternalHistogram) aggs.iterator().next();
        InternalHistogram.Factory factory = histo.getFactory();
        List<InternalHistogram.Bucket> histoBuckets = histo.getBuckets();
        Long newBucketKey = null;
        Long lastValue = null;
        Map<String, Double> lastMetricValues = null;
        List<InternalHistogram.Bucket> newBuckets = new ArrayList<>();
        double xValue = 1.0;
        for (InternalHistogram.Bucket histoBucket : histoBuckets) {
            long thisbucketDocCount = histoBucket.getDocCount();
            if (thisbucketDocCount == 0) {
                switch (gapPolicy) {
                case ignore:
                    newBucketKey = null;
                    continue;
                case interpolate:
                    xValue++;
                    continue;
                default:
                    break;
                }
            }
            Map<String, Double> thisBucketMetricValues = new HashMap<>();
            for (Aggregation aggregation : histoBucket.getAggregations()) {
                if (aggregation instanceof InternalNumericMetricsAggregation.SingleValue) {
                    InternalNumericMetricsAggregation.SingleValue metricAgg = (InternalNumericMetricsAggregation.SingleValue) aggregation;
                    thisBucketMetricValues.put(metricAgg.getName(), metricAgg.value());
                }
                // NOCOMMIT implement this for multi-value metrics
            }
            if (newBucketKey != null) {
                List<InternalAggregation> metricsAggregations = new ArrayList<>();
                double docCountDeriv = (thisbucketDocCount - lastValue) / xValue;
                InternalSimpleValue docCountDerivAgg = new InternalSimpleValue("_doc_count", docCountDeriv, null); // NOCOMMIT change the name of this to something less confusing
                metricsAggregations.add(docCountDerivAgg);
                for (Entry<String, Double> entry : thisBucketMetricValues.entrySet()) {
                    double metricDeriv = (entry.getValue() - lastMetricValues.get(entry.getKey())) / xValue;
                    InternalSimpleValue metricAgg = new InternalSimpleValue(entry.getKey(), metricDeriv, null);
                    metricsAggregations.add(metricAgg);
                }
                InternalAggregations metricsAggs = new InternalAggregations(metricsAggregations);
                newBuckets.add(factory.createBucket(newBucketKey, 0, metricsAggs, keyed(), formatter()));
            }
            lastValue = thisbucketDocCount;
            lastMetricValues = thisBucketMetricValues;
            newBucketKey = histoBucket.getKeyAsNumber().longValue();
        }
        return new InternalDerivative<>(getName(), newBuckets, formatter(), keyed(), gapPolicy, metaData);
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        super.doWriteTo(out);
        if (aggregations == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            aggregations.writeTo(out);
        }
        gapPolicy.writeTo(out);
    }

    @Override
    protected void doReadFrom(StreamInput in) throws IOException {
        super.doReadFrom(in);
        if (in.readBoolean()) {
            aggregations = InternalAggregations.readAggregations(in);
        } else {
            aggregations = null;
        }
        gapPolicy = GapPolicy.readFrom(in);
    }

}
