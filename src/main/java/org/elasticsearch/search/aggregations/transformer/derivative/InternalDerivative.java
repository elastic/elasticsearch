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
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationStreams;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.InternalMultiBucketAggregation;
import org.elasticsearch.search.aggregations.bucket.histogram.InternalHistogram;
import org.elasticsearch.search.aggregations.metrics.InternalNumericMetricsAggregation;
import org.elasticsearch.search.aggregations.support.format.ValueFormatter;
import org.elasticsearch.search.aggregations.support.format.ValueFormatterStreams;
import org.elasticsearch.search.aggregations.transformer.InternalSimpleValue;
import org.elasticsearch.search.aggregations.transformer.derivative.Derivative.GapPolicy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class InternalDerivative<B extends InternalHistogram.Bucket> extends InternalMultiBucketAggregation {

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

    private boolean keyed;
    private @Nullable
    ValueFormatter formatter;
    private GapPolicy gapPolicy;
    private InternalAggregations aggregations;

    private InternalHistogram<?> derivativeResult;

    InternalDerivative() {
    }

    public InternalDerivative(String name, boolean keyed, @Nullable ValueFormatter formatter, GapPolicy gapPolicy,
            InternalAggregations subAggregations, Map<String, Object> metaData) {
        super(name, metaData);
        this.keyed = keyed;
        this.formatter = formatter;
        this.gapPolicy = gapPolicy;
        this.aggregations = subAggregations;
    }

    public InternalDerivative(String name, boolean keyed, @Nullable ValueFormatter formatter, GapPolicy gapPolicy,
            Map<String, Object> metaData, InternalHistogram<?> derivativeResult) {
        this(name, keyed, formatter, gapPolicy, null, metaData);
        this.derivativeResult = derivativeResult;
    }

    @Override
    public Type type() {
        return TYPE;
    }

    @Override
    public List<? extends Bucket> getBuckets() {
        return derivativeResult.getBuckets();
    }

    @Override
    public <B extends Bucket> B getBucketByKey(String key) {
        return getBucketByKey(key);
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
                double docCountDeriv = (thisbucketDocCount - lastValue) / xValue;
                List<InternalAggregation> metricsAggregations = new ArrayList<>();
                for (Entry<String, Double> entry : thisBucketMetricValues.entrySet()) {
                    double metricDeriv = (entry.getValue() - lastMetricValues.get(entry.getKey())) / xValue;
                    InternalSimpleValue metricAgg = new InternalSimpleValue(entry.getKey(), metricDeriv, null);
                    metricsAggregations.add(metricAgg);
                }
                InternalSimpleValue docCountDerivAgg = new InternalSimpleValue("_doc_count", docCountDeriv, null);
                metricsAggregations.add(docCountDerivAgg);
                InternalAggregations metricsAggs = new InternalAggregations(metricsAggregations);
                newBuckets.add(factory.createBucket(newBucketKey, 0, metricsAggs, keyed, formatter));
            }
            lastValue = thisbucketDocCount;
            lastMetricValues = thisBucketMetricValues;
            newBucketKey = histoBucket.getKeyAsNumber().longValue();
        }
        InternalHistogram<InternalHistogram.Bucket> derivativeHisto = factory.create(name, newBuckets, null, 1, null, formatter, keyed,
                null);
        return new InternalDerivative<>(getName(), keyed, formatter, gapPolicy, metaData, derivativeHisto);
    }

    @Override
    public Object getProperty(List<String> path) {
        return null;
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        if (this.derivativeResult != null) {
            this.derivativeResult.doXContentBody(builder, params);
        }
        return builder;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        //        InternalOrder.Streams.writeOrder(order, out); // NOCOMMIT implement order
        ValueFormatterStreams.writeOptional(formatter, out);
        out.writeBoolean(keyed);
        aggregations.writeTo(out);
        out.writeBytesReference(derivativeResult.type().stream());
        derivativeResult.writeTo(out);
    }

    @Override
    protected void doReadFrom(StreamInput in) throws IOException {
        //        order = InternalOrder.Streams.readOrder(in); // NOCOMMIT implement order
        formatter = ValueFormatterStreams.readOptional(in);
        keyed = in.readBoolean();
        aggregations = InternalAggregations.readAggregations(in);
        BytesReference type = in.readBytesReference();
        derivativeResult = (InternalHistogram<?>) AggregationStreams.stream(type).readResult(in);
    }

}
