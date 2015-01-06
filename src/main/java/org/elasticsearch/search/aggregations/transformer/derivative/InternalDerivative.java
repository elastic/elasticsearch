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
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.AggregationStreams;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.bucket.histogram.InternalHistogram;
import org.elasticsearch.search.aggregations.support.format.ValueFormatter;
import org.elasticsearch.search.aggregations.support.format.ValueFormatterStreams;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class InternalDerivative<B extends InternalHistogram.Bucket> extends InternalAggregation {

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
    private @Nullable ValueFormatter formatter;
    private InternalAggregations aggregations;

    public InternalDerivative() {
    }

    public InternalDerivative(String name, boolean keyed, @Nullable ValueFormatter formatter, InternalAggregations subAggregations,
            Map<String, Object> metaData) {
        super(name, metaData);
        this.keyed = keyed;
        this.formatter = formatter;
        this.aggregations = subAggregations;
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
        Long lastValue = null;
        Long newBucketKey = null;
        List<InternalHistogram.Bucket> newBuckets = new ArrayList<>();
        for (InternalHistogram.Bucket histoBucket : histoBuckets) {
            long thisbucketDocCount = histoBucket.getDocCount();
            if (lastValue != null) {
                long diff = thisbucketDocCount - lastValue;
                newBuckets.add(factory.createBucket(newBucketKey, diff, InternalAggregations.EMPTY, keyed, formatter));
            }
            lastValue = thisbucketDocCount;
            newBucketKey = histoBucket.getKeyAsNumber().longValue();
        }
        InternalHistogram<InternalHistogram.Bucket> derivativeHisto = factory.create(name, newBuckets, null, 1, null, formatter, keyed,
                null);
        return derivativeHisto;
    }

    @Override
    public Object getProperty(List<String> path) {
        return null;
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        return builder;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
//        InternalOrder.Streams.writeOrder(order, out); // NOCOMMIT implement order
        ValueFormatterStreams.writeOptional(formatter, out);
        out.writeBoolean(keyed);
        aggregations.writeTo(out);
    }

    @Override
    protected void doReadFrom(StreamInput in) throws IOException {
//        order = InternalOrder.Streams.readOrder(in); // NOCOMMIT implement order
        formatter = ValueFormatterStreams.readOptional(in);
        keyed = in.readBoolean();
        aggregations = InternalAggregations.readAggregations(in);

    }

}
