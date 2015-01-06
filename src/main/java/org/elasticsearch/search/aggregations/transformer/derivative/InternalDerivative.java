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

import com.carrotsearch.hppc.LongObjectOpenHashMap;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.AggregationStreams;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.InternalMultiBucketAggregation;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.histogram.InternalHistogram;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class InternalDerivative<B extends InternalHistogram.Bucket> extends InternalMultiBucketAggregation implements Histogram {

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

    public InternalDerivative() {
    }

    public InternalDerivative(String name, InternalAggregations subAggregations, Map<String, Object> metaData) {
        super(name, metaData);
        this.aggregations = subAggregations;
    }

    protected List<B> buckets;
    private LongObjectOpenHashMap<B> bucketsMap;
    private InternalAggregations aggregations;

    @Override
    public Type type() {
        return TYPE;
    }

    @Override
    public List<B> getBuckets() {
        return buckets;
    }

    @Override
    public B getBucketByKey(String key) {
        return getBucketByKey(Long.valueOf(key));
    }

    @Override
    public B getBucketByKey(Number key) {
        if (bucketsMap == null) {
            bucketsMap = new LongObjectOpenHashMap<>(buckets.size());
            for (B bucket : buckets) {
                bucketsMap.put(bucket.getKeyAsNumber().longValue(), bucket);
            }
        }
        return bucketsMap.get(key.longValue());
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
                newBuckets.add(factory.createBucket(newBucketKey, diff, InternalAggregations.EMPTY, histo.keyed(), histo.formatter())); // NOCOMMIT get keyed and formatter from histoBucket
            }
            lastValue = thisbucketDocCount;
            newBucketKey = histoBucket.getKeyAsNumber().longValue();
        }
        InternalHistogram<InternalHistogram.Bucket> derivativeHisto = factory.create(name, newBuckets, null, 1, null, histo.formatter(), histo.keyed(), null);
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
    }

    @Override
    protected void doReadFrom(StreamInput in) throws IOException {
    }

}
