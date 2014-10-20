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
package org.elasticsearch.search.aggregations.bucket.sampler;

import org.apache.lucene.index.AtomicReaderContext;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.BucketCollector;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.bucket.BestDocsDeferringCollector;
import org.elasticsearch.search.aggregations.bucket.BucketsAggregator;
import org.elasticsearch.search.aggregations.bucket.DeferringBucketCollector;
import org.elasticsearch.search.aggregations.bucket.RandomDocsSampleDeferringCollector;
import org.elasticsearch.search.aggregations.support.AggregationContext;

import java.io.IOException;
import java.util.Map;

/**
 * Aggregate on only the top-scoring docs on a shard
 */
public class SamplerAggregator extends BucketsAggregator {

    private final int shardSize;
    private BestDocsDeferringCollector bdd;
    private boolean useRandomSample;

    public SamplerAggregator(String name, int shardSize, boolean useRandomSample, AggregatorFactories factories,
            AggregationContext aggregationContext, Aggregator parent, Map<String, Object> metaData) {
        super(name, BucketAggregationMode.PER_BUCKET, factories, 1, aggregationContext, parent, metaData);
        this.shardSize = shardSize;
        this.useRandomSample = useRandomSample;
    }

    @Override
    protected DeferringBucketCollector createDeferrerImpl(BucketCollector deferred, AggregationContext context) {
        if (useRandomSample) {
            bdd = new RandomDocsSampleDeferringCollector(deferred, context, shardSize);
        } else {
            bdd = new BestDocsDeferringCollector(deferred, context, shardSize);
        }
        return bdd;
    }

    @Override
    public void setNextReader(AtomicReaderContext reader) {
    }

    @Override
    public void collect(int doc, long owningBucketOrdinal) throws IOException {
        collectBucket(doc, 0);
    }

    @Override
    protected boolean shouldDefer(Aggregator aggregator) {
        return true;
    }

    @Override
    public InternalAggregation buildAggregation(long owningBucketOrdinal) {
        runDeferredCollections(0);
        return new InternalSampler(name, bdd == null ? 0 : bdd.getDocCount(), bucketAggregations(owningBucketOrdinal), getMetaData());
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalSampler(name, 0, buildEmptySubAggregations(), getMetaData());
    }

    public static class Factory extends AggregatorFactory {

        private int shardSize;
        private boolean useRandomSample;

        public Factory(String name, int shardSize, boolean useRandomSample) {
            super(name, InternalSampler.TYPE.name());
            this.shardSize = shardSize;
            this.useRandomSample = useRandomSample;
        }

        @Override
        protected Aggregator createInternal(AggregationContext context, Aggregator parent, long expectedBucketsCount,
                Map<String, Object> metaData) {
            return new SamplerAggregator(name, shardSize, useRandomSample, factories, context, parent, metaData);
        }

    }
    @Override
    public boolean shouldCollect() {
        return true;
    }
}


