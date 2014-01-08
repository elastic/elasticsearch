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
package org.elasticsearch.search.aggregations.bucket.terms;

import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.support.AggregationContext;

import java.io.IOException;

/**
 *
 */
public class UnmappedTermsAggregator extends Aggregator {

    private final InternalOrder order;
    private final int requiredSize;
    private final long minDocCount;

    public UnmappedTermsAggregator(String name, InternalOrder order, int requiredSize, long minDocCount, AggregationContext aggregationContext, Aggregator parent) {
        super(name, BucketAggregationMode.PER_BUCKET, AggregatorFactories.EMPTY, 0, aggregationContext, parent);
        this.order = order;
        this.requiredSize = requiredSize;
        this.minDocCount = minDocCount;
    }

    @Override
    public boolean shouldCollect() {
        return false;
    }

    @Override
    public void collect(int doc, long owningBucketOrdinal) throws IOException {
    }

    @Override
    public InternalAggregation buildAggregation(long owningBucketOrdinal) {
        assert owningBucketOrdinal == 0;
        return new UnmappedTerms(name, order, requiredSize, minDocCount);
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new UnmappedTerms(name, order, requiredSize, minDocCount);
    }
}
