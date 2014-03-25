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

package org.elasticsearch.search.aggregations;

import org.apache.lucene.index.AtomicReaderContext;
import org.elasticsearch.search.aggregations.support.AggregationContext;

import java.io.IOException;

/**
 * An aggregator that is not collected, this can typically be used when running an aggregation over a field that doesn't have
 * a mapping.
 */
public abstract class NonCollectingAggregator extends Aggregator {

    protected NonCollectingAggregator(String name, AggregationContext context, Aggregator parent) {
        super(name, BucketAggregationMode.MULTI_BUCKETS, AggregatorFactories.EMPTY, 0, context, parent);
    }

    private void fail() {
        throw new IllegalStateException("This aggregator should not be collected");
    }

    @Override
    public final void setNextReader(AtomicReaderContext reader) {
        fail();
    }

    @Override
    public final boolean shouldCollect() {
        return false;
    }

    @Override
    public final void collect(int doc, long owningBucketOrdinal) throws IOException {
        fail();
    }

    @Override
    public final InternalAggregation buildAggregation(long owningBucketOrdinal) {
        return buildEmptyAggregation();
    }

}
