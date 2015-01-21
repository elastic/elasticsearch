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

package org.elasticsearch.search.aggregations.transformer;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.bucket.SingleBucketAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;

import java.io.IOException;
import java.util.Map;

public abstract class Transformer extends SingleBucketAggregator {

    protected Transformer(String name, AggregatorFactories factories, AggregationContext aggregationContext, Aggregator parent,
            Map<String, Object> metaData) throws IOException {
        super(name, factories, aggregationContext, parent, metaData);
    }

    @Override
    public void setNextReader(LeafReaderContext reader) {
        // Do nothing - we don't filter the documents in the collect phase, we
        // just pass them straight through to the sub-aggregations
    }

    @Override
    public void collect(int doc, long owningBucketOrdinal) throws IOException {
        collectBucket(doc, owningBucketOrdinal);
    }

    @Override
    public InternalAggregation buildAggregation(long owningBucketOrdinal) throws IOException {
        return buildAggregation(name, bucketDocCount(owningBucketOrdinal), bucketAggregations(owningBucketOrdinal));
    }

    protected abstract InternalAggregation buildAggregation(String name, int bucketDocCount, InternalAggregations bucketAggregations);
}
