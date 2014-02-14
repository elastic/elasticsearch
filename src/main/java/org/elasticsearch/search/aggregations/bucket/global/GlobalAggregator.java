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
package org.elasticsearch.search.aggregations.bucket.global;

import org.apache.lucene.index.AtomicReaderContext;
import org.elasticsearch.search.aggregations.*;
import org.elasticsearch.search.aggregations.bucket.SingleBucketAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;

import java.io.IOException;

/**
 *
 */
public class GlobalAggregator extends SingleBucketAggregator {

    public GlobalAggregator(String name, AggregatorFactories subFactories, AggregationContext aggregationContext) {
        super(name, subFactories, aggregationContext, null);
    }

    @Override
    public void setNextReader(AtomicReaderContext reader) {
    }

    @Override
    public void collect(int doc, long owningBucketOrdinal) throws IOException {
        assert owningBucketOrdinal == 0 : "global aggregator can only be a top level aggregator";
        collectBucket(doc, owningBucketOrdinal);
    }

    @Override
    public InternalAggregation buildAggregation(long owningBucketOrdinal) {
        assert owningBucketOrdinal == 0 : "global aggregator can only be a top level aggregator";
        return new InternalGlobal(name, bucketDocCount(owningBucketOrdinal), bucketAggregations(owningBucketOrdinal));
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        throw new UnsupportedOperationException("global aggregations cannot serve as sub-aggregations, hence should never be called on #buildEmptyAggregations");
    }

    public static class Factory extends AggregatorFactory {

        public Factory(String name) {
            super(name, InternalGlobal.TYPE.name());
        }

        @Override
        public Aggregator create(AggregationContext context, Aggregator parent, long expectedBucketsCount) {
            if (parent != null) {
                throw new AggregationExecutionException("Aggregation [" + parent.name() + "] cannot have a global " +
                        "sub-aggregation [" + name + "]. Global aggregations can only be defined as top level aggregations");
            }
            return new GlobalAggregator(name, factories, context);
        }

    }
}
