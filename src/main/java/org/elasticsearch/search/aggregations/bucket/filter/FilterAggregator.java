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
package org.elasticsearch.search.aggregations.bucket.filter;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.Filter;
import org.apache.lucene.util.Bits;
import org.elasticsearch.common.lucene.docset.DocIdSets;
import org.elasticsearch.search.aggregations.*;
import org.elasticsearch.search.aggregations.bucket.SingleBucketAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;

import java.io.IOException;

/**
 * Aggregate all docs that match a filter.
 */
public class FilterAggregator extends SingleBucketAggregator {

    private final Filter filter;

    private Bits bits;

    public FilterAggregator(String name,
                            org.apache.lucene.search.Filter filter,
                            AggregatorFactories factories,
                            AggregationContext aggregationContext,
                            Aggregator parent) {
        super(name, factories, aggregationContext, parent);
        this.filter = filter;
    }

    @Override
    public void setNextReader(AtomicReaderContext reader) {
        try {
            bits = DocIdSets.toSafeBits(reader.reader(), filter.getDocIdSet(reader, reader.reader().getLiveDocs()));
        } catch (IOException ioe) {
            throw new AggregationExecutionException("Failed to aggregate filter aggregator [" + name + "]", ioe);
        }
    }

    @Override
    public void collect(int doc, long owningBucketOrdinal) throws IOException {
        if (bits.get(doc)) {
            collectBucket(doc, owningBucketOrdinal);
        }
    }

    @Override
    public InternalAggregation buildAggregation(long owningBucketOrdinal) {
        return new InternalFilter(name, bucketDocCount(owningBucketOrdinal), bucketAggregations(owningBucketOrdinal));
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalFilter(name, 0, buildEmptySubAggregations());
    }

    public static class Factory extends AggregatorFactory {

        private org.apache.lucene.search.Filter filter;

        public Factory(String name, Filter filter) {
            super(name, InternalFilter.TYPE.name());
            this.filter = filter;
        }

        @Override
        public Aggregator create(AggregationContext context, Aggregator parent, long expectedBucketsCount) {
            return new FilterAggregator(name, filter, factories, context, parent);
        }

    }
}


