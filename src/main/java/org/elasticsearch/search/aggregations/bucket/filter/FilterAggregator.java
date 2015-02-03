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

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Filter;
import org.apache.lucene.util.Bits;
import org.elasticsearch.common.lucene.docset.DocIdSets;
import org.elasticsearch.search.aggregations.*;
import org.elasticsearch.search.aggregations.bucket.SingleBucketAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;

import java.io.IOException;
import java.util.Map;

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
                            Aggregator parent,
                            Map<String, Object> metaData) throws IOException {
        super(name, factories, aggregationContext, parent, metaData);
        this.filter = filter;
    }

    @Override
    public void setNextReader(LeafReaderContext reader) {
        try {
            bits = DocIdSets.asSequentialAccessBits(reader.reader().maxDoc(), filter.getDocIdSet(reader, null));
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
    public InternalAggregation buildAggregation(long owningBucketOrdinal) throws IOException {
        return new InternalFilter(name, bucketDocCount(owningBucketOrdinal), bucketAggregations(owningBucketOrdinal), metaData());
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalFilter(name, 0, buildEmptySubAggregations(), metaData());
    }

    public static class Factory extends AggregatorFactory {

        private org.apache.lucene.search.Filter filter;

        public Factory(String name, Filter filter) {
            super(name, InternalFilter.TYPE.name());
            this.filter = filter;
        }

        @Override
        public Aggregator createInternal(AggregationContext context, Aggregator parent, boolean collectsFromSingleBucket, Map<String, Object> metaData) throws IOException {
            return new FilterAggregator(name, filter, factories, context, parent, metaData);
        }

    }
}


