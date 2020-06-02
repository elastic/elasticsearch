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
package org.elasticsearch.join.aggregations;

import org.apache.lucene.search.Query;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Map;

public class ParentToChildrenAggregator extends ParentJoinAggregator {

    static final ParseField TYPE_FIELD = new ParseField("type");

    public ParentToChildrenAggregator(String name, AggregatorFactories factories,
            SearchContext context, Aggregator parent, Query childFilter,
            Query parentFilter, ValuesSource.Bytes.WithOrdinals valuesSource,
            long maxOrd, Map<String, Object> metadata) throws IOException {
        super(name, factories, context, parent, parentFilter, childFilter, valuesSource, maxOrd, metadata);
    }

    @Override
    public InternalAggregation[] buildAggregations(long[] owningBucketOrds) throws IOException {
        return buildAggregationsForSingleBucket(owningBucketOrds, (owningBucketOrd, subAggregationResults) ->
            new InternalChildren(name, bucketDocCount(owningBucketOrd), subAggregationResults, metadata()));
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalChildren(name, 0, buildEmptySubAggregations(), metadata());
    }
}
