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

package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorBase;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Map;

public abstract class MetricsAggregator extends AggregatorBase {
    protected MetricsAggregator(String name, SearchContext context, Aggregator parent, Map<String, Object> metadata) throws IOException {
        super(name, AggregatorFactories.EMPTY, context, parent, CardinalityUpperBound.NONE, metadata);
        /*
         * MetricsAggregators may not have sub aggregators so it is safe for
         * us to pass NONE for the super ctor's subAggregatorCardinality.
         */
    }

    /**
     * Build an aggregation for data that has been collected into
     * {@code owningBucketOrd}. 
     */
    public abstract InternalAggregation buildAggregation(long owningBucketOrd) throws IOException;

    @Override
    public final InternalAggregation[] buildAggregations(long[] owningBucketOrds) throws IOException {
        InternalAggregation[] results = new InternalAggregation[owningBucketOrds.length];
        for (int ordIdx = 0; ordIdx < owningBucketOrds.length; ordIdx++) {
            results[ordIdx] = buildAggregation(owningBucketOrds[ordIdx]);
        }
        return results;
    }
}
