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
import java.util.Collections;
import java.util.Map;

abstract class AbstractStringTermsAggregator extends TermsAggregator {

    protected final boolean showTermDocCountError;

    public AbstractStringTermsAggregator(String name, AggregatorFactories factories,
            AggregationContext context, Aggregator parent,
            Terms.Order order, BucketCountThresholds bucketCountThresholds,
            SubAggCollectionMode subAggCollectMode, boolean showTermDocCountError, Map<String, Object> metaData) throws IOException {
        super(name, factories, context, parent, bucketCountThresholds, order, subAggCollectMode, metaData);
        this.showTermDocCountError = showTermDocCountError;
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new StringTerms(name, order, bucketCountThresholds.getRequiredSize(), bucketCountThresholds.getShardSize(), bucketCountThresholds.getMinDocCount(), Collections.<InternalTerms.Bucket>emptyList(), showTermDocCountError, 0, 0, metaData());
    }

}
