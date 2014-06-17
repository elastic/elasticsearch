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

import java.util.Collections;

abstract class AbstractStringTermsAggregator extends TermsAggregator {


    public AbstractStringTermsAggregator(String name, AggregatorFactories factories,
            long estimatedBucketsCount, AggregationContext context, Aggregator parent,
            InternalOrder order, BucketCountThresholds bucketCountThresholds, SubAggCollectionMode subAggCollectMode) {
        super(name, BucketAggregationMode.PER_BUCKET, factories, estimatedBucketsCount, context, parent, bucketCountThresholds, order, subAggCollectMode);
    }

    @Override
    public boolean shouldCollect() {
        return true;
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new StringTerms(name, order, bucketCountThresholds.getRequiredSize(), bucketCountThresholds.getMinDocCount(), Collections.<InternalTerms.Bucket>emptyList());
    }

}
