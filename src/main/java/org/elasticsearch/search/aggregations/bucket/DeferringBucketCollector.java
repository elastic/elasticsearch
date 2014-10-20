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

package org.elasticsearch.search.aggregations.bucket;

import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.search.aggregations.BucketCollector;
import org.elasticsearch.search.aggregations.RecordingBucketCollector;
import org.elasticsearch.search.aggregations.support.AggregationContext;

/**
 * Buffers the matches in a collect stream and can replay a subset of the collected buckets
 * to a deferred set of collectors.
 * The rationale for not bundling all this logic into {@link RecordingBucketCollector} is to allow
 * the possibility of alternative recorder impl choices while keeping the logic in here for
 * setting {@link AggregationContext}'s setNextReader method and preparing the appropriate choice 
 * of filtering logic for stream replay. These felt like agg-specific functions that should be kept away
 * from the {@link RecordingBucketCollector} impl which is concentrated on efficient storage of doc and bucket IDs  
 */
public abstract class DeferringBucketCollector extends BucketCollector implements Releasable {
    
    protected final BucketCollector deferred;
    protected final AggregationContext context;

    protected DeferringBucketCollector(BucketCollector deferred, AggregationContext context) {
        this.deferred = deferred;
        this.context = context;
    }

    public abstract void prepareSelectedBuckets(long... survivingBucketOrds);

}
