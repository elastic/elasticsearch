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

package org.elasticsearch.search.profile.aggregation;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.support.AggregationContext;

import java.io.IOException;

public class ProfilingAggregator extends Aggregator {

    private final Aggregator delegate;
    private final AggregationProfiler profiler;
    private AggregationProfileBreakdown profileBreakdown;

    public ProfilingAggregator(Aggregator delegate, AggregationProfiler profiler) throws IOException {
        this.profiler = profiler;
        this.delegate = delegate;
    }

    @Override
    public void close() {
        delegate.close();
    }

    @Override
    public boolean needsScores() {
        return delegate.needsScores();
    }

    @Override
    public String name() {
        return delegate.name();
    }

    @Override
    public AggregationContext context() {
        return delegate.context();
    }

    @Override
    public Aggregator parent() {
        return delegate.parent();
    }

    @Override
    public Aggregator subAggregator(String name) {
        return delegate.subAggregator(name);
    }

    @Override
    public InternalAggregation buildAggregation(long bucket) throws IOException {
        profileBreakdown.startTime(AggregationTimingType.BUILD_AGGREGATION);
        InternalAggregation result = delegate.buildAggregation(bucket);
        profileBreakdown.stopAndRecordTime();
        return result;
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return delegate.buildEmptyAggregation();
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx) throws IOException {
        return new ProfilingLeafBucketCollector(delegate.getLeafCollector(ctx), profileBreakdown);
    }

    @Override
    public void preCollection() throws IOException {
        this.profileBreakdown = profiler.getQueryBreakdown(delegate);
        profileBreakdown.startTime(AggregationTimingType.INITIALIZE);
        delegate.preCollection();
        profileBreakdown.stopAndRecordTime();
        profiler.pollLastElement();
    }

    @Override
    public void postCollection() throws IOException {
        delegate.postCollection();
    }

}
