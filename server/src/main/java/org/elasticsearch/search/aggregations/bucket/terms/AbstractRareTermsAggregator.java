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

import org.elasticsearch.common.util.ExactBloomFilter;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.bucket.DeferableBucketAggregator;
import org.elasticsearch.search.aggregations.bucket.DeferringBucketCollector;
import org.elasticsearch.search.aggregations.bucket.MergingBucketsDeferringCollector;
import org.elasticsearch.search.aggregations.bucket.nested.NestedAggregator;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

public abstract class AbstractRareTermsAggregator<T extends ValuesSource, U extends IncludeExclude.Filter>
    extends DeferableBucketAggregator {

    // TODO review question: What to set this at?
    /**
     Sets the number of "removed" values to accumulate before we purge ords
     via the MergingBucketCollector's mergeBuckets() method
     */
    final long GC_THRESHOLD = 10;

    MergingBucketsDeferringCollector deferringCollector;
    protected final ExactBloomFilter bloom;
    protected final long maxDocCount;
    protected final DocValueFormat format;
    protected final T valuesSource;
    protected final U includeExclude;


    AbstractRareTermsAggregator(String name, AggregatorFactories factories, SearchContext context,
                                          Aggregator parent, List<PipelineAggregator> pipelineAggregators,
                                          Map<String, Object> metaData, long maxDocCount, DocValueFormat format,
                                          T valuesSource, U includeExclude) throws IOException {
        super(name, factories, context, parent, pipelineAggregators, metaData);

        // TODO review: should we expose the BF settings?  What's a good default?
        this.bloom = new ExactBloomFilter(1000000, 0.03, 7000); // ~7mb
        this.addRequestCircuitBreakerBytes(bloom.getSizeInBytes());
        this.maxDocCount = maxDocCount;
        this.format = format;
        this.valuesSource = valuesSource;
        this.includeExclude = includeExclude;
        String scoringAgg = subAggsNeedScore();
        String nestedAgg = descendsFromNestedAggregator(parent);
        if (scoringAgg != null && nestedAgg != null) {
            /*
             * Terms agg would force the collect mode to depth_first here, because
             * we need to access the score of nested documents in a sub-aggregation
             * and we are not able to generate this score while replaying deferred documents.
             *
             * But the RareTerms agg _must_ execute in breadth first since it relies on
             * deferring execution, so we just have to throw up our hands and refuse
             */
            throw new IllegalStateException("RareTerms agg [" + name() + "] is the child of the nested agg [" + nestedAgg
                + "], and also has a scoring child agg [" + scoringAgg + "].  This combination is not supported because " +
                "it requires executing in [depth_first] mode, which the RareTerms agg cannot do.");
        }
    }

    @Override
    protected boolean shouldDefer(Aggregator aggregator) {
        return true;
    }

    @Override
    public DeferringBucketCollector getDeferringCollector() {
        deferringCollector = new MergingBucketsDeferringCollector(context);
        return deferringCollector;
    }

    @Override
    protected void doPostCollection() {
        // Make sure we do one final GC to clean up any deleted ords
        // that may be lingering (but still below GC threshold)
        gcDeletedEntries(null);
    }

    private String subAggsNeedScore() {
        for (Aggregator subAgg : subAggregators) {
            if (subAgg.scoreMode().needsScores()) {
                return subAgg.name();
            }
        }
        return null;
    }

    private String descendsFromNestedAggregator(Aggregator parent) {
        while (parent != null) {
            if (parent.getClass() == NestedAggregator.class) {
                return parent.name();
            }
            parent = parent.parent();
        }
        return null;
    }

    protected abstract void gcDeletedEntries(Long numDeleted);
}
