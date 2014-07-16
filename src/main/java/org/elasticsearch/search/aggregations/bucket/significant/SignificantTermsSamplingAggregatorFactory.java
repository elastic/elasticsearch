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
package org.elasticsearch.search.aggregations.bucket.significant;

import org.apache.lucene.search.Filter;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.bucket.significant.heuristics.SignificanceHeuristic;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregator;
import org.elasticsearch.search.aggregations.bucket.terms.support.IncludeExclude;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.internal.SearchContext;

/**
 *
 */
public class SignificantTermsSamplingAggregatorFactory extends AggregatorFactory implements Releasable {

    private final IncludeExclude includeExclude;
    private String indexedFieldName;
    private int numberOfAggregatorsCreated = 0;
    private final TermsAggregator.BucketCountThresholds bucketCountThresholds;
    private final SampleSettings samplerSettings;
    private TermFrequencyProvider termFrequencyProvider;
    private SignificanceHeuristic significanceHeuristic;

    public SignificantTermsSamplingAggregatorFactory(SearchContext context,String name, String indexedFieldName, TermsAggregator.BucketCountThresholds bucketCountThresholds, IncludeExclude includeExclude,
                                             String executionHint, Filter filter, SignificanceHeuristic significanceHeuristic, SampleSettings samplerSettings) {

        super(name, SignificantStringTerms.TYPE.name());
        this.bucketCountThresholds = bucketCountThresholds;        
        this.includeExclude = includeExclude;
        this.indexedFieldName = indexedFieldName;
        this.samplerSettings = samplerSettings;
        this.significanceHeuristic = significanceHeuristic;
        FieldMapper mapper = context.smartNameFieldMapper(indexedFieldName);
        termFrequencyProvider=new TermFrequencyProvider(indexedFieldName,filter,mapper);
    }

    @Override
    public void close() throws ElasticsearchException {
        Releasables.close(termFrequencyProvider);
    }

    @Override
    public Aggregator create(AggregationContext context, Aggregator parent, long expectedBucketsCount) {
        numberOfAggregatorsCreated++;
        if (numberOfAggregatorsCreated > 1) {
            termFrequencyProvider.setUseCaching(true);
        }
        long estimatedBucketCount = 1000;// TODO not sure how best to estimate?
        return new SignificantStringTermsSamplingAggregator(name, factories, indexedFieldName, estimatedBucketCount, bucketCountThresholds,
                includeExclude, context, parent, termFrequencyProvider, significanceHeuristic, samplerSettings);
    }
}
