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
import org.apache.lucene.search.IndexSearcher;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.search.aggregations.*;
import org.elasticsearch.search.aggregations.bucket.significant.heuristics.SignificanceHeuristic;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregator;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregatorFactory;
import org.elasticsearch.search.aggregations.bucket.terms.support.IncludeExclude;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.internal.SearchContext;

/**
 *
 */
public class SignificantTermsAggregatorFactory extends ValuesSourceAggregatorFactory implements Releasable {

    public SignificanceHeuristic getSignificanceHeuristic() {
        return significanceHeuristic;
    }

    public enum ExecutionMode {

        MAP(new ParseField("map")) {

            @Override
            Aggregator create(String name, AggregatorFactories factories, ValuesSource valuesSource, long estimatedBucketCount,
                              TermsAggregator.BucketCountThresholds bucketCountThresholds, IncludeExclude includeExclude,
                              AggregationContext aggregationContext, Aggregator parent, TermFrequencyProvider termsAggregatorFactory, SignificanceHeuristic significanceHeuristic) {
                return new SignificantStringTermsAggregator(name, factories, valuesSource, estimatedBucketCount, bucketCountThresholds, includeExclude, aggregationContext, parent, termsAggregatorFactory, significanceHeuristic);
            }

            @Override
            boolean needsGlobalOrdinals() {
                return false;
            }

        },
        GLOBAL_ORDINALS(new ParseField("global_ordinals")) {

            @Override
            Aggregator create(String name, AggregatorFactories factories, ValuesSource valuesSource, long estimatedBucketCount,
                              TermsAggregator.BucketCountThresholds bucketCountThresholds, IncludeExclude includeExclude,
                              AggregationContext aggregationContext, Aggregator parent, TermFrequencyProvider termsAggregatorFactory, SignificanceHeuristic significanceHeuristic) {
                ValuesSource.Bytes.WithOrdinals valueSourceWithOrdinals = (ValuesSource.Bytes.WithOrdinals) valuesSource;
                IndexSearcher indexSearcher = aggregationContext.searchContext().searcher();
                long maxOrd = valueSourceWithOrdinals.globalMaxOrd(indexSearcher);
                return new GlobalOrdinalsSignificantTermsAggregator(name, factories,
                        (ValuesSource.Bytes.WithOrdinals.FieldData) valuesSource, estimatedBucketCount, maxOrd, bucketCountThresholds,
                        includeExclude, aggregationContext, parent, termsAggregatorFactory, significanceHeuristic);
            }

            @Override
            boolean needsGlobalOrdinals() {
                return true;
            }

        },
        GLOBAL_ORDINALS_HASH(new ParseField("global_ordinals_hash")) {

            @Override
            Aggregator create(String name, AggregatorFactories factories, ValuesSource valuesSource, long estimatedBucketCount,
                              TermsAggregator.BucketCountThresholds bucketCountThresholds, IncludeExclude includeExclude,
                              AggregationContext aggregationContext, Aggregator parent, TermFrequencyProvider termFrequencyProvider, SignificanceHeuristic significanceHeuristic) {
                return new GlobalOrdinalsSignificantTermsAggregator.WithHash(name, factories,
                        (ValuesSource.Bytes.WithOrdinals.FieldData) valuesSource, estimatedBucketCount, bucketCountThresholds,
                        includeExclude, aggregationContext, parent, termFrequencyProvider, significanceHeuristic);
            }

            @Override
            boolean needsGlobalOrdinals() {
                return true;
            }
        };

        public static ExecutionMode fromString(String value) {
            for (ExecutionMode mode : values()) {
                if (mode.parseField.match(value)) {
                    return mode;
                }
            }
            throw new ElasticsearchIllegalArgumentException("Unknown `execution_hint`: [" + value + "], expected any of " + values());
        }

        private final ParseField parseField;

        ExecutionMode(ParseField parseField) {
            this.parseField = parseField;
        }

        abstract Aggregator create(String name, AggregatorFactories factories, ValuesSource valuesSource, long estimatedBucketCount,
                                   TermsAggregator.BucketCountThresholds bucketCountThresholds, IncludeExclude includeExclude,
                                   AggregationContext aggregationContext, Aggregator parent, TermFrequencyProvider termFrequencyProvider, SignificanceHeuristic significanceHeuristic);

        abstract boolean needsGlobalOrdinals();

        @Override
        public String toString() {
            return parseField.getPreferredName();
        }
    }
    private final IncludeExclude includeExclude;
    private final String executionHint;
    private String indexedFieldName;
    private int numberOfAggregatorsCreated = 0;
    private final TermsAggregator.BucketCountThresholds bucketCountThresholds;
    private final SignificanceHeuristic significanceHeuristic;

    protected TermsAggregator.BucketCountThresholds getBucketCountThresholds() {
        return new TermsAggregator.BucketCountThresholds(bucketCountThresholds);
    }
    TermFrequencyProvider termFrequencyProvider;
    private FieldMapper mapper;

    public SignificantTermsAggregatorFactory(String name, ValuesSourceConfig valueSourceConfig, TermsAggregator.BucketCountThresholds bucketCountThresholds, IncludeExclude includeExclude,
                                             String executionHint, Filter filter, SignificanceHeuristic significanceHeuristic) {
        super(name, SignificantStringTerms.TYPE.name(), valueSourceConfig);
        this.bucketCountThresholds = bucketCountThresholds;        
        this.includeExclude = includeExclude;
        this.executionHint = executionHint;
        this.significanceHeuristic = significanceHeuristic;
        if (!valueSourceConfig.unmapped()) {
            this.indexedFieldName = config.fieldContext().field();
            mapper = SearchContext.current().smartNameFieldMapper(indexedFieldName);
        }
        this.termFrequencyProvider=new TermFrequencyProvider(indexedFieldName, filter, mapper);
    }

    @Override
    protected Aggregator createUnmapped(AggregationContext aggregationContext, Aggregator parent) {
        final InternalAggregation aggregation = new UnmappedSignificantTerms(name, bucketCountThresholds.getRequiredSize(), bucketCountThresholds.getMinDocCount());
        return new NonCollectingAggregator(name, aggregationContext, parent) {
            @Override
            public InternalAggregation buildEmptyAggregation() {
                return aggregation;
            }
        };
    }

    @Override
    protected Aggregator create(ValuesSource valuesSource, long expectedBucketsCount, AggregationContext aggregationContext, Aggregator parent) {
        numberOfAggregatorsCreated++;
        if (numberOfAggregatorsCreated > 1) {
            termFrequencyProvider.setUseCaching(true);
        }
        
        long estimatedBucketCount = TermsAggregatorFactory.estimatedBucketCount(valuesSource, parent);
        if (valuesSource instanceof ValuesSource.Bytes) {
            ExecutionMode execution = null;
            if (executionHint != null) {
                execution = ExecutionMode.fromString(executionHint);
            }
            if (!(valuesSource instanceof ValuesSource.Bytes.WithOrdinals)) {
                execution = ExecutionMode.MAP;
            }
            if (execution == null) {
                if (Aggregator.hasParentBucketAggregator(parent)) {
                    execution = ExecutionMode.GLOBAL_ORDINALS_HASH;
                } else {
                    execution = ExecutionMode.GLOBAL_ORDINALS;
                }
            }
            assert execution != null;
            valuesSource.setNeedsGlobalOrdinals(execution.needsGlobalOrdinals());
            return execution.create(name, factories, valuesSource, estimatedBucketCount, bucketCountThresholds, includeExclude, aggregationContext, parent, termFrequencyProvider, significanceHeuristic);
        }

        
        if ((includeExclude != null) && (includeExclude.isRegexBased())) {
            throw new AggregationExecutionException("Aggregation [" + name + "] cannot support regular expression style include/exclude " +
                    "settings as they can only be applied to string fields. Use an array of numeric values for include/exclude clauses used to filter numeric fields");
        }

        if (valuesSource instanceof ValuesSource.Numeric) {

            if (((ValuesSource.Numeric) valuesSource).isFloatingPoint()) {
                throw new UnsupportedOperationException("No support for examining floating point numerics");
            }
            IncludeExclude.LongFilter longFilter = null;
            if (includeExclude != null) {
                longFilter = includeExclude.convertToLongFilter();
            }
            return new SignificantLongTermsAggregator(name, factories, (ValuesSource.Numeric) valuesSource, config.format(),
                    estimatedBucketCount, bucketCountThresholds, aggregationContext, parent, termFrequencyProvider, longFilter,
                    significanceHeuristic);
        }

        throw new AggregationExecutionException("sigfnificant_terms aggregation cannot be applied to field [" + config.fieldContext().field() +
                "]. It can only be applied to numeric or string fields.");
    }

    @Override
    public void close() throws ElasticsearchException {
        Releasables.close(termFrequencyProvider);
    }
}
