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

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lucene.index.FilterableTermsEnum;
import org.elasticsearch.common.lucene.index.FreqTermsEnum;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.search.aggregations.*;
import org.elasticsearch.search.aggregations.bucket.significant.heuristics.SignificanceHeuristic;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregator;
import org.elasticsearch.search.aggregations.bucket.terms.support.IncludeExclude;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class SignificantTermsAggregatorFactory extends ValuesSourceAggregatorFactory<ValuesSource> implements Releasable {

    public SignificanceHeuristic getSignificanceHeuristic() {
        return significanceHeuristic;
    }

    public enum ExecutionMode {

        MAP(new ParseField("map")) {

            @Override
            Aggregator create(String name, AggregatorFactories factories, ValuesSource valuesSource,
                    TermsAggregator.BucketCountThresholds bucketCountThresholds, IncludeExclude includeExclude,
                    AggregationContext aggregationContext, Aggregator parent, SignificantTermsAggregatorFactory termsAggregatorFactory,
                    List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) throws IOException {
                final IncludeExclude.StringFilter filter = includeExclude == null ? null : includeExclude.convertToStringFilter();
                return new SignificantStringTermsAggregator(name, factories, valuesSource, bucketCountThresholds, filter,
                        aggregationContext, parent, termsAggregatorFactory, pipelineAggregators, metaData);
            }

        },
        GLOBAL_ORDINALS(new ParseField("global_ordinals")) {

            @Override
            Aggregator create(String name, AggregatorFactories factories, ValuesSource valuesSource,
                              TermsAggregator.BucketCountThresholds bucketCountThresholds, IncludeExclude includeExclude,
                    AggregationContext aggregationContext, Aggregator parent, SignificantTermsAggregatorFactory termsAggregatorFactory,
                    List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) throws IOException {
                ValuesSource.Bytes.WithOrdinals valueSourceWithOrdinals = (ValuesSource.Bytes.WithOrdinals) valuesSource;
                IndexSearcher indexSearcher = aggregationContext.searchContext().searcher();
                final IncludeExclude.OrdinalsFilter filter = includeExclude == null ? null : includeExclude.convertToOrdinalsFilter();
                return new GlobalOrdinalsSignificantTermsAggregator(name, factories,
                        (ValuesSource.Bytes.WithOrdinals.FieldData) valuesSource, bucketCountThresholds, filter, aggregationContext,
                        parent, termsAggregatorFactory, pipelineAggregators, metaData);
            }

        },
        GLOBAL_ORDINALS_HASH(new ParseField("global_ordinals_hash")) {

            @Override
            Aggregator create(String name, AggregatorFactories factories, ValuesSource valuesSource,
                              TermsAggregator.BucketCountThresholds bucketCountThresholds, IncludeExclude includeExclude,
                    AggregationContext aggregationContext, Aggregator parent, SignificantTermsAggregatorFactory termsAggregatorFactory,
                    List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) throws IOException {
                final IncludeExclude.OrdinalsFilter filter = includeExclude == null ? null : includeExclude.convertToOrdinalsFilter();
                return new GlobalOrdinalsSignificantTermsAggregator.WithHash(name, factories,
                        (ValuesSource.Bytes.WithOrdinals.FieldData) valuesSource, bucketCountThresholds, filter,
 aggregationContext,
                        parent, termsAggregatorFactory, pipelineAggregators, metaData);
            }
        };

        public static ExecutionMode fromString(String value, ParseFieldMatcher parseFieldMatcher) {
            for (ExecutionMode mode : values()) {
                if (parseFieldMatcher.match(value, mode.parseField)) {
                    return mode;
                }
            }
            throw new IllegalArgumentException("Unknown `execution_hint`: [" + value + "], expected any of " + values());
        }

        private final ParseField parseField;

        ExecutionMode(ParseField parseField) {
            this.parseField = parseField;
        }

        abstract Aggregator create(String name, AggregatorFactories factories, ValuesSource valuesSource,
                                   TermsAggregator.BucketCountThresholds bucketCountThresholds, IncludeExclude includeExclude,
                AggregationContext aggregationContext, Aggregator parent, SignificantTermsAggregatorFactory termsAggregatorFactory,
                List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) throws IOException;

        @Override
        public String toString() {
            return parseField.getPreferredName();
        }
    }
    private final IncludeExclude includeExclude;
    private final String executionHint;
    private String indexedFieldName;
    private MappedFieldType fieldType;
    private FilterableTermsEnum termsEnum;
    private int numberOfAggregatorsCreated = 0;
    private final Query filter;
    private final TermsAggregator.BucketCountThresholds bucketCountThresholds;
    private final SignificanceHeuristic significanceHeuristic;

    protected TermsAggregator.BucketCountThresholds getBucketCountThresholds() {
        return new TermsAggregator.BucketCountThresholds(bucketCountThresholds);
    }

    public SignificantTermsAggregatorFactory(String name, ValuesSourceConfig valueSourceConfig, TermsAggregator.BucketCountThresholds bucketCountThresholds, IncludeExclude includeExclude,
                                             String executionHint, Query filter, SignificanceHeuristic significanceHeuristic) {

        super(name, SignificantStringTerms.TYPE.name(), valueSourceConfig);
        this.bucketCountThresholds = bucketCountThresholds;
        this.includeExclude = includeExclude;
        this.executionHint = executionHint;
        this.significanceHeuristic = significanceHeuristic;
        if (!valueSourceConfig.unmapped()) {
            this.indexedFieldName = config.fieldContext().field();
            fieldType = SearchContext.current().smartNameFieldType(indexedFieldName);
        }
        this.filter = filter;
    }

    @Override
    protected Aggregator createUnmapped(AggregationContext aggregationContext, Aggregator parent,
            List<PipelineAggregator> pipelineAggregators,
            Map<String, Object> metaData) throws IOException {
        final InternalAggregation aggregation = new UnmappedSignificantTerms(name, bucketCountThresholds.getRequiredSize(),
                bucketCountThresholds.getMinDocCount(), pipelineAggregators, metaData);
        return new NonCollectingAggregator(name, aggregationContext, parent, pipelineAggregators, metaData) {
            @Override
            public InternalAggregation buildEmptyAggregation() {
                return aggregation;
            }
        };
    }

    @Override
    protected Aggregator doCreateInternal(ValuesSource valuesSource, AggregationContext aggregationContext, Aggregator parent,
            boolean collectsFromSingleBucket, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData)
            throws IOException {
        if (collectsFromSingleBucket == false) {
            return asMultiBucketAggregator(this, aggregationContext, parent);
        }

        numberOfAggregatorsCreated++;

        if (valuesSource instanceof ValuesSource.Bytes) {
            ExecutionMode execution = null;
            if (executionHint != null) {
                execution = ExecutionMode.fromString(executionHint, aggregationContext.searchContext().parseFieldMatcher());
            }
            if (!(valuesSource instanceof ValuesSource.Bytes.WithOrdinals)) {
                execution = ExecutionMode.MAP;
            }
            if (execution == null) {
                if (Aggregator.descendsFromBucketAggregator(parent)) {
                    execution = ExecutionMode.GLOBAL_ORDINALS_HASH;
                } else {
                    execution = ExecutionMode.GLOBAL_ORDINALS;
                }
            }
            assert execution != null;
            return execution.create(name, factories, valuesSource, bucketCountThresholds, includeExclude, aggregationContext, parent, this,
                    pipelineAggregators, metaData);
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
                    bucketCountThresholds, aggregationContext, parent, this, longFilter, pipelineAggregators, metaData);
        }

        throw new AggregationExecutionException("sigfnificant_terms aggregation cannot be applied to field [" + config.fieldContext().field() +
                "]. It can only be applied to numeric or string fields.");
    }

    /**
     * Creates the TermsEnum (if not already created) and must be called before any calls to getBackgroundFrequency
     * @param context The aggregation context 
     * @return The number of documents in the index (after an optional filter might have been applied)
     */
    public long prepareBackground(AggregationContext context) {
        if (termsEnum != null) {
            // already prepared - return 
            return termsEnum.getNumDocs();
        }
        SearchContext searchContext = context.searchContext();
        IndexReader reader = searchContext.searcher().getIndexReader();
        try {
            if (numberOfAggregatorsCreated == 1) {
                // Setup a termsEnum for sole use by one aggregator
                termsEnum = new FilterableTermsEnum(reader, indexedFieldName, PostingsEnum.NONE, filter);
            } else {
                // When we have > 1 agg we have possibility of duplicate term frequency lookups 
                // and so use a TermsEnum that caches results of all term lookups
                termsEnum = new FreqTermsEnum(reader, indexedFieldName, true, false, filter, searchContext.bigArrays());
            }
        } catch (IOException e) {
            throw new ElasticsearchException("failed to build terms enumeration", e);
        }
        return termsEnum.getNumDocs();
    }

    public long getBackgroundFrequency(BytesRef termBytes) {
        assert termsEnum != null; // having failed to find a field in the index we don't expect any calls for frequencies
        long result = 0;
        try {
            if (termsEnum.seekExact(termBytes)) {
                result = termsEnum.docFreq();
            }
        } catch (IOException e) {
            throw new ElasticsearchException("IOException loading background document frequency info", e);
        }
        return result;
    }


    public long getBackgroundFrequency(long term) {
        BytesRef indexedVal = fieldType.indexedValueForSearch(term);
        return getBackgroundFrequency(indexedVal);
    }

    @Override
    public void close() {
        try {
            if (termsEnum instanceof Releasable) {
                ((Releasable) termsEnum).close();
            }
        } finally {
            termsEnum = null;
        }
    }
}
