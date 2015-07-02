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

import org.apache.lucene.search.IndexSearcher;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.search.aggregations.*;
import org.elasticsearch.search.aggregations.Aggregator.SubAggCollectionMode;
import org.elasticsearch.search.aggregations.bucket.terms.support.IncludeExclude;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class TermsAggregatorFactory extends ValuesSourceAggregatorFactory<ValuesSource> {

    public enum ExecutionMode {

        MAP(new ParseField("map")) {

            @Override
            Aggregator create(String name, AggregatorFactories factories, ValuesSource valuesSource, Terms.Order order,
                    TermsAggregator.BucketCountThresholds bucketCountThresholds, IncludeExclude includeExclude,
                    AggregationContext aggregationContext, Aggregator parent, SubAggCollectionMode subAggCollectMode,
                    boolean showTermDocCountError, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData)
                    throws IOException {
                final IncludeExclude.StringFilter filter = includeExclude == null ? null : includeExclude.convertToStringFilter();
                return new StringTermsAggregator(name, factories, valuesSource, order, bucketCountThresholds, filter, aggregationContext,
                        parent, subAggCollectMode, showTermDocCountError, pipelineAggregators, metaData);
            }

            @Override
            boolean needsGlobalOrdinals() {
                return false;
            }

        },
        GLOBAL_ORDINALS(new ParseField("global_ordinals")) {

            @Override
            Aggregator create(String name, AggregatorFactories factories, ValuesSource valuesSource, Terms.Order order,
                    TermsAggregator.BucketCountThresholds bucketCountThresholds, IncludeExclude includeExclude,
                    AggregationContext aggregationContext, Aggregator parent, SubAggCollectionMode subAggCollectMode,
                    boolean showTermDocCountError, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData)
                    throws IOException {
                final IncludeExclude.OrdinalsFilter filter = includeExclude == null ? null : includeExclude.convertToOrdinalsFilter();
                return new GlobalOrdinalsStringTermsAggregator(name, factories, (ValuesSource.Bytes.WithOrdinals) valuesSource, order,
                        bucketCountThresholds, filter, aggregationContext, parent, subAggCollectMode, showTermDocCountError,
                        pipelineAggregators, metaData);
            }

            @Override
            boolean needsGlobalOrdinals() {
                return true;
            }

        },
        GLOBAL_ORDINALS_HASH(new ParseField("global_ordinals_hash")) {

            @Override
            Aggregator create(String name, AggregatorFactories factories, ValuesSource valuesSource, Terms.Order order,
                    TermsAggregator.BucketCountThresholds bucketCountThresholds, IncludeExclude includeExclude,
                    AggregationContext aggregationContext, Aggregator parent, SubAggCollectionMode subAggCollectMode,
                    boolean showTermDocCountError, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData)
                    throws IOException {
                final IncludeExclude.OrdinalsFilter filter = includeExclude == null ? null : includeExclude.convertToOrdinalsFilter();
                return new GlobalOrdinalsStringTermsAggregator.WithHash(name, factories,
                        (ValuesSource.Bytes.WithOrdinals.FieldData) valuesSource, order, bucketCountThresholds, filter, aggregationContext,
                        parent, subAggCollectMode, showTermDocCountError, pipelineAggregators, metaData);
            }

            @Override
            boolean needsGlobalOrdinals() {
                return true;
            }
        },
        GLOBAL_ORDINALS_LOW_CARDINALITY(new ParseField("global_ordinals_low_cardinality")) {

            @Override
            Aggregator create(String name, AggregatorFactories factories, ValuesSource valuesSource, Terms.Order order,
                    TermsAggregator.BucketCountThresholds bucketCountThresholds, IncludeExclude includeExclude,
                    AggregationContext aggregationContext, Aggregator parent, SubAggCollectionMode subAggCollectMode,
                    boolean showTermDocCountError, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData)
                    throws IOException {
                if (includeExclude != null || factories.count() > 0) {
                    return GLOBAL_ORDINALS.create(name, factories, valuesSource, order, bucketCountThresholds, includeExclude,
                            aggregationContext, parent, subAggCollectMode, showTermDocCountError, pipelineAggregators, metaData);
                }
                return new GlobalOrdinalsStringTermsAggregator.LowCardinality(name, factories,
                        (ValuesSource.Bytes.WithOrdinals) valuesSource, order, bucketCountThresholds, aggregationContext, parent,
                        subAggCollectMode, showTermDocCountError, pipelineAggregators, metaData);
            }

            @Override
            boolean needsGlobalOrdinals() {
                return true;
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

        abstract Aggregator create(String name, AggregatorFactories factories, ValuesSource valuesSource, Terms.Order order,
                TermsAggregator.BucketCountThresholds bucketCountThresholds, IncludeExclude includeExclude,
                AggregationContext aggregationContext, Aggregator parent, SubAggCollectionMode subAggCollectMode,
                boolean showTermDocCountError, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData)
                throws IOException;

        abstract boolean needsGlobalOrdinals();

        @Override
        public String toString() {
            return parseField.getPreferredName();
        }
    }

    private final Terms.Order order;
    private final IncludeExclude includeExclude;
    private final String executionHint;
    private final SubAggCollectionMode collectMode;
    private final TermsAggregator.BucketCountThresholds bucketCountThresholds;
    private final boolean showTermDocCountError;

    public TermsAggregatorFactory(String name, ValuesSourceConfig config, Terms.Order order,
            TermsAggregator.BucketCountThresholds bucketCountThresholds, IncludeExclude includeExclude, String executionHint,
            SubAggCollectionMode executionMode, boolean showTermDocCountError) {
        super(name, StringTerms.TYPE.name(), config);
        this.order = order;
        this.includeExclude = includeExclude;
        this.executionHint = executionHint;
        this.bucketCountThresholds = bucketCountThresholds;
        this.collectMode = executionMode;
        this.showTermDocCountError = showTermDocCountError;
    }

    @Override
    protected Aggregator createUnmapped(AggregationContext aggregationContext, Aggregator parent,
            List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) throws IOException {
        final InternalAggregation aggregation = new UnmappedTerms(name, order, bucketCountThresholds.getRequiredSize(),
                bucketCountThresholds.getShardSize(), bucketCountThresholds.getMinDocCount(), pipelineAggregators, metaData);
        return new NonCollectingAggregator(name, aggregationContext, parent, factories, pipelineAggregators, metaData) {
            {
                // even in the case of an unmapped aggregator, validate the order
                InternalOrder.validate(order, this);
            }

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
        if (valuesSource instanceof ValuesSource.Bytes) {
            ExecutionMode execution = null;
            if (executionHint != null) {
                execution = ExecutionMode.fromString(executionHint, aggregationContext.searchContext().parseFieldMatcher());
            }

            // In some cases, using ordinals is just not supported: override it
            if (!(valuesSource instanceof ValuesSource.Bytes.WithOrdinals)) {
                execution = ExecutionMode.MAP;
            }

            final long maxOrd;
            final double ratio;
            if (execution == null || execution.needsGlobalOrdinals()) {
                ValuesSource.Bytes.WithOrdinals valueSourceWithOrdinals = (ValuesSource.Bytes.WithOrdinals) valuesSource;
                IndexSearcher indexSearcher = aggregationContext.searchContext().searcher();
                maxOrd = valueSourceWithOrdinals.globalMaxOrd(indexSearcher);
                ratio = maxOrd / ((double) indexSearcher.getIndexReader().numDocs());
            } else {
                maxOrd = -1;
                ratio = -1;
            }

            // Let's try to use a good default
            if (execution == null) {
                // if there is a parent bucket aggregator the number of instances of this aggregator is going
                // to be unbounded and most instances may only aggregate few documents, so use hashed based
                // global ordinals to keep the bucket ords dense.
                if (Aggregator.descendsFromBucketAggregator(parent)) {
                    execution = ExecutionMode.GLOBAL_ORDINALS_HASH;
                } else {
                    if (factories == AggregatorFactories.EMPTY) {
                        if (ratio <= 0.5 && maxOrd <= 2048) {
                            // 0.5: At least we need reduce the number of global ordinals look-ups by half
                            // 2048: GLOBAL_ORDINALS_LOW_CARDINALITY has additional memory usage, which directly linked to maxOrd, so we need to limit.
                            execution = ExecutionMode.GLOBAL_ORDINALS_LOW_CARDINALITY;
                        } else {
                            execution = ExecutionMode.GLOBAL_ORDINALS;
                        }
                    } else {
                        execution = ExecutionMode.GLOBAL_ORDINALS;
                    }
                }
            }

            return execution.create(name, factories, valuesSource, order, bucketCountThresholds, includeExclude, aggregationContext,
                    parent, collectMode, showTermDocCountError, pipelineAggregators, metaData);
        }

        if ((includeExclude != null) && (includeExclude.isRegexBased())) {
            throw new AggregationExecutionException(
                    "Aggregation ["
                            + name
                            + "] cannot support regular expression style include/exclude "
                            + "settings as they can only be applied to string fields. Use an array of numeric values for include/exclude clauses used to filter numeric fields");
        }

        if (valuesSource instanceof ValuesSource.Numeric) {
            IncludeExclude.LongFilter longFilter = null;
            if (((ValuesSource.Numeric) valuesSource).isFloatingPoint()) {
                if (includeExclude != null) {
                    longFilter = includeExclude.convertToDoubleFilter();
                }
                return new DoubleTermsAggregator(name, factories, (ValuesSource.Numeric) valuesSource, config.format(), order,
                        bucketCountThresholds, aggregationContext, parent, collectMode, showTermDocCountError, longFilter,
                        pipelineAggregators, metaData);
            }
            if (includeExclude != null) {
                longFilter = includeExclude.convertToLongFilter();
            }
            return new LongTermsAggregator(name, factories, (ValuesSource.Numeric) valuesSource, config.format(), order,
                    bucketCountThresholds, aggregationContext, parent, collectMode, showTermDocCountError, longFilter, pipelineAggregators,
                    metaData);
        }

        throw new AggregationExecutionException("terms aggregation cannot be applied to field [" + config.fieldContext().field()
                + "]. It can only be applied to numeric or string fields.");
    }

}
