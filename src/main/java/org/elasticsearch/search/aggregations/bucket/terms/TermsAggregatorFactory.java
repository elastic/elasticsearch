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
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.search.aggregations.*;
import org.elasticsearch.search.aggregations.bucket.terms.support.IncludeExclude;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;

/**
 *
 */
public class TermsAggregatorFactory extends ValuesSourceAggregatorFactory {

    public enum ExecutionMode {

        MAP(new ParseField("map")) {

            @Override
            Aggregator create(String name, AggregatorFactories factories, ValuesSource valuesSource, long estimatedBucketCount,
                              long maxOrd, InternalOrder order, TermsAggregator.BucketCountThresholds bucketCountThresholds, IncludeExclude includeExclude,
                              AggregationContext aggregationContext, Aggregator parent) {
                return new StringTermsAggregator(name, factories, valuesSource, estimatedBucketCount, order, bucketCountThresholds, includeExclude, aggregationContext, parent);
            }

            @Override
            boolean needsGlobalOrdinals() {
                return false;
            }

        },
        ORDINALS(new ParseField("ordinals")) {

            @Override
            Aggregator create(String name, AggregatorFactories factories, ValuesSource valuesSource, long estimatedBucketCount,
                              long maxOrd, InternalOrder order, TermsAggregator.BucketCountThresholds bucketCountThresholds, IncludeExclude includeExclude,
                              AggregationContext aggregationContext, Aggregator parent) {
                if (includeExclude != null) {
                    return MAP.create(name, factories, valuesSource, estimatedBucketCount, maxOrd, order, bucketCountThresholds, includeExclude, aggregationContext, parent);
                }
                return new StringTermsAggregator.WithOrdinals(name, factories, (ValuesSource.Bytes.WithOrdinals) valuesSource, estimatedBucketCount, order, bucketCountThresholds, aggregationContext, parent);
            }

            @Override
            boolean needsGlobalOrdinals() {
                return false;
            }

        },
        GLOBAL_ORDINALS(new ParseField("global_ordinals")) {

            @Override
            Aggregator create(String name, AggregatorFactories factories, ValuesSource valuesSource, long estimatedBucketCount,
                              long maxOrd, InternalOrder order, TermsAggregator.BucketCountThresholds bucketCountThresholds, IncludeExclude includeExclude,
                              AggregationContext aggregationContext, Aggregator parent) {
                return new GlobalOrdinalsStringTermsAggregator(name, factories, (ValuesSource.Bytes.WithOrdinals.FieldData) valuesSource, estimatedBucketCount, maxOrd, order, bucketCountThresholds, includeExclude, aggregationContext, parent);
            }

            @Override
            boolean needsGlobalOrdinals() {
                return true;
            }

        },
        GLOBAL_ORDINALS_HASH(new ParseField("global_ordinals_hash")) {

            @Override
            Aggregator create(String name, AggregatorFactories factories, ValuesSource valuesSource, long estimatedBucketCount,
                              long maxOrd, InternalOrder order, TermsAggregator.BucketCountThresholds bucketCountThresholds, IncludeExclude includeExclude,
                              AggregationContext aggregationContext, Aggregator parent) {
                return new GlobalOrdinalsStringTermsAggregator.WithHash(name, factories, (ValuesSource.Bytes.WithOrdinals.FieldData) valuesSource, estimatedBucketCount, maxOrd, order, bucketCountThresholds, includeExclude, aggregationContext, parent);
            }

            @Override
            boolean needsGlobalOrdinals() {
                return true;
            }
        },
        GLOBAL_ORDINALS_LOW_CARDINALITY(new ParseField("global_ordinals_low_cardinality")) {

            @Override
            Aggregator create(String name, AggregatorFactories factories, ValuesSource valuesSource, long estimatedBucketCount,
                              long maxOrd, InternalOrder order, TermsAggregator.BucketCountThresholds bucketCountThresholds, IncludeExclude includeExclude,
                              AggregationContext aggregationContext, Aggregator parent) {
                if (includeExclude != null || factories != null) {
                    return GLOBAL_ORDINALS.create(name, factories, valuesSource, estimatedBucketCount, maxOrd, order, bucketCountThresholds, includeExclude, aggregationContext, parent);
                }
                return new GlobalOrdinalsStringTermsAggregator.LowCardinality(name, factories, (ValuesSource.Bytes.WithOrdinals.FieldData) valuesSource, estimatedBucketCount, maxOrd, order, bucketCountThresholds, aggregationContext, parent);
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
                                   long maxOrd, InternalOrder order, TermsAggregator.BucketCountThresholds bucketCountThresholds,
                                   IncludeExclude includeExclude, AggregationContext aggregationContext, Aggregator parent);

        abstract boolean needsGlobalOrdinals();

        @Override
        public String toString() {
            return parseField.getPreferredName();
        }
    }

    private final InternalOrder order;
    private final IncludeExclude includeExclude;
    private final String executionHint;
    private final TermsAggregator.BucketCountThresholds bucketCountThresholds;

    public TermsAggregatorFactory(String name, ValuesSourceConfig config, InternalOrder order, TermsAggregator.BucketCountThresholds bucketCountThresholds, IncludeExclude includeExclude, String executionHint) {

        super(name, StringTerms.TYPE.name(), config);
        this.order = order;
        this.includeExclude = includeExclude;
        this.executionHint = executionHint;
        this.bucketCountThresholds = bucketCountThresholds;
    }

    @Override
    protected Aggregator createUnmapped(AggregationContext aggregationContext, Aggregator parent) {
        final InternalAggregation aggregation = new UnmappedTerms(name, order, bucketCountThresholds.getRequiredSize(), bucketCountThresholds.getMinDocCount());
        return new NonCollectingAggregator(name, aggregationContext, parent) {
            @Override
            public InternalAggregation buildEmptyAggregation() {
                return aggregation;
            }
        };
    }

    public static long estimatedBucketCount(ValuesSource valuesSource, Aggregator parent) {
        long estimatedBucketCount = valuesSource.metaData().maxAtomicUniqueValuesCount();
        if (estimatedBucketCount < 0) {
            // there isn't an estimation available.. 50 should be a good start
            estimatedBucketCount = 50;
        }

        // adding an upper bound on the estimation as some atomic field data in the future (binary doc values) and not
        // going to know their exact cardinality and will return upper bounds in AtomicFieldData.getNumberUniqueValues()
        // that may be largely over-estimated.. the value chosen here is arbitrary just to play nice with typical CPU cache
        //
        // Another reason is that it may be faster to resize upon growth than to start directly with the appropriate size.
        // And that all values are not necessarily visited by the matches.
        estimatedBucketCount = Math.min(estimatedBucketCount, 512);

        if (Aggregator.hasParentBucketAggregator(parent)) {
            // There is a parent that creates buckets, potentially with a very long tail of buckets with few documents
            // Let's be conservative with memory in that case
            estimatedBucketCount = Math.min(estimatedBucketCount, 8);
        }

        return estimatedBucketCount;
    }

    @Override
    protected Aggregator create(ValuesSource valuesSource, long expectedBucketsCount, AggregationContext aggregationContext, Aggregator parent) {
        long estimatedBucketCount = estimatedBucketCount(valuesSource, parent);

        if (valuesSource instanceof ValuesSource.Bytes) {
            ExecutionMode execution = null;
            if (executionHint != null) {
                execution = ExecutionMode.fromString(executionHint);
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
                if (Aggregator.hasParentBucketAggregator(parent)) {
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

            assert execution != null;
            valuesSource.setNeedsGlobalOrdinals(execution.needsGlobalOrdinals());
            return execution.create(name, factories, valuesSource, estimatedBucketCount, maxOrd, order, bucketCountThresholds, includeExclude, aggregationContext, parent);
        }

        if (includeExclude != null) {
            throw new AggregationExecutionException("Aggregation [" + name + "] cannot support the include/exclude " +
                    "settings as it can only be applied to string values");
        }

        if (valuesSource instanceof ValuesSource.Numeric) {
            if (((ValuesSource.Numeric) valuesSource).isFloatingPoint()) {
                return new DoubleTermsAggregator(name, factories, (ValuesSource.Numeric) valuesSource, config.format(), estimatedBucketCount, order, bucketCountThresholds, aggregationContext, parent);
            }
            return new LongTermsAggregator(name, factories, (ValuesSource.Numeric) valuesSource, config.format(), estimatedBucketCount, order, bucketCountThresholds, aggregationContext, parent);
        }

        throw new AggregationExecutionException("terms aggregation cannot be applied to field [" + config.fieldContext().field() +
                "]. It can only be applied to numeric or string fields.");
    }

}
