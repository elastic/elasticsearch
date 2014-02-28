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

import org.apache.lucene.index.AtomicReaderContext;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.Aggregator.BucketAggregationMode;
import org.elasticsearch.search.aggregations.bucket.terms.support.IncludeExclude;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValueSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.bytes.BytesValuesSource;
import org.elasticsearch.search.aggregations.support.numeric.NumericValuesSource;

/**
 *
 */
public class TermsAggregatorFactory extends ValueSourceAggregatorFactory {

    public static final String EXECUTION_HINT_VALUE_MAP = "map";
    public static final String EXECUTION_HINT_VALUE_ORDINALS = "ordinals";

    private final InternalOrder order;
    private final int requiredSize;
    private final int shardSize;
    private final long minDocCount;
    private final IncludeExclude includeExclude;
    private final String executionHint;

    public TermsAggregatorFactory(String name, ValuesSourceConfig valueSourceConfig, InternalOrder order, int requiredSize, int shardSize, long minDocCount, IncludeExclude includeExclude, String executionHint) {
        super(name, StringTerms.TYPE.name(), valueSourceConfig);
        this.order = order;
        this.requiredSize = requiredSize;
        this.shardSize = shardSize;
        this.minDocCount = minDocCount;
        this.includeExclude = includeExclude;
        this.executionHint = executionHint;
    }

    @Override
    protected Aggregator createUnmapped(AggregationContext aggregationContext, Aggregator parent) {
        return new UnmappedTermsAggregator(name, order, requiredSize, minDocCount, aggregationContext, parent);
    }

    private static boolean hasParentBucketAggregator(Aggregator parent) {
        if (parent == null) {
            return false;
        } else if (parent.bucketAggregationMode() == BucketAggregationMode.PER_BUCKET) {
            return true;
        } else {
            return hasParentBucketAggregator(parent.parent());
        }
    }

    private boolean shouldUseOrdinals(Aggregator parent, ValuesSource valuesSource, AggregationContext context) {
        // if there is a parent bucket aggregator the number of instances of this aggregator is going to be unbounded and most instances
        // may only aggregate few documents, so don't use ordinals
        if (hasParentBucketAggregator(parent)) {
            return false;
        }

        // be defensive: if the number of unique values is unknown, don't use ordinals
        final long maxNumUniqueValues = valuesSource.metaData().maxAtomicUniqueValuesCount();
        if (maxNumUniqueValues == -1) {
            return false;
        }

        // if the number of unique values is high compared to the document count, then ordinals are only going to make things slower
        int maxDoc = 0;
        for (AtomicReaderContext ctx : context.searchContext().searcher().getTopReaderContext().reader().leaves()) {
            maxDoc = Math.max(maxDoc, ctx.reader().maxDoc());
        }
        if (maxNumUniqueValues > (maxDoc >>> 4)) {
            return false;
        }

        return true;
    }

    @Override
    protected Aggregator create(ValuesSource valuesSource, long expectedBucketsCount, AggregationContext aggregationContext, Aggregator parent) {
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

        if (valuesSource instanceof BytesValuesSource) {
            if (executionHint != null && !executionHint.equals(EXECUTION_HINT_VALUE_MAP) && !executionHint.equals(EXECUTION_HINT_VALUE_ORDINALS)) {
                throw new ElasticsearchIllegalArgumentException("execution_hint can only be '" + EXECUTION_HINT_VALUE_MAP + "' or '" + EXECUTION_HINT_VALUE_ORDINALS + "', not " + executionHint);
            }
            String execution = executionHint;
            if (!(valuesSource instanceof BytesValuesSource.WithOrdinals)) {
                execution = EXECUTION_HINT_VALUE_MAP;
            } else if (includeExclude != null) {
                execution = EXECUTION_HINT_VALUE_MAP;
            }
            if (execution == null) {
                if ((valuesSource instanceof BytesValuesSource.WithOrdinals)
                        && shouldUseOrdinals(parent, valuesSource, aggregationContext)) {
                    execution = EXECUTION_HINT_VALUE_ORDINALS;
                } else {
                    execution = EXECUTION_HINT_VALUE_MAP;
                }
            }
            assert execution != null;

            if (execution.equals(EXECUTION_HINT_VALUE_ORDINALS)) {
                assert includeExclude == null;
                return new StringTermsAggregator.WithOrdinals(name, factories, (BytesValuesSource.WithOrdinals) valuesSource, estimatedBucketCount, order, requiredSize, shardSize, minDocCount, aggregationContext, parent);
            } else {
                return new StringTermsAggregator(name, factories, valuesSource, estimatedBucketCount, order, requiredSize, shardSize, minDocCount, includeExclude, aggregationContext, parent);
            }
        }

        if (includeExclude != null) {
            throw new AggregationExecutionException("Aggregation [" + name + "] cannot support the include/exclude " +
                    "settings as it can only be applied to string values");
        }

        if (valuesSource instanceof NumericValuesSource) {
            if (((NumericValuesSource) valuesSource).isFloatingPoint()) {
                return new DoubleTermsAggregator(name, factories, (NumericValuesSource) valuesSource, estimatedBucketCount, order, requiredSize, shardSize, minDocCount, aggregationContext, parent);
            }
            return new LongTermsAggregator(name, factories, (NumericValuesSource) valuesSource, estimatedBucketCount, order, requiredSize, shardSize, minDocCount, aggregationContext, parent);
        }

        throw new AggregationExecutionException("terms aggregation cannot be applied to field [" + valuesSourceConfig.fieldContext().field() +
                "]. It can only be applied to numeric or string fields.");
    }

}
