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

import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Filter;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lucene.index.FilterableTermsEnum;
import org.elasticsearch.common.lucene.index.FreqTermsEnum;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.Aggregator.BucketAggregationMode;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.NonCollectingAggregator;
import org.elasticsearch.search.aggregations.bucket.terms.support.IncludeExclude;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;

/**
 *
 */
public class SignificantTermsAggregatorFactory extends ValuesSourceAggregatorFactory implements Releasable {

    public static final String EXECUTION_HINT_VALUE_MAP = "map";
    public static final String EXECUTION_HINT_VALUE_ORDINALS = "ordinals";

    private final int requiredSize;
    private final int shardSize;
    private final long minDocCount;
    private final IncludeExclude includeExclude;
    private final String executionHint;
    private String indexedFieldName;
    private FieldMapper mapper;
    private FilterableTermsEnum termsEnum;
    private int numberOfAggregatorsCreated = 0;
    private Filter filter;

    public SignificantTermsAggregatorFactory(String name, ValuesSourceConfig valueSourceConfig, int requiredSize,
                                             int shardSize, long minDocCount, IncludeExclude includeExclude,
                                             String executionHint, Filter filter) {

        super(name, SignificantStringTerms.TYPE.name(), valueSourceConfig);
        this.requiredSize = requiredSize;
        this.shardSize = shardSize;
        this.minDocCount = minDocCount;
        this.includeExclude = includeExclude;
        this.executionHint = executionHint;
        if (!valueSourceConfig.unmapped()) {
            this.indexedFieldName = config.fieldContext().field();
            mapper = SearchContext.current().smartNameFieldMapper(indexedFieldName);
        }
        this.filter = filter;
    }

    @Override
    protected Aggregator createUnmapped(AggregationContext aggregationContext, Aggregator parent) {
        final InternalAggregation aggregation = new UnmappedSignificantTerms(name, requiredSize, minDocCount);
        return new NonCollectingAggregator(name, aggregationContext, parent) {
            @Override
            public InternalAggregation buildEmptyAggregation() {
                return aggregation;
            }
        };
    }

    private static boolean hasParentBucketAggregator(Aggregator parent) {
        if (parent == null) {
            return false;
        } else if (parent.bucketAggregationMode() == BucketAggregationMode.PER_BUCKET) {
            return true;
        }
        return hasParentBucketAggregator(parent.parent());
    }

    @Override
    protected Aggregator create(ValuesSource valuesSource, long expectedBucketsCount, AggregationContext aggregationContext, Aggregator parent) {
        numberOfAggregatorsCreated++;

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

        if (valuesSource instanceof ValuesSource.Bytes) {
            if (executionHint != null && !executionHint.equals(EXECUTION_HINT_VALUE_MAP) && !executionHint.equals(EXECUTION_HINT_VALUE_ORDINALS)) {
                throw new ElasticsearchIllegalArgumentException("execution_hint can only be '" + EXECUTION_HINT_VALUE_MAP + "' or '" + EXECUTION_HINT_VALUE_ORDINALS + "', not " + executionHint);
            }
            String execution = executionHint;
            if (!(valuesSource instanceof ValuesSource.Bytes.WithOrdinals)) {
                execution = EXECUTION_HINT_VALUE_MAP;
            } else if (includeExclude != null) {
                execution = EXECUTION_HINT_VALUE_MAP;
            }
            if (execution == null) {
                if ((valuesSource instanceof ValuesSource.Bytes.WithOrdinals)
                        && !hasParentBucketAggregator(parent)) {
                    execution = EXECUTION_HINT_VALUE_ORDINALS;
                } else {
                    execution = EXECUTION_HINT_VALUE_MAP;
                }
            }
            assert execution != null;

            if (execution.equals(EXECUTION_HINT_VALUE_ORDINALS)) {
                assert includeExclude == null;
                return new SignificantStringTermsAggregator.WithOrdinals(name, factories, (ValuesSource.Bytes.WithOrdinals) valuesSource, estimatedBucketCount, requiredSize, shardSize, minDocCount, aggregationContext, parent, this);
            }
            return new SignificantStringTermsAggregator(name, factories, valuesSource, estimatedBucketCount, requiredSize, shardSize, minDocCount, includeExclude, aggregationContext, parent, this);
        }

        if (includeExclude != null) {
            throw new AggregationExecutionException("Aggregation [" + name + "] cannot support the include/exclude " +
                    "settings as it can only be applied to string values");
        }

        if (valuesSource instanceof ValuesSource.Numeric) {

            if (((ValuesSource.Numeric) valuesSource).isFloatingPoint()) {
                throw new UnsupportedOperationException("No support for examining floating point numerics");
            }
            return new SignificantLongTermsAggregator(name, factories, (ValuesSource.Numeric) valuesSource, config.format(), estimatedBucketCount, requiredSize, shardSize, minDocCount, aggregationContext, parent, this);
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
                termsEnum = new FilterableTermsEnum(reader, indexedFieldName, DocsEnum.FLAG_NONE, filter);
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
        BytesRef indexedVal = mapper.indexedValueForSearch(term);
        return getBackgroundFrequency(indexedVal);
    }

    @Override
    public boolean release() throws ElasticsearchException {
        try {
            if (termsEnum instanceof Releasable) {
                ((Releasable) termsEnum).release();
            }
        } finally {
            termsEnum = null;
        }
        return true;
    }
}
