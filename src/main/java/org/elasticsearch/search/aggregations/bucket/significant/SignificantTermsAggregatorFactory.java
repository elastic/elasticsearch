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

import com.carrotsearch.hppc.ObjectObjectOpenHashMap;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.lucene.HashedBytesRef;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.Aggregator.BucketAggregationMode;
//import org.elasticsearch.search.aggregations.bucket.terms.LongTermsAggregator;
//import org.elasticsearch.search.aggregations.bucket.terms.DoubleTermsAggregator;
//import org.elasticsearch.search.aggregations.bucket.terms.LongTermsAggregator;
//import org.elasticsearch.search.aggregations.bucket.terms.StringTermsAggregator;
import org.elasticsearch.search.aggregations.bucket.terms.support.IncludeExclude;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValueSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.bytes.BytesValuesSource;
import org.elasticsearch.search.aggregations.support.numeric.NumericValuesSource;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;

/**
 *
 */
public class SignificantTermsAggregatorFactory extends ValueSourceAggregatorFactory {

    public static final String EXECUTION_HINT_VALUE_MAP = "map";
    public static final String EXECUTION_HINT_VALUE_ORDINALS = "ordinals";

    private final int requiredSize;
    private final int shardSize;
    private final long minDocCount;
    private final IncludeExclude includeExclude;
    private final String executionHint;
    private String indexedFieldName;
    private FieldMapper mapper;

      public SignificantTermsAggregatorFactory(String name, ValuesSourceConfig valueSourceConfig,  int requiredSize, int shardSize, long minDocCount, IncludeExclude includeExclude, String executionHint) {
        super(name, SignificantStringTerms.TYPE.name(), valueSourceConfig);
        this.requiredSize = requiredSize;
        this.shardSize = shardSize;
        this.minDocCount = minDocCount;
        this.includeExclude = includeExclude;
        this.executionHint = executionHint;
        if (!valueSourceConfig.unmapped()) {
            this.indexedFieldName = valuesSourceConfig.fieldContext().field();
            mapper = SearchContext.current().smartNameFieldMapper(indexedFieldName);
        }
    }

    @Override
    protected Aggregator createUnmapped(AggregationContext aggregationContext, Aggregator parent) {
        return new UnmappedSignificantTermsAggregator(name, requiredSize, minDocCount, aggregationContext, parent);
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
                        && !hasParentBucketAggregator(parent)) {
                    execution = EXECUTION_HINT_VALUE_ORDINALS;
                } else {
                    execution = EXECUTION_HINT_VALUE_MAP;
                }
            }
            assert execution != null;

            if (execution.equals(EXECUTION_HINT_VALUE_ORDINALS)) {
                assert includeExclude == null;
                return new SignificantStringTermsAggregator.WithOrdinals(name, factories, (BytesValuesSource.WithOrdinals) valuesSource, this, estimatedBucketCount, requiredSize, shardSize, minDocCount, aggregationContext, parent);
            } else {
                return new SignificantStringTermsAggregator(name, factories, valuesSource, this, estimatedBucketCount, requiredSize, shardSize, minDocCount, includeExclude, aggregationContext, parent);
            }
        }

        if (includeExclude != null) {
            throw new AggregationExecutionException("Aggregation [" + name + "] cannot support the include/exclude " +
                    "settings as it can only be applied to string values");
        }

        if (valuesSource instanceof NumericValuesSource) {
            
            if (((NumericValuesSource) valuesSource).isFloatingPoint()) {
              throw new UnsupportedOperationException("No support for examining floating point numerics");
            }
            return new SignificantLongTermsAggregator(name, factories, (NumericValuesSource) valuesSource, this, estimatedBucketCount, requiredSize, shardSize, minDocCount, aggregationContext, parent);
        }

        throw new AggregationExecutionException("sigfnificant_terms aggregation cannot be applied to field [" + valuesSourceConfig.fieldContext().field() +
                "]. It can only be applied to numeric or string fields.");
    }

    //Cache used to avoid multiple aggs hitting IndexReaders for docFreq info for the same term
    final ObjectObjectOpenHashMap<HashedBytesRef, Integer> cachedDocFreqs = new ObjectObjectOpenHashMap<HashedBytesRef, Integer>();
    HashedBytesRef spare = new HashedBytesRef();
    
    //Many child aggs may ask for the same docFreq information so cache docFreq values for these terms
    public long getBackgroundFrequency(IndexReader topReader, BytesRef termBytes) {
        spare.reset(termBytes, termBytes.hashCode());
        Integer result = cachedDocFreqs.get(spare);
        if (result == null) {
            try {
                result = topReader.docFreq(new Term(indexedFieldName, termBytes));
                HashedBytesRef key = new HashedBytesRef(BytesRef.deepCopyOf(termBytes), spare.hash);
                cachedDocFreqs.put(key, result);
            } catch (IOException e) {
                throw new ElasticsearchException("IOException reading document frequency", e);
            }
        }
        return result;
    }

    // Many child aggs may ask for the same docFreq information so cache docFreq
    // values for these terms
    public long getBackgroundFrequency(IndexReader topReader, long term) {
        BytesRef indexedVal = mapper.indexedValueForSearch(term);
        return getBackgroundFrequency(topReader, indexedVal);
    }

}
