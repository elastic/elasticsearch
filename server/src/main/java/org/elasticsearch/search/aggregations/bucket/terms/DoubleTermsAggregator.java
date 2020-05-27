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

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.index.fielddata.FieldData;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.bucket.terms.LongTerms.Bucket;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSource.Numeric;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class DoubleTermsAggregator extends LongTermsAggregator {

    DoubleTermsAggregator(String name, AggregatorFactories factories, ValuesSource.Numeric valuesSource, DocValueFormat format,
            BucketOrder order, BucketCountThresholds bucketCountThresholds, SearchContext aggregationContext, Aggregator parent,
            SubAggCollectionMode collectionMode, boolean showTermDocCountError, IncludeExclude.LongFilter longFilter,
            boolean collectsFromSingleBucket, Map<String, Object> metadata) throws IOException {
        super(name, factories, valuesSource, format, order, bucketCountThresholds, aggregationContext, parent, collectionMode,
                showTermDocCountError, longFilter, collectsFromSingleBucket, metadata);
    }

    @Override
    protected SortedNumericDocValues getValues(Numeric valuesSource, LeafReaderContext ctx) throws IOException {
        return FieldData.toSortableLongBits(valuesSource.doubleValues(ctx));
    }

    @Override
    protected InternalAggregation buildResult(long otherDocCount, List<Bucket> buckets) {
        return convertToDouble((LongTerms) super.buildResult(otherDocCount, buckets));
    }

    @Override
    public DoubleTerms buildEmptyAggregation() {
        final LongTerms terms = (LongTerms) super.buildEmptyAggregation();
        return convertToDouble(terms);
    }

    private static DoubleTerms convertToDouble(LongTerms terms) {
        List<DoubleTerms.Bucket> buckets = terms.buckets.stream().map(DoubleTermsAggregator::convertToDouble).collect(Collectors.toList());
        return new DoubleTerms(terms.getName(), terms.order, terms.requiredSize, terms.minDocCount,
                terms.getMetadata(), terms.format, terms.shardSize, terms.showTermDocCountError, terms.otherDocCount, buckets,
                terms.docCountError);
    }

    private static DoubleTerms.Bucket convertToDouble(LongTerms.Bucket bucket) {
        double value = NumericUtils.sortableLongToDouble(bucket.term);
        return new DoubleTerms.Bucket(value, bucket.docCount, bucket.aggregations, bucket.showDocCountError, bucket.docCountError,
                bucket.format);
    }
}
