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
import org.elasticsearch.common.Nullable;
import org.elasticsearch.index.fielddata.FieldData;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.bucket.terms.support.IncludeExclude;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSource.Numeric;
import org.elasticsearch.search.aggregations.support.format.ValueFormat;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

/**
 *
 */
public class DoubleTermsAggregator extends LongTermsAggregator {

    public DoubleTermsAggregator(String name, AggregatorFactories factories, ValuesSource.Numeric valuesSource, @Nullable ValueFormat format,
            Terms.Order order, BucketCountThresholds bucketCountThresholds, AggregationContext aggregationContext, Aggregator parent, SubAggCollectionMode collectionMode, boolean showTermDocCountError, IncludeExclude.LongFilter longFilter, Map<String, Object> metaData) throws IOException {
        super(name, factories, valuesSource, format, order, bucketCountThresholds, aggregationContext, parent, collectionMode, showTermDocCountError, longFilter, metaData);
    }

    @Override
    protected SortedNumericDocValues getValues(Numeric valuesSource, LeafReaderContext ctx) throws IOException {
        return FieldData.toSortableLongBits(valuesSource.doubleValues(ctx));
    }

    @Override
    public DoubleTerms buildAggregation(long owningBucketOrdinal) throws IOException {
        final LongTerms terms = (LongTerms) super.buildAggregation(owningBucketOrdinal);
        return convertToDouble(terms);
    }

    @Override
    public DoubleTerms buildEmptyAggregation() {
        final LongTerms terms = (LongTerms) super.buildEmptyAggregation();
        return convertToDouble(terms);
    }

    private static DoubleTerms.Bucket convertToDouble(InternalTerms.Bucket bucket) {
        final long term = ((Number) bucket.getKey()).longValue();
        final double value = NumericUtils.sortableLongToDouble(term);
        return new DoubleTerms.Bucket(value, bucket.docCount, bucket.aggregations, bucket.showDocCountError, bucket.docCountError, bucket.formatter);
    }

    private static DoubleTerms convertToDouble(LongTerms terms) {
        final InternalTerms.Bucket[] buckets = terms.getBuckets().toArray(new InternalTerms.Bucket[0]);
        for (int i = 0; i < buckets.length; ++i) {
            buckets[i] = convertToDouble(buckets[i]);
        }
        return new DoubleTerms(terms.getName(), terms.order, terms.formatter, terms.requiredSize, terms.shardSize, terms.minDocCount, Arrays.asList(buckets), terms.showTermDocCountError, terms.docCountError, terms.otherDocCount, terms.getMetaData());
    }

}
