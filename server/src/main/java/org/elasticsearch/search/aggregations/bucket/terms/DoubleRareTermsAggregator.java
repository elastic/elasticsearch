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
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSource.Numeric;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class DoubleRareTermsAggregator extends LongRareTermsAggregator {

    DoubleRareTermsAggregator(String name, AggregatorFactories factories, ValuesSource.Numeric valuesSource,
                                     DocValueFormat format, SearchContext aggregationContext, Aggregator parent,
                                     IncludeExclude.LongFilter longFilter, int maxDocCount, List<PipelineAggregator> pipelineAggregators,
                                     Map<String, Object> metaData) throws IOException {
        super(name, factories, valuesSource, format, aggregationContext, parent,
            longFilter, maxDocCount, pipelineAggregators, metaData);
    }

    @Override
    protected SortedNumericDocValues getValues(Numeric valuesSource, LeafReaderContext ctx) throws IOException {
        return FieldData.toSortableLongBits(valuesSource.doubleValues(ctx));
    }

    @Override
    public DoubleRareTerms buildAggregation(long owningBucketOrdinal) throws IOException {
        final LongRareTerms terms = (LongRareTerms) super.buildAggregation(owningBucketOrdinal);
        return convertToDouble(terms);
    }

    @Override
    public DoubleRareTerms buildEmptyAggregation() {
        final LongRareTerms terms = (LongRareTerms) super.buildEmptyAggregation();
        return convertToDouble(terms);
    }

    private static DoubleRareTerms convertToDouble(LongRareTerms terms) {
        List<DoubleTerms.Bucket> buckets = terms.buckets.stream().map(DoubleRareTermsAggregator::convertToDouble)
            .collect(Collectors.toList());
        return new DoubleRareTerms(terms.getName(), terms.order, terms.pipelineAggregators(),
            terms.getMetaData(), terms.format, buckets, terms.getMaxDocCount(), terms.getBloom());
    }

    private static DoubleTerms.Bucket convertToDouble(LongTerms.Bucket bucket) {
        double value = NumericUtils.sortableLongToDouble(bucket.term);
        return new DoubleTerms.Bucket(value, bucket.docCount, bucket.aggregations, bucket.showDocCountError, bucket.docCountError,
            bucket.format);
    }
}
