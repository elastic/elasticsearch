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

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.ParsedMultiBucketAggregation;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class LongTermsTests extends InternalTermsTestCase {

    @Override
    protected InternalTerms<?, ?> createTestInstance(String name,
                                                     List<PipelineAggregator> pipelineAggregators,
                                                     Map<String, Object> metaData,
                                                     InternalAggregations aggregations,
                                                     boolean showTermDocCountError,
                                                     long docCountError) {
        BucketOrder order = BucketOrder.count(false);
        long minDocCount = 1;
        int requiredSize = 3;
        int shardSize = requiredSize + 2;
        DocValueFormat format = randomNumericDocValueFormat();
        long otherDocCount = 0;
        List<LongTerms.Bucket> buckets = new ArrayList<>();
        final int numBuckets = randomNumberOfBuckets();
        Set<Long> terms = new HashSet<>();
        for (int i = 0; i < numBuckets; ++i) {
            long term = randomValueOtherThanMany(l -> terms.add(l) == false, random()::nextLong);
            int docCount = randomIntBetween(1, 100);
            buckets.add(new LongTerms.Bucket(term, docCount, aggregations, showTermDocCountError, docCountError, format));
        }
        return new LongTerms(name, order, requiredSize, minDocCount, pipelineAggregators,
                metaData, format, shardSize, showTermDocCountError, otherDocCount, buckets, docCountError);
    }

    @Override
    protected Reader<InternalTerms<?, ?>> instanceReader() {
        return LongTerms::new;
    }

    @Override
    protected Class<? extends ParsedMultiBucketAggregation> implementationClass() {
        return ParsedLongTerms.class;
    }
}
