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

import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.ParsedMultiBucketAggregation;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class LongTermsTests extends InternalTermsTestCase {

    @Override
    protected InternalTerms<?, ?> createTestInstance(String name,
                                                     Map<String, Object> metadata,
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
        return new LongTerms(name, order, requiredSize, minDocCount,
                metadata, format, shardSize, showTermDocCountError, otherDocCount, buckets, docCountError);
    }

    @Override
    protected Class<? extends ParsedMultiBucketAggregation> implementationClass() {
        return ParsedLongTerms.class;
    }

    @Override
    protected InternalTerms<?, ?> mutateInstance(InternalTerms<?, ?> instance) {
        if (instance instanceof LongTerms) {
            LongTerms longTerms = (LongTerms) instance;
            String name = longTerms.getName();
            BucketOrder order = longTerms.order;
            int requiredSize = longTerms.requiredSize;
            long minDocCount = longTerms.minDocCount;
            DocValueFormat format = longTerms.format;
            int shardSize = longTerms.getShardSize();
            boolean showTermDocCountError = longTerms.showTermDocCountError;
            long otherDocCount = longTerms.getSumOfOtherDocCounts();
            List<LongTerms.Bucket> buckets = longTerms.getBuckets();
            long docCountError = longTerms.getDocCountError();
            Map<String, Object> metadata = longTerms.getMetadata();
            switch (between(0, 8)) {
            case 0:
                name += randomAlphaOfLength(5);
                break;
            case 1:
                requiredSize += between(1, 100);
                break;
            case 2:
                minDocCount += between(1, 100);
                break;
            case 3:
                shardSize += between(1, 100);
                break;
            case 4:
                showTermDocCountError = showTermDocCountError == false;
                break;
            case 5:
                otherDocCount += between(1, 100);
                break;
            case 6:
                docCountError += between(1, 100);
                break;
            case 7:
                buckets = new ArrayList<>(buckets);
                buckets.add(new LongTerms.Bucket(randomLong(), randomNonNegativeLong(), InternalAggregations.EMPTY, showTermDocCountError,
                        docCountError, format));
                break;
            case 8:
                if (metadata == null) {
                    metadata = new HashMap<>(1);
                } else {
                    metadata = new HashMap<>(instance.getMetadata());
                }
                metadata.put(randomAlphaOfLength(15), randomInt());
                break;
            default:
                throw new AssertionError("Illegal randomisation branch");
            }
            return new LongTerms(name, order, requiredSize, minDocCount, metadata, format, shardSize,
                    showTermDocCountError, otherDocCount, buckets, docCountError);
        } else {
            String name = instance.getName();
            BucketOrder order = instance.order;
            int requiredSize = instance.requiredSize;
            long minDocCount = instance.minDocCount;
            Map<String, Object> metadata = instance.getMetadata();
            switch (between(0, 3)) {
            case 0:
                name += randomAlphaOfLength(5);
                break;
            case 1:
                requiredSize += between(1, 100);
                break;
            case 2:
                minDocCount += between(1, 100);
                break;
            case 3:
                if (metadata == null) {
                    metadata = new HashMap<>(1);
                } else {
                    metadata = new HashMap<>(instance.getMetadata());
                }
                metadata.put(randomAlphaOfLength(15), randomInt());
                break;
            default:
                throw new AssertionError("Illegal randomisation branch");
            }
            return new UnmappedTerms(name, order, requiredSize, minDocCount, metadata);
        }
    }
}
