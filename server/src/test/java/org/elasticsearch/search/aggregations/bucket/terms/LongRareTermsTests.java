/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.aggregations.bucket.terms;

import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.util.SetBackedScalingCuckooFilter;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.ParsedMultiBucketAggregation;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LongRareTermsTests extends InternalRareTermsTestCase {

    @Override
    protected InternalRareTerms<?, ?> createTestInstance(String name,
                                                         Map<String, Object> metadata,
                                                         InternalAggregations aggregations,
                                                         long maxDocCount) {
        BucketOrder order = BucketOrder.count(false);
        DocValueFormat format = randomNumericDocValueFormat();
        List<LongRareTerms.Bucket> buckets = new ArrayList<>();
        final int numBuckets = randomNumberOfBuckets();
        for (int i = 0; i < numBuckets; ++i) {
            long term = randomLong();
            int docCount = randomIntBetween(1, 100);
            buckets.add(new LongRareTerms.Bucket(term, docCount, aggregations, format));
        }
        SetBackedScalingCuckooFilter filter = new SetBackedScalingCuckooFilter(1000, Randomness.get(), 0.01);
        return new LongRareTerms(name, order, metadata, format, buckets, maxDocCount, filter);
    }

    @Override
    protected Class<? extends ParsedMultiBucketAggregation> implementationClass() {
        return ParsedLongRareTerms.class;
    }

    @Override
    protected InternalRareTerms<?, ?> mutateInstance(InternalRareTerms<?, ?> instance) {
        if (instance instanceof LongRareTerms) {
            LongRareTerms longRareTerms = (LongRareTerms) instance;
            String name = longRareTerms.getName();
            BucketOrder order = longRareTerms.order;
            DocValueFormat format = longRareTerms.format;
            long maxDocCount = longRareTerms.maxDocCount;
            Map<String, Object> metadata = longRareTerms.getMetadata();
            List<LongRareTerms.Bucket> buckets = longRareTerms.getBuckets();
            switch (between(0, 3)) {
                case 0:
                    name += randomAlphaOfLength(5);
                    break;
                case 1:
                    maxDocCount = between(1, 5);
                    break;
                case 2:
                    buckets = new ArrayList<>(buckets);
                    buckets.add(new LongRareTerms.Bucket(randomLong(), randomNonNegativeLong(), InternalAggregations.EMPTY, format));
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
            return new LongRareTerms(name, order, metadata, format, buckets, maxDocCount, null);
        } else {
            String name = instance.getName();
            Map<String, Object> metadata = instance.getMetadata();
            switch (between(0, 1)) {
                case 0:
                    name += randomAlphaOfLength(5);
                    break;
                case 1:
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
            return new UnmappedRareTerms(name, metadata);
        }
    }
}
