/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.terms;

import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.util.SetBackedScalingCuckooFilter;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.InternalAggregations;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LongRareTermsTests extends InternalRareTermsTestCase {

    @Override
    protected InternalRareTerms<?, ?> createTestInstance(
        String name,
        Map<String, Object> metadata,
        InternalAggregations aggregations,
        long maxDocCount
    ) {
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
    protected Class<ParsedLongRareTerms> implementationClass() {
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
