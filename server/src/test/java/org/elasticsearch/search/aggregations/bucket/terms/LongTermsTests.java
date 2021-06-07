/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.terms;

import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.InternalAggregations;

import java.util.ArrayList;
import java.util.Collections;
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
        BucketOrder reduceOrder = rarely() ? order : BucketOrder.key(true);
        Collections.sort(buckets, reduceOrder.comparator());
        return new LongTerms(name, reduceOrder, order, requiredSize, minDocCount,
                metadata, format, shardSize, showTermDocCountError, otherDocCount, buckets, docCountError);
    }

    @Override
    protected Class<ParsedLongTerms> implementationClass() {
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
            Collections.sort(buckets, longTerms.reduceOrder.comparator());
            return new LongTerms(name, longTerms.reduceOrder, order, requiredSize, minDocCount, metadata, format, shardSize,
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
