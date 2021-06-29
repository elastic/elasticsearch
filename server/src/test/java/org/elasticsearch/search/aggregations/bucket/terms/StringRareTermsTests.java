/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.terms;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.util.SetBackedScalingCuckooFilter;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.InternalAggregations;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class StringRareTermsTests extends InternalRareTermsTestCase {

    @Override
    protected InternalRareTerms<?, ?> createTestInstance(String name,
                                                         Map<String, Object> metadata,
                                                         InternalAggregations aggregations,
                                                         long maxDocCount) {
        BucketOrder order = BucketOrder.count(false);
        DocValueFormat format = DocValueFormat.RAW;
        List<StringRareTerms.Bucket> buckets = new ArrayList<>();
        final int numBuckets = randomNumberOfBuckets();
        for (int i = 0; i < numBuckets; ++i) {
            Set<BytesRef> terms = new HashSet<>();
            BytesRef term = randomValueOtherThanMany(b -> terms.add(b) == false, () -> new BytesRef(randomAlphaOfLength(10)));
            int docCount = randomIntBetween(1, 100);
            buckets.add(new StringRareTerms.Bucket(term, docCount, aggregations, format));
        }
        SetBackedScalingCuckooFilter filter = new SetBackedScalingCuckooFilter(1000, Randomness.get(), 0.01);
        return new StringRareTerms(name, order, metadata, format, buckets, maxDocCount, filter);
    }

    @Override
    protected Class<ParsedStringRareTerms> implementationClass() {
        return ParsedStringRareTerms.class;
    }

    @Override
    protected InternalRareTerms<?, ?> mutateInstance(InternalRareTerms<?, ?> instance) {
        if (instance instanceof StringRareTerms) {
            StringRareTerms stringRareTerms = (StringRareTerms) instance;
            String name = stringRareTerms.getName();
            BucketOrder order = stringRareTerms.order;
            DocValueFormat format = stringRareTerms.format;
            long maxDocCount = stringRareTerms.maxDocCount;
            Map<String, Object> metadata = stringRareTerms.getMetadata();
            List<StringRareTerms.Bucket> buckets = stringRareTerms.getBuckets();
            switch (between(0, 3)) {
                case 0:
                    name += randomAlphaOfLength(5);
                    break;
                case 1:
                    maxDocCount = between(1, 5);
                    break;
                case 2:
                    buckets = new ArrayList<>(buckets);
                    buckets.add(new StringRareTerms.Bucket(new BytesRef(randomAlphaOfLengthBetween(1, 10)), randomNonNegativeLong(),
                        InternalAggregations.EMPTY, format));
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
            return new StringRareTerms(name, order, metadata, format, buckets, maxDocCount, null);
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
