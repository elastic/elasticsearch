/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.terms;

import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.bucket.terms.heuristic.SignificanceHeuristic;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class SignificantLongTermsTests extends InternalSignificantTermsTestCase {

    private DocValueFormat format;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        format = randomNumericDocValueFormat();
    }

    @Override
    protected InternalSignificantTerms<?, ?> createTestInstance(String name,
                                                          Map<String, Object> metadata,
                                                          InternalAggregations aggs,
                                                          int requiredSize, int numBuckets,
                                                          long subsetSize, int[] subsetDfs,
                                                          long supersetSize, int[] supersetDfs,
                                                          SignificanceHeuristic significanceHeuristic) {

        List<SignificantLongTerms.Bucket> buckets = new ArrayList<>(numBuckets);
        Set<Long> terms = new HashSet<>();
        for (int i = 0; i < numBuckets; ++i) {
            long term = randomValueOtherThanMany(l -> terms.add(l) == false, random()::nextLong);
            SignificantLongTerms.Bucket bucket = new SignificantLongTerms.Bucket(subsetDfs[i], subsetSize,
                    supersetDfs[i], supersetSize, term, aggs, format, 0);
            bucket.updateScore(significanceHeuristic);
            buckets.add(bucket);
        }
        return new SignificantLongTerms(name, requiredSize, 1L, metadata, format, subsetSize,
                supersetSize, significanceHeuristic, buckets);
    }

    @Override
    protected Class<ParsedSignificantLongTerms> implementationClass() {
        return ParsedSignificantLongTerms.class;
    }

    @Override
    protected InternalSignificantTerms<?, ?> mutateInstance(InternalSignificantTerms<?, ?> instance) {
        if (instance instanceof SignificantLongTerms) {
            SignificantLongTerms longTerms = (SignificantLongTerms) instance;
            String name = longTerms.getName();
            int requiredSize = longTerms.requiredSize;
            long minDocCount = longTerms.minDocCount;
            DocValueFormat format = longTerms.format;
            long subsetSize = longTerms.getSubsetSize();
            long supersetSize = longTerms.getSupersetSize();
            List<SignificantLongTerms.Bucket> buckets = longTerms.getBuckets();
            SignificanceHeuristic significanceHeuristic = longTerms.significanceHeuristic;
            Map<String, Object> metadata = longTerms.getMetadata();
            switch (between(0, 5)) {
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
                subsetSize += between(1, 100);
                break;
            case 4:
                supersetSize += between(1, 100);
                break;
            case 5:
                buckets = new ArrayList<>(buckets);
                buckets.add(new SignificantLongTerms.Bucket(randomLong(), randomNonNegativeLong(), randomNonNegativeLong(),
                        randomNonNegativeLong(), randomNonNegativeLong(), InternalAggregations.EMPTY, format, 0));
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
            return new SignificantLongTerms(name, requiredSize, minDocCount, metadata, format, subsetSize,
                    supersetSize, significanceHeuristic, buckets);
        } else {
            String name = instance.getName();
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
            return new UnmappedSignificantTerms(name, requiredSize, minDocCount, metadata);
        }
    }
}
