/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.terms;

import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.aggregations.bucket.terms.heuristic.ChiSquare;
import org.elasticsearch.search.aggregations.bucket.terms.heuristic.GND;
import org.elasticsearch.search.aggregations.bucket.terms.heuristic.JLHScore;
import org.elasticsearch.search.aggregations.bucket.terms.heuristic.MutualInformation;
import org.elasticsearch.search.aggregations.bucket.terms.heuristic.SignificanceHeuristic;
import org.elasticsearch.search.aggregations.support.SamplingContext;
import org.elasticsearch.test.InternalMultiBucketAggregationTestCase;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;

public abstract class InternalSignificantTermsTestCase extends InternalMultiBucketAggregationTestCase<InternalSignificantTerms<?, ?>> {

    private SignificanceHeuristic significanceHeuristic;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        significanceHeuristic = randomSignificanceHeuristic();
    }

    @Override
    protected boolean supportsSampling() {
        return true;
    }

    @Override
    protected void assertSampled(
        InternalSignificantTerms<?, ?> sampled,
        InternalSignificantTerms<?, ?> reduced,
        SamplingContext samplingContext
    ) {
        assertThat(sampled.getSubsetSize(), equalTo(samplingContext.scaleUp(reduced.getSubsetSize())));
        assertThat(sampled.getSupersetSize(), equalTo(samplingContext.scaleUp(reduced.getSupersetSize())));
        assertEquals(sampled.getBuckets().size(), reduced.getBuckets().size());
        Iterator<? extends InternalSignificantTerms.Bucket<?>> sampledIt = sampled.getBuckets().iterator();
        for (InternalSignificantTerms.Bucket<?> reducedBucket : reduced.getBuckets()) {
            InternalSignificantTerms.Bucket<?> sampledBucket = sampledIt.next();
            assertEquals(sampledBucket.subsetDf, samplingContext.scaleUp(reducedBucket.subsetDf));
            assertEquals(sampledBucket.supersetDf, samplingContext.scaleUp(reducedBucket.supersetDf));
            assertEquals(sampledBucket.subsetSize, samplingContext.scaleUp(reducedBucket.subsetSize));
            assertEquals(sampledBucket.supersetSize, samplingContext.scaleUp(reducedBucket.supersetSize));
            assertThat(sampledBucket.score, closeTo(reducedBucket.score, 1e-14));
        }
    }

    @Override
    protected final InternalSignificantTerms<?, ?> createTestInstance(
        String name,
        Map<String, Object> metadata,
        InternalAggregations aggregations
    ) {
        final int requiredSize = randomIntBetween(1, 5);
        final int numBuckets = randomNumberOfBuckets();

        long subsetSize = 0;
        long supersetSize = 0;

        int[] subsetDfs = new int[numBuckets];
        int[] supersetDfs = new int[numBuckets];

        for (int i = 0; i < numBuckets; ++i) {
            int subsetDf = randomIntBetween(1, 10);
            subsetDfs[i] = subsetDf;

            int supersetDf = randomIntBetween(subsetDf, 20);
            supersetDfs[i] = supersetDf;

            subsetSize += subsetDf;
            supersetSize += supersetDf;
        }
        return createTestInstance(
            name,
            metadata,
            aggregations,
            requiredSize,
            numBuckets,
            subsetSize,
            subsetDfs,
            supersetSize,
            supersetDfs,
            significanceHeuristic
        );
    }

    protected abstract InternalSignificantTerms<?, ?> createTestInstance(
        String name,
        Map<String, Object> metadata,
        InternalAggregations aggregations,
        int requiredSize,
        int numBuckets,
        long subsetSize,
        int[] subsetDfs,
        long supersetSize,
        int[] supersetDfs,
        SignificanceHeuristic significanceHeuristic
    );

    @Override
    protected InternalSignificantTerms<?, ?> createUnmappedInstance(String name, Map<String, Object> metadata) {
        InternalSignificantTerms<?, ?> testInstance = createTestInstance(name, metadata);
        return new UnmappedSignificantTerms(name, testInstance.requiredSize, testInstance.minDocCount, metadata);
    }

    @Override
    protected void assertReduced(InternalSignificantTerms<?, ?> reduced, List<InternalSignificantTerms<?, ?>> inputs) {
        assertEquals(inputs.stream().mapToLong(InternalSignificantTerms::getSubsetSize).sum(), reduced.getSubsetSize());
        assertEquals(inputs.stream().mapToLong(InternalSignificantTerms::getSupersetSize).sum(), reduced.getSupersetSize());

        List<Function<SignificantTerms.Bucket, Long>> counts = Arrays.asList(
            SignificantTerms.Bucket::getSubsetDf,
            SignificantTerms.Bucket::getSupersetDf,
            SignificantTerms.Bucket::getDocCount
        );

        for (Function<SignificantTerms.Bucket, Long> count : counts) {
            Map<Object, Long> reducedCounts = toCounts(reduced.getBuckets().stream(), count);
            Map<Object, Long> totalCounts = toCounts(inputs.stream().map(SignificantTerms::getBuckets).flatMap(List::stream), count);

            Map<Object, Long> expectedReducedCounts = new HashMap<>(totalCounts);
            expectedReducedCounts.keySet().retainAll(reducedCounts.keySet());
            assertEquals(expectedReducedCounts, reducedCounts);
        }
    }

    @Override
    protected void assertMultiBucketsAggregation(MultiBucketsAggregation expected, MultiBucketsAggregation actual, boolean checkOrder) {
        super.assertMultiBucketsAggregation(expected, actual, checkOrder);

        assertTrue(expected instanceof InternalSignificantTerms);
        assertTrue(actual instanceof ParsedSignificantTerms);

        InternalSignificantTerms<?, ?> expectedSigTerms = (InternalSignificantTerms<?, ?>) expected;
        ParsedSignificantTerms actualSigTerms = (ParsedSignificantTerms) actual;
        assertEquals(expectedSigTerms.getSubsetSize(), actualSigTerms.getSubsetSize());
        assertEquals(expectedSigTerms.getSupersetSize(), actualSigTerms.getSupersetSize());

        for (SignificantTerms.Bucket bucket : (SignificantTerms) expected) {
            String key = bucket.getKeyAsString();
            assertBucket(expectedSigTerms.getBucketByKey(key), actualSigTerms.getBucketByKey(key), checkOrder);
        }
    }

    @Override
    protected void assertBucket(MultiBucketsAggregation.Bucket expected, MultiBucketsAggregation.Bucket actual, boolean checkOrder) {
        super.assertBucket(expected, actual, checkOrder);

        assertTrue(expected instanceof InternalSignificantTerms.Bucket);
        assertTrue(actual instanceof ParsedSignificantTerms.ParsedBucket);

        SignificantTerms.Bucket expectedSigTerm = (SignificantTerms.Bucket) expected;
        SignificantTerms.Bucket actualSigTerm = (SignificantTerms.Bucket) actual;

        assertEquals(expectedSigTerm.getSignificanceScore(), actualSigTerm.getSignificanceScore(), 0.0);
        assertEquals(expectedSigTerm.getSubsetDf(), actualSigTerm.getSubsetDf());
        assertEquals(expectedSigTerm.getDocCount(), actualSigTerm.getSubsetDf());
        assertEquals(expectedSigTerm.getSupersetDf(), actualSigTerm.getSupersetDf());
        assertEquals(expectedSigTerm.getSubsetSize(), actualSigTerm.getSubsetSize());
        assertEquals(expectedSigTerm.getSupersetSize(), actualSigTerm.getSupersetSize());
    }

    private static Map<Object, Long> toCounts(
        Stream<? extends SignificantTerms.Bucket> buckets,
        Function<SignificantTerms.Bucket, Long> fn
    ) {
        return buckets.collect(Collectors.toMap(SignificantTerms.Bucket::getKey, fn, Long::sum));
    }

    private static SignificanceHeuristic randomSignificanceHeuristic() {
        return randomFrom(
            new JLHScore(),
            new MutualInformation(randomBoolean(), randomBoolean()),
            new GND(randomBoolean()),
            new ChiSquare(randomBoolean(), randomBoolean())
        );
    }
}
