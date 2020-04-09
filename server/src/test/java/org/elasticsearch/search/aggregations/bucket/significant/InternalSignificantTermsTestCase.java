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

package org.elasticsearch.search.aggregations.bucket.significant;

import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.test.InternalMultiBucketAggregationTestCase;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.aggregations.bucket.significant.heuristics.ChiSquare;
import org.elasticsearch.search.aggregations.bucket.significant.heuristics.GND;
import org.elasticsearch.search.aggregations.bucket.significant.heuristics.JLHScore;
import org.elasticsearch.search.aggregations.bucket.significant.heuristics.MutualInformation;
import org.elasticsearch.search.aggregations.bucket.significant.heuristics.SignificanceHeuristic;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public abstract class InternalSignificantTermsTestCase extends InternalMultiBucketAggregationTestCase<InternalSignificantTerms<?, ?>> {

    private SignificanceHeuristic significanceHeuristic;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        significanceHeuristic = randomSignificanceHeuristic();
    }

    @Override
    protected final InternalSignificantTerms createTestInstance(String name,
                                                          List<PipelineAggregator> pipelineAggregators,
                                                          Map<String, Object> metaData,
                                                          InternalAggregations aggregations) {
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
        return createTestInstance(name, pipelineAggregators, metaData, aggregations, requiredSize, numBuckets, subsetSize, subsetDfs,
                supersetSize, supersetDfs, significanceHeuristic);
    }

    protected abstract InternalSignificantTerms createTestInstance(String name,
                                                                   List<PipelineAggregator> pipelineAggregators,
                                                                   Map<String, Object> metaData,
                                                                   InternalAggregations aggregations,
                                                                   int requiredSize, int numBuckets,
                                                                   long subsetSize, int[] subsetDfs,
                                                                   long supersetSize, int[] supersetDfs,
                                                                   SignificanceHeuristic significanceHeuristic);

    @Override
    protected InternalSignificantTerms createUnmappedInstance(String name,
                                                              List<PipelineAggregator> pipelineAggregators,
                                                              Map<String, Object> metaData) {
        InternalSignificantTerms<?, ?> testInstance = createTestInstance(name, pipelineAggregators, metaData);
        return new UnmappedSignificantTerms(name, testInstance.requiredSize, testInstance.minDocCount, pipelineAggregators, metaData);
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

        InternalSignificantTerms expectedSigTerms = (InternalSignificantTerms) expected;
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

    private static Map<Object, Long> toCounts(Stream<? extends SignificantTerms.Bucket> buckets,
                                              Function<SignificantTerms.Bucket, Long> fn) {
        return buckets.collect(Collectors.toMap(SignificantTerms.Bucket::getKey, fn, Long::sum));
    }

    private static SignificanceHeuristic randomSignificanceHeuristic() {
        return randomFrom(
                new JLHScore(),
                new MutualInformation(randomBoolean(), randomBoolean()),
                new GND(randomBoolean()),
                new ChiSquare(randomBoolean(), randomBoolean()));
    }
}
