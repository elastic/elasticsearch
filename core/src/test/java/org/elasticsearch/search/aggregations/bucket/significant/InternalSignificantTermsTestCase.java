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

import org.elasticsearch.search.aggregations.InternalAggregationTestCase;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public abstract class InternalSignificantTermsTestCase extends InternalAggregationTestCase<InternalSignificantTerms<?, ?>> {

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

    private static Map<Object, Long> toCounts(Stream<? extends SignificantTerms.Bucket> buckets,
                                              Function<SignificantTerms.Bucket, Long> fn) {
        return buckets.collect(Collectors.toMap(SignificantTerms.Bucket::getKey, fn, Long::sum));
    }
}
