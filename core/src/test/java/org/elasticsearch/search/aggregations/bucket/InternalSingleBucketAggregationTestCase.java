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

package org.elasticsearch.search.aggregations.bucket;

import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregationTestCase;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.ParsedAggregation;
import org.elasticsearch.search.aggregations.metrics.max.InternalMax;
import org.elasticsearch.search.aggregations.metrics.min.InternalMin;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;

public abstract class InternalSingleBucketAggregationTestCase<T extends InternalSingleBucketAggregation>
        extends InternalAggregationTestCase<T> {
    private final boolean hasInternalMax = randomBoolean();
    private final boolean hasInternalMin = randomBoolean();

    protected abstract T createTestInstance(String name, long docCount, InternalAggregations aggregations,
            List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData);
    protected abstract void extraAssertReduced(T reduced, List<T> inputs);

    @Override
    protected final T createTestInstance(String name, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) {
        List<InternalAggregation> internal = new ArrayList<>();
        // TODO randomize this beyond two types of aggregations
        if (hasInternalMax) {
            internal.add(new InternalMax("max", randomDouble(), randomNumericDocValueFormat(), emptyList(), emptyMap()));
        }
        if (hasInternalMin) {
            internal.add(new InternalMin("min", randomDouble(), randomNumericDocValueFormat(), emptyList(), emptyMap()));
        }
        // we shouldn't use the full long range here since we sum doc count on reduce, and don't want to overflow the long range there
        long docCount = between(0, Integer.MAX_VALUE);
        return createTestInstance(name, docCount, new InternalAggregations(internal), pipelineAggregators, metaData);
    }

    @Override
    protected final void assertReduced(T reduced, List<T> inputs) {
        assertEquals(inputs.stream().mapToLong(InternalSingleBucketAggregation::getDocCount).sum(), reduced.getDocCount());
        if (hasInternalMax) {
            double expected = inputs.stream().mapToDouble(i -> {
                        InternalMax max = i.getAggregations().get("max");
                        return max.getValue();
                    }).max().getAsDouble();
            InternalMax reducedMax = reduced.getAggregations().get("max");
            assertEquals(expected, reducedMax.getValue(), 0);
        }
        if (hasInternalMin) {
            double expected = inputs.stream().mapToDouble(i -> {
                        InternalMin min = i.getAggregations().get("min");
                        return min.getValue();
                    }).min().getAsDouble();
            InternalMin reducedMin = reduced.getAggregations().get("min");
            assertEquals(expected, reducedMin.getValue(), 0);
        }
        extraAssertReduced(reduced, inputs);
    }

    @Override
    protected void assertFromXContent(T aggregation, ParsedAggregation parsedAggregation) {
        assertTrue(parsedAggregation instanceof ParsedSingleBucketAggregation);
        ParsedSingleBucketAggregation parsed = (ParsedSingleBucketAggregation) parsedAggregation;

        assertEquals(aggregation.getDocCount(), parsed.getDocCount());
        InternalAggregations aggregations = aggregation.getAggregations();
        Map<String, Aggregation> expectedAggregations = new HashMap<>();
        int expectedNumberOfAggregations = 0;
        for (Aggregation expectedAggregation : aggregations) {
            // since we shuffle xContent, we cannot rely on the order of the original inner aggregations for comparison
            assertTrue(expectedAggregation instanceof InternalAggregation);
            expectedAggregations.put(expectedAggregation.getName(), expectedAggregation);
            expectedNumberOfAggregations++;
        }
        int parsedNumberOfAggregations = 0;
        for (Aggregation parsedAgg : parsed.getAggregations()) {
            assertTrue(parsedAgg instanceof ParsedAggregation);
            assertTrue(expectedAggregations.keySet().contains(parsedAgg.getName()));
            // TODO check aggregation equality, e.g. same xContent
            parsedNumberOfAggregations++;
        }
        assertEquals(expectedNumberOfAggregations, parsedNumberOfAggregations);
        Class<? extends ParsedSingleBucketAggregation> parsedClass = implementationClass();
        assertTrue(parsedClass != null && parsedClass.isInstance(parsedAggregation));
    }

    // TODO make abstract
    protected Class<? extends ParsedSingleBucketAggregation> implementationClass() {
        return ParsedSingleBucketAggregation.class;
    }
}
