/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.filter;

import org.elasticsearch.search.aggregations.AggregationReduceContext;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.InternalSingleBucketAggregationTestCase;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator.PipelineTree;
import org.elasticsearch.search.aggregations.support.SamplingContext;

import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;

public class InternalFilterTests extends InternalSingleBucketAggregationTestCase<InternalFilter> {
    @Override
    protected InternalFilter createTestInstance(
        String name,
        long docCount,
        InternalAggregations aggregations,
        Map<String, Object> metadata
    ) {
        return new InternalFilter(name, docCount, aggregations, metadata);
    }

    @Override
    protected boolean supportsSampling() {
        return true;
    }

    @Override
    protected void assertSampled(InternalFilter sampled, InternalFilter reduced, SamplingContext samplingContext) {
        assertThat(sampled.getDocCount(), equalTo(samplingContext.scaleUp(reduced.getDocCount())));
    }

    @Override
    protected void extraAssertReduced(InternalFilter reduced, List<InternalFilter> inputs) {
        // Nothing extra to assert
    }

    public void testReducePipelinesReturnsSameInstanceWithoutPipelines() {
        InternalFilter test = createTestInstance();
        assertThat(test.reducePipelines(test, emptyReduceContextBuilder().forFinalReduction(), PipelineTree.EMPTY), sameInstance(test));
    }

    public void testReducePipelinesReducesBucketPipelines() {
        /*
         * Tests that a pipeline buckets by creating a mock pipeline that
         * replaces "inner" with "dummy".
         */
        InternalFilter dummy = createTestInstance();
        InternalFilter inner = createTestInstance();

        InternalAggregations sub = InternalAggregations.from(List.of(inner));
        InternalFilter test = createTestInstance("test", randomNonNegativeLong(), sub, emptyMap());
        PipelineAggregator mockPipeline = new PipelineAggregator(null, null, null) {
            @Override
            public InternalAggregation reduce(InternalAggregation aggregation, AggregationReduceContext reduceContext) {
                return dummy;
            }
        };
        PipelineTree tree = new PipelineTree(Map.of(inner.getName(), new PipelineTree(emptyMap(), List.of(mockPipeline))), emptyList());
        InternalFilter reduced = (InternalFilter) test.reducePipelines(test, emptyReduceContextBuilder().forFinalReduction(), tree);
        assertThat(reduced.getAggregations().get(dummy.getName()), sameInstance(dummy));
    }
}
