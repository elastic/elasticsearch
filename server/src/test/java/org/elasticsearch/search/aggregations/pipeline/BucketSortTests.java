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
package org.elasticsearch.search.aggregations.pipeline;

import org.elasticsearch.search.aggregations.BasePipelineAggregationTestCase;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.equalTo;

public class BucketSortTests extends BasePipelineAggregationTestCase<BucketSortPipelineAggregationBuilder> {

    @Override
    protected BucketSortPipelineAggregationBuilder createTestAggregatorFactory() {
        int sortCount = randomIntBetween(0, 5);
        List<FieldSortBuilder> sorts = new ArrayList<>(sortCount);
        for (int i = 0; i < sortCount; ++i) {
            String sortField = randomAlphaOfLengthBetween(3, 20);
            SortOrder sortOrder = randomFrom(SortOrder.values());
            FieldSortBuilder fieldSortBuilder = new FieldSortBuilder(sortField);
            fieldSortBuilder.order(sortOrder);
            sorts.add(fieldSortBuilder);
        }
        BucketSortPipelineAggregationBuilder factory = new BucketSortPipelineAggregationBuilder(randomAlphaOfLengthBetween(3, 20), sorts);
        Integer from = randomIntBetween(0, 20);
        Integer size = randomBoolean() ? randomIntBetween(1, 1000) : null;
        if (randomBoolean()) {
            factory.from(from);
        }
        if (size != null) {
            factory.size(size);
        }
        if (randomBoolean()) {
            factory.gapPolicy(randomFrom(BucketHelpers.GapPolicy.values()));
        }
        // Check if the combination ended up being invalid
        if (sorts.isEmpty() && size == null && from == 0) {
            factory.size(42);
        }
        return factory;
    }

    public void testNegativeFrom() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> new BucketSortPipelineAggregationBuilder("foo", Collections.emptyList()).from(-1));
        assertThat(e.getMessage(), equalTo("[from] must be a non-negative integer: [-1]"));
    }

    public void testNegativeSize() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> new BucketSortPipelineAggregationBuilder("foo", Collections.emptyList()).size(-1));
        assertThat(e.getMessage(), equalTo("[size] must be a positive integer: [-1]"));
    }

    public void testZeroSize() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> new BucketSortPipelineAggregationBuilder("foo", Collections.emptyList()).size(0));
        assertThat(e.getMessage(), equalTo("[size] must be a positive integer: [0]"));
    }

    public void testNullGapPolicy() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> new BucketSortPipelineAggregationBuilder("foo", Collections.emptyList()).gapPolicy(null));
        assertThat(e.getMessage(), equalTo("[gap_policy] must not be null: [foo]"));
    }

    public void testNoParent() {
        List<FieldSortBuilder> sorts = singletonList(new FieldSortBuilder("bar"));
        assertThat(validate(emptyList(), new BucketSortPipelineAggregationBuilder("foo", sorts)), 
            equalTo("Validation Failed: 1: bucket_sort aggregation [foo] must be declared inside of another aggregation;"));
    }
}
