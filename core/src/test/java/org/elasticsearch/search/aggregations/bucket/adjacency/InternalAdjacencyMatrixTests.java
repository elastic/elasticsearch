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

package org.elasticsearch.search.aggregations.bucket.adjacency;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.test.InternalMultiBucketAggregationTestCase;
import org.elasticsearch.search.aggregations.ParsedMultiBucketAggregation;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class InternalAdjacencyMatrixTests extends InternalMultiBucketAggregationTestCase<InternalAdjacencyMatrix> {

    private List<String> keys;

    @Override
    protected int maxNumberOfBuckets() {
        return 10;
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        keys = new ArrayList<>();
        // InternalAdjacencyMatrix represents the upper triangular matrix:
        // 2 filters (matrix of 2x2) generates 3 buckets
        // 3 filters generates 6 buckets
        // 4 filters generates 10 buckets
        int numFilters = randomIntBetween(2, 4);
        String[] filters = new String[numFilters];
        for (int i = 0; i < numFilters; i++) {
            filters[i] = randomAlphaOfLength(5);
        }
        for (int i = 0; i < filters.length; i++) {
            keys.add(filters[i]);
            for (int j = i + 1; j < filters.length; j++) {
                if (filters[i].compareTo(filters[j]) <= 0) {
                    keys.add(filters[i] + "&" + filters[j]);
                } else {
                    keys.add(filters[j] + "&" + filters[i]);
                }
            }
        }
    }

    @Override
    protected InternalAdjacencyMatrix createTestInstance(String name, List<PipelineAggregator> pipelineAggregators,
            Map<String, Object> metaData, InternalAggregations aggregations) {
        final List<InternalAdjacencyMatrix.InternalBucket> buckets = new ArrayList<>();
        for (int i = 0; i < keys.size(); ++i) {
            String key = keys.get(i);
            int docCount = randomIntBetween(0, 1000);
            buckets.add(new InternalAdjacencyMatrix.InternalBucket(key, docCount, aggregations));
        }
        return new InternalAdjacencyMatrix(name, buckets, pipelineAggregators, metaData);
    }

    @Override
    protected void assertReduced(InternalAdjacencyMatrix reduced, List<InternalAdjacencyMatrix> inputs) {
        final Map<String, Long> expectedCounts = new TreeMap<>();
        for (InternalAdjacencyMatrix input : inputs) {
            for (InternalAdjacencyMatrix.InternalBucket bucket : input.getBuckets()) {
                expectedCounts.compute(bucket.getKeyAsString(),
                        (key, oldValue) -> (oldValue == null ? 0 : oldValue) + bucket.getDocCount());
            }
        }
        final Map<String, Long> actualCounts = new TreeMap<>();
        for (InternalAdjacencyMatrix.InternalBucket bucket : reduced.getBuckets()) {
            actualCounts.compute(bucket.getKeyAsString(),
                    (key, oldValue) -> (oldValue == null ? 0 : oldValue) + bucket.getDocCount());
        }
        assertEquals(expectedCounts, actualCounts);
    }

    @Override
    protected Reader<InternalAdjacencyMatrix> instanceReader() {
        return InternalAdjacencyMatrix::new;
    }

    @Override
    protected Class<? extends ParsedMultiBucketAggregation> implementationClass() {
        return ParsedAdjacencyMatrix.class;
    }

    @Override
    protected InternalAdjacencyMatrix mutateInstance(InternalAdjacencyMatrix instance) {
        String name = instance.getName();
        List<InternalAdjacencyMatrix.InternalBucket> buckets = instance.getBuckets();
        List<PipelineAggregator> pipelineAggregators = instance.pipelineAggregators();
        Map<String, Object> metaData = instance.getMetaData();
        switch (between(0, 2)) {
        case 0:
            name += randomAlphaOfLength(5);
            break;
        case 1:
            buckets = new ArrayList<>(buckets);
            buckets.add(new InternalAdjacencyMatrix.InternalBucket(randomAlphaOfLength(10), randomNonNegativeLong(),
                    InternalAggregations.EMPTY));
            break;
        case 2:
            if (metaData == null) {
                metaData = new HashMap<>(1);
            } else {
                metaData = new HashMap<>(instance.getMetaData());
            }
            metaData.put(randomAlphaOfLength(15), randomInt());
            break;
        default:
            throw new AssertionError("Illegal randomisation branch");
        }
        return new InternalAdjacencyMatrix(name, buckets, pipelineAggregators, metaData);
    }
}
