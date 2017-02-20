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
package org.elasticsearch.search.aggregations.bucket.range;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.InternalAggregationTestCase;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class InternalBinaryRangeTests extends InternalAggregationTestCase<InternalBinaryRange> {
    private Tuple<BytesRef, BytesRef>[] RANGES;

    @Before
    public void randomSortedRanges() {
        int numRanges = randomIntBetween(1, 10);
        Tuple<BytesRef, BytesRef>[] ranges = new Tuple[numRanges];
        for (int i = 0; i < numRanges; i++) {
            BytesRef[] values = new BytesRef[2];
            values[0] = new BytesRef(randomAsciiOfLength(15));
            values[1] = new BytesRef(randomAsciiOfLength(15));
            Arrays.sort(values);
            ranges[i] = new Tuple(values[0], values[1]);
        }
        Arrays.sort(ranges, (t1, t2) -> t1.v1().compareTo(t2.v1()));
        RANGES = ranges;
    }


    @Override
    protected InternalBinaryRange createTestInstance(String name, List<PipelineAggregator> pipelineAggregators,
                                                     Map<String, Object> metaData) {
        boolean keyed = randomBoolean();
        DocValueFormat format = DocValueFormat.RAW;
        List<InternalBinaryRange.Bucket> buckets = new ArrayList<>();
        for (int i = 0; i < RANGES.length; ++i) {
            final int docCount = randomIntBetween(1, 100);
            buckets.add(new InternalBinaryRange.Bucket(format, keyed, randomAsciiOfLength(10),
                RANGES[i].v1(), RANGES[i].v2(), docCount, InternalAggregations.EMPTY));
        }
        return new InternalBinaryRange(name, format, keyed, buckets, pipelineAggregators, Collections.emptyMap());
    }

    @Override
    protected Writeable.Reader<InternalBinaryRange> instanceReader() {
        return InternalBinaryRange::new;
    }

    @Override
    protected void assertReduced(InternalBinaryRange reduced, List<InternalBinaryRange> inputs) {
        int pos = 0;
        for (InternalBinaryRange input : inputs) {
            assertEquals(reduced.getBuckets().size(), input.getBuckets().size());
        }
        for (Range.Bucket bucket : reduced.getBuckets()) {
            int expectedCount = 0;
            for (InternalBinaryRange input : inputs) {
                expectedCount += input.getBuckets().get(pos).getDocCount();
            }
            assertEquals(expectedCount, bucket.getDocCount());
            pos ++;
        }
    }
}
