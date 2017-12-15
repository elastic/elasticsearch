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

package org.elasticsearch.search.aggregations.bucket.histogram;

import org.apache.lucene.util.TestUtil;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregation.ReduceContext;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.test.InternalMultiBucketAggregationTestCase;
import org.elasticsearch.search.aggregations.ParsedMultiBucketAggregation;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class InternalHistogramTests extends InternalMultiBucketAggregationTestCase<InternalHistogram> {

    private boolean keyed;
    private DocValueFormat format;

    @Override
    public void setUp() throws Exception{
        super.setUp();
        keyed = randomBoolean();
        format = randomNumericDocValueFormat();
    }

    @Override
    protected InternalHistogram createTestInstance(String name,
                                                   List<PipelineAggregator> pipelineAggregators,
                                                   Map<String, Object> metaData,
                                                   InternalAggregations aggregations) {
        final int base = randomInt(50) - 30;
        final int numBuckets = randomNumberOfBuckets();
        final int interval = randomIntBetween(1, 3);
        List<InternalHistogram.Bucket> buckets = new ArrayList<>();
        for (int i = 0; i < numBuckets; ++i) {
            final int docCount = TestUtil.nextInt(random(), 1, 50);
            buckets.add(new InternalHistogram.Bucket(base + i * interval, docCount, keyed, format, aggregations));
        }
        BucketOrder order = BucketOrder.key(randomBoolean());
        return new InternalHistogram(name, buckets, order, 1, null, format, keyed, pipelineAggregators, metaData);
    }

    // issue 26787
    public void testHandlesNaN() {
        InternalHistogram histogram = createTestInstance();
        InternalHistogram histogram2 = createTestInstance();
        List<InternalHistogram.Bucket> buckets = histogram.getBuckets();
        if (buckets == null || buckets.isEmpty()) {
            return;
        }

        // Set the key of one bucket to NaN. Must be the last bucket because NaN is greater than everything else.
        List<InternalHistogram.Bucket> newBuckets = new ArrayList<>(buckets.size());
        if (buckets.size() > 1) {
            newBuckets.addAll(buckets.subList(0, buckets.size() - 1));
        }
        InternalHistogram.Bucket b = buckets.get(buckets.size() - 1);
        newBuckets.add(new InternalHistogram.Bucket(Double.NaN, b.docCount, keyed, b.format, b.aggregations));
        
        InternalHistogram newHistogram = histogram.create(newBuckets);
        newHistogram.doReduce(Arrays.asList(newHistogram, histogram2), new InternalAggregation.ReduceContext(null, null, false));
    }

    @Override
    protected void assertReduced(InternalHistogram reduced, List<InternalHistogram> inputs) {
        Map<Double, Long> expectedCounts = new TreeMap<>();
        for (Histogram histogram : inputs) {
            for (Histogram.Bucket bucket : histogram.getBuckets()) {
                expectedCounts.compute((Double) bucket.getKey(),
                        (key, oldValue) -> (oldValue == null ? 0 : oldValue) + bucket.getDocCount());
            }
        }
        Map<Double, Long> actualCounts = new TreeMap<>();
        for (Histogram.Bucket bucket : reduced.getBuckets()) {
            actualCounts.compute((Double) bucket.getKey(),
                    (key, oldValue) -> (oldValue == null ? 0 : oldValue) + bucket.getDocCount());
        }
        assertEquals(expectedCounts, actualCounts);
    }

    @Override
    protected Reader<InternalHistogram> instanceReader() {
        return InternalHistogram::new;
    }

    @Override
    protected Class<? extends ParsedMultiBucketAggregation> implementationClass() {
        return ParsedHistogram.class;
    }

    @Override
    protected InternalHistogram mutateInstance(InternalHistogram instance) {
        String name = instance.getName();
        List<InternalHistogram.Bucket> buckets = instance.getBuckets();
        BucketOrder order = instance.getOrder();
        long minDocCount = instance.getMinDocCount();
        List<PipelineAggregator> pipelineAggregators = instance.pipelineAggregators();
        Map<String, Object> metaData = instance.getMetaData();
        switch (between(0, 4)) {
        case 0:
            name += randomAlphaOfLength(5);
            break;
        case 1:
            buckets = new ArrayList<>(buckets);
            buckets.add(new InternalHistogram.Bucket(randomNonNegativeLong(), randomIntBetween(1, 100), keyed, format,
                    InternalAggregations.EMPTY));
            break;
        case 2:
            order = BucketOrder.count(randomBoolean());
            break;
        case 3:
            minDocCount += between(1, 10);
            break;
        case 4:
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
        return new InternalHistogram(name, buckets, order, minDocCount, null, format, keyed, pipelineAggregators, metaData);
    }
}
