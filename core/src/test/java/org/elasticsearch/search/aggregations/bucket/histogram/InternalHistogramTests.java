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
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.InternalMultiBucketAggregationTestCase;
import org.elasticsearch.search.aggregations.ParsedMultiBucketAggregation;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

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
        InternalOrder order = (InternalOrder) randomFrom(InternalHistogram.Order.KEY_ASC, InternalHistogram.Order.KEY_DESC);
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
    protected Class<? extends ParsedMultiBucketAggregation> implementationClass() {
        return ParsedHistogram.class;
    }
}
