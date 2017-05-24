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

package org.elasticsearch.search.aggregations.pipeline.bucketmetrics;

import org.elasticsearch.search.aggregations.ParsedAggregation;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.test.InternalAggregationTestCase;

import java.util.List;
import java.util.Map;

public class InternalBucketMetricValueTests extends InternalAggregationTestCase<InternalBucketMetricValue> {

    @Override
    protected InternalBucketMetricValue createTestInstance(String name, List<PipelineAggregator> pipelineAggregators,
            Map<String, Object> metaData) {
        double value = frequently() ? randomDoubleBetween(-10000, 100000, true)
                : randomFrom(new Double[] { Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, Double.NaN });
        String[] keys = new String[randomIntBetween(0, 5)];
        for (int i = 0; i < keys.length; i++) {
            keys[i] = randomAlphaOfLength(10);
        }
        return new InternalBucketMetricValue(name, keys, value, randomNumericDocValueFormat(), pipelineAggregators, metaData);
    }

    @Override
    protected void assertFromXContent(InternalBucketMetricValue bucketMetricValue, ParsedAggregation parsedAggregation) {
        BucketMetricValue parsed = ((BucketMetricValue) parsedAggregation);
        assertArrayEquals(bucketMetricValue.keys(), parsed.keys());
        if (Double.isInfinite(bucketMetricValue.value()) == false) {
            assertEquals(bucketMetricValue.value(), parsed.value(), 0);
            assertEquals(bucketMetricValue.getValueAsString(), parsed.getValueAsString());
        } else {
            // we write Double.NEGATIVE_INFINITY and Double.POSITIVE_INFINITY to xContent as 'null', so we
            // cannot differentiate between them. Also we cannot recreate the exact String representation
            assertEquals(parsed.value(), Double.NEGATIVE_INFINITY, 0);
        }
    }
}
