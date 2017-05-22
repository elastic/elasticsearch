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

package org.elasticsearch.search.aggregations.metrics;


import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.ParsedAggregation;
import org.elasticsearch.search.aggregations.metrics.max.InternalMax;
import org.elasticsearch.search.aggregations.metrics.max.ParsedMax;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.test.InternalAggregationTestCase;

import java.util.List;
import java.util.Map;

public class InternalMaxTests extends InternalAggregationTestCase<InternalMax> {

    @Override
    protected InternalMax createTestInstance(String name, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) {
        double value = frequently() ? randomDouble() : randomFrom(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY);
        DocValueFormat formatter = randomNumericDocValueFormat();
        return new InternalMax(name, value, formatter, pipelineAggregators, metaData);
    }

    @Override
    protected void assertFromXContent(InternalMax max, ParsedAggregation parsedAggregation) {
        ParsedMax parsed = ((ParsedMax) parsedAggregation);
        if (Double.isInfinite(max.getValue()) == false) {
            assertEquals(max.getValue(), parsed.getValue(), Double.MIN_VALUE);
            assertEquals(max.getValueAsString(), parsed.getValueAsString());
        } else {
            // we write Double.NEGATIVE_INFINITY and Double.POSITIVE_INFINITY to xContent as 'null', so we
            // cannot differentiate between them. Also we cannot recreate the exact String representation
            assertEquals(parsed.getValue(), Double.NEGATIVE_INFINITY, 0);
        }
    }
}
