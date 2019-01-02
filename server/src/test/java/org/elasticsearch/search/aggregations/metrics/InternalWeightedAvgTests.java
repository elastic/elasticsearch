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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.ParsedAggregation;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.test.InternalAggregationTestCase;

public class InternalWeightedAvgTests extends InternalAggregationTestCase<InternalWeightedAvg> {

    @Override
    protected InternalWeightedAvg createTestInstance(
        String name,
        List<PipelineAggregator> pipelineAggregators,
        Map<String, Object> metaData
    ) {
        DocValueFormat formatter = randomNumericDocValueFormat();
        return new InternalWeightedAvg(
            name,
            randomDoubleBetween(0, 100000, true),
            randomDoubleBetween(0, 100000, true),
            formatter, pipelineAggregators, metaData);
    }

    @Override
    protected Reader<InternalWeightedAvg> instanceReader() {
        return InternalWeightedAvg::new;
    }

    @Override
    protected void assertReduced(InternalWeightedAvg reduced, List<InternalWeightedAvg> inputs) {
        double sum = 0;
        double weight = 0;
        for (InternalWeightedAvg in : inputs) {
            sum += in.getSum();
            weight += in.getWeight();
        }
        assertEquals(sum, reduced.getSum(), 0.0000001);
        assertEquals(weight, reduced.getWeight(), 0.0000001);
        assertEquals(sum / weight, reduced.getValue(), 0.0000001);
    }

    @Override
    protected void assertFromXContent(InternalWeightedAvg avg, ParsedAggregation parsedAggregation) {
        ParsedWeightedAvg parsed = ((ParsedWeightedAvg) parsedAggregation);
        assertEquals(avg.getValue(), parsed.getValue(), Double.MIN_VALUE);
        // we don't print out VALUE_AS_STRING for avg.getCount() == 0, so we cannot get the exact same value back
        if (avg.getWeight() != 0) {
            assertEquals(avg.getValueAsString(), parsed.getValueAsString());
        }
    }

    @Override
    protected InternalWeightedAvg mutateInstance(InternalWeightedAvg instance) {
        String name = instance.getName();
        double sum = instance.getSum();
        double weight = instance.getWeight();
        DocValueFormat formatter = instance.getFormatter();
        List<PipelineAggregator> pipelineAggregators = instance.pipelineAggregators();
        Map<String, Object> metaData = instance.getMetaData();
        switch (between(0, 2)) {
        case 0:
            name += randomAlphaOfLength(5);
            break;
        case 1:
            if (Double.isFinite(sum)) {
                sum += between(1, 100);
            } else {
                sum = between(1, 100);
            }
            break;
        case 2:
            if (Double.isFinite(weight)) {
                weight += between(1, 100);
            } else {
                weight = between(1, 100);
            }
            break;
        case 3:
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
        return new InternalWeightedAvg(name, sum, weight, formatter, pipelineAggregators, metaData);
    }
}
