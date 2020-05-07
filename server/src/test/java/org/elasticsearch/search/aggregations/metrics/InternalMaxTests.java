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
import org.elasticsearch.test.InternalAggregationTestCase;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class InternalMaxTests extends InternalAggregationTestCase<InternalMax> {

    @Override
    protected InternalMax createTestInstance(String name, Map<String, Object> metadata) {
        double value = frequently() ? randomDouble() : randomFrom(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY);
        DocValueFormat formatter = randomNumericDocValueFormat();
        return new InternalMax(name, value, formatter, metadata);
    }

    @Override
    protected void assertReduced(InternalMax reduced, List<InternalMax> inputs) {
        assertEquals(inputs.stream().mapToDouble(InternalMax::value).max().getAsDouble(), reduced.value(), 0);
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

    @Override
    protected InternalMax mutateInstance(InternalMax instance) {
        String name = instance.getName();
        double value = instance.getValue();
        DocValueFormat formatter = instance.format;
        Map<String, Object> metadata = instance.getMetadata();
        switch (between(0, 2)) {
        case 0:
            name += randomAlphaOfLength(5);
            break;
        case 1:
            if (Double.isFinite(value)) {
                value += between(1, 100);
            } else {
                value = between(1, 100);
            }
            break;
        case 2:
            if (metadata == null) {
                metadata = new HashMap<>(1);
            } else {
                metadata = new HashMap<>(instance.getMetadata());
            }
            metadata.put(randomAlphaOfLength(15), randomInt());
            break;
        default:
            throw new AssertionError("Illegal randomisation branch");
        }
        return new InternalMax(name, value, formatter, metadata);
    }
}
