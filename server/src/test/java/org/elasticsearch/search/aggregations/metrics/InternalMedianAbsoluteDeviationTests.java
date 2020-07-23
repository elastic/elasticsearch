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

import org.elasticsearch.search.aggregations.ParsedAggregation;
import org.elasticsearch.test.InternalAggregationTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class InternalMedianAbsoluteDeviationTests extends InternalAggregationTestCase<InternalMedianAbsoluteDeviation> {

    @Override
    protected InternalMedianAbsoluteDeviation createTestInstance(String name, Map<String, Object> metadata) {
        final TDigestState valuesSketch = new TDigestState(randomDoubleBetween(20, 1000, true));
        final int numberOfValues = frequently()
            ? randomIntBetween(0, 1000)
            : 0;
        for (int i = 0; i < numberOfValues; i++) {
            valuesSketch.add(randomDouble());
        }

        return new InternalMedianAbsoluteDeviation(name, metadata, randomNumericDocValueFormat(), valuesSketch);
    }

    @Override
    protected void assertReduced(InternalMedianAbsoluteDeviation reduced, List<InternalMedianAbsoluteDeviation> inputs) {
        final TDigestState expectedValuesSketch = new TDigestState(reduced.getValuesSketch().compression());

        long totalCount = 0;
        for (InternalMedianAbsoluteDeviation input : inputs) {
            expectedValuesSketch.add(input.getValuesSketch());
            totalCount += input.getValuesSketch().size();
        }

        assertEquals(totalCount, reduced.getValuesSketch().size());
        if (totalCount > 0) {
            assertEquals(expectedValuesSketch.quantile(0), reduced.getValuesSketch().quantile(0), 0d);
            assertEquals(expectedValuesSketch.quantile(1), reduced.getValuesSketch().quantile(1), 0d);
        }
    }

    @Override
    protected void assertFromXContent(InternalMedianAbsoluteDeviation internalMAD, ParsedAggregation parsedAggregation) throws IOException {
        assertTrue(parsedAggregation instanceof ParsedMedianAbsoluteDeviation);
        ParsedMedianAbsoluteDeviation parsedMAD = (ParsedMedianAbsoluteDeviation) parsedAggregation;
        // Double.compare handles NaN, which we use for no result
        assertEquals(internalMAD.getMedianAbsoluteDeviation(), parsedMAD.getMedianAbsoluteDeviation(), 0);
    }

    @Override
    protected InternalMedianAbsoluteDeviation mutateInstance(InternalMedianAbsoluteDeviation instance) throws IOException {
        String name = instance.getName();
        TDigestState valuesSketch = instance.getValuesSketch();
        Map<String, Object> metadata = instance.getMetadata();

        switch (between(0, 2)) {
            case 0:
                name += randomAlphaOfLengthBetween(2, 10);
                break;
            case 1:
                final TDigestState newValuesSketch = new TDigestState(instance.getValuesSketch().compression());
                final int numberOfValues = between(10, 100);
                for (int i = 0; i < numberOfValues; i++) {
                    newValuesSketch.add(randomDouble());
                }
                valuesSketch = newValuesSketch;
                break;
            case 2:
                if (metadata == null) {
                    metadata = new HashMap<>(1);
                } else {
                    metadata = new HashMap<>(metadata);
                }
                metadata.put(randomAlphaOfLengthBetween(2, 10), randomInt());
                break;
        }

        return new InternalMedianAbsoluteDeviation(name, metadata, instance.format, valuesSketch);
    }
}
