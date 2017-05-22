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

package org.elasticsearch.search.aggregations.metrics.geobounds;

import org.elasticsearch.search.aggregations.ParsedAggregation;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.test.InternalAggregationTestCase;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class InternalGeoBoundsTests extends InternalAggregationTestCase<InternalGeoBounds> {

    @Override
    protected InternalGeoBounds createTestInstance(String name, List<PipelineAggregator> pipelineAggregators,
                                                   Map<String, Object> metaData) {
        // we occasionally want to test top = Double.NEGATIVE_INFINITY since this triggers empty xContent object
        double top = frequently() ? randomDouble() : Double.NEGATIVE_INFINITY;
        InternalGeoBounds geo = new InternalGeoBounds(name,
            top, randomDouble(), randomDouble(), randomDouble(),
            randomDouble(), randomDouble(), randomBoolean(),
            pipelineAggregators, Collections.emptyMap());
        return geo;
    }

    @Override
    protected void assertFromXContent(InternalGeoBounds aggregation, ParsedAggregation parsedAggregation) {
        assertTrue(parsedAggregation instanceof ParsedGeoBounds);
        ParsedGeoBounds parsed = (ParsedGeoBounds) parsedAggregation;

        assertEquals(aggregation.topLeft(), parsed.topLeft());
        assertEquals(aggregation.bottomRight(), parsed.bottomRight());
    }
}
