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

package org.elasticsearch.search.aggregations.bucket.missing;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.ParsedAggregation;
import org.elasticsearch.search.aggregations.bucket.InternalSingleBucketAggregationTestCase;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class InternalMissingTests extends InternalSingleBucketAggregationTestCase<InternalMissing> {
    @Override
    protected InternalMissing createTestInstance(String name, long docCount, InternalAggregations aggregations,
            List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) {
        return new InternalMissing(name, docCount, aggregations, pipelineAggregators, metaData);
    }

    @Override
    protected void extraAssertReduced(InternalMissing reduced, List<InternalMissing> inputs) {
        // Nothing extra to assert
    }

    @Override
    protected Reader<InternalMissing> instanceReader() {
        return InternalMissing::new;
    }

    @Override
    protected void assertFromXContent(InternalMissing aggregation, ParsedAggregation parsedAggregation) {
        assertTrue(parsedAggregation instanceof ParsedMissing);
        ParsedMissing parsed = (ParsedMissing) parsedAggregation;

        assertEquals(aggregation.getDocCount(), parsed.getDocCount());
        InternalAggregations aggregations = aggregation.getAggregations();
        Iterator<Aggregation> parsedAggregations = parsed.getAggregations().iterator();
        for (Aggregation expectedAggregation : aggregations) {
            assertTrue(parsedAggregations.hasNext());
            Aggregation parsedAgg = parsedAggregations.next();
            assertTrue(expectedAggregation instanceof InternalAggregation);
            assertTrue(parsedAggregation instanceof ParsedAggregation);
            assertEquals(expectedAggregation.getName(), parsedAgg.getName());
            // TODO check aggregation equality, e.g. same xContent
        }
        assertFalse(parsedAggregations.hasNext());
    }

}
