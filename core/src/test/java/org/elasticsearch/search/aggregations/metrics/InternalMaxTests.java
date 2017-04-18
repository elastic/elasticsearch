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

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.InternalAggregationTestCase;
import org.elasticsearch.search.aggregations.metrics.max.InternalMax;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;

import java.util.List;
import java.util.Map;

public class InternalMaxTests extends InternalAggregationTestCase<InternalMax> {
    @Override
    protected InternalMax createTestInstance(String name, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) {
        return new InternalMax(name, randomDouble(),
                randomFrom(DocValueFormat.BOOLEAN, DocValueFormat.GEOHASH, DocValueFormat.IP, DocValueFormat.RAW), pipelineAggregators,
                metaData);
    }

    @Override
    protected Reader<InternalMax> instanceReader() {
        return InternalMax::new;
    }

    @Override
    protected void assertReduced(InternalMax reduced, List<InternalMax> inputs) {
        assertEquals(inputs.stream().mapToDouble(InternalMax::value).max().getAsDouble(), reduced.value(), 0);
    }
}
