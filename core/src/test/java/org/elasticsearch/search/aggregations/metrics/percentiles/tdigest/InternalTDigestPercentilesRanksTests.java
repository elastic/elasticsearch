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

package org.elasticsearch.search.aggregations.metrics.percentiles.tdigest;

import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.metrics.percentiles.InternalPercentilesRanksTestCase;
import org.elasticsearch.search.aggregations.metrics.percentiles.ParsedPercentiles;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class InternalTDigestPercentilesRanksTests extends InternalPercentilesRanksTestCase<InternalTDigestPercentileRanks> {

    @Override
    protected InternalTDigestPercentileRanks createTestInstance(String name, List<PipelineAggregator> aggregators,
                                                                Map<String, Object> metadata,
                                                                boolean keyed, DocValueFormat format, double[] percents, double[] values) {
        final TDigestState state = new TDigestState(100);
        Arrays.stream(values).forEach(state::add);

        assertEquals(state.centroidCount(), values.length);
        return new InternalTDigestPercentileRanks(name, percents, state, keyed, format, aggregators, metadata);
    }

    @Override
    protected Class<? extends ParsedPercentiles> implementationClass() {
        return ParsedTDigestPercentileRanks.class;
    }
}
