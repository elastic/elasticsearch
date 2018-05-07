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

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.metrics.percentiles.InternalPercentilesRanksTestCase;
import org.elasticsearch.search.aggregations.metrics.percentiles.ParsedPercentiles;
import org.elasticsearch.search.aggregations.metrics.percentiles.Percentile;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

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
    protected void assertReduced(InternalTDigestPercentileRanks reduced, List<InternalTDigestPercentileRanks> inputs) {
        // it is hard to check the values due to the inaccuracy of the algorithm
        // the min/max values should be accurate due to the way the algo works so we can at least test those
        double min = Double.POSITIVE_INFINITY;
        double max = Double.NEGATIVE_INFINITY;
        long totalCount = 0;
        for (InternalTDigestPercentileRanks ranks : inputs) {
            if (ranks.state.centroidCount() == 0) {
                // quantiles would return NaN
                continue;
            }
            totalCount += ranks.state.size();
            min = Math.min(ranks.state.quantile(0), min);
            max = Math.max(ranks.state.quantile(1), max);
        }
        assertEquals(totalCount, reduced.state.size());
        if (totalCount > 0) {
            assertEquals(reduced.state.quantile(0), min, 0d);
            assertEquals(reduced.state.quantile(1), max, 0d);
        }
    }

    @Override
    protected Reader<InternalTDigestPercentileRanks> instanceReader() {
        return InternalTDigestPercentileRanks::new;
    }

    @Override
    protected Class<? extends ParsedPercentiles> implementationClass() {
        return ParsedTDigestPercentileRanks.class;
    }

    @Override
    protected InternalTDigestPercentileRanks mutateInstance(InternalTDigestPercentileRanks instance) {
        String name = instance.getName();
        double[] percents = instance.keys;
        TDigestState state = instance.state;
        boolean keyed = instance.keyed;
        DocValueFormat formatter = instance.formatter();
        List<PipelineAggregator> pipelineAggregators = instance.pipelineAggregators();
        Map<String, Object> metaData = instance.getMetaData();
        switch (between(0, 4)) {
        case 0:
            name += randomAlphaOfLength(5);
            break;
        case 1:
            percents = Arrays.copyOf(percents, percents.length + 1);
            percents[percents.length - 1] = randomDouble() * 100;
            Arrays.sort(percents);
            break;
        case 2:
            TDigestState newState = new TDigestState(state.compression());
            newState.add(state);
            for (int i = 0; i < between(10, 100); i++) {
                newState.add(randomDouble());
            }
            state = newState;
            break;
        case 3:
            keyed = keyed == false;
            break;
        case 4:
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
        return new InternalTDigestPercentileRanks(name, percents, state, keyed, formatter, pipelineAggregators, metaData);
    }

    public void testEmptyRanksXContent() throws IOException {
        double[] percents = new double[]{1,2,3};
        boolean keyed = randomBoolean();
        DocValueFormat docValueFormat = randomNumericDocValueFormat();

        final TDigestState state = new TDigestState(100);
        InternalTDigestPercentileRanks agg = new InternalTDigestPercentileRanks("test", percents, state, keyed, docValueFormat,
            Collections.emptyList(), Collections.emptyMap());

        for (Percentile percentile : agg) {
            Double value = percentile.getValue();
            assertThat(agg.percent(value), equalTo(Double.NaN));
            assertThat(agg.percentAsString(value), equalTo("NaN"));
        }


        XContentBuilder builder = JsonXContent.contentBuilder().prettyPrint();
        builder.startObject();
        agg.doXContentBody(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        String expected;
        if (keyed && docValueFormat.equals(DocValueFormat.RAW)) {
            expected = "{\n" +
                "  \"values\" : {\n" +
                "    \"1.0\" : null,\n" +
                "    \"2.0\" : null,\n" +
                "    \"3.0\" : null\n" +
                "  }\n" +
                "}";
        } else if (keyed) {
            expected = "{\n" +
                "  \"values\" : {\n" +
                "    \"1.0\" : null,\n" +
                "    \"1.0_as_string\" : null,\n" +
                "    \"2.0\" : null,\n" +
                "    \"2.0_as_string\" : null,\n" +
                "    \"3.0\" : null,\n" +
                "    \"3.0_as_string\" : null\n" +
                "  }\n" +
                "}";
        } else if (keyed == false && docValueFormat.equals(DocValueFormat.RAW)) {
            expected = "{\n" +
                "  \"values\" : [\n" +
                "    {\n" +
                "      \"key\" : 1.0,\n" +
                "      \"value\" : null\n" +
                "    },\n" +
                "    {\n" +
                "      \"key\" : 2.0,\n" +
                "      \"value\" : null\n" +
                "    },\n" +
                "    {\n" +
                "      \"key\" : 3.0,\n" +
                "      \"value\" : null\n" +
                "    }\n" +
                "  ]\n" +
                "}";
        } else {
            expected = "{\n" +
                "  \"values\" : [\n" +
                "    {\n" +
                "      \"key\" : 1.0,\n" +
                "      \"value\" : null,\n" +
                "      \"value_as_string\" : null\n" +
                "    },\n" +
                "    {\n" +
                "      \"key\" : 2.0,\n" +
                "      \"value\" : null,\n" +
                "      \"value_as_string\" : null\n" +
                "    },\n" +
                "    {\n" +
                "      \"key\" : 3.0,\n" +
                "      \"value\" : null,\n" +
                "      \"value_as_string\" : null\n" +
                "    }\n" +
                "  ]\n" +
                "}";
        }
        assertThat(Strings.toString(builder), equalTo(expected));
    }
}
