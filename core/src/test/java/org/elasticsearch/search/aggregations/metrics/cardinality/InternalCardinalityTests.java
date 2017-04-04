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

package org.elasticsearch.search.aggregations.metrics.cardinality;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.rest.action.search.RestSearchAction;
import org.elasticsearch.search.aggregations.InternalAggregationTestCase;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentHelper.toXContent;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertToXContentEquivalent;

public class InternalCardinalityTests extends InternalAggregationTestCase<InternalCardinality> {
    private static List<HyperLogLogPlusPlus> algos;
    private static int p;

    @Before
    public void setup() {
        algos = new ArrayList<>();
        p = randomIntBetween(HyperLogLogPlusPlus.MIN_PRECISION, HyperLogLogPlusPlus.MAX_PRECISION);
    }

    @Override
    protected InternalCardinality createTestInstance(String name,
            List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) {
        HyperLogLogPlusPlus hllpp = new HyperLogLogPlusPlus(p,
                new MockBigArrays(Settings.EMPTY, new NoneCircuitBreakerService()), 1);
        algos.add(hllpp);
        for (int i = 0; i < 100; i++) {
            hllpp.collect(0, randomIntBetween(1, 100));
        }
        return new InternalCardinality(name, hllpp, pipelineAggregators, metaData);
    }

    @Override
    protected Reader<InternalCardinality> instanceReader() {
        return InternalCardinality::new;
    }

    @Override
    protected void assertReduced(InternalCardinality reduced, List<InternalCardinality> inputs) {
        HyperLogLogPlusPlus[] algos = inputs.stream().map(InternalCardinality::getState)
                .toArray(size -> new HyperLogLogPlusPlus[size]);
        if (algos.length > 0) {
            HyperLogLogPlusPlus result = algos[0];
            for (int i = 1; i < algos.length; i++) {
                result.merge(0, algos[i], 0);
            }
            assertEquals(result.cardinality(0), reduced.value(), 0);
        }
    }

    public void testFromXContent() throws IOException {
        InternalCardinality cardinality = createTestInstance();
        ToXContent.Params params = new ToXContent.MapParams(Collections.singletonMap(RestSearchAction.TYPED_KEYS_PARAM, "true"));
        boolean humanReadable = randomBoolean();
        XContentType xContentType = randomFrom(XContentType.values());
        BytesReference originalBytes = toXContent(cardinality, xContentType, params, humanReadable);

        InternalCardinality parsed;
        try (XContentParser parser = createParser(xContentType.xContent(), originalBytes)) {
            assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
            assertEquals(XContentParser.Token.FIELD_NAME, parser.nextToken());
            assertEquals(cardinality.getWriteableName() + "#" + cardinality.getName(), parser.currentName());
            assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
            parsed = InternalCardinality.parseXContentBody(cardinality.getName(), parser);
            assertEquals(XContentParser.Token.END_OBJECT, parser.currentToken());
            assertEquals(XContentParser.Token.END_OBJECT, parser.nextToken());
            assertNull(parser.nextToken());
        }
        assertEquals(cardinality.getName(), parsed.getName());
        assertEquals(cardinality.getValue(), parsed.getValue(), Double.MIN_VALUE);
        assertEquals(cardinality.getValueAsString(), parsed.getValueAsString());
        assertEquals(cardinality.getMetaData(), parsed.getMetaData());
        assertToXContentEquivalent(originalBytes, toXContent(parsed, xContentType, params, humanReadable), xContentType);
    }

    @After
    public void cleanup() {
        Releasables.close(algos);
        algos.clear();
        algos = null;
    }
}
