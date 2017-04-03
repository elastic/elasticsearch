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

package org.elasticsearch.search.aggregations.pipeline.derivative;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.action.search.RestSearchAction;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.InternalAggregationTestCase;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentHelper.toXContent;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertToXContentEquivalent;

public class InternalDerivativeTests extends InternalAggregationTestCase<InternalDerivative> {

    Double[] valueEdgeCases = new Double[] {Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, Double.NaN};

    @Override
    protected InternalDerivative createTestInstance(String name,
            List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) {
        DocValueFormat formatter = randomFrom(new DocValueFormat.Decimal("###.##"),
                DocValueFormat.BOOLEAN, DocValueFormat.RAW);
        double value = frequently() ? randomDoubleBetween(0, 100000, true) : randomFrom(valueEdgeCases);
        double normalizationFactor = frequently() ? randomDoubleBetween(0, 100000, true) : randomFrom(valueEdgeCases);
        return new InternalDerivative(name, value, normalizationFactor, formatter, pipelineAggregators, metaData);
    }

    @Override
    public void testReduceRandom() {
        expectThrows(UnsupportedOperationException.class,
                () -> createTestInstance("name", Collections.emptyList(), null).reduce(null,
                        null));
    }

    @Override
    protected void assertReduced(InternalDerivative reduced, List<InternalDerivative> inputs) {
        // no test since reduce operation is unsupported
    }

    @Override
    protected Reader<InternalDerivative> instanceReader() {
        return InternalDerivative::new;
    }

    public void testFromXContent() throws IOException {
        InternalDerivative derivative = createTestInstance();
        boolean humanReadable = randomBoolean();
        ToXContent.Params params = new ToXContent.MapParams(Collections.singletonMap(
                RestSearchAction.TYPED_KEYS_PARAM, "true"));
        XContentType xContentType = randomFrom(XContentType.values());
        BytesReference originalBytes = toXContent(derivative, xContentType, params, humanReadable);

        InternalDerivative parsed;
        try (XContentParser parser = createParser(xContentType.xContent(), originalBytes)) {
            assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
            assertEquals(XContentParser.Token.FIELD_NAME, parser.nextToken());
            assertEquals(derivative.getWriteableName() + "#" + derivative.getName(),
                    parser.currentName());
            assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
            parsed = InternalDerivative.parseXContentBody(derivative.getName(), parser);
            assertEquals(XContentParser.Token.END_OBJECT, parser.currentToken());
            assertEquals(XContentParser.Token.END_OBJECT, parser.nextToken());
            assertNull(parser.nextToken());
        }
        assertEquals(derivative.getName(), parsed.getName());
        if (Double.isFinite(derivative.getValue())) {
            assertEquals(derivative.getValue(), parsed.getValue(), Double.MIN_VALUE);
            assertEquals(derivative.getValueAsString(), parsed.getValueAsString());
        } else {
            assertEquals(parsed.getValue(), Double.NaN, Double.MIN_VALUE);
        }
        if (Double.isFinite(derivative.normalizedValue())) {
            assertEquals(derivative.normalizedValue(), parsed.normalizedValue(), Double.MIN_VALUE);
            assertEquals(derivative.normalizedValueAsString(), parsed.normalizedValueAsString());
        } else {
            assertEquals(parsed.normalizedValue(), Double.NaN, Double.MIN_VALUE);
        }
        if (Double.isFinite(derivative.getValue())
                && Double.isFinite(derivative.normalizedValue())) {
            assertToXContentEquivalent(originalBytes,
                    toXContent(parsed, xContentType, params, humanReadable), xContentType);
        }
    }
}
