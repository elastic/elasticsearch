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

package org.elasticsearch.search.aggregations;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.action.search.RestSearchAction;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertToXContentEquivalent;
import static org.hamcrest.CoreMatchers.instanceOf;

public class ParsedAggregationTests extends ESTestCase {

    //TODO maybe this test will no longer be needed once we have real tests for ParsedAggregation subclasses
    public void testParse() throws IOException {
        String name = randomAlphaOfLengthBetween(5, 10);
        Map<String, Object> meta = null;
        if (randomBoolean()) {
            int numMetas = randomIntBetween(0, 5);
            meta = new HashMap<>(numMetas);
            for (int i = 0; i < numMetas; i++) {
                meta.put(randomAlphaOfLengthBetween(3, 10), randomAlphaOfLengthBetween(3, 10));
            }
        }
        TestInternalAggregation testAgg = new TestInternalAggregation(name, meta);
        XContentType xContentType = randomFrom(XContentType.values());
        ToXContent.MapParams params = new ToXContent.MapParams(Collections.singletonMap(RestSearchAction.TYPED_KEYS_PARAM, "true"));
        BytesReference bytesAgg = XContentHelper.toXContent(testAgg, xContentType, params, randomBoolean());
        try (XContentParser parser = createParser(xContentType.xContent(), bytesAgg)) {
            parser.nextToken();
            assert parser.currentToken() == XContentParser.Token.START_OBJECT;
            parser.nextToken();
            assert parser.currentToken() == XContentParser.Token.FIELD_NAME;
            String currentName = parser.currentName();
            int i = currentName.indexOf(InternalAggregation.TYPED_KEYS_DELIMITER);
            String aggType = currentName.substring(0, i);
            String aggName = currentName.substring(i + 1);
            Aggregation parsedAgg = parser.namedObject(Aggregation.class, aggType, aggName);
            assertThat(parsedAgg, instanceOf(TestParsedAggregation.class));
            assertEquals(testAgg.getName(), parsedAgg.getName());
            assertEquals(testAgg.getMetaData(), parsedAgg.getMetaData());
            if (meta != null) {
                expectThrows(UnsupportedOperationException.class, () -> parsedAgg.getMetaData().put("test", "test"));
            }
            BytesReference finalAgg = XContentHelper.toXContent((ToXContent) parsedAgg, xContentType, randomBoolean());
            assertToXContentEquivalent(bytesAgg, finalAgg, xContentType);
        }
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        NamedXContentRegistry.Entry entry = new NamedXContentRegistry.Entry(Aggregation.class, new ParseField("type"),
                (parser, name) -> TestParsedAggregation.fromXContent(parser, (String)name));
        return new NamedXContentRegistry(Collections.singletonList(entry));
    }

    private static class TestParsedAggregation extends ParsedAggregation {
        private static ObjectParser<TestParsedAggregation, Void> PARSER = new ObjectParser<>("testAggParser", TestParsedAggregation::new);

        static {
            ParsedAggregation.declareAggregationFields(PARSER);
        }

        @Override
        protected String getType() {
            return "type";
        }

        @Override
        protected XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
            return builder;
        }

        public static TestParsedAggregation fromXContent(XContentParser parser, String name) throws IOException {
            TestParsedAggregation parsedAgg = PARSER.parse(parser, null);
            parsedAgg.setName(name);
            return parsedAgg;
        }
    }

    private static class TestInternalAggregation extends InternalAggregation {

        private TestInternalAggregation(String name, Map<String, Object> metaData) {
            super(name, Collections.emptyList(), metaData);
        }

        @Override
        public String getWriteableName() {
            throw new UnsupportedOperationException();
        }

        @Override
        protected String getType() {
            return "type";
        }

        @Override
        protected void doWriteTo(StreamOutput out) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public InternalAggregation doReduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Object getProperty(List<String> path) {
            throw new UnsupportedOperationException();
        }

        @Override
        public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
            return builder;
        }
    }
}
