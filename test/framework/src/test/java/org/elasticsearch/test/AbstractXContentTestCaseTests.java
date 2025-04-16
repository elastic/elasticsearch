/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test;

import com.carrotsearch.randomizedtesting.RandomizedContext;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class AbstractXContentTestCaseTests extends ESTestCase {

    public void testInsertRandomFieldsAndShuffle() throws Exception {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        {
            builder.field("field", 1);
        }
        builder.endObject();
        BytesReference insertRandomFieldsAndShuffle = RandomizedContext.current()
            .runWithPrivateRandomness(
                1,
                () -> AbstractXContentTestCase.insertRandomFieldsAndShuffle(
                    BytesReference.bytes(builder),
                    XContentType.JSON,
                    true,
                    new String[] {},
                    null,
                    this::createParser
                )
            );
        try (XContentParser parser = createParser(XContentType.JSON.xContent(), insertRandomFieldsAndShuffle)) {
            Map<String, Object> mapOrdered = parser.mapOrdered();
            assertThat(mapOrdered.size(), equalTo(2));
            assertThat(mapOrdered.keySet().iterator().next(), not(equalTo("field")));
        }
    }

    private record TestToXContent(String field, String value) implements ToXContentFragment {

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.field(field, value);
        }
    }

    public void testYamlXContentRoundtripSanitization() throws Exception {
        var test = new AbstractXContentTestCase<TestToXContent>() {

            @Override
            protected TestToXContent createTestInstance() {
                // we need to randomly create both a "problematic" and an okay version in order to ensure that the sanitization code
                // can draw at least one okay version if polled often enough
                return randomBoolean() ? new TestToXContent("a\u0085b", "def") : new TestToXContent("a b", "def");
            }

            @Override
            protected TestToXContent doParseInstance(XContentParser parser) throws IOException {
                assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
                assertEquals(XContentParser.Token.FIELD_NAME, parser.nextToken());
                String name = parser.currentName();
                assertEquals(XContentParser.Token.VALUE_STRING, parser.nextToken());
                String value = parser.text();
                assertEquals(XContentParser.Token.END_OBJECT, parser.nextToken());
                return new TestToXContent(name, value);
            };

            @Override
            protected boolean supportsUnknownFields() {
                return false;
            }
        };
        // testFromXContent runs 20 repetitions, enough to hit a YAML xcontent version very likely
        test.testFromXContent();
    }
}
