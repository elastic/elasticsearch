/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.profile.query;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

public class CollectorResultTests extends AbstractXContentSerializingTestCase<CollectorResult> {
    public static CollectorResult createTestItem(int depth) {
        String name = randomAlphaOfLengthBetween(5, 10);
        String reason = randomAlphaOfLengthBetween(5, 10);
        long time = randomNonNegativeLong();
        if (randomBoolean()) {
            // also often use relatively "small" values, otherwise we will mostly test huge longs
            time = time % 100000;
        }
        int size = randomIntBetween(0, 5);
        List<CollectorResult> children = new ArrayList<>(size);
        if (depth > 0) {
            for (int i = 0; i < size; i++) {
                children.add(createTestItem(depth - 1));
            }
        }
        return new CollectorResult(name, reason, time, children);
    }

    @Override
    protected CollectorResult createTestInstance() {
        return createTestItem(1);
    }

    @Override
    protected CollectorResult mutateInstance(CollectorResult instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    @Override
    protected CollectorResult doParseInstance(XContentParser parser) throws IOException {
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
        CollectorResult result = CollectorResult.fromXContent(parser);
        ensureExpectedToken(null, parser.nextToken(), parser);
        return result;
    }

    @Override
    protected Reader<CollectorResult> instanceReader() {
        return CollectorResult::new;
    }

    public void testToXContent() throws IOException {
        List<CollectorResult> children = new ArrayList<>();
        children.add(new CollectorResult("child1", "reason1", 100L, Collections.emptyList()));
        children.add(new CollectorResult("child2", "reason1", 123356L, Collections.emptyList()));
        CollectorResult result = new CollectorResult("collectorName", "some reason", 123456L, children);
        XContentBuilder builder = XContentFactory.jsonBuilder().prettyPrint();
        result.toXContent(builder, ToXContent.EMPTY_PARAMS);
        assertEquals("""
            {
              "name" : "collectorName",
              "reason" : "some reason",
              "time_in_nanos" : 123456,
              "children" : [
                {
                  "name" : "child1",
                  "reason" : "reason1",
                  "time_in_nanos" : 100
                },
                {
                  "name" : "child2",
                  "reason" : "reason1",
                  "time_in_nanos" : 123356
                }
              ]
            }""", Strings.toString(builder));

        builder = XContentFactory.jsonBuilder().prettyPrint().humanReadable(true);
        result.toXContent(builder, ToXContent.EMPTY_PARAMS);
        assertEquals("""
            {
              "name" : "collectorName",
              "reason" : "some reason",
              "time" : "123.4micros",
              "time_in_nanos" : 123456,
              "children" : [
                {
                  "name" : "child1",
                  "reason" : "reason1",
                  "time" : "100nanos",
                  "time_in_nanos" : 100
                },
                {
                  "name" : "child2",
                  "reason" : "reason1",
                  "time" : "123.3micros",
                  "time_in_nanos" : 123356
                }
              ]
            }""", Strings.toString(builder));

        result = new CollectorResult("collectorName", "some reason", 12345678L, Collections.emptyList());
        builder = XContentFactory.jsonBuilder().prettyPrint().humanReadable(true);
        result.toXContent(builder, ToXContent.EMPTY_PARAMS);
        assertEquals("""
            {
              "name" : "collectorName",
              "reason" : "some reason",
              "time" : "12.3ms",
              "time_in_nanos" : 12345678
            }""", Strings.toString(builder));

        result = new CollectorResult("collectorName", "some reason", 1234567890L, Collections.emptyList());
        builder = XContentFactory.jsonBuilder().prettyPrint().humanReadable(true);
        result.toXContent(builder, ToXContent.EMPTY_PARAMS);
        assertEquals("""
            {
              "name" : "collectorName",
              "reason" : "some reason",
              "time" : "1.2s",
              "time_in_nanos" : 1234567890
            }""", Strings.toString(builder));
    }
}
