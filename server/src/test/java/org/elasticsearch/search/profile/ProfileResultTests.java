/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.profile;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

public class ProfileResultTests extends AbstractSerializingTestCase<ProfileResult> {
    public static final Predicate<String> RANDOM_FIELDS_EXCLUDE_FILTER = s -> s.endsWith(ProfileResult.BREAKDOWN.getPreferredName())
        || s.endsWith(ProfileResult.DEBUG.getPreferredName());

    public static ProfileResult createTestItem(int depth) {
        String type = randomAlphaOfLengthBetween(5, 10);
        String description = randomAlphaOfLengthBetween(5, 10);
        int breakdownsSize = randomIntBetween(0, 5);
        Map<String, Long> breakdown = new HashMap<>(breakdownsSize);
        while (breakdown.size() < breakdownsSize) {
            long value = randomNonNegativeLong();
            if (randomBoolean()) {
                // also often use "small" values in tests
                value = value % 10000;
            }
            breakdown.put(randomAlphaOfLengthBetween(5, 10), value);
        }
        int debugSize = randomIntBetween(0, 5);
        Map<String, Object> debug = new HashMap<>(debugSize);
        while (debug.size() < debugSize) {
            debug.put(randomAlphaOfLength(5), randomAlphaOfLength(4));
        }
        int childrenSize = depth > 0 ? randomIntBetween(0, 1) : 0;
        List<ProfileResult> children = new ArrayList<>(childrenSize);
        for (int i = 0; i < childrenSize; i++) {
            children.add(createTestItem(depth - 1));
        }
        return new ProfileResult(type, description, breakdown, debug, randomNonNegativeLong(), children);
    }

    @Override
    protected ProfileResult createTestInstance() {
        return createTestItem(2);
    }

    @Override
    protected Reader<ProfileResult> instanceReader() {
        return ProfileResult::new;
    }

    @Override
    protected ProfileResult doParseInstance(XContentParser parser) throws IOException {
        return ProfileResult.fromXContent(parser);
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        return RANDOM_FIELDS_EXCLUDE_FILTER;
    }

    public void testToXContent() throws IOException {
        List<ProfileResult> children = new ArrayList<>();
        children.add(new ProfileResult("child1", "desc1", Map.of("key1", 100L), Map.of(), 100L, List.of()));
        children.add(new ProfileResult("child2", "desc2", Map.of("key1", 123356L), Map.of(), 123356L, List.of()));
        Map<String, Long> breakdown = new LinkedHashMap<>();
        breakdown.put("key1", 123456L);
        breakdown.put("stuff", 10000L);
        Map<String, Object> debug = new LinkedHashMap<>();
        debug.put("a", "foo");
        debug.put("b", "bar");
        ProfileResult result = new ProfileResult("someType", "some description", breakdown, debug, 223456L, children);
        XContentBuilder builder = XContentFactory.jsonBuilder().prettyPrint();
        result.toXContent(builder, ToXContent.EMPTY_PARAMS);
        assertEquals("{\n" +
                "  \"type\" : \"someType\",\n" +
                "  \"description\" : \"some description\",\n" +
                "  \"time_in_nanos\" : 223456,\n" +
                "  \"breakdown\" : {\n" +
                "    \"key1\" : 123456,\n" +
                "    \"stuff\" : 10000\n" +
                "  },\n" +
                "  \"debug\" : {\n" +
                "    \"a\" : \"foo\",\n" +
                "    \"b\" : \"bar\"\n" +
                "  },\n" +
                "  \"children\" : [\n" +
                "    {\n" +
                "      \"type\" : \"child1\",\n" +
                "      \"description\" : \"desc1\",\n" +
                "      \"time_in_nanos\" : 100,\n" +
                "      \"breakdown\" : {\n" +
                "        \"key1\" : 100\n" +
                "      }\n" +
                "    },\n" +
                "    {\n" +
                "      \"type\" : \"child2\",\n" +
                "      \"description\" : \"desc2\",\n" +
                "      \"time_in_nanos\" : 123356,\n" +
                "      \"breakdown\" : {\n" +
                "        \"key1\" : 123356\n" +
                "      }\n" +
                "    }\n" +
                "  ]\n" +
          "}", Strings.toString(builder));

        builder = XContentFactory.jsonBuilder().prettyPrint().humanReadable(true);
        result.toXContent(builder, ToXContent.EMPTY_PARAMS);
        assertEquals("{\n" +
                "  \"type\" : \"someType\",\n" +
                "  \"description\" : \"some description\",\n" +
                "  \"time\" : \"223.4micros\",\n" +
                "  \"time_in_nanos\" : 223456,\n" +
                "  \"breakdown\" : {\n" +
                "    \"key1\" : 123456,\n" +
                "    \"stuff\" : 10000\n" +
                "  },\n" +
                "  \"debug\" : {\n" +
                "    \"a\" : \"foo\",\n" +
                "    \"b\" : \"bar\"\n" +
                "  },\n" +
                "  \"children\" : [\n" +
                "    {\n" +
                "      \"type\" : \"child1\",\n" +
                "      \"description\" : \"desc1\",\n" +
                "      \"time\" : \"100nanos\",\n" +
                "      \"time_in_nanos\" : 100,\n" +
                "      \"breakdown\" : {\n" +
                "        \"key1\" : 100\n" +
                "      }\n" +
                "    },\n" +
                "    {\n" +
                "      \"type\" : \"child2\",\n" +
                "      \"description\" : \"desc2\",\n" +
                "      \"time\" : \"123.3micros\",\n" +
                "      \"time_in_nanos\" : 123356,\n" +
                "      \"breakdown\" : {\n" +
                "        \"key1\" : 123356\n" +
                "      }\n" +
                "    }\n" +
                "  ]\n" +
          "}", Strings.toString(builder));

        result = new ProfileResult("profileName", "some description", Map.of("key1", 12345678L), Map.of(), 12345678L, List.of());
        builder = XContentFactory.jsonBuilder().prettyPrint().humanReadable(true);
        result.toXContent(builder, ToXContent.EMPTY_PARAMS);
        assertEquals("{\n" +
                "  \"type\" : \"profileName\",\n" +
                "  \"description\" : \"some description\",\n" +
                "  \"time\" : \"12.3ms\",\n" +
                "  \"time_in_nanos\" : 12345678,\n" +
                "  \"breakdown\" : {\n" +
                "    \"key1\" : 12345678\n" +
                "  }\n" +
              "}", Strings.toString(builder));

        result = new ProfileResult("profileName", "some description", Map.of("key1", 1234567890L), Map.of(), 1234567890L, List.of());
        builder = XContentFactory.jsonBuilder().prettyPrint().humanReadable(true);
        result.toXContent(builder, ToXContent.EMPTY_PARAMS);
        assertEquals("{\n" +
                "  \"type\" : \"profileName\",\n" +
                "  \"description\" : \"some description\",\n" +
                "  \"time\" : \"1.2s\",\n" +
                "  \"time_in_nanos\" : 1234567890,\n" +
                "  \"breakdown\" : {\n" +
                "    \"key1\" : 1234567890\n" +
                "  }\n" +
              "}", Strings.toString(builder));
    }
}
