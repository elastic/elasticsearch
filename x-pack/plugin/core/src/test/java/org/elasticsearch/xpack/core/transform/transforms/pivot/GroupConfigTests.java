/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.transform.transforms.pivot;

import org.elasticsearch.Version;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xpack.core.transform.transforms.pivot.SingleGroupSource.Type;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

public class GroupConfigTests extends AbstractSerializingTestCase<GroupConfig> {

    // array of illegal characters, see {@link AggregatorFactories#VALID_AGG_NAME}
    private static final char[] ILLEGAL_FIELD_NAME_CHARACTERS = { '[', ']', '>' };

    public static GroupConfig randomGroupConfig() {
        return randomGroupConfig(Version.CURRENT);
    }

    public static GroupConfig randomGroupConfig(Version version) {
        Map<String, Object> source = new LinkedHashMap<>();
        Map<String, SingleGroupSource> groups = new LinkedHashMap<>();

        // ensure that the unlikely does not happen: 2 group_by's share the same name
        Set<String> names = new HashSet<>();
        for (int i = 0; i < randomIntBetween(1, 20); ++i) {
            String targetFieldName = randomAlphaOfLengthBetween(1, 20);
            if (names.add(targetFieldName)) {
                SingleGroupSource groupBy = null;
                Type type = randomFrom(SingleGroupSource.Type.values());
                switch (type) {
                    case TERMS:
                        groupBy = TermsGroupSourceTests.randomTermsGroupSource(version);
                        break;
                    case HISTOGRAM:
                        groupBy = HistogramGroupSourceTests.randomHistogramGroupSource(version);
                        break;
                    case DATE_HISTOGRAM:
                        groupBy = DateHistogramGroupSourceTests.randomDateHistogramGroupSource(version);
                        break;
                    case GEOTILE_GRID:
                        groupBy = GeoTileGroupSourceTests.randomGeoTileGroupSource(version);
                        break;
                    default:
                        fail("unknown group source type, please implement tests and add support here");
                }
                source.put(targetFieldName, Collections.singletonMap(type.value(), getSource(groupBy)));
                groups.put(targetFieldName, groupBy);
            }
        }

        return new GroupConfig(source, groups);
    }

    @Override
    protected GroupConfig doParseInstance(XContentParser parser) throws IOException {
        return GroupConfig.fromXContent(parser, false);
    }

    @Override
    protected GroupConfig createTestInstance() {
        return randomGroupConfig();
    }

    @Override
    protected Reader<GroupConfig> instanceReader() {
        return GroupConfig::new;
    }

    public void testEmptyGroupBy() throws IOException {
        String source = "{}";

        // lenient, passes but reports invalid
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, source)) {
            GroupConfig groupConfig = GroupConfig.fromXContent(parser, true);
            assertFalse(groupConfig.isValid());
        }

        // strict throws
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, source)) {
            expectThrows(IllegalArgumentException.class, () -> GroupConfig.fromXContent(parser, false));
        }
    }

    public void testInvalidGroupByNames() throws IOException {

        String invalidName = randomAlphaOfLengthBetween(0, 5) + ILLEGAL_FIELD_NAME_CHARACTERS[randomIntBetween(
            0,
            ILLEGAL_FIELD_NAME_CHARACTERS.length - 1
        )] + randomAlphaOfLengthBetween(0, 5);

        XContentBuilder source = JsonXContent.contentBuilder()
            .startObject()
            .startObject(invalidName)
            .startObject("terms")
            .field("field", "user")
            .endObject()
            .endObject()
            .endObject();

        // lenient, passes but reports invalid
        try (XContentParser parser = createParser(source)) {
            GroupConfig groupConfig = GroupConfig.fromXContent(parser, true);
            assertFalse(groupConfig.isValid());
        }

        // strict throws
        try (XContentParser parser = createParser(source)) {
            Exception e = expectThrows(ParsingException.class, () -> GroupConfig.fromXContent(parser, false));
            assertTrue(e.getMessage().startsWith("Invalid group name"));
        }
    }

    private static Map<String, Object> getSource(SingleGroupSource groupSource) {
        try (XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()) {
            XContentBuilder content = groupSource.toXContent(xContentBuilder, ToXContent.EMPTY_PARAMS);
            return XContentHelper.convertToMap(BytesReference.bytes(content), true, XContentType.JSON).v2();
        } catch (IOException e) {
            // should not happen
            fail("failed to create random single group source");
        }
        return null;
    }
}
