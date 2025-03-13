/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform.transforms.pivot;

import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.transform.TransformConfigVersion;
import org.elasticsearch.xpack.core.transform.transforms.pivot.SingleGroupSource.Type;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.startsWith;

public class GroupConfigTests extends AbstractXContentSerializingTestCase<GroupConfig> {

    // array of illegal characters, see {@link AggregatorFactories#VALID_AGG_NAME}
    private static final char[] ILLEGAL_FIELD_NAME_CHARACTERS = { '[', ']', '>' };

    public static GroupConfig randomGroupConfig() {
        return randomGroupConfig(TransformConfigVersion.CURRENT);
    }

    public static GroupConfig randomGroupConfig(TransformConfigVersion version) {
        return randomGroupConfig(() -> randomSingleGroupSource(version));
    }

    public static GroupConfig randomGroupConfig(Supplier<SingleGroupSource> singleGroupSourceSupplier) {
        Map<String, Object> source = new LinkedHashMap<>();
        Map<String, SingleGroupSource> groups = new LinkedHashMap<>();

        // ensure that the unlikely does not happen: 2 group_by's share the same name
        Set<String> names = new HashSet<>();
        for (int i = 0; i < randomIntBetween(1, 20); ++i) {
            String targetFieldName = "group_" + randomAlphaOfLengthBetween(1, 20);
            if (names.add(targetFieldName)) {
                SingleGroupSource groupBy = singleGroupSourceSupplier.get();
                source.put(targetFieldName, Collections.singletonMap(groupBy.getType().value(), getSource(groupBy)));
                groups.put(targetFieldName, groupBy);
            }
        }

        return new GroupConfig(source, groups);
    }

    public static SingleGroupSource randomSingleGroupSource(TransformConfigVersion version) {
        Type type = randomFrom(SingleGroupSource.Type.values());
        switch (type) {
            case TERMS:
                return TermsGroupSourceTests.randomTermsGroupSource(version);
            case HISTOGRAM:
                return HistogramGroupSourceTests.randomHistogramGroupSource(version);
            case DATE_HISTOGRAM:
                return DateHistogramGroupSourceTests.randomDateHistogramGroupSource(version);
            case GEOTILE_GRID:
                return GeoTileGroupSourceTests.randomGeoTileGroupSource(version);
            default:
                fail("unknown group source type, please implement tests and add support here");
        }
        return null;
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
    protected GroupConfig mutateInstance(GroupConfig instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
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
            ValidationException validationException = groupConfig.validate(null);
            assertThat(validationException, is(notNullValue()));
            assertThat(validationException.getMessage(), containsString("pivot.groups must not be null"));
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
            ValidationException validationException = groupConfig.validate(null);
            assertThat(validationException, is(notNullValue()));
            assertThat(validationException.getMessage(), containsString("pivot.groups must not be null"));
        }

        // strict throws
        try (XContentParser parser = createParser(source)) {
            Exception e = expectThrows(ParsingException.class, () -> GroupConfig.fromXContent(parser, false));
            assertTrue(e.getMessage().startsWith("Invalid group name"));
        }
    }

    public void testInvalidGroupSourceValidation() throws IOException {
        XContentBuilder source = JsonXContent.contentBuilder()
            .startObject()
            .startObject(randomAlphaOfLengthBetween(1, 20))
            .startObject("terms")
            .endObject()
            .endObject()
            .endObject();

        // lenient, passes but reports invalid
        try (XContentParser parser = createParser(source)) {
            GroupConfig groupConfig = GroupConfig.fromXContent(parser, true);
            ValidationException validationException = groupConfig.validate(null);
            assertThat(validationException, is(notNullValue()));
            assertThat(validationException.getMessage(), containsString("Required one of fields [field, script], but none were specified"));
        }

        // strict throws
        try (XContentParser parser = createParser(source)) {
            Exception e = expectThrows(IllegalArgumentException.class, () -> GroupConfig.fromXContent(parser, false));
            assertThat(e.getMessage(), startsWith("Required one of fields [field, script], but none were specified"));
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
