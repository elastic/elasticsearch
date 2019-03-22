/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.dataframe.transforms;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContent.Params;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.xpack.core.dataframe.DataFrameField;
import org.elasticsearch.xpack.core.dataframe.transforms.pivot.PivotConfigTests;
import org.junit.Before;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.test.TestMatchers.matchesPattern;

public class DataFrameTransformConfigTests extends AbstractSerializingDataFrameTestCase<DataFrameTransformConfig> {

    private static Params TO_XCONTENT_PARAMS = new ToXContent.MapParams(
            Collections.singletonMap(DataFrameField.FOR_INTERNAL_STORAGE, "true"));

    private String transformId;
    private boolean runWithHeaders;

    public static DataFrameTransformConfig randomDataFrameTransformConfigWithoutHeaders() {
        return new DataFrameTransformConfig(randomAlphaOfLengthBetween(1, 10), randomAlphaOfLengthBetween(1, 10),
                randomAlphaOfLengthBetween(1, 10), null, QueryConfigTests.randomQueryConfig(), PivotConfigTests.randomPivotConfig());
    }

    public static DataFrameTransformConfig randomDataFrameTransformConfig() {
        return new DataFrameTransformConfig(randomAlphaOfLengthBetween(1, 10), randomAlphaOfLengthBetween(1, 10),
                randomAlphaOfLengthBetween(1, 10), randomHeaders(), QueryConfigTests.randomQueryConfig(),
                PivotConfigTests.randomPivotConfig());
    }

    public static DataFrameTransformConfig randomDataFrameTransformConfigWithoutHeaders(String id) {
        return new DataFrameTransformConfig(id, randomAlphaOfLengthBetween(1, 10), randomAlphaOfLengthBetween(1, 10), null,
                QueryConfigTests.randomQueryConfig(), PivotConfigTests.randomPivotConfig());
    }

    public static DataFrameTransformConfig randomDataFrameTransformConfig(String id) {
        return new DataFrameTransformConfig(id, randomAlphaOfLengthBetween(1, 10), randomAlphaOfLengthBetween(1, 10), randomHeaders(),
                QueryConfigTests.randomQueryConfig(), PivotConfigTests.randomPivotConfig());
    }

    public static DataFrameTransformConfig randomInvalidDataFrameTransformConfig() {
        if (randomBoolean()) {
            return new DataFrameTransformConfig(randomAlphaOfLengthBetween(1, 10), randomAlphaOfLengthBetween(1, 10),
                    randomAlphaOfLengthBetween(1, 10), randomHeaders(), QueryConfigTests.randomInvalidQueryConfig(),
                    PivotConfigTests.randomPivotConfig());
        } // else
        return new DataFrameTransformConfig(randomAlphaOfLengthBetween(1, 10), randomAlphaOfLengthBetween(1, 10),
                randomAlphaOfLengthBetween(1, 10), randomHeaders(), QueryConfigTests.randomQueryConfig(),
                PivotConfigTests.randomInvalidPivotConfig());
    }

    @Before
    public void setUpOptionalId() {
        transformId = randomAlphaOfLengthBetween(1, 10);
        runWithHeaders = randomBoolean();
    }

    @Override
    protected DataFrameTransformConfig doParseInstance(XContentParser parser) throws IOException {
        if (randomBoolean()) {
            return DataFrameTransformConfig.fromXContent(parser, transformId, runWithHeaders);
        } else {
            return DataFrameTransformConfig.fromXContent(parser, null, runWithHeaders);
        }
    }

    @Override
    protected DataFrameTransformConfig createTestInstance() {
        return runWithHeaders ? randomDataFrameTransformConfig(transformId) : randomDataFrameTransformConfigWithoutHeaders(transformId);
    }

    @Override
    protected Reader<DataFrameTransformConfig> instanceReader() {
        return DataFrameTransformConfig::new;
    }

    @Override
    protected ToXContent.Params getToXContentParams() {
        return TO_XCONTENT_PARAMS;
    }

    private static Map<String, String> randomHeaders() {
        Map<String, String> headers = new HashMap<>(1);
        headers.put("key", "value");

        return headers;
    }

    public void testDefaultMatchAll() throws IOException {
        String pivotTransform = "{"
                + " \"source\" : \"src\","
                + " \"dest\" : \"dest\","
                + " \"pivot\" : {"
                + " \"group_by\": {"
                + "   \"id\": {"
                + "     \"terms\": {"
                + "       \"field\": \"id\""
                + "} } },"
                + " \"aggs\": {"
                + "   \"avg\": {"
                + "     \"avg\": {"
                + "       \"field\": \"points\""
                + "} } } } }";

        DataFrameTransformConfig dataFrameTransformConfig = createDataFrameTransformConfigFromString(pivotTransform, "test_match_all");
        assertNotNull(dataFrameTransformConfig.getQueryConfig());
        assertTrue(dataFrameTransformConfig.getQueryConfig().isValid());

        try (XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()) {
            XContentBuilder content = dataFrameTransformConfig.toXContent(xContentBuilder, ToXContent.EMPTY_PARAMS);
            String pivotTransformWithIdAndDefaults = Strings.toString(content);

            assertThat(pivotTransformWithIdAndDefaults, matchesPattern(".*\"match_all\"\\s*:\\s*\\{\\}.*"));
        }
    }

    public void testPreventHeaderInjection() throws IOException {
        String pivotTransform = "{"
                + " \"headers\" : {\"key\" : \"value\" },"
                + " \"source\" : \"src\","
                + " \"dest\" : \"dest\","
                + " \"pivot\" : {"
                + " \"group_by\": {"
                + "   \"id\": {"
                + "     \"terms\": {"
                + "       \"field\": \"id\""
                + "} } },"
                + " \"aggs\": {"
                + "   \"avg\": {"
                + "     \"avg\": {"
                + "       \"field\": \"points\""
                + "} } } } }";

        expectThrows(IllegalArgumentException.class,
                () -> createDataFrameTransformConfigFromString(pivotTransform, "test_header_injection"));
    }

    public void testXContentForInternalStorage() throws IOException {
        DataFrameTransformConfig dataFrameTransformConfig = randomDataFrameTransformConfig();

        try (XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()) {
            XContentBuilder content = dataFrameTransformConfig.toXContent(xContentBuilder, getToXContentParams());
            String doc = Strings.toString(content);

            assertThat(doc, matchesPattern(".*\"doc_type\"\\s*:\\s*\"data_frame_transform_config\".*"));
        }

        try (XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()) {
            XContentBuilder content = dataFrameTransformConfig.toXContent(xContentBuilder, ToXContent.EMPTY_PARAMS);
            String doc = Strings.toString(content);

            assertFalse(doc.contains("doc_type"));
        }
    }

    public void testSetIdInBody() throws IOException {
        String pivotTransform = "{"
                + " \"id\" : \"body_id\","
                + " \"source\" : \"src\","
                + " \"dest\" : \"dest\","
                + " \"pivot\" : {"
                + " \"group_by\": {"
                + "   \"id\": {"
                + "     \"terms\": {"
                + "       \"field\": \"id\""
                + "} } },"
                + " \"aggs\": {"
                + "   \"avg\": {"
                + "     \"avg\": {"
                + "       \"field\": \"points\""
                + "} } } } }";

        DataFrameTransformConfig dataFrameTransformConfig = createDataFrameTransformConfigFromString(pivotTransform, "body_id");
        assertEquals("body_id", dataFrameTransformConfig.getId());

        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class,
                () -> createDataFrameTransformConfigFromString(pivotTransform, "other_id"));

        assertEquals("Inconsistent id; 'body_id' specified in the body differs from 'other_id' specified as a URL argument",
                ex.getCause().getMessage());
    }

    private DataFrameTransformConfig createDataFrameTransformConfigFromString(String json, String id) throws IOException {
        final XContentParser parser = XContentType.JSON.xContent().createParser(xContentRegistry(),
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION, json);
        return DataFrameTransformConfig.fromXContent(parser, id, false);
    }
}
