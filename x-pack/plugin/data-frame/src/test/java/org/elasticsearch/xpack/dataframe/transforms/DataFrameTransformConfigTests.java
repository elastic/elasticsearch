/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.dataframe.transforms;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.xpack.dataframe.transforms.pivot.PivotConfigTests;
import org.junit.Before;

import java.io.IOException;

import static org.elasticsearch.test.TestMatchers.matchesPattern;

public class DataFrameTransformConfigTests extends AbstractSerializingDataFrameTestCase<DataFrameTransformConfig> {

    private String transformId;

    public static DataFrameTransformConfig randomDataFrameTransformConfig() {
        return new DataFrameTransformConfig(randomAlphaOfLengthBetween(1, 10), randomAlphaOfLengthBetween(1, 10),
                randomAlphaOfLengthBetween(1, 10), QueryConfigTests.randomQueryConfig(), PivotConfigTests.randomPivotConfig());
    }

    public static DataFrameTransformConfig randomInvalidDataFrameTransformConfig() {
        if (randomBoolean()) {
            return new DataFrameTransformConfig(randomAlphaOfLengthBetween(1, 10), randomAlphaOfLengthBetween(1, 10),
                    randomAlphaOfLengthBetween(1, 10), QueryConfigTests.randomInvalidQueryConfig(), PivotConfigTests.randomPivotConfig());
        } // else
        return new DataFrameTransformConfig(randomAlphaOfLengthBetween(1, 10), randomAlphaOfLengthBetween(1, 10),
                randomAlphaOfLengthBetween(1, 10), QueryConfigTests.randomQueryConfig(), PivotConfigTests.randomInvalidPivotConfig());
    }

    @Before
    public void setUpOptionalId() {
        transformId = randomAlphaOfLengthBetween(1, 10);
    }

    @Override
    protected DataFrameTransformConfig doParseInstance(XContentParser parser) throws IOException {
        if (randomBoolean()) {
            return DataFrameTransformConfig.fromXContent(parser, transformId, false);
        } else {
            return DataFrameTransformConfig.fromXContent(parser, null, false);
        }
    }

    @Override
    protected DataFrameTransformConfig createTestInstance() {
        return randomDataFrameTransformConfig();
    }

    @Override
    protected Reader<DataFrameTransformConfig> instanceReader() {
        return DataFrameTransformConfig::new;
    }

    public void testDefaultMatchAll( ) throws IOException {
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

    private DataFrameTransformConfig createDataFrameTransformConfigFromString(String json, String id) throws IOException {
        final XContentParser parser = XContentType.JSON.xContent().createParser(xContentRegistry(),
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION, json);
        return DataFrameTransformConfig.fromXContent(parser, id, false);
    }
}
