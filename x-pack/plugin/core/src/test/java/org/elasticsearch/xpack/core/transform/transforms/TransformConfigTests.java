/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.transform.transforms;

import org.elasticsearch.Version;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.xpack.core.transform.transforms.pivot.PivotConfigTests;
import org.junit.Before;

import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.test.TestMatchers.matchesPattern;
import static org.elasticsearch.xpack.core.transform.transforms.DestConfigTests.randomDestConfig;
import static org.elasticsearch.xpack.core.transform.transforms.SourceConfigTests.randomInvalidSourceConfig;
import static org.elasticsearch.xpack.core.transform.transforms.SourceConfigTests.randomSourceConfig;
import static org.hamcrest.Matchers.equalTo;

public class TransformConfigTests extends AbstractSerializingTransformTestCase<TransformConfig> {

    private String transformId;
    private boolean runWithHeaders;

    public static TransformConfig randomTransformConfigWithoutHeaders() {
        return randomTransformConfigWithoutHeaders(randomAlphaOfLengthBetween(1, 10));
    }

    public static TransformConfig randomTransformConfig() {
        return randomTransformConfig(randomAlphaOfLengthBetween(1, 10));
    }

    public static TransformConfig randomTransformConfigWithoutHeaders(String id) {
        return new TransformConfig(
            id,
            randomSourceConfig(),
            randomDestConfig(),
            randomBoolean() ? null : TimeValue.timeValueMillis(randomIntBetween(1_000, 3_600_000)),
            randomBoolean() ? null : randomSyncConfig(),
            null,
            PivotConfigTests.randomPivotConfig(),
            randomBoolean() ? null : randomAlphaOfLengthBetween(1, 1000),
            null,
            null
        );
    }

    public static TransformConfig randomTransformConfig(String id) {
        return new TransformConfig(
            id,
            randomSourceConfig(),
            randomDestConfig(),
            randomBoolean() ? null : TimeValue.timeValueMillis(randomIntBetween(1_000, 3_600_000)),
            randomBoolean() ? null : randomSyncConfig(),
            randomHeaders(),
            PivotConfigTests.randomPivotConfig(),
            randomBoolean() ? null : randomAlphaOfLengthBetween(1, 1000),
            randomBoolean() ? null : Instant.now(),
            randomBoolean() ? null : Version.CURRENT.toString()
        );
    }

    public static TransformConfig randomInvalidTransformConfig() {
        if (randomBoolean()) {
            return new TransformConfig(
                randomAlphaOfLengthBetween(1, 10),
                randomInvalidSourceConfig(),
                randomDestConfig(),
                null,
                randomBoolean() ? randomSyncConfig() : null,
                randomHeaders(),
                PivotConfigTests.randomPivotConfig(),
                randomBoolean() ? null : randomAlphaOfLengthBetween(1, 1000)
            );
        } // else
        return new TransformConfig(
            randomAlphaOfLengthBetween(1, 10),
            randomSourceConfig(),
            randomDestConfig(),
            null,
            randomBoolean() ? randomSyncConfig() : null,
            randomHeaders(),
            PivotConfigTests.randomInvalidPivotConfig(),
            randomBoolean() ? null : randomAlphaOfLengthBetween(1, 1000)
        );
    }

    public static SyncConfig randomSyncConfig() {
        return TimeSyncConfigTests.randomTimeSyncConfig();
    }

    @Before
    public void setUpOptionalId() {
        transformId = randomAlphaOfLengthBetween(1, 10);
        runWithHeaders = randomBoolean();
    }

    @Override
    protected TransformConfig doParseInstance(XContentParser parser) throws IOException {
        if (randomBoolean()) {
            return TransformConfig.fromXContent(parser, transformId, runWithHeaders);
        } else {
            return TransformConfig.fromXContent(parser, null, runWithHeaders);
        }
    }

    @Override
    protected TransformConfig createTestInstance() {
        return runWithHeaders ? randomTransformConfig(transformId) : randomTransformConfigWithoutHeaders(transformId);
    }

    @Override
    protected Reader<TransformConfig> instanceReader() {
        return TransformConfig::new;
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
            + " \"source\" : {\"index\":\"src\"},"
            + " \"dest\" : {\"index\": \"dest\"},"
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

        TransformConfig transformConfig = createTransformConfigFromString(pivotTransform, "test_match_all");
        assertNotNull(transformConfig.getSource().getQueryConfig());
        assertTrue(transformConfig.getSource().getQueryConfig().isValid());

        try (XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()) {
            XContentBuilder content = transformConfig.toXContent(xContentBuilder, ToXContent.EMPTY_PARAMS);
            String pivotTransformWithIdAndDefaults = Strings.toString(content);

            assertThat(pivotTransformWithIdAndDefaults, matchesPattern(".*\"match_all\"\\s*:\\s*\\{\\}.*"));
        }
    }

    public void testPreventHeaderInjection() {
        String pivotTransform = "{"
            + " \"headers\" : {\"key\" : \"value\" },"
            + " \"source\" : {\"index\":\"src\"},"
            + " \"dest\" : {\"index\": \"dest\"},"
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

        expectThrows(IllegalArgumentException.class, () -> createTransformConfigFromString(pivotTransform, "test_header_injection"));
    }

    public void testPreventCreateTimeInjection() {
        String pivotTransform = "{"
            + " \"create_time\" : "
            + Instant.now().toEpochMilli()
            + " },"
            + " \"source\" : {\"index\":\"src\"},"
            + " \"dest\" : {\"index\": \"dest\"},"
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

        expectThrows(IllegalArgumentException.class, () -> createTransformConfigFromString(pivotTransform, "test_createTime_injection"));
    }

    public void testPreventVersionInjection() {
        String pivotTransform = "{"
            + " \"version\" : \"7.3.0\","
            + " \"source\" : {\"index\":\"src\"},"
            + " \"dest\" : {\"index\": \"dest\"},"
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

        expectThrows(IllegalArgumentException.class, () -> createTransformConfigFromString(pivotTransform, "test_createTime_injection"));
    }

    public void testXContentForInternalStorage() throws IOException {
        TransformConfig transformConfig = randomTransformConfig();

        try (XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()) {
            XContentBuilder content = transformConfig.toXContent(xContentBuilder, getToXContentParams());
            String doc = Strings.toString(content);

            assertThat(doc, matchesPattern(".*\"doc_type\"\\s*:\\s*\"data_frame_transform_config\".*"));
        }

        try (XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()) {
            XContentBuilder content = transformConfig.toXContent(xContentBuilder, ToXContent.EMPTY_PARAMS);
            String doc = Strings.toString(content);

            assertFalse(doc.contains("doc_type"));
        }
    }

    public void testMaxLengthDescription() {
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> new TransformConfig(
                "id",
                randomSourceConfig(),
                randomDestConfig(),
                null,
                null,
                null,
                PivotConfigTests.randomPivotConfig(),
                randomAlphaOfLength(1001)
            )
        );
        assertThat(exception.getMessage(), equalTo("[description] must be less than 1000 characters in length."));
        String description = randomAlphaOfLength(1000);
        TransformConfig config = new TransformConfig(
            "id",
            randomSourceConfig(),
            randomDestConfig(),
            null,
            null,
            null,
            PivotConfigTests.randomPivotConfig(),
            description
        );
        assertThat(description, equalTo(config.getDescription()));
    }

    public void testSetIdInBody() throws IOException {
        String pivotTransform = "{"
            + " \"id\" : \"body_id\","
            + " \"source\" : {\"index\":\"src\"},"
            + " \"dest\" : {\"index\": \"dest\"},"
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

        TransformConfig transformConfig = createTransformConfigFromString(pivotTransform, "body_id");
        assertEquals("body_id", transformConfig.getId());

        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> createTransformConfigFromString(pivotTransform, "other_id")
        );

        assertEquals(
            "Inconsistent id; 'body_id' specified in the body differs from 'other_id' specified as a URL argument",
            ex.getCause().getMessage()
        );
    }

    private TransformConfig createTransformConfigFromString(String json, String id) throws IOException {
        final XContentParser parser = XContentType.JSON.xContent()
            .createParser(xContentRegistry(), DeprecationHandler.THROW_UNSUPPORTED_OPERATION, json);
        return TransformConfig.fromXContent(parser, id, false);
    }
}
