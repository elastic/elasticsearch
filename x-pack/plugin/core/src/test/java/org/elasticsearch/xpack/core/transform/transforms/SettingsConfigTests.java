/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform.transforms;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.transform.AbstractSerializingTransformTestCase;
import org.elasticsearch.xpack.core.watcher.watch.Payload.XContent;
import org.junit.Before;

import java.io.IOException;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class SettingsConfigTests extends AbstractSerializingTransformTestCase<SettingsConfig> {

    private boolean lenient;

    public static SettingsConfig randomSettingsConfig() {
        return new SettingsConfig(
            randomBoolean() ? null : randomIntBetween(10, 10_000),
            randomBoolean() ? null : randomFloat(),
            randomBoolean() ? null : randomIntBetween(0, 1)
        );
    }

    public static SettingsConfig randomNonEmptySettingsConfig() {
        return new SettingsConfig(randomIntBetween(10, 10_000), randomFloat(), randomIntBetween(0, 1));
    }

    @Before
    public void setRandomFeatures() {
        lenient = randomBoolean();
    }

    @Override
    protected SettingsConfig doParseInstance(XContentParser parser) throws IOException {
        return SettingsConfig.fromXContent(parser, lenient);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return lenient;
    }

    @Override
    protected SettingsConfig createTestInstance() {
        return randomSettingsConfig();
    }

    @Override
    protected Reader<SettingsConfig> instanceReader() {
        return SettingsConfig::new;
    }

    public void testExplicitNullParsing() throws IOException {

        // explicit null
        assertThat(fromString("{\"max_page_search_size\" : null}").getMaxPageSearchSize(), equalTo(-1));
        // not set
        assertNull(fromString("{}").getMaxPageSearchSize());

        assertThat(fromString("{\"docs_per_second\" : null}").getDocsPerSecond(), equalTo(-1F));
        assertNull(fromString("{}").getDocsPerSecond());

        assertThat(fromString("{\"dates_as_epoch_millis\" : null}").getDatesAsEpochMillisForUpdate(), equalTo(-1));
        assertNull(fromString("{}").getDatesAsEpochMillisForUpdate());
    }

    public void testUpdateUsingBuilder() throws IOException {
        SettingsConfig config = fromString("{\"max_page_search_size\" : 10000, \"docs_per_second\" :42, \"dates_as_epoch_millis\": true}");

        SettingsConfig.Builder builder = new SettingsConfig.Builder(config);
        builder.update(fromString("{\"max_page_search_size\" : 100}"));

        assertThat(builder.build().getMaxPageSearchSize(), equalTo(100));
        assertThat(builder.build().getDocsPerSecond(), equalTo(42F));
        assertThat(builder.build().getDatesAsEpochMillisForUpdate(), equalTo(1));

        builder.update(fromString("{\"max_page_search_size\" : null}"));
        assertNull(builder.build().getMaxPageSearchSize());
        assertThat(builder.build().getDocsPerSecond(), equalTo(42F));
        assertThat(builder.build().getDatesAsEpochMillisForUpdate(), equalTo(1));

        builder.update(fromString("{\"max_page_search_size\" : 77, \"docs_per_second\" :null, \"dates_as_epoch_millis\": null}"));
        assertThat(builder.build().getMaxPageSearchSize(), equalTo(77));
        assertNull(builder.build().getDocsPerSecond());
        assertNull(builder.build().getDatesAsEpochMillisForUpdate());
    }

    public void testOmmitDefaultsOnWriteParser() throws IOException {
        // test that an explicit null is handled differently than not set
        SettingsConfig config = fromString("{\"max_page_search_size\" : null}");
        assertThat(config.getMaxPageSearchSize(), equalTo(-1));

        Map<String, Object> settingsAsMap = xContentToMap(config);
        assertTrue(settingsAsMap.isEmpty());

        SettingsConfig emptyConfig = fromString("{}");
        assertNull(emptyConfig.getMaxPageSearchSize());

        settingsAsMap = xContentToMap(emptyConfig);
        assertTrue(settingsAsMap.isEmpty());

        config = fromString("{\"docs_per_second\" : null}");
        assertThat(config.getDocsPerSecond(), equalTo(-1F));

        settingsAsMap = xContentToMap(config);
        assertTrue(settingsAsMap.isEmpty());

        config = fromString("{\"dates_as_epoch_millis\" : null}");
        assertThat(config.getDatesAsEpochMillisForUpdate(), equalTo(-1));

        settingsAsMap = xContentToMap(config);
        assertTrue(settingsAsMap.isEmpty());
    }

    public void testOmmitDefaultsOnWriteBuilder() throws IOException {
        // test that an explicit null is handled differently than not set
        SettingsConfig config = new SettingsConfig.Builder().setMaxPageSearchSize(null).build();
        assertThat(config.getMaxPageSearchSize(), equalTo(-1));

        Map<String, Object> settingsAsMap = xContentToMap(config);
        assertTrue(settingsAsMap.isEmpty());

        SettingsConfig emptyConfig = new SettingsConfig.Builder().build();
        assertNull(emptyConfig.getMaxPageSearchSize());

        settingsAsMap = xContentToMap(emptyConfig);
        assertTrue(settingsAsMap.isEmpty());

        config = new SettingsConfig.Builder().setRequestsPerSecond(null).build();
        assertThat(config.getDocsPerSecond(), equalTo(-1F));

        settingsAsMap = xContentToMap(config);
        assertTrue(settingsAsMap.isEmpty());

        config = new SettingsConfig.Builder().setDatesAsEpochMillis(null).build();
        assertThat(config.getDatesAsEpochMillisForUpdate(), equalTo(-1));

        settingsAsMap = xContentToMap(config);
        assertTrue(settingsAsMap.isEmpty());
    }

    private Map<String, Object> xContentToMap(ToXContent xcontent) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        xcontent.toXContent(builder, XContent.EMPTY_PARAMS);
        XContentParser parser = XContentType.JSON.xContent()
            .createParser(xContentRegistry(), LoggingDeprecationHandler.INSTANCE, BytesReference.bytes(builder).streamInput());
        return parser.map();
    }

    private SettingsConfig fromString(String source) throws IOException {
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, source)) {
            return SettingsConfig.fromXContent(parser, false);
        }
    }
}
