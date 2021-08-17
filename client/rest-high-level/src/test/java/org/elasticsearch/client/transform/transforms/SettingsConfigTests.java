/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.transform.transforms;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.AbstractXContentTestCase;
import org.elasticsearch.xpack.core.watcher.watch.Payload.XContent;

import java.io.IOException;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class SettingsConfigTests extends AbstractXContentTestCase<SettingsConfig> {

    public static SettingsConfig randomSettingsConfig() {
        return new SettingsConfig(
            randomBoolean() ? null : randomIntBetween(10, 10_000),
            randomBoolean() ? null : randomFloat(),
            randomBoolean() ? null : randomIntBetween(-1, 1),
            randomBoolean() ? null : randomIntBetween(-1, 1)
        );
    }

    @Override
    protected SettingsConfig createTestInstance() {
        return randomSettingsConfig();
    }

    @Override
    protected SettingsConfig doParseInstance(XContentParser parser) throws IOException {
        return SettingsConfig.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    public void testExplicitNullOnWriteParser() throws IOException {
        // test that an explicit null is handled differently than not set
        SettingsConfig config = fromString("{\"max_page_search_size\" : null}");
        assertThat(config.getMaxPageSearchSize(), equalTo(-1));

        Map<String, Object> settingsAsMap = xContentToMap(config);
        assertNull(settingsAsMap.getOrDefault("max_page_search_size", "not_set"));
        assertThat(settingsAsMap.getOrDefault("docs_per_second", "not_set"), equalTo("not_set"));

        SettingsConfig emptyConfig = fromString("{}");
        assertNull(emptyConfig.getMaxPageSearchSize());
        assertNull(emptyConfig.getDatesAsEpochMillis());

        settingsAsMap = xContentToMap(emptyConfig);
        assertTrue(settingsAsMap.isEmpty());

        config = fromString("{\"docs_per_second\" : null}");
        assertThat(config.getDocsPerSecond(), equalTo(-1F));

        settingsAsMap = xContentToMap(config);
        assertThat(settingsAsMap.getOrDefault("max_page_search_size", "not_set"), equalTo("not_set"));
        assertNull(settingsAsMap.getOrDefault("docs_per_second", "not_set"));
        assertThat(settingsAsMap.getOrDefault("dates_as_epoch_millis", "not_set"), equalTo("not_set"));
        assertThat(settingsAsMap.getOrDefault("interim_results", "not_set"), equalTo("not_set"));

        config = fromString("{\"dates_as_epoch_millis\" : null}");
        assertFalse(config.getDatesAsEpochMillis());

        settingsAsMap = xContentToMap(config);
        assertThat(settingsAsMap.getOrDefault("max_page_search_size", "not_set"), equalTo("not_set"));
        assertThat(settingsAsMap.getOrDefault("docs_per_second", "not_set"), equalTo("not_set"));
        assertNull(settingsAsMap.getOrDefault("dates_as_epoch_millis", "not_set"));
        assertThat(settingsAsMap.getOrDefault("interim_results", "not_set"), equalTo("not_set"));

        config = fromString("{\"interim_results\" : null}");
        assertFalse(config.getInterimResults());

        settingsAsMap = xContentToMap(config);
        assertThat(settingsAsMap.getOrDefault("max_page_search_size", "not_set"), equalTo("not_set"));
        assertThat(settingsAsMap.getOrDefault("docs_per_second", "not_set"), equalTo("not_set"));
        assertThat(settingsAsMap.getOrDefault("dates_as_epoch_millis", "not_set"), equalTo("not_set"));
        assertNull(settingsAsMap.getOrDefault("interim_results", "not_set"));
    }

    public void testExplicitNullOnWriteBuilder() throws IOException {
        // test that an explicit null is handled differently than not set
        SettingsConfig config = new SettingsConfig.Builder().setMaxPageSearchSize(null).build();
        assertThat(config.getMaxPageSearchSize(), equalTo(-1));

        Map<String, Object> settingsAsMap = xContentToMap(config);
        assertNull(settingsAsMap.getOrDefault("max_page_search_size", "not_set"));
        assertThat(settingsAsMap.getOrDefault("docs_per_second", "not_set"), equalTo("not_set"));
        assertThat(settingsAsMap.getOrDefault("dates_as_epoch_millis", "not_set"), equalTo("not_set"));
        assertThat(settingsAsMap.getOrDefault("interim_results", "not_set"), equalTo("not_set"));

        SettingsConfig emptyConfig = new SettingsConfig.Builder().build();
        assertNull(emptyConfig.getMaxPageSearchSize());
        assertNull(emptyConfig.getDatesAsEpochMillis());
        assertNull(emptyConfig.getInterimResults());

        settingsAsMap = xContentToMap(emptyConfig);
        assertTrue(settingsAsMap.isEmpty());

        config = new SettingsConfig.Builder().setRequestsPerSecond(null).build();
        assertThat(config.getDocsPerSecond(), equalTo(-1F));

        settingsAsMap = xContentToMap(config);
        assertThat(settingsAsMap.getOrDefault("max_page_search_size", "not_set"), equalTo("not_set"));
        assertNull(settingsAsMap.getOrDefault("docs_per_second", "not_set"));
        assertThat(settingsAsMap.getOrDefault("dates_as_epoch_millis", "not_set"), equalTo("not_set"));
        assertThat(settingsAsMap.getOrDefault("interim_results", "not_set"), equalTo("not_set"));

        config = new SettingsConfig.Builder().setDatesAsEpochMillis(null).build();
        // returns false, however it's `null` as in "use default", checked next
        assertFalse(config.getDatesAsEpochMillis());

        settingsAsMap = xContentToMap(config);
        assertThat(settingsAsMap.getOrDefault("max_page_search_size", "not_set"), equalTo("not_set"));
        assertThat(settingsAsMap.getOrDefault("docs_per_second", "not_set"), equalTo("not_set"));
        assertNull(settingsAsMap.getOrDefault("dates_as_epoch_millis", "not_set"));
        assertThat(settingsAsMap.getOrDefault("interim_results", "not_set"), equalTo("not_set"));

        config = new SettingsConfig.Builder().setInterimResults(null).build();
        // returns false, however it's `null` as in "use default", checked next
        assertFalse(config.getInterimResults());

        settingsAsMap = xContentToMap(config);
        assertThat(settingsAsMap.getOrDefault("max_page_search_size", "not_set"), equalTo("not_set"));
        assertThat(settingsAsMap.getOrDefault("docs_per_second", "not_set"), equalTo("not_set"));
        assertThat(settingsAsMap.getOrDefault("dates_as_epoch_millis", "not_set"), equalTo("not_set"));
        assertNull(settingsAsMap.getOrDefault("interim_results", "not_set"));
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
            return SettingsConfig.fromXContent(parser);
        }
    }

}
