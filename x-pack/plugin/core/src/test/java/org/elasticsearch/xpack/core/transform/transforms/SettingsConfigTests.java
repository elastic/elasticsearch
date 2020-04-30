/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.transform.transforms;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.junit.Before;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class SettingsConfigTests extends AbstractSerializingTransformTestCase<SettingsConfig> {

    private boolean lenient;

    public static SettingsConfig randomSettingsConfig() {
        return new SettingsConfig(randomBoolean() ? null : randomIntBetween(10, 10_000), randomBoolean() ? null : randomFloat());
    }

    public static SettingsConfig randomNonEmptySettingsConfig() {
        return new SettingsConfig(randomIntBetween(10, 10_000), randomFloat());
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
    }

    public void testUpdateUsingBuilder() throws IOException {
        SettingsConfig config = fromString("{\"max_page_search_size\" : 10000, \"docs_per_second\" :42}");

        SettingsConfig.Builder builder = new SettingsConfig.Builder(config);
        builder.update(fromString("{\"max_page_search_size\" : 100}"));

        assertThat(builder.build().getMaxPageSearchSize(), equalTo(100));
        assertThat(builder.build().getDocsPerSecond(), equalTo(42F));

        builder.update(fromString("{\"max_page_search_size\" : null}"));
        assertNull(builder.build().getMaxPageSearchSize());
        assertThat(builder.build().getDocsPerSecond(), equalTo(42F));

        builder.update(fromString("{\"max_page_search_size\" : 77, \"docs_per_second\" :null}"));
        assertThat(builder.build().getMaxPageSearchSize(), equalTo(77));
        assertNull(builder.build().getDocsPerSecond());
    }

    public void testOmmitDefaultsOnWrite() throws IOException {
        SettingsConfig config = fromString("{\"max_page_search_size\" : null}");
        assertThat(config.getMaxPageSearchSize(), equalTo(-1));

        SettingsConfig parsedSettings = SettingsConfig.fromXContent(
            XContentHelper.createParser(
                xContentRegistry(),
                null,
                XContentHelper.toXContent(config, XContentType.JSON, false),
                XContentType.JSON
            ),
            false
        );
        assertEquals(null, parsedSettings.getMaxPageSearchSize());
    }

    private SettingsConfig fromString(String source) throws IOException {
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, source)) {
            return SettingsConfig.fromXContent(parser, false);
        }
    }
}
