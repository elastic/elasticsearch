/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
        return new SettingsConfig(randomBoolean() ? null : randomIntBetween(10, 10_000), randomBoolean() ? null : randomFloat());
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

        settingsAsMap = xContentToMap(emptyConfig);
        assertTrue(settingsAsMap.isEmpty());

        config = fromString("{\"docs_per_second\" : null}");
        assertThat(config.getDocsPerSecond(), equalTo(-1F));

        settingsAsMap = xContentToMap(config);
        assertThat(settingsAsMap.getOrDefault("max_page_search_size", "not_set"), equalTo("not_set"));
        assertNull(settingsAsMap.getOrDefault("docs_per_second", "not_set"));
    }

    public void testExplicitNullOnWriteBuilder() throws IOException {
        // test that an explicit null is handled differently than not set
        SettingsConfig config = new SettingsConfig.Builder().setMaxPageSearchSize(null).build();
        assertThat(config.getMaxPageSearchSize(), equalTo(-1));

        Map<String, Object> settingsAsMap = xContentToMap(config);
        assertNull(settingsAsMap.getOrDefault("max_page_search_size", "not_set"));
        assertThat(settingsAsMap.getOrDefault("docs_per_second", "not_set"), equalTo("not_set"));

        SettingsConfig emptyConfig = new SettingsConfig.Builder().build();
        assertNull(emptyConfig.getMaxPageSearchSize());

        settingsAsMap = xContentToMap(emptyConfig);
        assertTrue(settingsAsMap.isEmpty());

        config = new SettingsConfig.Builder().setRequestsPerSecond(null).build();
        assertThat(config.getDocsPerSecond(), equalTo(-1F));

        settingsAsMap = xContentToMap(config);
        assertThat(settingsAsMap.getOrDefault("max_page_search_size", "not_set"), equalTo("not_set"));
        assertNull(settingsAsMap.getOrDefault("docs_per_second", "not_set"));
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
