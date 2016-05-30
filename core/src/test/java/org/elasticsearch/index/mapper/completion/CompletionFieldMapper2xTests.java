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
package org.elasticsearch.index.mapper.completion;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.core.CompletionFieldMapper2x;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.test.VersionUtils;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import static com.carrotsearch.randomizedtesting.RandomizedTest.getRandom;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class CompletionFieldMapper2xTests extends ESSingleNodeTestCase {
    private final Version PRE2X_VERSION = VersionUtils.randomVersionBetween(getRandom(), Version.V_2_0_0, Version.V_2_3_1);

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(InternalSettingsPlugin.class);
    }

    public void testDefaultConfiguration() throws IOException {
        String mapping = jsonBuilder().startObject().startObject("type1")
            .startObject("properties").startObject("completion")
            .field("type", "completion")
            .endObject().endObject()
            .endObject().endObject().string();

        DocumentMapper defaultMapper = createIndex("test",
            Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, PRE2X_VERSION.id).build())
            .mapperService().documentMapperParser().parse("type1", new CompressedXContent(mapping));

        FieldMapper fieldMapper = defaultMapper.mappers().getMapper("completion");
        assertThat(fieldMapper, instanceOf(CompletionFieldMapper2x.class));

        CompletionFieldMapper2x completionFieldMapper = (CompletionFieldMapper2x) fieldMapper;
        assertThat(completionFieldMapper.isStoringPayloads(), is(false));
    }

    public void testThatSerializationIncludesAllElements() throws Exception {
        String mapping = jsonBuilder().startObject().startObject("type1")
            .startObject("properties").startObject("completion")
            .field("type", "completion")
            .field("analyzer", "simple")
            .field("search_analyzer", "standard")
            .field("payloads", true)
            .field("preserve_separators", false)
            .field("preserve_position_increments", true)
            .field("max_input_length", 14)

            .endObject().endObject()
            .endObject().endObject().string();

        DocumentMapper defaultMapper = createIndex("test",
            Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, PRE2X_VERSION.id).build())
            .mapperService().documentMapperParser().parse("type1", new CompressedXContent(mapping));

        FieldMapper fieldMapper = defaultMapper.mappers().getMapper("completion");
        assertThat(fieldMapper, instanceOf(CompletionFieldMapper2x.class));

        CompletionFieldMapper2x completionFieldMapper = (CompletionFieldMapper2x) fieldMapper;
        XContentBuilder builder = jsonBuilder().startObject();
        completionFieldMapper.toXContent(builder, null).endObject();
        builder.close();
        Map<String, Object> serializedMap;
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(builder.bytes())) {
            serializedMap = parser.map();
        }
        Map<String, Object> configMap = (Map<String, Object>) serializedMap.get("completion");
        assertThat(configMap.get("analyzer").toString(), is("simple"));
        assertThat(configMap.get("search_analyzer").toString(), is("standard"));
        assertThat(Boolean.valueOf(configMap.get("payloads").toString()), is(true));
        assertThat(Boolean.valueOf(configMap.get("preserve_separators").toString()), is(false));
        assertThat(Boolean.valueOf(configMap.get("preserve_position_increments").toString()), is(true));
        assertThat(Integer.valueOf(configMap.get("max_input_length").toString()), is(14));
    }

    public void testThatSerializationCombinesToOneAnalyzerFieldIfBothAreEqual() throws Exception {
        String mapping = jsonBuilder().startObject().startObject("type1")
            .startObject("properties").startObject("completion")
            .field("type", "completion")
            .field("analyzer", "simple")
            .field("search_analyzer", "simple")
            .endObject().endObject()
            .endObject().endObject().string();

        DocumentMapper defaultMapper = createIndex("test",
            Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, PRE2X_VERSION.id).build())
            .mapperService().documentMapperParser().parse("type1", new CompressedXContent(mapping));

        FieldMapper fieldMapper = defaultMapper.mappers().getMapper("completion");
        assertThat(fieldMapper, instanceOf(CompletionFieldMapper2x.class));

        CompletionFieldMapper2x completionFieldMapper = (CompletionFieldMapper2x) fieldMapper;
        XContentBuilder builder = jsonBuilder().startObject();
        completionFieldMapper.toXContent(builder, null).endObject();
        builder.close();
        Map<String, Object> serializedMap;
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(builder.bytes())) {
            serializedMap = parser.map();
        }
        Map<String, Object> configMap = (Map<String, Object>) serializedMap.get("completion");
        assertThat(configMap.get("analyzer").toString(), is("simple"));
    }

}
