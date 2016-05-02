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

package org.elasticsearch.index.mapper.core;

import com.carrotsearch.randomizedtesting.generators.RandomPicks;

import org.apache.lucene.index.IndexOptions;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.DocumentMapperParser;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.core.TextFieldMapper.TextFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;

public class StringMappingUpgradeTests extends ESSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(InternalSettingsPlugin.class);
    }

    public void testUpgradeDefaults() throws IOException {
        IndexService indexService = createIndex("test");
        DocumentMapperParser parser = indexService.mapperService().documentMapperParser();
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field").field("type", "string").endObject().endObject()
                .endObject().endObject().string();
        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));
        FieldMapper field = mapper.mappers().getMapper("field");
        assertThat(field, instanceOf(TextFieldMapper.class));
    }

    public void testUpgradeAnalyzedString() throws IOException {
        IndexService indexService = createIndex("test");
        DocumentMapperParser parser = indexService.mapperService().documentMapperParser();
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field").field("type", "string").field("index", "analyzed").endObject().endObject()
                .endObject().endObject().string();
        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));
        FieldMapper field = mapper.mappers().getMapper("field");
        assertThat(field, instanceOf(TextFieldMapper.class));
    }

    public void testUpgradeNotAnalyzedString() throws IOException {
        IndexService indexService = createIndex("test");
        DocumentMapperParser parser = indexService.mapperService().documentMapperParser();
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field").field("type", "string")
                .field("index", "not_analyzed").endObject().endObject()
                .endObject().endObject().string();
        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));
        FieldMapper field = mapper.mappers().getMapper("field");
        assertThat(field, instanceOf(KeywordFieldMapper.class));
    }

    public void testUpgradeNotIndexedString() throws IOException {
        IndexService indexService = createIndex("test");
        DocumentMapperParser parser = indexService.mapperService().documentMapperParser();
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field").field("type", "string").field("index", "no").endObject().endObject()
                .endObject().endObject().string();
        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));
        FieldMapper field = mapper.mappers().getMapper("field");
        assertThat(field, instanceOf(KeywordFieldMapper.class));
        assertEquals(IndexOptions.NONE, field.fieldType().indexOptions());
    }

    public void testIllegalIndexValue() throws IOException {
        IndexService indexService = createIndex("test");
        DocumentMapperParser parser = indexService.mapperService().documentMapperParser();
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties")
                    .startObject("field")
                        .field("type", "string")
                        .field("index", false)
                    .endObject()
                .endObject() .endObject().endObject().string();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> parser.parse("type", new CompressedXContent(mapping)));
        assertThat(e.getMessage(),
                containsString("Can't parse [index] value [false] for field [field], expected [no], [not_analyzed] or [analyzed]"));
    }

    public void testNotSupportedUpgrade() throws IOException {
        IndexService indexService = createIndex("test");
        DocumentMapperParser parser = indexService.mapperService().documentMapperParser();
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field").field("type", "string")
                .field("index", "not_analyzed").field("analyzer", "keyword").endObject().endObject()
                .endObject().endObject().string();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> parser.parse("type", new CompressedXContent(mapping)));
        assertThat(e.getMessage(), containsString("The [string] type is removed in 5.0"));
    }

    public void testUpgradeFielddataSettings() throws IOException {
        IndexService indexService = createIndex("test");
        DocumentMapperParser parser = indexService.mapperService().documentMapperParser();
        String format = randomFrom("paged_bytes", "disabled");
        String loading = randomFrom("lazy", "eager", "eager_global_ordinals");
        boolean keyword = random().nextBoolean();
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties")
                    .startObject("field")
                        .field("type", "string")
                        .field("index", keyword ? "not_analyzed" : "analyzed")
                        .startObject("fielddata")
                            .field("format", format)
                            .field("loading", loading)
                            .startObject("filter")
                                .startObject("frequency")
                                    .field("min", 3)
                                .endObject()
                            .endObject()
                        .endObject()
                    .endObject()
                .endObject()
                .endObject().endObject().string();
        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));
        FieldMapper field = mapper.mappers().getMapper("field");
        if (keyword) {
            assertThat(field, instanceOf(KeywordFieldMapper.class));
        } else {
            assertThat(field, instanceOf(TextFieldMapper.class));
            TextFieldType fieldType = (TextFieldType) field.fieldType();
            assertEquals("disabled".equals(format) == false, fieldType.fielddata());
            assertEquals(3, fieldType.fielddataMinFrequency(), 0d);
            assertEquals(Integer.MAX_VALUE, fieldType.fielddataMaxFrequency(), 0d);
        }
        assertEquals("eager_global_ordinals".equals(loading), field.fieldType().eagerGlobalOrdinals());
    }

    public void testUpgradeIgnoreAbove() throws IOException {
        IndexService indexService = createIndex("test");
        DocumentMapperParser parser = indexService.mapperService().documentMapperParser();
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field").field("type", "string")
                .field("index", "not_analyzed").field("ignore_above", 200).endObject().endObject()
                .endObject().endObject().string();
        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));
        FieldMapper field = mapper.mappers().getMapper("field");
        assertThat(field, instanceOf(KeywordFieldMapper.class));
        assertEquals(200, ((KeywordFieldMapper) field).ignoreAbove());
    }

    public void testUpgradeAnalyzer() throws IOException {
        IndexService indexService = createIndex("test");
        DocumentMapperParser parser = indexService.mapperService().documentMapperParser();
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field").field("type", "string")
                .field("analyzer", "standard")
                .field("search_analyzer", "whitespace")
                .field("search_quote_analyzer", "keyword").endObject().endObject()
                .endObject().endObject().string();
        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));
        FieldMapper field = mapper.mappers().getMapper("field");
        assertThat(field, instanceOf(TextFieldMapper.class));
        assertEquals("standard", field.fieldType().indexAnalyzer().name());
        assertEquals("whitespace", field.fieldType().searchAnalyzer().name());
        assertEquals("keyword", field.fieldType().searchQuoteAnalyzer().name());
    }

    public void testUpgradeRandomMapping() throws IOException {
        final int iters = 20;
        for (int i = 0; i < iters; ++i) {
            doTestUpgradeRandomMapping(i);
        }
    }

    private void doTestUpgradeRandomMapping(int iter) throws IOException {
        IndexService indexService;
        boolean oldIndex = randomBoolean();
        String indexName = "test" + iter;
        if (oldIndex) {
            Settings settings = Settings.builder()
                    .put(IndexMetaData.SETTING_VERSION_CREATED, Version.V_2_3_0)
                    .build();
            indexService = createIndex(indexName, settings);
        } else {
            indexService = createIndex(indexName);
        }
        DocumentMapperParser parser = indexService.mapperService().documentMapperParser();
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field").field("type", "string");
        boolean keyword = randomBoolean();
        boolean hasNorms = keyword == false;
        boolean shouldUpgrade = true;
        if (keyword) {
            mapping.field("index", randomBoolean() ? "not_analyzed" : "no");
        } else if (randomBoolean()) {
            mapping.field("index", "analyzed");
        }
        if (randomBoolean()) {
            mapping.field("store", RandomPicks.randomFrom(random(), Arrays.asList("yes", "no", true, false)));
        }
        if (keyword && randomBoolean()) {
            mapping.field("doc_values", randomBoolean());
        }
        if (keyword == false && randomBoolean()) {
            mapping.field("analyzer", "keyword");
        }
        if (randomBoolean()) {
            hasNorms = randomBoolean();
            if (randomBoolean()) {
                mapping.field("omit_norms", hasNorms == false);
            } else {
                mapping.field("norms", Collections.singletonMap("enabled", hasNorms));
            }
        }
        if (randomBoolean()) {
            Map<String, Object> fielddata = new HashMap<>();
            if (randomBoolean()) {
                fielddata.put("format", randomFrom("paged_bytes", "disabled"));
            }
            if (randomBoolean()) {
                fielddata.put("loading", randomFrom("lazy", "eager", "eager_global_ordinals"));
            }
            if (randomBoolean()) {
                Map<String, Object> frequencyFilter = new HashMap<>();
                frequencyFilter.put("min", 10);
                frequencyFilter.put("max", 1000);
                frequencyFilter.put("min_segment_size", 10000);
            }
        }
        if (randomBoolean()) {
            mapping.startObject("fields").startObject("raw").field("type", "keyword").endObject().endObject();
        }
        if (randomBoolean()) {
            mapping.field("copy_to", "bar");
        }
        if (randomBoolean()) {
            // this option is not upgraded automatically
            mapping.field("index_options", "docs");
            shouldUpgrade = false;
        }
        mapping.endObject().endObject().endObject().endObject();

        if (oldIndex == false && shouldUpgrade == false) {
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                    () -> parser.parse("type", new CompressedXContent(mapping.string())));
            assertThat(e.getMessage(), containsString("The [string] type is removed in 5.0"));
        } else {
            DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping.string()));
            FieldMapper field = mapper.mappers().getMapper("field");
            if (oldIndex) {
                assertThat(field, instanceOf(StringFieldMapper.class));
            } else if (keyword) {
                assertThat(field, instanceOf(KeywordFieldMapper.class));
            } else {
                assertThat(field, instanceOf(TextFieldMapper.class));
            }
            if (field.fieldType().indexOptions() != IndexOptions.NONE) {
                assertEquals(hasNorms, field.fieldType().omitNorms() == false);
            }
        }
    }

    public void testUpgradeTemplateWithDynamicType() throws IOException {
        IndexService indexService = createIndex("test");
        DocumentMapperParser parser = indexService.mapperService().documentMapperParser();
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startArray("dynamic_templates")
                    .startObject()
                        .startObject("my_template")
                            .field("match_mapping_type", "string")
                            .startObject("mapping")
                                .field("store", true)
                            .endObject()
                        .endObject()
                    .endObject()
                .endArray()
                .endObject().endObject().string();
        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));
        BytesReference source = XContentFactory.jsonBuilder().startObject().field("foo", "bar").endObject().bytes();
        ParsedDocument doc = mapper.parse("test", "type", "id", source);
        Mapper fooMapper = doc.dynamicMappingsUpdate().root().getMapper("foo");
        assertThat(fooMapper, instanceOf(TextFieldMapper.class));
        assertTrue(((TextFieldMapper) fooMapper).fieldType().stored());
    }

    public void testUpgradeTemplateWithDynamicType2() throws IOException {
        IndexService indexService = createIndex("test");
        DocumentMapperParser parser = indexService.mapperService().documentMapperParser();
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startArray("dynamic_templates")
                    .startObject()
                        .startObject("my_template")
                            .field("match_mapping_type", "string")
                            .startObject("mapping")
                                .field("type", "{dynamic_type}")
                                .field("store", true)
                            .endObject()
                        .endObject()
                    .endObject()
                .endArray()
                .endObject().endObject().string();
        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));
        BytesReference source = XContentFactory.jsonBuilder().startObject().field("foo", "bar").endObject().bytes();
        ParsedDocument doc = mapper.parse("test", "type", "id", source);
        Mapper fooMapper = doc.dynamicMappingsUpdate().root().getMapper("foo");
        assertThat(fooMapper, instanceOf(TextFieldMapper.class));
        assertTrue(((TextFieldMapper) fooMapper).fieldType().stored());
    }

    public void testUpgradeTemplateWithDynamicTypeKeyword() throws IOException {
        IndexService indexService = createIndex("test");
        DocumentMapperParser parser = indexService.mapperService().documentMapperParser();
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startArray("dynamic_templates")
                    .startObject()
                        .startObject("my_template")
                            .field("match_mapping_type", "string")
                            .startObject("mapping")
                                .field("index", "not_analyzed")
                            .endObject()
                        .endObject()
                    .endObject()
                .endArray()
                .endObject().endObject().string();
        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));
        BytesReference source = XContentFactory.jsonBuilder().startObject().field("foo", "bar").endObject().bytes();
        ParsedDocument doc = mapper.parse("test", "type", "id", source);
        Mapper fooMapper = doc.dynamicMappingsUpdate().root().getMapper("foo");
        assertThat(fooMapper, instanceOf(KeywordFieldMapper.class));
    }

    public void testUpgradeTemplateWithDynamicTypeKeyword2() throws IOException {
        IndexService indexService = createIndex("test");
        DocumentMapperParser parser = indexService.mapperService().documentMapperParser();
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startArray("dynamic_templates")
                    .startObject()
                        .startObject("my_template")
                            .field("match_mapping_type", "string")
                            .startObject("mapping")
                                .field("type", "{dynamic_type}")
                                .field("index", "not_analyzed")
                            .endObject()
                        .endObject()
                    .endObject()
                .endArray()
                .endObject().endObject().string();
        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));
        BytesReference source = XContentFactory.jsonBuilder().startObject().field("foo", "bar").endObject().bytes();
        ParsedDocument doc = mapper.parse("test", "type", "id", source);
        Mapper fooMapper = doc.dynamicMappingsUpdate().root().getMapper("foo");
        assertThat(fooMapper, instanceOf(KeywordFieldMapper.class));
    }
}
