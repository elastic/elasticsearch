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

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.IndexableFieldType;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.mapper.Mapper.BuilderContext;
import org.elasticsearch.index.mapper.ParseContext.Document;
import org.elasticsearch.index.mapper.StringFieldMapper.Builder;
import org.elasticsearch.index.mapper.StringFieldMapper.StringFieldType;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.junit.Before;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

/**
 */
public class LegacyStringMappingTests extends ESSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(InternalSettingsPlugin.class);
    }

    IndexService indexService;
    DocumentMapperParser parser;

    @Before
    public void before() {
        indexService = createIndex("test",
                // we need 2.x since string is deprecated in 5.0
                Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.V_2_3_0).build());
        parser = indexService.mapperService().documentMapperParser();
    }

    public void testLimit() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field").field("type", "string").field("ignore_above", 5).endObject().endObject()
                .endObject().endObject().string();

        DocumentMapper defaultMapper = parser.parse("type", new CompressedXContent(mapping));

        ParsedDocument doc = defaultMapper.parse("test", "type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .field("field", "1234")
                .endObject()
                .bytes());

        assertThat(doc.rootDoc().getField("field"), notNullValue());

        doc = defaultMapper.parse("test", "type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .field("field", "12345")
                .endObject()
                .bytes());

        assertThat(doc.rootDoc().getField("field"), notNullValue());

        doc = defaultMapper.parse("test", "type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .field("field", "123456")
                .endObject()
                .bytes());

        assertThat(doc.rootDoc().getField("field"), nullValue());
    }

    private void assertDefaultAnalyzedFieldType(IndexableFieldType fieldType) {
        assertThat(fieldType.omitNorms(), equalTo(false));
        assertThat(fieldType.indexOptions(), equalTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS));
        assertThat(fieldType.storeTermVectors(), equalTo(false));
        assertThat(fieldType.storeTermVectorOffsets(), equalTo(false));
        assertThat(fieldType.storeTermVectorPositions(), equalTo(false));
        assertThat(fieldType.storeTermVectorPayloads(), equalTo(false));
    }

    private void assertEquals(IndexableFieldType ft1, IndexableFieldType ft2) {
        assertEquals(ft1.tokenized(), ft2.tokenized());
        assertEquals(ft1.omitNorms(), ft2.omitNorms());
        assertEquals(ft1.indexOptions(), ft2.indexOptions());
        assertEquals(ft1.storeTermVectors(), ft2.storeTermVectors());
        assertEquals(ft1.docValuesType(), ft2.docValuesType());
    }

    private void assertParseIdemPotent(IndexableFieldType expected, DocumentMapper mapper) throws Exception {
        String mapping = mapper.toXContent(XContentFactory.jsonBuilder().startObject(), new ToXContent.MapParams(emptyMap())).endObject().string();
        mapper = parser.parse("type", new CompressedXContent(mapping));
        ParsedDocument doc = mapper.parse("test", "type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .field("field", "2345")
                .endObject()
                .bytes());
        assertEquals(expected, doc.rootDoc().getField("field").fieldType());
    }

    public void testDefaultsForAnalyzed() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field").field("type", "string").endObject().endObject()
                .endObject().endObject().string();

        DocumentMapper defaultMapper = parser.parse("type", new CompressedXContent(mapping));

        ParsedDocument doc = defaultMapper.parse("test", "type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .field("field", "1234")
                .endObject()
                .bytes());

        IndexableFieldType fieldType = doc.rootDoc().getField("field").fieldType();
        assertDefaultAnalyzedFieldType(fieldType);
        assertParseIdemPotent(fieldType, defaultMapper);
    }

    public void testDefaultsForNotAnalyzed() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field").field("type", "string").field("index", "not_analyzed").endObject().endObject()
                .endObject().endObject().string();

        DocumentMapper defaultMapper = parser.parse("type", new CompressedXContent(mapping));

        ParsedDocument doc = defaultMapper.parse("test", "type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .field("field", "1234")
                .endObject()
                .bytes());

        IndexableFieldType fieldType = doc.rootDoc().getField("field").fieldType();
        assertThat(fieldType.omitNorms(), equalTo(true));
        assertThat(fieldType.indexOptions(), equalTo(IndexOptions.DOCS));
        assertThat(fieldType.storeTermVectors(), equalTo(false));
        assertThat(fieldType.storeTermVectorOffsets(), equalTo(false));
        assertThat(fieldType.storeTermVectorPositions(), equalTo(false));
        assertThat(fieldType.storeTermVectorPayloads(), equalTo(false));
        assertParseIdemPotent(fieldType, defaultMapper);

        // now test it explicitly set

        mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field").field("type", "string").field("index", "not_analyzed").startObject("norms").field("enabled", true).endObject().field("index_options", "freqs").endObject().endObject()
                .endObject().endObject().string();

        defaultMapper = parser.parse("type", new CompressedXContent(mapping));

        doc = defaultMapper.parse("test", "type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .field("field", "1234")
                .endObject()
                .bytes());

        fieldType = doc.rootDoc().getField("field").fieldType();
        assertThat(fieldType.omitNorms(), equalTo(false));
        assertThat(fieldType.indexOptions(), equalTo(IndexOptions.DOCS_AND_FREQS));
        assertThat(fieldType.storeTermVectors(), equalTo(false));
        assertThat(fieldType.storeTermVectorOffsets(), equalTo(false));
        assertThat(fieldType.storeTermVectorPositions(), equalTo(false));
        assertThat(fieldType.storeTermVectorPayloads(), equalTo(false));
        assertParseIdemPotent(fieldType, defaultMapper);

        // also test the deprecated omit_norms

        mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field").field("type", "string").field("index", "not_analyzed").field("omit_norms", false).endObject().endObject()
                .endObject().endObject().string();

        defaultMapper = parser.parse("type", new CompressedXContent(mapping));

        doc = defaultMapper.parse("test", "type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .field("field", "1234")
                .endObject()
                .bytes());

        fieldType = doc.rootDoc().getField("field").fieldType();
        assertThat(fieldType.omitNorms(), equalTo(false));
        assertParseIdemPotent(fieldType, defaultMapper);
    }

    public void testSearchQuoteAnalyzerSerialization() throws Exception {
        // Cases where search_quote_analyzer should not be added to the mapping.
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties")
                .startObject("field1")
                    .field("type", "string")
                    .field("position_increment_gap", 1000)
                .endObject()
                .startObject("field2")
                    .field("type", "string")
                    .field("position_increment_gap", 1000)
                    .field("analyzer", "standard")
                .endObject()
                .startObject("field3")
                    .field("type", "string")
                    .field("position_increment_gap", 1000)
                    .field("analyzer", "standard")
                    .field("search_analyzer", "simple")
                .endObject()
                .startObject("field4")
                    .field("type", "string")
                    .field("position_increment_gap", 1000)
                    .field("analyzer", "standard")
                    .field("search_analyzer", "simple")
                    .field("search_quote_analyzer", "simple")
                .endObject()
                .endObject()
                .endObject().endObject().string();

        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));
        for (String fieldName : Arrays.asList("field1", "field2", "field3", "field4")) {
            Map<String, Object> serializedMap = getSerializedMap(fieldName, mapper);
            assertFalse(fieldName, serializedMap.containsKey("search_quote_analyzer"));
        }

        // Cases where search_quote_analyzer should be present.
        mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties")
                .startObject("field")
                    .field("type", "string")
                    .field("position_increment_gap", 1000)
                    .field("analyzer", "standard")
                    .field("search_analyzer", "standard")
                    .field("search_quote_analyzer", "simple")
                .endObject()
                .endObject()
                .endObject().endObject().string();

        mapper = parser.parse("type", new CompressedXContent(mapping));
        Map<String, Object> serializedMap = getSerializedMap("field", mapper);
        assertEquals(serializedMap.get("search_quote_analyzer"), "simple");
    }

    public void testSearchAnalyzerSerialization() throws IOException {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties")
                    .startObject("field")
                        .field("type", "string")
                        .field("analyzer", "standard")
                        .field("search_analyzer", "keyword")
                    .endObject()
                .endObject().endObject().endObject().string();

        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));
        assertEquals(mapping,  mapper.mappingSource().toString());

        // special case: default index analyzer
        mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties")
                    .startObject("field")
                        .field("type", "string")
                        .field("analyzer", "default")
                        .field("search_analyzer", "keyword")
                    .endObject()
                .endObject().endObject().endObject().string();

        mapper = parser.parse("type", new CompressedXContent(mapping));
        assertEquals(mapping,  mapper.mappingSource().toString());

        // special case: default search analyzer
        mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("properties")
            .startObject("field")
            .field("type", "string")
            .field("analyzer", "keyword")
            .endObject()
            .endObject().endObject().endObject().string();

        mapper = parser.parse("type", new CompressedXContent(mapping));
        assertEquals(mapping,  mapper.mappingSource().toString());

        // special case: default search analyzer
        mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("properties")
            .startObject("field")
            .field("type", "string")
            .field("analyzer", "keyword")
            .field("search_analyzer", "default")
            .endObject()
            .endObject().endObject().endObject().string();

        mapper = parser.parse("type", new CompressedXContent(mapping));
        assertEquals(mapping,  mapper.mappingSource().toString());


        mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("properties")
            .startObject("field")
            .field("type", "string")
            .field("analyzer", "keyword")
            .endObject()
            .endObject().endObject().endObject().string();
        mapper = parser.parse("type", new CompressedXContent(mapping));

        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        mapper.toXContent(builder, new ToXContent.MapParams(Collections.singletonMap("include_defaults", "true")));
        builder.endObject();

        String mappingString = builder.string();
        assertTrue(mappingString.contains("analyzer"));
        assertTrue(mappingString.contains("search_analyzer"));
        assertTrue(mappingString.contains("search_quote_analyzer"));
    }

    private Map<String, Object> getSerializedMap(String fieldName, DocumentMapper mapper) throws Exception {
        FieldMapper fieldMapper = mapper.mappers().smartNameFieldMapper(fieldName);
        XContentBuilder builder = JsonXContent.contentBuilder().startObject();
        fieldMapper.toXContent(builder, ToXContent.EMPTY_PARAMS).endObject();
        builder.close();

        Map<String, Object> fieldMap;
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(builder.bytes())) {
            fieldMap = parser.map();
        }
        @SuppressWarnings("unchecked")
        Map<String, Object> result = (Map<String, Object>) fieldMap.get(fieldName);
        return result;
    }

    public void testTermVectors() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties")
                .startObject("field1")
                    .field("type", "string")
                    .field("term_vector", "no")
                .endObject()
                .startObject("field2")
                    .field("type", "string")
                    .field("term_vector", "yes")
                .endObject()
                .startObject("field3")
                    .field("type", "string")
                    .field("term_vector", "with_offsets")
                .endObject()
                .startObject("field4")
                    .field("type", "string")
                    .field("term_vector", "with_positions")
                .endObject()
                .startObject("field5")
                    .field("type", "string")
                    .field("term_vector", "with_positions_offsets")
                .endObject()
                .startObject("field6")
                    .field("type", "string")
                    .field("term_vector", "with_positions_offsets_payloads")
                .endObject()
                .endObject()
                .endObject().endObject().string();

        DocumentMapper defaultMapper = parser.parse("type", new CompressedXContent(mapping));

        ParsedDocument doc = defaultMapper.parse("test", "type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .field("field1", "1234")
                .field("field2", "1234")
                .field("field3", "1234")
                .field("field4", "1234")
                .field("field5", "1234")
                .field("field6", "1234")
                .endObject()
                .bytes());

        assertThat(doc.rootDoc().getField("field1").fieldType().storeTermVectors(), equalTo(false));
        assertThat(doc.rootDoc().getField("field1").fieldType().storeTermVectorOffsets(), equalTo(false));
        assertThat(doc.rootDoc().getField("field1").fieldType().storeTermVectorPositions(), equalTo(false));
        assertThat(doc.rootDoc().getField("field1").fieldType().storeTermVectorPayloads(), equalTo(false));

        assertThat(doc.rootDoc().getField("field2").fieldType().storeTermVectors(), equalTo(true));
        assertThat(doc.rootDoc().getField("field2").fieldType().storeTermVectorOffsets(), equalTo(false));
        assertThat(doc.rootDoc().getField("field2").fieldType().storeTermVectorPositions(), equalTo(false));
        assertThat(doc.rootDoc().getField("field2").fieldType().storeTermVectorPayloads(), equalTo(false));

        assertThat(doc.rootDoc().getField("field3").fieldType().storeTermVectors(), equalTo(true));
        assertThat(doc.rootDoc().getField("field3").fieldType().storeTermVectorOffsets(), equalTo(true));
        assertThat(doc.rootDoc().getField("field3").fieldType().storeTermVectorPositions(), equalTo(false));
        assertThat(doc.rootDoc().getField("field3").fieldType().storeTermVectorPayloads(), equalTo(false));

        assertThat(doc.rootDoc().getField("field4").fieldType().storeTermVectors(), equalTo(true));
        assertThat(doc.rootDoc().getField("field4").fieldType().storeTermVectorOffsets(), equalTo(false));
        assertThat(doc.rootDoc().getField("field4").fieldType().storeTermVectorPositions(), equalTo(true));
        assertThat(doc.rootDoc().getField("field4").fieldType().storeTermVectorPayloads(), equalTo(false));

        assertThat(doc.rootDoc().getField("field5").fieldType().storeTermVectors(), equalTo(true));
        assertThat(doc.rootDoc().getField("field5").fieldType().storeTermVectorOffsets(), equalTo(true));
        assertThat(doc.rootDoc().getField("field5").fieldType().storeTermVectorPositions(), equalTo(true));
        assertThat(doc.rootDoc().getField("field5").fieldType().storeTermVectorPayloads(), equalTo(false));

        assertThat(doc.rootDoc().getField("field6").fieldType().storeTermVectors(), equalTo(true));
        assertThat(doc.rootDoc().getField("field6").fieldType().storeTermVectorOffsets(), equalTo(true));
        assertThat(doc.rootDoc().getField("field6").fieldType().storeTermVectorPositions(), equalTo(true));
        assertThat(doc.rootDoc().getField("field6").fieldType().storeTermVectorPayloads(), equalTo(true));
    }

    public void testDocValues() throws Exception {
        // doc values only work on non-analyzed content
        final BuilderContext ctx = new BuilderContext(indexService.getIndexSettings().getSettings(), new ContentPath(1));
        try {
            new StringFieldMapper.Builder("anything").docValues(true).build(ctx);
            fail();
        } catch (Exception e) { /* OK */ }

        assertFalse(new Builder("anything").index(false).build(ctx).fieldType().hasDocValues());
        assertTrue(new Builder("anything").index(true).tokenized(false).build(ctx).fieldType().hasDocValues());
        assertFalse(new Builder("anything").index(true).tokenized(true).build(ctx).fieldType().hasDocValues());
        assertFalse(new Builder("anything").index(false).tokenized(false).docValues(false).build(ctx).fieldType().hasDocValues());
        assertTrue(new Builder("anything").index(false).docValues(true).build(ctx).fieldType().hasDocValues());

        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties")
                .startObject("str1")
                    .field("type", "string")
                    .field("index", "no")
                .endObject()
                .startObject("str2")
                    .field("type", "string")
                    .field("index", "not_analyzed")
                .endObject()
                .startObject("str3")
                    .field("type", "string")
                    .field("index", "analyzed")
                .endObject()
                .startObject("str4")
                    .field("type", "string")
                    .field("index", "not_analyzed")
                    .field("doc_values", false)
                .endObject()
                .startObject("str5")
                    .field("type", "string")
                    .field("index", "no")
                    .field("doc_values", false)
                .endObject()
                .endObject()
                .endObject().endObject().string();

        DocumentMapper defaultMapper = parser.parse("type", new CompressedXContent(mapping));

        ParsedDocument parsedDoc = defaultMapper.parse("test", "type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .field("str1", "1234")
                .field("str2", "1234")
                .field("str3", "1234")
                .field("str4", "1234")
                .field("str5", "1234")
                .endObject()
                .bytes());
        final Document doc = parsedDoc.rootDoc();
        assertEquals(DocValuesType.NONE, docValuesType(doc, "str1"));
        assertEquals(DocValuesType.SORTED_SET, docValuesType(doc, "str2"));
        assertEquals(DocValuesType.NONE, docValuesType(doc, "str3"));
        assertEquals(DocValuesType.NONE, docValuesType(doc, "str4"));
        assertEquals(DocValuesType.NONE, docValuesType(doc, "str5"));

    }

    public void testBwCompatDocValues() throws Exception {
        Settings oldIndexSettings = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.V_2_2_0).build();
        indexService = createIndex("test_old", oldIndexSettings);
        parser = indexService.mapperService().documentMapperParser();
        // doc values only work on non-analyzed content
        final BuilderContext ctx = new BuilderContext(indexService.getIndexSettings().getSettings(), new ContentPath(1));
        try {
            new StringFieldMapper.Builder("anything").docValues(true).build(ctx);
            fail();
        } catch (Exception e) { /* OK */ }

        assertFalse(new Builder("anything").index(false).build(ctx).fieldType().hasDocValues());
        assertTrue(new Builder("anything").index(true).tokenized(false).build(ctx).fieldType().hasDocValues());
        assertFalse(new Builder("anything").index(true).tokenized(true).build(ctx).fieldType().hasDocValues());
        assertFalse(new Builder("anything").index(false).tokenized(false).docValues(false).build(ctx).fieldType().hasDocValues());
        assertTrue(new Builder("anything").index(false).docValues(true).build(ctx).fieldType().hasDocValues());

        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties")
                .startObject("str1")
                    .field("type", "string")
                    .field("index", "no")
                .endObject()
                .startObject("str2")
                    .field("type", "string")
                    .field("index", "not_analyzed")
                .endObject()
                .startObject("str3")
                    .field("type", "string")
                    .field("index", "analyzed")
                .endObject()
                .startObject("str4")
                    .field("type", "string")
                    .field("index", "not_analyzed")
                    .field("doc_values", false)
                .endObject()
                .startObject("str5")
                    .field("type", "string")
                    .field("index", "no")
                    .field("doc_values", true)
                .endObject()
                .endObject()
                .endObject().endObject().string();

        DocumentMapper defaultMapper = parser.parse("type", new CompressedXContent(mapping));

        ParsedDocument parsedDoc = defaultMapper.parse("test", "type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .field("str1", "1234")
                .field("str2", "1234")
                .field("str3", "1234")
                .field("str4", "1234")
                .field("str5", "1234")
                .endObject()
                .bytes());
        final Document doc = parsedDoc.rootDoc();
        assertEquals(DocValuesType.NONE, docValuesType(doc, "str1"));
        assertEquals(DocValuesType.SORTED_SET, docValuesType(doc, "str2"));
        assertEquals(DocValuesType.NONE, docValuesType(doc, "str3"));
        assertEquals(DocValuesType.NONE, docValuesType(doc, "str4"));
        assertEquals(DocValuesType.SORTED_SET, docValuesType(doc, "str5"));

    }

    // TODO: this function shouldn't be necessary.  parsing should just add a single field that is indexed and dv
    public static DocValuesType docValuesType(Document document, String fieldName) {
        for (IndexableField field : document.getFields(fieldName)) {
            if (field.fieldType().docValuesType() != DocValuesType.NONE) {
                return field.fieldType().docValuesType();
            }
        }
        return DocValuesType.NONE;
    }

    public void testDisableNorms() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field").field("type", "string").endObject().endObject()
                .endObject().endObject().string();

        MapperService mapperService = indexService.mapperService();
        DocumentMapper defaultMapper = mapperService.merge("type", new CompressedXContent(mapping), MapperService.MergeReason.MAPPING_UPDATE, false);

        ParsedDocument doc = defaultMapper.parse("test", "type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .field("field", "1234")
                .endObject()
                .bytes());

        IndexableFieldType fieldType = doc.rootDoc().getField("field").fieldType();
        assertEquals(false, fieldType.omitNorms());

        String updatedMapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field").field("type", "string").startObject("norms").field("enabled", false).endObject()
                .endObject().endObject().endObject().endObject().string();
        defaultMapper = mapperService.merge("type", new CompressedXContent(updatedMapping), MapperService.MergeReason.MAPPING_UPDATE, false);

        doc = defaultMapper.parse("test", "type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .field("field", "1234")
                .endObject()
                .bytes());

        fieldType = doc.rootDoc().getField("field").fieldType();
        assertEquals(true, fieldType.omitNorms());

        updatedMapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field").field("type", "string").startObject("norms").field("enabled", true).endObject()
                .endObject().endObject().endObject().endObject().string();
        try {
            mapperService.merge("type", new CompressedXContent(updatedMapping), MapperService.MergeReason.MAPPING_UPDATE, false);
            fail();
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("different [norms]"));
        }
    }

    /**
     * Test that expected exceptions are thrown when creating a new index with position_offset_gap
     */
    public void testPositionOffsetGapDeprecation() throws Exception {
        // test deprecation exceptions on newly created indexes
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties")
                .startObject("field1")
                .field("type", "string")
                .field("position_increment_gap", 10)
                .endObject()
                .startObject("field2")
                .field("type", "string")
                .field("position_offset_gap", 50)
                .field("analyzer", "standard")
                .endObject().endObject().endObject().endObject().string();
        try {
            parser.parse("type", new CompressedXContent(mapping));
            fail("Mapping definition should fail with the position_offset_gap setting");
        }catch (MapperParsingException e) {
            assertEquals(e.getMessage(), "Mapping definition for [field2] has unsupported parameters:  [position_offset_gap : 50]");
        }
    }

    public void testFielddataLoading() throws IOException {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field")
                    .field("type", "string")
                    .startObject("fielddata")
                        .field("loading", "eager_global_ordinals")
                    .endObject()
                .endObject().endObject()
                .endObject().endObject().string();

        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));

        String expectedMapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field")
                .field("type", "string")
                .field("eager_global_ordinals", true)
            .endObject().endObject()
            .endObject().endObject().string();

        assertEquals(expectedMapping, mapper.mappingSource().toString());
        assertTrue(mapper.mappers().getMapper("field").fieldType().eagerGlobalOrdinals());
    }

    public void testFielddataFilter() throws IOException {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field")
                    .field("type", "string")
                    .startObject("fielddata")
                        .startObject("filter")
                            .startObject("frequency")
                                .field("min", 2d)
                                .field("min_segment_size", 1000)
                            .endObject()
                            .startObject("regex")
                                .field("pattern", "^#.*")
                            .endObject()
                        .endObject()
                    .endObject()
                .endObject().endObject()
                .endObject().endObject().string();

        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));

        String expectedMapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field")
                .field("type", "string")
                .startObject("fielddata_frequency_filter")
                    .field("min", 2d)
                    .field("min_segment_size", 1000)
                .endObject()
            .endObject().endObject()
            .endObject().endObject().string();

        assertEquals(expectedMapping, mapper.mappingSource().toString());
        StringFieldType fieldType = (StringFieldType) mapper.mappers().getMapper("field").fieldType();
        assertThat(fieldType.fielddataMinFrequency(), equalTo(2d));
        assertThat(fieldType.fielddataMaxFrequency(), equalTo((double) Integer.MAX_VALUE));
        assertThat(fieldType.fielddataMinSegmentSize(), equalTo(1000));
    }

    public void testDisabledFielddata() throws IOException {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field")
                    .field("type", "string")
                    .startObject("fielddata")
                        .field("format", "disabled")
                    .endObject()
                .endObject().endObject()
                .endObject().endObject().string();

        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));

        String expectedMapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field")
                .field("type", "string")
                .field("fielddata", false)
            .endObject().endObject()
            .endObject().endObject().string();

        assertEquals(expectedMapping, mapper.mappingSource().toString());
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> mapper.mappers().getMapper("field").fieldType().fielddataBuilder());
        assertThat(e.getMessage(), containsString("Fielddata is disabled"));
    }

    public void testNonAnalyzedFieldPositionIncrement() throws IOException {
        for (String index : Arrays.asList("no", "not_analyzed")) {
            String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field")
                .field("type", "string")
                .field("index", index)
                .field("position_increment_gap", 10)
                .endObject().endObject().endObject().endObject().string();

            IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> parser.parse("type", new CompressedXContent(mapping)));
            assertEquals("Cannot set position_increment_gap on field [field] without positions enabled", e.getMessage());
        }
    }

    public void testAnalyzedFieldPositionIncrementWithoutPositions() throws IOException {
        for (String indexOptions : Arrays.asList("docs", "freqs")) {
            String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field")
                .field("type", "string")
                .field("index_options", indexOptions)
                .field("position_increment_gap", 10)
                .endObject().endObject().endObject().endObject().string();

            IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> parser.parse("type", new CompressedXContent(mapping)));
            assertEquals("Cannot set position_increment_gap on field [field] without positions enabled", e.getMessage());
        }
    }

    public void testKeywordFieldAsStringWithUnsupportedField() throws IOException {
        String mapping = mappingForTestField(b -> b.field("type", "keyword").field("fielddata", true)).string();
        Exception e = expectThrows(IllegalArgumentException.class, () -> parser.parse("test_type", new CompressedXContent(mapping)));
        assertEquals("Automatic downgrade from [keyword] to [string] failed because parameters [fielddata] are not supported for "
                + "automatic downgrades.", e.getMessage());
    }

    public void testMergeKeywordIntoString() throws IOException {
        Map<String, Object> expectedMapping = new HashMap<>();
        expectedMapping.put("type", "string");
        expectedMapping.put("index", "not_analyzed");
        expectedMapping.put("fielddata", false);
        mergeMappingStep(expectedMapping, b -> b.field("type", "string").field("index", "not_analyzed"));
        mergeMappingStep(expectedMapping, b -> b.field("type", "keyword"));
    }

    public void testMergeKeywordIntoStringWithIndexFalse() throws IOException {
        Map<String, Object> expectedMapping = new HashMap<>();
        expectedMapping.put("type", "string");
        expectedMapping.put("index", "no");
        mergeMappingStep(expectedMapping, b -> b.field("type", "string").field("index", "no"));
        mergeMappingStep(expectedMapping, b -> b.field("type", "keyword").field("index", false));
    }

    public void testMergeKeywordIntoStringWithStore() throws IOException {
        Map<String, Object> expectedMapping = new HashMap<>();
        expectedMapping.put("type", "string");
        expectedMapping.put("index", "not_analyzed");
        expectedMapping.put("fielddata", false);
        expectedMapping.put("store", true);
        mergeMappingStep(expectedMapping, b -> b.field("type", "string").field("index", "not_analyzed").field("store", true));
        mergeMappingStep(expectedMapping, b -> b.field("type", "keyword").field("store", true));
    }

    public void testMergeKeywordIntoStringWithDocValues() throws IOException {
        Map<String, Object> expectedMapping = new HashMap<>();
        expectedMapping.put("type", "string");
        expectedMapping.put("index", "not_analyzed");
        expectedMapping.put("doc_values", false);
        mergeMappingStep(expectedMapping, b -> b.field("type", "string").field("index", "not_analyzed").field("doc_values", false));
        mergeMappingStep(expectedMapping, b -> b.field("type", "keyword").field("doc_values", false));
    }

    public void testMergeKeywordIntoStringWithNorms() throws IOException {
        Map<String, Object> expectedMapping = new HashMap<>();
        expectedMapping.put("type", "string");
        expectedMapping.put("index", "not_analyzed");
        expectedMapping.put("fielddata", false);
        expectedMapping.put("norms", true);
        mergeMappingStep(expectedMapping, b -> b.field("type", "string").field("index", "not_analyzed").field("norms", true));
        mergeMappingStep(expectedMapping, b -> b.field("type", "keyword").field("norms", true));
        // norms can be an array but it'll just get squashed into true/false
        mergeMappingStep(expectedMapping, b -> b.field("type", "string").field("index", "not_analyzed")
                .startObject("norms")
                    .field("enabled", true)
                    .field("loading", randomAsciiOfLength(5)) // Totally ignored even though it used to be eager/lazy
                .endObject());
        mergeMappingStep(expectedMapping, b -> b.field("type", "keyword")
                .startObject("norms")
                    .field("enabled", true)
                    .field("loading", randomAsciiOfLength(5))
                .endObject());
    }

    public void testMergeKeywordIntoStringWithBoost() throws IOException {
        Map<String, Object> expectedMapping = new HashMap<>();
        expectedMapping.put("type", "string");
        expectedMapping.put("index", "not_analyzed");
        expectedMapping.put("fielddata", false);
        expectedMapping.put("boost", 1.5);
        expectedMapping.put("norms", true); // Implied by having a boost
        mergeMappingStep(expectedMapping, b -> b.field("type", "string").field("index", "not_analyzed").field("boost", 1.5));
        mergeMappingStep(expectedMapping, b -> b.field("type", "keyword").field("boost", 1.5));
        expectedMapping.put("boost", 1.4);
        mergeMappingStep(expectedMapping, b -> b.field("type", "keyword").field("boost", 1.4));
    }

    public void testMergeKeywordIntoStringWithFields() throws IOException {
        Map<String, Object> expectedMapping = new HashMap<>();
        expectedMapping.put("type", "string");
        expectedMapping.put("index", "not_analyzed");
        expectedMapping.put("fielddata", false);
        Map<String, Object> expectedFields = new HashMap<>();
        expectedMapping.put("fields", expectedFields);
        Map<String, Object> expectedFoo = new HashMap<>();
        expectedFields.put("foo", expectedFoo);
        expectedFoo.put("type", "string");
        expectedFoo.put("analyzer", "standard");
        mergeMappingStep(expectedMapping, b -> b.field("type", "string").field("index", "not_analyzed")
                .startObject("fields")
                    .startObject("foo")
                        .field("type", "string")
                        .field("analyzer", "standard")
                    .endObject()
                .endObject());
        mergeMappingStep(expectedMapping, b -> b.field("type", "keyword")
                .startObject("fields")
                    .startObject("foo")
                        .field("type", "string")
                        .field("analyzer", "standard")
                    .endObject()
                .endObject());

        Map<String, Object> expectedBar = new HashMap<>();
        expectedFields.put("bar", expectedBar);
        expectedBar.put("type", "string");
        expectedBar.put("analyzer", "whitespace");
        mergeMappingStep(expectedMapping, b -> b.field("type", "string").field("index", "not_analyzed")
                .startObject("fields")
                    .startObject("foo")
                        .field("type", "string")
                        .field("analyzer", "standard")
                    .endObject()
                    .startObject("bar")
                        .field("type", "string")
                        .field("analyzer", "whitespace")
                    .endObject()
                .endObject());
        mergeMappingStep(expectedMapping, b -> b.field("type", "keyword")
                .startObject("fields")
                    .startObject("foo")
                        .field("type", "string")
                        .field("analyzer", "standard")
                    .endObject()
                    .startObject("bar")
                        .field("type", "string")
                        .field("analyzer", "whitespace")
                    .endObject()
                .endObject());
    }

    public void testMergeKeywordIntoStringWithCopyTo() throws IOException {
        Map<String, Object> expectedMapping = new HashMap<>();
        expectedMapping.put("type", "string");
        expectedMapping.put("index", "not_analyzed");
        expectedMapping.put("fielddata", false);
        expectedMapping.put("copy_to", singletonList("another_field"));
        mergeMappingStep(expectedMapping, b -> b.field("type", "string").field("index", "not_analyzed").field("copy_to", "another_field"));
        mergeMappingStep(expectedMapping, b -> b.field("type", "keyword").field("copy_to", "another_field"));
    }

    public void testMergeKeywordIntoStringWithIncludeInAll() throws IOException {
        Map<String, Object> expectedMapping = new HashMap<>();
        expectedMapping.put("type", "string");
        expectedMapping.put("index", "not_analyzed");
        expectedMapping.put("fielddata", false);
        expectedMapping.put("include_in_all", false);
        mergeMappingStep(expectedMapping, b -> b.field("type", "string").field("index", "not_analyzed").field("include_in_all", false));
        mergeMappingStep(expectedMapping, b -> b.field("type", "keyword").field("include_in_all", false));
    }

    public void testMergeKeywordIntoStringWithIgnoreAbove() throws IOException {
        Map<String, Object> expectedMapping = new HashMap<>();
        expectedMapping.put("type", "string");
        expectedMapping.put("index", "not_analyzed");
        expectedMapping.put("fielddata", false);
        expectedMapping.put("ignore_above", 128);
        mergeMappingStep(expectedMapping, b -> b.field("type", "string").field("index", "not_analyzed").field("ignore_above", 128));
        mergeMappingStep(expectedMapping, b -> b.field("type", "keyword").field("ignore_above", 128));
    }

    public void testMergeKeywordIntoStringWithIndexOptions() throws IOException {
        Map<String, Object> expectedMapping = new HashMap<>();
        expectedMapping.put("type", "string");
        expectedMapping.put("index", "not_analyzed");
        expectedMapping.put("fielddata", false);
        expectedMapping.put("index_options", "freqs");
        mergeMappingStep(expectedMapping, b -> b.field("type", "string").field("index", "not_analyzed").field("index_options", "freqs"));
        mergeMappingStep(expectedMapping, b -> b.field("type", "keyword").field("index_options", "freqs"));
    }

    public void testMergeKeywordIntoStringWithSimilarity() throws IOException {
        Map<String, Object> expectedMapping = new HashMap<>();
        expectedMapping.put("type", "string");
        expectedMapping.put("index", "not_analyzed");
        expectedMapping.put("fielddata", false);
        expectedMapping.put("similarity", "BM25");
        mergeMappingStep(expectedMapping, b -> b.field("type", "string").field("index", "not_analyzed").field("similarity", "BM25"));
        mergeMappingStep(expectedMapping, b -> b.field("type", "keyword").field("similarity", "BM25"));
    }

    public void testTextFieldAsStringWithUnsupportedField() throws IOException {
        String mapping = mappingForTestField(b -> b.field("type", "text").field("null_value", "kitten")).string();
        Exception e = expectThrows(IllegalArgumentException.class, () -> parser.parse("test_type", new CompressedXContent(mapping)));
        assertEquals("Automatic downgrade from [text] to [string] failed because parameters [null_value] are not supported for "
                + "automatic downgrades.", e.getMessage());
    }

    public void testMergeTextIntoString() throws IOException {
        Map<String, Object> expectedMapping = new HashMap<>();
        expectedMapping.put("type", "string");
        mergeMappingStep(expectedMapping, b -> b.field("type", "string"));
        mergeMappingStep(expectedMapping, b -> b.field("type", "text").field("fielddata", true));
    }

    public void testMergeTextIntoStringWithStore() throws IOException {
        Map<String, Object> expectedMapping = new HashMap<>();
        expectedMapping.put("type", "string");
        expectedMapping.put("store", true);
        mergeMappingStep(expectedMapping, b -> b.field("type", "string").field("store", true));
        mergeMappingStep(expectedMapping, b -> b.field("type", "text").field("store", true).field("fielddata", true));
    }

    public void testMergeTextIntoStringWithDocValues() throws IOException {
        Map<String, Object> expectedMapping = new HashMap<>();
        expectedMapping.put("type", "string");
        mergeMappingStep(expectedMapping, b -> b.field("type", "string").field("doc_values", false));
        mergeMappingStep(expectedMapping, b -> b.field("type", "text").field("doc_values", false).field("fielddata", true));
    }

    public void testMergeTextIntoStringWithNorms() throws IOException {
        Map<String, Object> expectedMapping = new HashMap<>();
        expectedMapping.put("type", "string");
        expectedMapping.put("norms", false);
        mergeMappingStep(expectedMapping, b -> b.field("type", "string").field("norms", false));
        mergeMappingStep(expectedMapping, b -> b.field("type", "text").field("norms", false).field("fielddata", true));
    }

    public void testMergeTextIntoStringWithBoost() throws IOException {
        Map<String, Object> expectedMapping = new HashMap<>();
        expectedMapping.put("type", "string");
        expectedMapping.put("boost", 1.5);
        mergeMappingStep(expectedMapping, b -> b.field("type", "string").field("boost", 1.5));
        mergeMappingStep(expectedMapping, b -> b.field("type", "text").field("boost", 1.5).field("fielddata", true));
        expectedMapping.put("boost", 1.4);
        mergeMappingStep(expectedMapping, b -> b.field("type", "text").field("boost", 1.4).field("fielddata", true));
    }

    public void testMergeTextIntoStringWithFields() throws IOException {
        Map<String, Object> expectedMapping = new HashMap<>();
        expectedMapping.put("type", "string");
        Map<String, Object> expectedFields = new HashMap<>();
        expectedMapping.put("fields", expectedFields);
        Map<String, Object> expectedFoo = new HashMap<>();
        expectedFields.put("foo", expectedFoo);
        expectedFoo.put("type", "string");
        expectedFoo.put("analyzer", "standard");
        mergeMappingStep(expectedMapping, b -> b.field("type", "string")
                .startObject("fields")
                    .startObject("foo")
                        .field("type", "string")
                        .field("analyzer", "standard")
                    .endObject()
                .endObject());
        mergeMappingStep(expectedMapping, b -> b.field("type", "text").field("fielddata", true)
                .startObject("fields")
                    .startObject("foo")
                        .field("type", "string")
                        .field("analyzer", "standard")
                    .endObject()
                .endObject());

        Map<String, Object> expectedBar = new HashMap<>();
        expectedFields.put("bar", expectedBar);
        expectedBar.put("type", "string");
        expectedBar.put("analyzer", "whitespace");
        mergeMappingStep(expectedMapping, b -> b.field("type", "string")
                .startObject("fields")
                    .startObject("foo")
                        .field("type", "string")
                        .field("analyzer", "standard")
                    .endObject()
                    .startObject("bar")
                        .field("type", "string")
                        .field("analyzer", "whitespace")
                    .endObject()
                .endObject());
        mergeMappingStep(expectedMapping, b -> b.field("type", "text").field("fielddata", true)
                .startObject("fields")
                    .startObject("foo")
                        .field("type", "string")
                        .field("analyzer", "standard")
                    .endObject()
                    .startObject("bar")
                        .field("type", "string")
                        .field("analyzer", "whitespace")
                    .endObject()
                .endObject());
    }

    public void testMergeTextIntoStringWithCopyTo() throws IOException {
        Map<String, Object> expectedMapping = new HashMap<>();
        expectedMapping.put("type", "string");
        expectedMapping.put("copy_to", singletonList("another_field"));
        mergeMappingStep(expectedMapping, b -> b.field("type", "string").field("copy_to", "another_field"));
        mergeMappingStep(expectedMapping, b -> b.field("type", "text").field("copy_to", "another_field").field("fielddata", true));
    }

    public void testMergeTextIntoStringWithFileddataDisabled() throws IOException {
        Map<String, Object> expectedMapping = new HashMap<>();
        expectedMapping.put("type", "string");
        expectedMapping.put("fielddata", false);
        mergeMappingStep(expectedMapping, b -> b.field("type", "string").field("fielddata", false));
        mergeMappingStep(expectedMapping, b -> b.field("type", "text"));
    }

    public void testMergeTextIntoStringWithEagerGlobalOrdinals() throws IOException {
        Map<String, Object> expectedMapping = new HashMap<>();
        expectedMapping.put("type", "string");
        expectedMapping.put("eager_global_ordinals", true);
        mergeMappingStep(expectedMapping, b -> b.field("type", "string").startObject("fielddata")
                    .field("format", "pagedbytes")
                    .field("loading", "eager_global_ordinals")
                .endObject());
        mergeMappingStep(expectedMapping, b -> b.field("type", "text").field("fielddata", true).field("eager_global_ordinals", true));
    }

    public void testMergeTextIntoStringWithFielddataFrequencyFilter() throws IOException {
        Map<String, Object> expectedMapping = new HashMap<>();
        expectedMapping.put("type", "string");
        Map<String, Object> fielddataFrequencyFilter = new HashMap<>();
        expectedMapping.put("fielddata_frequency_filter", fielddataFrequencyFilter);
        fielddataFrequencyFilter.put("min", 0.001);
        fielddataFrequencyFilter.put("max", 0.1);
        fielddataFrequencyFilter.put("min_segment_size", 100);
        mergeMappingStep(expectedMapping, b -> b.field("type", "string").startObject("fielddata")
                    .field("format", "pagedbytes")
                    .startObject("filter")
                        .startObject("frequency")
                            .field("min", 0.001)
                            .field("max", 0.1)
                            .field("min_segment_size", 100)
                        .endObject()
                    .endObject()
                .endObject());
        mergeMappingStep(expectedMapping, b -> b.field("type", "text").field("fielddata", true)
                .startObject("fielddata_frequency_filter")
                    .field("min", 0.001)
                    .field("max", 0.1)
                    .field("min_segment_size", 100)
                .endObject());
    }

    public void testMergeTextIntoStringWithIncludeInAll() throws IOException {
        Map<String, Object> expectedMapping = new HashMap<>();
        expectedMapping.put("type", "string");
        expectedMapping.put("include_in_all", false);
        mergeMappingStep(expectedMapping, b -> b.field("type", "string").field("include_in_all", false));
        mergeMappingStep(expectedMapping, b -> b.field("type", "text").field("include_in_all", false).field("fielddata", true));
    }

    public void testMergeTextIntoStringWithSearchQuoteAnayzer() throws IOException {
        Map<String, Object> expectedMapping = new HashMap<>();
        expectedMapping.put("type", "string");
        expectedMapping.put("analyzer", "standard");
        expectedMapping.put("search_analyzer", "whitespace");
        expectedMapping.put("search_quote_analyzer", "keyword");
        mergeMappingStep(expectedMapping, b -> b
                .field("type", "string")
                .field("analyzer", "standard")
                .field("search_analyzer", "whitespace")
                .field("search_quote_analyzer", "keyword"));
        mergeMappingStep(expectedMapping, b -> b
                .field("type", "text")
                .field("analyzer", "standard")
                .field("search_analyzer", "whitespace")
                .field("search_quote_analyzer", "keyword")
                .field("fielddata", true));
    }

    public void testMergeTextIntoStringWithIndexOptions() throws IOException {
        String indexOptions = randomIndexOptions();
        Map<String, Object> expectedMapping = new HashMap<>();
        expectedMapping.put("type", "string");
        if (false == "positions".equals(indexOptions)) {
            expectedMapping.put("index_options", indexOptions);
        }
        mergeMappingStep(expectedMapping, b -> b.field("type", "string").field("index_options", indexOptions));
        mergeMappingStep(expectedMapping, b -> b.field("type", "text").field("index_options", indexOptions).field("fielddata", true));
    }

    public void testMergeTextIntoStringWithPositionIncrementGap() throws IOException {
        int positionIncrementGap = between(0, 10000);
        Map<String, Object> expectedMapping = new HashMap<>();
        expectedMapping.put("type", "string");
        expectedMapping.put("position_increment_gap", positionIncrementGap);
        mergeMappingStep(expectedMapping, b -> b
                .field("type", "string")
                .field("position_increment_gap", positionIncrementGap));
        mergeMappingStep(expectedMapping, b -> b
                .field("type", "text")
                .field("position_increment_gap", positionIncrementGap)
                .field("fielddata", true));
    }

    public void testMergeStringIntoStringWithSimilarity() throws IOException {
        Map<String, Object> expectedMapping = new HashMap<>();
        expectedMapping.put("type", "string");
        expectedMapping.put("similarity", "BM25");
        mergeMappingStep(expectedMapping, b -> b.field("type", "string").field("similarity", "BM25"));
        mergeMappingStep(expectedMapping, b -> b.field("type", "text").field("similarity", "BM25").field("fielddata", true));
    }

    private interface FieldBuilder {
        void populateMappingForField(XContentBuilder b) throws IOException;
    }
    private void mergeMappingStep(Map<String, Object> expectedMapping, FieldBuilder fieldBuilder) throws IOException {
        XContentBuilder b = mappingForTestField(fieldBuilder);
        if (logger.isInfoEnabled()) {
            logger.info("--> Updating mapping to {}", b.string());
        }
        assertAcked(client().admin().indices().preparePutMapping("test").setType("test_type").setSource(b));
        GetMappingsResponse response = client().admin().indices().prepareGetMappings("test").get();
        ImmutableOpenMap<String, MappingMetaData> index = response.getMappings().get("test");
        assertNotNull("mapping for index not found", index);
        MappingMetaData type = index.get("test_type");
        assertNotNull("mapping for type not found", type);
        Map<?, ?> properties = (Map<?, ?>) type.sourceAsMap().get("properties");
        assertEquals(expectedMapping, properties.get("test_field"));
    }

    private XContentBuilder mappingForTestField(FieldBuilder fieldBuilder) throws IOException {
        XContentBuilder b = JsonXContent.contentBuilder();
        b.startObject(); {
            b.startObject("test_type"); {
                b.startObject("properties"); {
                    b.startObject("test_field"); {
                        fieldBuilder.populateMappingForField(b);
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endObject();
        }
        return b.endObject();
    }

    private String randomIndexOptions() {
        IndexOptions options = randomValueOtherThan(IndexOptions.NONE, () -> randomFrom(IndexOptions.values()));
        switch (options) {
        case DOCS:
            return "docs";
        case DOCS_AND_FREQS:
            return "freqs";
        case DOCS_AND_FREQS_AND_POSITIONS:
            return "positions";
        case DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS:
            return "offsets";
        default:
            throw new IllegalArgumentException("Unknown options [" + options + "]");
        }
    }
}
