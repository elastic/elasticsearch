/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;

import static org.hamcrest.Matchers.contains;

public class DocumentParserContextTests extends ESTestCase {

    private TestDocumentParserContext context = new TestDocumentParserContext();
    private final MapperBuilderContext root = MapperBuilderContext.root(false, false);

    public void testDynamicMapperSizeMultipleMappers() {
        context.addDynamicMapper(new TextFieldMapper.Builder("foo", createDefaultIndexAnalyzers(), false).build(root));
        assertEquals(1, context.getNewFieldsSize());
        context.addDynamicMapper(new TextFieldMapper.Builder("bar", createDefaultIndexAnalyzers(), false).build(root));
        assertEquals(2, context.getNewFieldsSize());
        context.addDynamicRuntimeField(new TestRuntimeField("runtime1", "keyword"));
        assertEquals(3, context.getNewFieldsSize());
        context.addDynamicRuntimeField(new TestRuntimeField("runtime2", "keyword"));
        assertEquals(4, context.getNewFieldsSize());
    }

    public void testDynamicMapperSizeSameFieldMultipleRuntimeFields() {
        context.addDynamicRuntimeField(new TestRuntimeField("foo", "keyword"));
        context.addDynamicRuntimeField(new TestRuntimeField("foo", "keyword"));
        assertEquals(context.getNewFieldsSize(), 1);
    }

    public void testDynamicMapperSizeSameFieldMultipleMappers() {
        context.addDynamicMapper(new TextFieldMapper.Builder("foo", createDefaultIndexAnalyzers(), false).build(root));
        assertEquals(1, context.getNewFieldsSize());
        context.addDynamicMapper(new TextFieldMapper.Builder("foo", createDefaultIndexAnalyzers(), false).build(root));
        assertEquals(1, context.getNewFieldsSize());
    }

    public void testAddRuntimeFieldWhenLimitIsReachedViaMapper() {
        context = new TestDocumentParserContext(
            Settings.builder()
                .put("index.mapping.total_fields.limit", 1)
                .put("index.mapping.total_fields.ignore_dynamic_beyond_limit", true)
                .build()
        );
        assertTrue(context.addDynamicMapper(new KeywordFieldMapper.Builder("keyword_field", IndexVersion.current()).build(root)));
        assertFalse(context.addDynamicRuntimeField(new TestRuntimeField("runtime_field", "keyword")));
        assertThat(context.getIgnoredFields(), contains("runtime_field"));
    }

    public void testAddFieldWhenLimitIsReachedViaRuntimeField() {
        context = new TestDocumentParserContext(
            Settings.builder()
                .put("index.mapping.total_fields.limit", 1)
                .put("index.mapping.total_fields.ignore_dynamic_beyond_limit", true)
                .build()
        );
        assertTrue(context.addDynamicRuntimeField(new TestRuntimeField("runtime_field", "keyword")));
        assertFalse(context.addDynamicMapper(new KeywordFieldMapper.Builder("keyword_field", IndexVersion.current()).build(root)));
        assertThat(context.getIgnoredFields(), contains("keyword_field"));
    }

    public void testSwitchParser() throws IOException {
        var settings = Settings.builder().put("index.mapping.total_fields.limit", 1).build();
        context = new TestDocumentParserContext(settings);
        XContentParser parser = createParser(JsonXContent.jsonXContent, "{ \"foo\": \"bar\" }");
        DocumentParserContext newContext = context.switchParser(parser);
        assertNotEquals(context.parser(), newContext.parser());
        assertEquals(context.indexSettings(), newContext.indexSettings());
        assertEquals(parser, newContext.parser());
        assertEquals("1", newContext.indexSettings().getSettings().get("index.mapping.total_fields.limit"));
    }

    public void testCreateDynamicMapperBuilderContextFromEmptyContext() throws IOException {
        var resultFromEmptyParserContext = context.createDynamicMapperBuilderContext();

        assertEquals("hey", resultFromEmptyParserContext.buildFullName("hey"));
        assertFalse(resultFromEmptyParserContext.isSourceSynthetic());
        assertFalse(resultFromEmptyParserContext.isDataStream());
        assertFalse(resultFromEmptyParserContext.parentObjectContainsDimensions());
        assertEquals(ObjectMapper.Defaults.DYNAMIC, resultFromEmptyParserContext.getDynamic());
        assertEquals(MapperService.MergeReason.MAPPING_UPDATE, resultFromEmptyParserContext.getMergeReason());
        assertFalse(resultFromEmptyParserContext.isInNestedContext());
    }

    public void testCreateDynamicMapperBuilderContext() throws IOException {
        var mapping = XContentBuilder.builder(XContentType.JSON.xContent())
            .startObject()
            .startObject("_doc")
            .startObject(DataStreamTimestampFieldMapper.NAME)
            .field("enabled", "true")
            .endObject()
            .startObject("properties")
            .startObject(DataStreamTimestampFieldMapper.DEFAULT_PATH)
            .field("type", "date")
            .endObject()
            .startObject("foo")
            .field("type", "passthrough")
            .field("time_series_dimension", "true")
            .field("priority", "100")
            .endObject()
            .endObject()
            .endObject()
            .endObject();
        var documentMapper = new MapperServiceTestCase() {

            @Override
            protected Settings getIndexSettings() {
                return Settings.builder().put("index.mapping.source.mode", "synthetic").build();
            }
        }.createDocumentMapper(mapping);
        var parserContext = new TestDocumentParserContext(documentMapper.mappers(), null);
        parserContext.path().add("foo");

        var resultFromParserContext = parserContext.createDynamicMapperBuilderContext();

        assertEquals("foo.hey", resultFromParserContext.buildFullName("hey"));
        assertTrue(resultFromParserContext.isDataStream());
        assertTrue(resultFromParserContext.parentObjectContainsDimensions());
        assertEquals(ObjectMapper.Defaults.DYNAMIC, resultFromParserContext.getDynamic());
        assertEquals(MapperService.MergeReason.MAPPING_UPDATE, resultFromParserContext.getMergeReason());
        assertFalse(resultFromParserContext.isInNestedContext());
    }
}
