/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.IndexableField;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class SourceFieldMapperTests extends MetadataMapperTestCase {

    @Override
    protected String fieldName() {
        return SourceFieldMapper.NAME;
    }

    @Override
    protected boolean isConfigurable() {
        return true;
    }

    @Override
    protected void registerParameters(ParameterChecker checker) throws IOException {
        checker.registerConflictCheck(
            "enabled",
            topMapping(b -> b.startObject(SourceFieldMapper.NAME).field("enabled", false).endObject()),
            topMapping(b -> b.startObject(SourceFieldMapper.NAME).field("enabled", true).endObject())
        );
        checker.registerUpdateCheck(
            topMapping(b -> b.startObject(SourceFieldMapper.NAME).field("enabled", true).endObject()),
            topMapping(b -> b.startObject(SourceFieldMapper.NAME).field("enabled", false).endObject()),
            dm -> assertFalse(dm.metadataMapper(SourceFieldMapper.class).enabled())
        );
        checker.registerUpdateCheck(
            topMapping(b -> b.startObject(SourceFieldMapper.NAME).field("mode", "stored").endObject()),
            topMapping(b -> b.startObject(SourceFieldMapper.NAME).field("mode", "synthetic").endObject()),
            dm -> assertTrue(dm.metadataMapper(SourceFieldMapper.class).isSynthetic())
        );
        checker.registerConflictCheck("includes", b -> b.array("includes", "foo*"));
        checker.registerConflictCheck("excludes", b -> b.array("excludes", "foo*"));
        checker.registerConflictCheck(
            "mode",
            topMapping(b -> b.startObject(SourceFieldMapper.NAME).field("mode", "synthetic").endObject()),
            topMapping(b -> b.startObject(SourceFieldMapper.NAME).field("mode", "stored").endObject())
        );
    }

    public void testNoFormat() throws Exception {

        DocumentMapper documentMapper = createDocumentMapper(topMapping(b -> b.startObject("_source").endObject()));
        ParsedDocument doc = documentMapper.parse(
            new SourceToParse(
                "1",
                BytesReference.bytes(XContentFactory.jsonBuilder().startObject().field("field", "value").endObject()),
                XContentType.JSON
            )
        );

        assertThat(XContentFactory.xContentType(doc.source().toBytesRef().bytes), equalTo(XContentType.JSON));

        doc = documentMapper.parse(
            new SourceToParse(
                "1",
                BytesReference.bytes(XContentFactory.smileBuilder().startObject().field("field", "value").endObject()),
                XContentType.SMILE
            )
        );

        assertThat(XContentHelper.xContentType(doc.source()), equalTo(XContentType.SMILE));
    }

    public void testIncludes() throws Exception {
        DocumentMapper documentMapper = createDocumentMapper(
            topMapping(b -> b.startObject("_source").array("includes", "path1*").endObject())
        );

        ParsedDocument doc = documentMapper.parse(source(b -> {
            b.startObject("path1").field("field1", "value1").endObject();
            b.startObject("path2").field("field2", "value2").endObject();
        }));

        IndexableField sourceField = doc.rootDoc().getField("_source");
        Map<String, Object> sourceAsMap;
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, new BytesArray(sourceField.binaryValue()))) {
            sourceAsMap = parser.map();
        }
        assertThat(sourceAsMap.containsKey("path1"), equalTo(true));
        assertThat(sourceAsMap.containsKey("path2"), equalTo(false));
    }

    public void testDuplicatedIncludes() throws Exception {
        DocumentMapper documentMapper = createDocumentMapper(
            topMapping(b -> b.startObject("_source").array("includes", "path1", "path1").endObject())
        );

        ParsedDocument doc = documentMapper.parse(source(b -> {
            b.startObject("path1").field("field1", "value1").endObject();
            b.startObject("path2").field("field2", "value2").endObject();
        }));

        IndexableField sourceField = doc.rootDoc().getField("_source");
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, new BytesArray(sourceField.binaryValue()))) {
            assertEquals(Map.of("path1", Map.of("field1", "value1")), parser.map());
        }
    }

    public void testExcludes() throws Exception {
        DocumentMapper documentMapper = createDocumentMapper(
            topMapping(b -> b.startObject("_source").array("excludes", "path1*").endObject())
        );

        ParsedDocument doc = documentMapper.parse(source(b -> {
            b.startObject("path1").field("field1", "value1").endObject();
            b.startObject("path2").field("field2", "value2").endObject();
        }));

        IndexableField sourceField = doc.rootDoc().getField("_source");
        Map<String, Object> sourceAsMap;
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, new BytesArray(sourceField.binaryValue()))) {
            sourceAsMap = parser.map();
        }
        assertThat(sourceAsMap.containsKey("path1"), equalTo(false));
        assertThat(sourceAsMap.containsKey("path2"), equalTo(true));
    }

    public void testDuplicatedExcludes() throws Exception {
        DocumentMapper documentMapper = createDocumentMapper(
            topMapping(b -> b.startObject("_source").array("excludes", "path1", "path1").endObject())
        );

        ParsedDocument doc = documentMapper.parse(source(b -> {
            b.startObject("path1").field("field1", "value1").endObject();
            b.startObject("path2").field("field2", "value2").endObject();
        }));

        IndexableField sourceField = doc.rootDoc().getField("_source");
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, new BytesArray(sourceField.binaryValue()))) {
            assertEquals(Map.of("path2", Map.of("field2", "value2")), parser.map());
        }
    }

    public void testComplete() throws Exception {

        assertTrue(createDocumentMapper(topMapping(b -> {})).sourceMapper().isComplete());

        assertFalse(
            createDocumentMapper(topMapping(b -> b.startObject("_source").field("enabled", false).endObject())).sourceMapper().isComplete()
        );

        assertFalse(
            createDocumentMapper(topMapping(b -> b.startObject("_source").array("includes", "foo*").endObject())).sourceMapper()
                .isComplete()
        );

        assertFalse(
            createDocumentMapper(topMapping(b -> b.startObject("_source").array("excludes", "foo*").endObject())).sourceMapper()
                .isComplete()
        );
    }

    public void testSourceObjectContainsExtraTokens() throws Exception {
        DocumentMapper documentMapper = createDocumentMapper(mapping(b -> {}));

        Exception exception = expectThrows(
            DocumentParsingException.class,
            // extra end object (invalid JSON))
            () -> documentMapper.parse(new SourceToParse("1", new BytesArray("{}}"), XContentType.JSON))
        );
        assertNotNull(exception.getCause());
        assertThat(exception.getCause().getMessage(), containsString("Unexpected close marker '}'"));
    }

    public void testSyntheticDisabledNotSupported() {
        Exception e = expectThrows(
            MapperParsingException.class,
            () -> createDocumentMapper(
                topMapping(b -> b.startObject("_source").field("enabled", false).field("mode", "synthetic").endObject())
            )
        );
        assertThat(e.getMessage(), containsString("Cannot set both [mode] and [enabled] parameters"));
    }

    public void testSyntheticUpdates() throws Exception {
        MapperService mapperService = createMapperService("""
            { "_doc" : { "_source" : { "mode" : "synthetic" } } }
            """);

        SourceFieldMapper mapper = mapperService.documentMapper().sourceMapper();
        assertTrue(mapper.enabled());
        assertTrue(mapper.isSynthetic());

        merge(mapperService, """
            { "_doc" : { "_source" : { "mode" : "synthetic" } } }
            """);
        mapper = mapperService.documentMapper().sourceMapper();
        assertTrue(mapper.enabled());
        assertTrue(mapper.isSynthetic());

        ParsedDocument doc = mapperService.documentMapper().parse(source("{}"));
        assertNull(doc.rootDoc().get(SourceFieldMapper.NAME));

        Exception e = expectThrows(IllegalArgumentException.class, () -> merge(mapperService, """
            { "_doc" : { "_source" : { "mode" : "stored" } } }
            """));
        assertThat(e.getMessage(), containsString("Cannot update parameter [mode] from [synthetic] to [stored]"));

        merge(mapperService, """
            { "_doc" : { "_source" : { "mode" : "disabled" } } }
            """);
        mapper = mapperService.documentMapper().sourceMapper();
        assertFalse(mapper.enabled());
        assertFalse(mapper.isSynthetic());
    }
}
