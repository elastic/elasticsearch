/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

public class DynamicFieldsBuilderTests extends ESTestCase {

    public void testCreateDynamicField() throws IOException {
        assertCreateFieldType("f1", "foobar", "text");
        assertCreateFieldType("f1", "true", "text");
        assertCreateFieldType("f1", true, "boolean");
        assertCreateFieldType("f1", 123456, "long");
        assertCreateFieldType("f1", 123.456, "float");
        // numeric detection for strings is off by default
        assertCreateFieldType("f1", "123456", "text");
        assertCreateFieldType("f1", "2023-02-25", "date");
        // illegal dates should result in text field mapping
        assertCreateFieldType("f1", "2023-51", "text");
    }

    public void assertCreateFieldType(String fieldname, Object value, String expectedType) throws IOException {
        if (value instanceof String) {
            value = "\"" + value + "\"";
        }
        String source = "{\"" + fieldname + "\": " + value + "}";
        XContentParser parser = createParser(JsonXContent.jsonXContent, source);
        SourceToParse sourceToParse = new SourceToParse("test", new BytesArray(source), XContentType.JSON);
        DocumentParserContext ctx = new TestDocumentParserContext(MappingLookup.EMPTY, sourceToParse) {
            @Override
            public XContentParser parser() {
                return parser;
            }
        };

        // position the parser on the value
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
        ensureExpectedToken(XContentParser.Token.FIELD_NAME, parser.nextToken(), parser);
        parser.nextToken();
        assertTrue(parser.currentToken().isValue());
        DynamicFieldsBuilder.DYNAMIC_TRUE.createDynamicFieldFromValue(ctx, fieldname);
        List<Mapper> dynamicMappers = ctx.getDynamicMappers();
        assertEquals(1, dynamicMappers.size());
        assertEquals(fieldname, dynamicMappers.get(0).fullPath());
        assertEquals(expectedType, dynamicMappers.get(0).typeName());
    }

    public void testCreateDynamicStringFieldAsKeywordForDimension() throws IOException {
        String source = "{\"f1\": \"foobar\"}";
        XContentParser parser = createParser(JsonXContent.jsonXContent, source);
        SourceToParse sourceToParse = new SourceToParse("test", new BytesArray(source), XContentType.JSON);

        SourceFieldMapper sourceMapper = new SourceFieldMapper.Builder(null, Settings.EMPTY, false).setSynthetic().build();
        RootObjectMapper root = new RootObjectMapper.Builder("_doc", Optional.empty()).add(
            new PassThroughObjectMapper.Builder("labels").setPriority(0).setContainsDimensions().dynamic(ObjectMapper.Dynamic.TRUE)
        ).build(MapperBuilderContext.root(false, false));
        Mapping mapping = new Mapping(root, new MetadataFieldMapper[] { sourceMapper }, Map.of());

        DocumentParserContext ctx = new TestDocumentParserContext(MappingLookup.fromMapping(mapping), sourceToParse) {
            @Override
            public XContentParser parser() {
                return parser;
            }
        };
        ctx.path().add("labels");

        // position the parser on the value
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
        ensureExpectedToken(XContentParser.Token.FIELD_NAME, parser.nextToken(), parser);
        parser.nextToken();
        assertTrue(parser.currentToken().isValue());
        DynamicFieldsBuilder.DYNAMIC_TRUE.createDynamicFieldFromValue(ctx, "f1");
        List<Mapper> dynamicMappers = ctx.getDynamicMappers();
        assertEquals(1, dynamicMappers.size());
        assertEquals("labels.f1", dynamicMappers.get(0).fullPath());
        assertEquals("keyword", dynamicMappers.get(0).typeName());
    }
}
