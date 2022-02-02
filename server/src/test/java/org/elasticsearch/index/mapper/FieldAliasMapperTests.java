/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.common.Strings;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;

public class FieldAliasMapperTests extends MapperServiceTestCase {

    public void testParsing() throws IOException {
        String mapping = Strings.toString(
            XContentFactory.jsonBuilder()
                .startObject()
                .startObject("_doc")
                .startObject("properties")
                .startObject("alias-field")
                .field("type", "alias")
                .field("path", "concrete-field")
                .endObject()
                .startObject("concrete-field")
                .field("type", "keyword")
                .endObject()
                .endObject()
                .endObject()
                .endObject()
        );
        DocumentMapper mapper = createDocumentMapper(mapping);
        assertEquals(mapping, mapper.mappingSource().toString());
    }

    public void testParsingWithMissingPath() {
        MapperParsingException exception = expectThrows(
            MapperParsingException.class,
            () -> createDocumentMapper(mapping(b -> b.startObject("alias-field").field("type", "alias").endObject()))
        );
        assertEquals("Failed to parse mapping: The [path] property must be specified for field [alias-field].", exception.getMessage());
    }

    public void testParsingWithExtraArgument() {
        MapperParsingException exception = expectThrows(MapperParsingException.class, () -> createDocumentMapper(mapping(b -> {
            b.startObject("alias-field");
            {
                b.field("type", "alias");
                b.field("path", "concrete-field");
                b.field("extra-field", "extra-value");
            }
            b.endObject();
        })));
        assertEquals(
            "Failed to parse mapping: " + "Mapping definition for [alias-field] has unsupported parameters:  [extra-field : extra-value]",
            exception.getMessage()
        );
    }

    public void testMerge() throws IOException {
        MapperService mapperService = createMapperService(mapping(b -> {
            b.startObject("first-field").field("type", "keyword").endObject();
            b.startObject("alias-field");
            {
                b.field("type", "alias");
                b.field("path", "first-field");
            }
            b.endObject();
        }));

        MappedFieldType firstFieldType = mapperService.fieldType("alias-field");
        assertEquals("first-field", firstFieldType.name());
        assertTrue(firstFieldType instanceof KeywordFieldMapper.KeywordFieldType);

        merge(mapperService, mapping(b -> {
            b.startObject("second-field").field("type", "text").endObject();
            b.startObject("alias-field");
            {
                b.field("type", "alias");
                b.field("path", "second-field");
            }
            b.endObject();
        }));

        MappedFieldType secondFieldType = mapperService.fieldType("alias-field");
        assertEquals("second-field", secondFieldType.name());
        assertTrue(secondFieldType instanceof TextFieldMapper.TextFieldType);
    }

    public void testMergeFailure() throws IOException {

        MapperService mapperService = createMapperService(mapping(b -> {
            b.startObject("concrete-field").field("type", "text").endObject();
            b.startObject("alias-field");
            {
                b.field("type", "alias");
                b.field("path", "concrete-field");
            }
            b.endObject();
        }));

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> merge(mapperService, mapping(b -> {
            b.startObject("alias-field");
            {
                b.field("type", "keyword");
            }
            b.endObject();
        })));
        assertEquals(
            "Cannot merge a field alias mapping [alias-field] with a mapping that is not for a field alias.",
            exception.getMessage()
        );
    }
}
