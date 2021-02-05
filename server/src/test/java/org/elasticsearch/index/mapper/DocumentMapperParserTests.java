/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

public class DocumentMapperParserTests extends MapperServiceTestCase {

    public void testFieldNameWithDots() throws Exception {
        DocumentMapper docMapper = createDocumentMapper(mapping(b -> {
            b.startObject("foo.bar").field("type", "text").endObject();
            b.startObject("foo.baz").field("type", "keyword").endObject();
        }));
        assertNotNull(docMapper.mappers().getMapper("foo.bar"));
        assertNotNull(docMapper.mappers().getMapper("foo.baz"));
        assertNotNull(docMapper.mappers().objectMappers().get("foo"));
    }

    public void testFieldNameWithDeepDots() throws Exception {
        DocumentMapper docMapper = createDocumentMapper(mapping(b -> {
            b.startObject("foo.bar").field("type", "text").endObject();
            b.startObject("foo.baz");
            {
                b.startObject("properties");
                {
                    b.startObject("deep.field").field("type", "keyword").endObject();
                }
                b.endObject();
            }
            b.endObject();
        }));
        assertNotNull(docMapper.mappers().getMapper("foo.bar"));
        assertNotNull(docMapper.mappers().getMapper("foo.baz.deep.field"));
        assertNotNull(docMapper.mappers().objectMappers().get("foo"));
    }

    public void testFieldNameWithDotsConflict() {
        MapperParsingException e = expectThrows(MapperParsingException.class, () -> createDocumentMapper(mapping(b -> {
            b.startObject("foo").field("type", "text").endObject();
            b.startObject("foo.baz").field("type", "keyword").endObject();
        })));
        assertTrue(e.getMessage(), e.getMessage().contains("mapper [foo] cannot be changed from type [text] to [ObjectMapper]"));
    }

    public void testMultiFieldsWithFieldAlias() {
        MapperParsingException e = expectThrows(MapperParsingException.class, () -> createDocumentMapper(mapping(b -> {
            b.startObject("field");
            {
                b.field("type", "text");
                b.startObject("fields");
                {
                    b.startObject("alias");
                    {
                        b.field("type", "alias");
                        b.field("path", "other-field");
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endObject();
            b.startObject("other-field").field("type", "keyword").endObject();
        })));
        assertEquals("Failed to parse mapping: Type [alias] cannot be used in multi field", e.getMessage());
    }
}
