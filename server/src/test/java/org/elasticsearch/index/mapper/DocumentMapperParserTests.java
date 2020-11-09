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
