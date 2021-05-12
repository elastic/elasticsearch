/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.index.mapper.flattened.FlattenedFieldMapper;

import java.io.IOException;

public class RuntimeAliasTests extends MapperServiceTestCase {

    public void testSimpleAlias() throws IOException {
        MapperService mapperService = createMapperService(topMapping(b -> {
            b.startObject("runtime");
            {
                b.startObject("alias-to-field").field("type", "alias").field("path", "field").endObject();
            }
            b.endObject();
            b.startObject("properties");
            {
                b.startObject("field").field("type", "keyword").endObject();
            }
            b.endObject();
        }));

        MappedFieldType aliased = mapperService.mappingLookup().getFieldType("alias-to-field");
        assertEquals("field", aliased.name());
        assertEquals(KeywordFieldMapper.KeywordFieldType.class, aliased.getClass());
    }

    public void testInvalidAlias() {
        Exception e = expectThrows(IllegalStateException.class, () -> createMapperService(topMapping(b -> {
            b.startObject("runtime");
            {
                b.startObject("alias-to-field").field("type", "alias").field("path", "field").endObject();
            }
            b.endObject();
        })));
        assertEquals("Cannot resolve alias [alias-to-field]: path [field] does not exist", e.getMessage());
    }

    public void testDynamicLookup() throws IOException {
        MapperService mapperService = createMapperService(topMapping(b -> {
            b.startObject("runtime");
            {
                b.startObject("dynamic-alias").field("type", "alias").field("path", "flattened").endObject();
            }
            b.endObject();
            b.startObject("properties");
            {
                b.startObject("flattened").field("type", "flattened").endObject();
            }
            b.endObject();
        }));

        MappedFieldType dynamic = mapperService.fieldType("flattened.key");
        assertEquals("flattened._keyed", dynamic.name());
        MappedFieldType aliased = mapperService.fieldType("dynamic-alias.key");
        assertNotNull(aliased);
        assertEquals("flattened._keyed", aliased.name());
        FlattenedFieldMapper.KeyedFlattenedFieldType keyed = (FlattenedFieldMapper.KeyedFlattenedFieldType) aliased;
        assertEquals("key", keyed.key());
    }

}
