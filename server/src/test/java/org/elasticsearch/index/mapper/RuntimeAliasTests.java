/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

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

    public void testInvalidAlias() throws IOException {
        MapperService mapperService = createMapperService(topMapping(b -> {
            b.startObject("runtime");
            {
                b.startObject("alias-to-field").field("type", "alias").field("path", "field").endObject();
            }
            b.endObject();
        }));
        Exception e = expectThrows(IllegalStateException.class, () -> mapperService.mappingLookup().getFieldType("alias-to-field"));
        assertEquals("Cannot resolve alias [alias-to-field]: path [field] does not exist", e.getMessage());
    }

    public void testAliasToAlias() throws IOException {
        MapperService mapperService = createMapperService(topMapping(b -> {
            b.startObject("runtime");
            {
                b.startObject("alias-to-field").field("type", "alias").field("path", "field").endObject();
                b.startObject("alias-to-alias").field("type", "alias").field("path", "alias-to-field").endObject();
            }
            b.endObject();
            b.startObject("properties");
            {
                b.startObject("field").field("type", "keyword").endObject();
            }
            b.endObject();
        }));

        MappedFieldType aliased = mapperService.mappingLookup().getFieldType("alias-to-alias");
        assertEquals("field", aliased.name());
        assertEquals(KeywordFieldMapper.KeywordFieldType.class, aliased.getClass());
    }

    public void testAliasLoops() throws IOException {
        MapperService mapperService = createMapperService(topMapping(b -> {
            b.startObject("runtime");
            {
                b.startObject("alias-loop1").field("type", "alias").field("path", "alias-loop2").endObject();
                b.startObject("alias-loop2").field("type", "alias").field("path", "alias-loop1").endObject();
            }
            b.endObject();
            b.startObject("properties");
            {
                b.startObject("field").field("type", "keyword").endObject();
            }
            b.endObject();
        }));

        Exception e = expectThrows(IllegalStateException.class, () -> mapperService.mappingLookup().getFieldType("alias-loop1"));
        assertEquals("Loop in field resolution detected: alias-loop1->alias-loop2->alias-loop1", e.getMessage());

    }

}
