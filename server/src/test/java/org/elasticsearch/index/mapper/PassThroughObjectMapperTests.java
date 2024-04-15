/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class PassThroughObjectMapperTests extends MapperServiceTestCase {

    public void testSimpleKeyword() throws IOException {
        MapperService mapperService = createMapperService(mapping(b -> {
            b.startObject("labels").field("type", "passthrough");
            {
                b.startObject("properties");
                b.startObject("dim").field("type", "keyword").endObject();
                b.endObject();
            }
            b.endObject();
        }));
        Mapper mapper = mapperService.mappingLookup().getMapper("labels.dim");
        assertThat(mapper, instanceOf(KeywordFieldMapper.class));
        assertFalse(((KeywordFieldMapper) mapper).fieldType().isDimension());
    }

    public void testKeywordDimension() throws IOException {
        MapperService mapperService = createMapperService(mapping(b -> {
            b.startObject("labels").field("type", "passthrough").field("time_series_dimension", "true");
            {
                b.startObject("properties");
                b.startObject("dim").field("type", "keyword").endObject();
                b.endObject();
            }
            b.endObject();
        }));
        Mapper mapper = mapperService.mappingLookup().getMapper("labels.dim");
        assertThat(mapper, instanceOf(KeywordFieldMapper.class));
        assertTrue(((KeywordFieldMapper) mapper).fieldType().isDimension());
    }

    public void testDynamic() throws IOException {
        MapperService mapperService = createMapperService(mapping(b -> {
            b.startObject("labels").field("type", "passthrough").field("dynamic", "true");
            {
                b.startObject("properties");
                b.startObject("dim").field("type", "keyword").endObject();
                b.endObject();
            }
            b.endObject();
        }));
        PassThroughObjectMapper mapper = (PassThroughObjectMapper) mapperService.mappingLookup().objectMappers().get("labels");
        assertEquals(ObjectMapper.Dynamic.TRUE, mapper.dynamic());
    }

    public void testEnabled() throws IOException {
        MapperService mapperService = createMapperService(mapping(b -> {
            b.startObject("labels").field("type", "passthrough").field("enabled", "false");
            {
                b.startObject("properties");
                b.startObject("dim").field("type", "keyword").endObject();
                b.endObject();
            }
            b.endObject();
        }));
        PassThroughObjectMapper mapper = (PassThroughObjectMapper) mapperService.mappingLookup().objectMappers().get("labels");
        assertEquals(false, mapper.isEnabled());
    }

    public void testSubobjectsThrows() throws IOException {
        MapperException exception = expectThrows(MapperException.class, () -> createMapperService(mapping(b -> {
            b.startObject("labels").field("type", "passthrough").field("subobjects", "true");
            {
                b.startObject("properties");
                b.startObject("dim").field("type", "keyword").endObject();
                b.endObject();
            }
            b.endObject();
        })));

        assertEquals(
            "Failed to parse mapping: Mapping definition for [labels] has unsupported parameters:  [subobjects : true]",
            exception.getMessage()
        );
    }

    public void testAddSubobjectAutoFlatten() throws IOException {
        MapperService mapperService = createMapperService(mapping(b -> {
            b.startObject("labels").field("type", "passthrough").field("time_series_dimension", "true");
            {
                b.startObject("properties");
                {
                    b.startObject("subobj").field("type", "object");
                    {
                        b.startObject("properties");
                        b.startObject("dim").field("type", "keyword").endObject();
                        b.endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endObject();
        }));

        var dim = mapperService.mappingLookup().getMapper("labels.subobj.dim");
        assertThat(dim, instanceOf(KeywordFieldMapper.class));
        assertTrue(((KeywordFieldMapper) dim).fieldType().isDimension());
    }

    public void testWithoutMappers() throws IOException {
        MapperService mapperService = createMapperService(mapping(b -> {
            b.startObject("labels").field("type", "passthrough");
            {
                b.startObject("properties");
                b.startObject("dim").field("type", "keyword").endObject();
                b.endObject();
            }
            b.endObject();
            b.startObject("shallow").field("type", "passthrough");
            b.endObject();
        }));

        var labels = mapperService.mappingLookup().objectMappers().get("labels");
        var shallow = mapperService.mappingLookup().objectMappers().get("shallow");
        assertThat(labels.withoutMappers().toString(), equalTo(shallow.toString().replace("shallow", "labels")));
    }
}
