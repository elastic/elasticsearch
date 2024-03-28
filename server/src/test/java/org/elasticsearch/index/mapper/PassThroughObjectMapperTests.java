/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.common.Explicit;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
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

    public void testSupersededByOne() throws IOException {
        MapperService mapperService = createMapperService(mapping(b -> {
            b.startObject("labels").field("type", "passthrough").field("superseded_by", "foo");
            {
                b.startObject("properties");
                b.startObject("dim").field("type", "keyword").endObject();
                b.endObject();
            }
            b.endObject();
        }));
        Mapper mapper = mapperService.mappingLookup().objectMappers().get("labels");
        assertThat(mapper, instanceOf(PassThroughObjectMapper.class));
        assertThat(((PassThroughObjectMapper) mapper).supersededBy(), contains("foo"));
    }

    public void testSupersededByMany() throws IOException {
        MapperService mapperService = createMapperService(mapping(b -> {
            b.startObject("labels").field("type", "passthrough").field("superseded_by", "far,too,many,deps,so,many");
            {
                b.startObject("properties");
                b.startObject("dim").field("type", "keyword").endObject();
                b.endObject();
            }
            b.endObject();
        }));
        Mapper mapper = mapperService.mappingLookup().objectMappers().get("labels");
        assertThat(mapper, instanceOf(PassThroughObjectMapper.class));
        assertThat(((PassThroughObjectMapper) mapper).supersededBy(), containsInAnyOrder("far", "too", "many", "deps", "so"));
    }

    public void testSupersededBySelfReference() throws IOException {
        MapperException exception = expectThrows(MapperException.class, () -> createMapperService(mapping(b -> {
            b.startObject("labels").field("type", "passthrough").field("superseded_by", "labels");
            {
                b.startObject("properties");
                b.startObject("dim").field("type", "keyword").endObject();
                b.endObject();
            }
            b.endObject();
        })));

        assertEquals(
            "Failed to parse mapping: Mapping definition for [labels] contains a self-reference in param [superseded_by]",
            exception.getMessage()
        );
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

    public void testCheckSupersedesForCircularDepsEmpty() throws IOException {
        assertTrue(PassThroughObjectMapper.checkSupersedesForCircularDeps(List.of()).isEmpty());
    }

    private PassThroughObjectMapper create(String name, Set<String> supersededBy) {
        return new PassThroughObjectMapper(
            name,
            name,
            Explicit.EXPLICIT_TRUE,
            ObjectMapper.Dynamic.FALSE,
            Map.of(),
            Explicit.EXPLICIT_FALSE,
            supersededBy
        );
    }

    public void testCheckSupersedesForCircularDepsOneElement() throws IOException {
        assertTrue(PassThroughObjectMapper.checkSupersedesForCircularDeps(List.of(create("foo", Set.of("bar")))).isEmpty());
    }

    public void testCheckSupersedesForCircularDepsTwoElementsNoDep() throws IOException {
        assertTrue(
            PassThroughObjectMapper.checkSupersedesForCircularDeps(
                List.of(create("foo", Set.of("A", "B", "C")), create("bar", Set.of("D", "E")))
            ).isEmpty()
        );
    }

    public void testCheckSupersedesForCircularDepsTwoElementsOneDep() throws IOException {
        assertTrue(
            PassThroughObjectMapper.checkSupersedesForCircularDeps(
                List.of(create("foo", Set.of("A", "B", "C")), create("bar", Set.of("foo", "D", "E")))
            ).isEmpty()
        );
    }

    public void testCheckSupersedesForCircularDepsTwoElementsCrossDep() throws IOException {
        assertThat(
            PassThroughObjectMapper.checkSupersedesForCircularDeps(
                List.of(create("foo", Set.of("A", "B", "bar")), create("bar", Set.of("foo", "D", "E")))
            ).stream().map(PassThroughObjectMapper::name).collect(Collectors.toList()),
            contains("foo", "bar", "foo")
        );
    }

    public void testCheckSupersedesForCircularDepsManyElementsWithCircle() throws IOException {
        assertThat(
            PassThroughObjectMapper.checkSupersedesForCircularDeps(
                List.of(
                    create("A", Set.of("B", "D")),
                    create("B", Set.of("E", "G")),
                    create("C", Set.of()),
                    create("D", Set.of("C")),
                    create("E", Set.of("C")),
                    create("F", Set.of("A", "B")),
                    create("G", Set.of("C", "H")),
                    create("H", Set.of("A", "C"))
                )
            ).stream().map(PassThroughObjectMapper::name).collect(Collectors.toList()),
            contains("A", "B", "G", "H", "A")
        );
    }

    public void testCheckSupersedesForCircularDepsManyElementsNoCircle() throws IOException {
        assertTrue(
            PassThroughObjectMapper.checkSupersedesForCircularDeps(
                List.of(
                    create("A", Set.of("B", "D")),
                    create("B", Set.of("E", "G")),
                    create("C", Set.of()),
                    create("D", Set.of("C")),
                    create("E", Set.of("C")),
                    create("F", Set.of("A", "B")),
                    create("G", Set.of("C", "H")),
                    create("H", Set.of("C", "E"))
                )
            ).isEmpty()
        );
    }
}
