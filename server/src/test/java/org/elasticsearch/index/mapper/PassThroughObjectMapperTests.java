/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.common.Explicit;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.elasticsearch.index.mapper.MapperService.MergeReason.MAPPING_UPDATE;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class PassThroughObjectMapperTests extends MapperServiceTestCase {

    public void testSimpleKeyword() throws IOException {
        MapperService mapperService = createMapperService(mapping(b -> {
            b.startObject("labels").field("type", "passthrough").field("priority", "0");
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
            b.startObject("labels").field("type", "passthrough").field("priority", "0").field("time_series_dimension", "true");
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

    public void testMissingPriority() throws IOException {
        MapperException e = expectThrows(MapperException.class, () -> createMapperService(mapping(b -> {
            b.startObject("labels").field("type", "passthrough");
            {
                b.startObject("properties");
                b.startObject("dim").field("type", "keyword").endObject();
                b.endObject();
            }
            b.endObject();
        })));
        assertThat(e.getMessage(), containsString("Pass-through object [labels] is missing a non-negative value for parameter [priority]"));
    }

    public void testNegativePriority() throws IOException {
        MapperException e = expectThrows(MapperException.class, () -> createMapperService(mapping(b -> {
            b.startObject("labels").field("type", "passthrough").field("priority", "-1");
            {
                b.startObject("properties");
                b.startObject("dim").field("type", "keyword").endObject();
                b.endObject();
            }
            b.endObject();
        })));
        assertThat(e.getMessage(), containsString("Pass-through object [labels] is missing a non-negative value for parameter [priority]"));
    }

    public void testPriorityParamSet() throws IOException {
        MapperService mapperService = createMapperService(mapping(b -> {
            b.startObject("labels").field("type", "passthrough").field("priority", "10");
            {
                b.startObject("properties");
                b.startObject("dim").field("type", "keyword").endObject();
                b.endObject();
            }
            b.endObject();
        }));
        Mapper mapper = mapperService.mappingLookup().objectMappers().get("labels");
        assertThat(mapper, instanceOf(PassThroughObjectMapper.class));
        assertEquals(10, ((PassThroughObjectMapper) mapper).priority());
    }

    public void testDynamic() throws IOException {
        MapperService mapperService = createMapperService(mapping(b -> {
            b.startObject("labels").field("type", "passthrough").field("priority", "0").field("dynamic", "true");
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
            b.startObject("labels").field("type", "passthrough").field("priority", "0").field("enabled", "false");
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
            b.startObject("labels").field("type", "passthrough").field("priority", "0").field("time_series_dimension", "true");
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
            b.startObject("labels").field("type", "passthrough").field("priority", "1");
            {
                b.startObject("properties");
                b.startObject("dim").field("type", "keyword").endObject();
                b.endObject();
            }
            b.endObject();
            b.startObject("shallow").field("type", "passthrough").field("priority", "2");
            b.endObject();
        }));

        assertEquals("passthrough", mapperService.mappingLookup().objectMappers().get("labels").typeName());
        assertEquals("passthrough", mapperService.mappingLookup().objectMappers().get("shallow").typeName());
    }

    public void testCheckForDuplicatePrioritiesEmpty() throws IOException {
        PassThroughObjectMapper.checkForDuplicatePriorities(List.of());
    }

    public void testMergingWithPassThrough() {
        boolean isSourceSynthetic = randomBoolean();
        var passThroughMapper = new RootObjectMapper.Builder("_doc").add(new PassThroughObjectMapper.Builder("metrics").setPriority(10))
            .build(MapperBuilderContext.root(isSourceSynthetic, true));
        var objectMapper = new RootObjectMapper.Builder("_doc").add(
            new ObjectMapper.Builder("metrics").add(new KeywordFieldMapper.Builder("cpu_usage", defaultIndexSettings()))
        ).build(MapperBuilderContext.root(isSourceSynthetic, true));

        RootObjectMapper merged = passThroughMapper.merge(
            objectMapper,
            MapperMergeContext.root(isSourceSynthetic, true, MAPPING_UPDATE, Long.MAX_VALUE)
        );
        assertThat(merged.getMapper("metrics"), instanceOf(PassThroughObjectMapper.class));

        var objectMapperWithSubObjectTrue = new RootObjectMapper.Builder("_doc").add(
            new ObjectMapper.Builder("metrics", Explicit.of(ObjectMapper.Subobjects.ENABLED)).add(
                new KeywordFieldMapper.Builder("cpu_usage", defaultIndexSettings())
            )
        ).build(MapperBuilderContext.root(isSourceSynthetic, true));

        IllegalArgumentException error = expectThrows(
            IllegalArgumentException.class,
            () -> passThroughMapper.merge(
                objectMapperWithSubObjectTrue,
                MapperMergeContext.root(isSourceSynthetic, true, MAPPING_UPDATE, Long.MAX_VALUE)
            )
        );
        assertThat(
            error.getMessage(),
            equalTo("can't merge a passthrough mapping [metrics] with an object mapping that is either root or has subobjects enabled")
        );

        var rootObjectMapper = new RootObjectMapper.Builder("metrics").add(
            new KeywordFieldMapper.Builder("cpu_usage", defaultIndexSettings())
        ).build(MapperBuilderContext.root(isSourceSynthetic, true));

        error = expectThrows(
            IllegalArgumentException.class,
            () -> new PassThroughObjectMapper.Builder("metrics").setPriority(10)
                .build(MapperBuilderContext.root(isSourceSynthetic, true))
                .merge(rootObjectMapper, MapperMergeContext.root(isSourceSynthetic, true, MAPPING_UPDATE, Long.MAX_VALUE))
        );
        assertThat(
            error.getMessage(),
            equalTo("can't merge a passthrough mapping [metrics] with an object mapping that is either root or has subobjects enabled")
        );
    }

    private PassThroughObjectMapper create(String name, int priority) {
        return new PassThroughObjectMapper(
            name,
            name,
            Explicit.EXPLICIT_TRUE,
            Optional.empty(),
            ObjectMapper.Dynamic.FALSE,
            Map.of(),
            Explicit.EXPLICIT_FALSE,
            priority
        );
    }

    public void testCheckForDuplicatePrioritiesOneElement() throws IOException {
        PassThroughObjectMapper.checkForDuplicatePriorities(List.of(create("foo", 0)));
        PassThroughObjectMapper.checkForDuplicatePriorities(List.of(create("foo", 10)));
    }

    public void testCheckForDuplicatePrioritiesManyValidElements() throws IOException {
        PassThroughObjectMapper.checkForDuplicatePriorities(
            List.of(create("foo", 1), create("bar", 2), create("baz", 3), create("bar", 4))
        );
    }

    public void testCheckForDuplicatePrioritiesManyElementsDuplicatePriority() throws IOException {
        MapperException e = expectThrows(
            MapperException.class,
            () -> PassThroughObjectMapper.checkForDuplicatePriorities(
                List.of(create("foo", 1), create("bar", 1), create("baz", 3), create("bar", 4))
            )
        );
        assertThat(e.getMessage(), containsString("Pass-through object [bar] has a conflicting param [priority=1] with object [foo]"));
    }

    public void testTimeSeriesDimensionAndMetricConflict() throws IOException {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> createMapperService(mapping(b -> {
            b.startObject("labels").field("type", "passthrough").field("priority", "0").field("time_series_dimension", "true");
            {
                b.startObject("properties");
                b.startObject("dim").field("type", "long").field("time_series_metric", "counter").endObject();
                b.endObject();
            }
            b.endObject();
        })));
        assertThat(
            e.getMessage(),
            containsString("[time_series_dimension] and [time_series_metric] cannot be set in conjunction with each other [labels.dim]")
        );
    }
}
