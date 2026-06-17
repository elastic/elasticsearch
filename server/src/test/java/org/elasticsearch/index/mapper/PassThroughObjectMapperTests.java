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
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.elasticsearch.index.mapper.MapperService.MergeReason.MAPPING_AUTO_UPDATE;
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
        var passThroughBuilder = new RootObjectMapper.Builder("_doc").add(new PassThroughObjectMapper.Builder("metrics").setPriority(10));
        var objectBuilder = new RootObjectMapper.Builder("_doc").add(
            new ObjectMapper.Builder("metrics").add(new KeywordFieldMapper.Builder("cpu_usage", defaultIndexSettings()))
        );

        MapperMergeContext mergeContext = MapperMergeContext.root(isSourceSynthetic, true, MAPPING_UPDATE, Long.MAX_VALUE);
        RootObjectMapper merged = (RootObjectMapper) passThroughBuilder.mergeWith(objectBuilder, mergeContext)
            .build(mergeContext.getMapperBuilderContext());
        assertThat(merged.getMapper("metrics"), instanceOf(PassThroughObjectMapper.class));

        var passThroughBuilder2 = new RootObjectMapper.Builder("_doc").add(new PassThroughObjectMapper.Builder("metrics").setPriority(10));
        var objectWithSubObjectTrue = new RootObjectMapper.Builder("_doc").add(
            new ObjectMapper.Builder("metrics", Explicit.of(ObjectMapper.Subobjects.ENABLED)).add(
                new KeywordFieldMapper.Builder("cpu_usage", defaultIndexSettings())
            )
        );

        IllegalArgumentException error = expectThrows(
            IllegalArgumentException.class,
            () -> passThroughBuilder2.mergeWith(
                objectWithSubObjectTrue,
                MapperMergeContext.root(isSourceSynthetic, true, MAPPING_UPDATE, Long.MAX_VALUE)
            )
        );
        assertThat(
            error.getMessage(),
            equalTo("can't merge a passthrough mapping [metrics] with an object mapping that is either root or has subobjects enabled")
        );

        var passThroughBuilder3 = new PassThroughObjectMapper.Builder("metrics").setPriority(10);
        var rootObjectBuilder = new RootObjectMapper.Builder("metrics").add(
            new KeywordFieldMapper.Builder("cpu_usage", defaultIndexSettings())
        );

        error = expectThrows(
            IllegalArgumentException.class,
            () -> passThroughBuilder3.mergeWith(
                rootObjectBuilder,
                MapperMergeContext.root(isSourceSynthetic, true, MAPPING_UPDATE, Long.MAX_VALUE)
            )
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
        assertThat(e.getMessage(), containsString("Pass-through source [bar] has a conflicting param [priority=1] with source [foo]"));
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

    /**
     * Passthrough objects must survive flattening in columnar/logsdb_columnar index modes so that
     * root-level field aliases are created at query time via {@link FieldTypeLookup}.  Previously
     * the {@code subobjects:false} flatten guard in
     * {@link ObjectMapper.Builder#flattenBuildersIfNeeded} dissolved every
     * {@code ObjectMapper.Builder} child — including {@code PassThroughObjectMapper.Builder} —
     * into bare dotted leaf fields, causing the passthrough node to disappear from the mapping tree
     * and no aliases to be registered.
     */
    public void testPassThroughSurvivesColumnarFlattening() throws IOException {
        assumeTrue("columnar index mode requires snapshot build", IndexMode.COLUMNAR_FEATURE_FLAG.isEnabled());
        for (IndexMode indexMode : List.of(IndexMode.COLUMNAR, IndexMode.LOGSDB_COLUMNAR)) {
            Settings settings = Settings.builder().put(IndexSettings.MODE.getKey(), indexMode.getName()).build();
            MapperService mapperService = createMapperService(settings, mapping(b -> {
                b.startObject("attributes").field("type", "passthrough").field("priority", "1").field("dynamic", true);
                {
                    b.startObject("properties");
                    b.startObject("env").field("type", "keyword").endObject();
                    b.endObject();
                }
                b.endObject();
            }));

            // The passthrough node must still exist in the objectMappers map
            Mapper passthrough = mapperService.mappingLookup().objectMappers().get("attributes");
            assertNotNull("passthrough node should survive columnar flattening [" + indexMode + "]", passthrough);
            assertThat(passthrough, instanceOf(PassThroughObjectMapper.class));

            // The static sub-field is accessible via its full dotted path
            assertNotNull("attributes.env should be a concrete field [" + indexMode + "]", mapperService.fieldType("attributes.env"));

            // A root-level alias for the sub-field must be registered
            assertNotNull("root-level alias 'env' should exist via passthrough [" + indexMode + "]", mapperService.fieldType("env"));
        }
    }

    /**
     * When two passthrough objects compete for the same leaf name the higher-priority passthrough
     * wins.  This must also hold in columnar modes after the fix.
     */
    public void testPassThroughPriorityWinsInColumnarMode() throws IOException {
        assumeTrue("columnar index mode requires snapshot build", IndexMode.COLUMNAR_FEATURE_FLAG.isEnabled());
        for (IndexMode indexMode : List.of(IndexMode.COLUMNAR, IndexMode.LOGSDB_COLUMNAR)) {
            Settings settings = Settings.builder().put(IndexSettings.MODE.getKey(), indexMode.getName()).build();
            MapperService mapperService = createMapperService(settings, mapping(b -> {
                // priority 1 — lower
                b.startObject("attributes").field("type", "passthrough").field("priority", "1").field("dynamic", true);
                {
                    b.startObject("properties");
                    b.startObject("region").field("type", "keyword").endObject();
                    b.endObject();
                }
                b.endObject();
                // priority 2 — higher; its alias for "region" should win
                b.startObject("resource").field("type", "passthrough").field("priority", "2").field("dynamic", true);
                {
                    b.startObject("properties");
                    b.startObject("region").field("type", "keyword").endObject();
                    b.endObject();
                }
                b.endObject();
            }));

            // Both concrete dotted paths must be reachable
            assertNotNull("attributes.region should be a concrete field [" + indexMode + "]", mapperService.fieldType("attributes.region"));
            assertNotNull("resource.region should be a concrete field [" + indexMode + "]", mapperService.fieldType("resource.region"));

            // The root-level alias "region" must resolve to resource.region (priority 2 wins)
            MappedFieldType rootAlias = mapperService.fieldType("region");
            assertNotNull("root-level alias 'region' should exist [" + indexMode + "]", rootAlias);
            assertEquals("higher-priority passthrough should win for root alias [" + indexMode + "]", "resource.region", rootAlias.name());
        }
    }

    /**
     * Verifies that after a dynamic mapping update a passthrough object in columnar mode still
     * appears with {@code "type": "passthrough"} in the serialized mapping source.
     * This exercises the path where a document with a flat dotted field (e.g.
     * {@code "attributes.env": "prod"}) triggers a dynamic mapping update that routes the new
     * leaf through the passthrough builder and merges it back.
     */
    @SuppressWarnings("unchecked")
    public void testPassThroughSurvivesDynamicUpdateInColumnarMode() throws IOException {
        assumeTrue("columnar index mode requires snapshot build", IndexMode.COLUMNAR_FEATURE_FLAG.isEnabled());
        for (IndexMode indexMode : List.of(IndexMode.COLUMNAR, IndexMode.LOGSDB_COLUMNAR)) {
            Settings settings = Settings.builder().put(IndexSettings.MODE.getKey(), indexMode.getName()).build();
            // Start with the passthrough having NO properties (simulates the template before any document)
            MapperService mapperService = createMapperService(settings, mapping(b -> {
                b.startObject("attributes").field("type", "passthrough").field("priority", 1).field("dynamic", true).endObject();
            }));

            // Simulate the dynamic mapping update that arrives after indexing {"attributes.env": "prod"}
            String dynamicUpdate = """
                {"_doc":{"properties":{"attributes":{"type":"passthrough","priority":1,"dynamic":"true",
                "properties":{"env":{"type":"keyword"},"service":{"type":"keyword"}}}}}}
                """;
            mapperService.merge("_doc", new CompressedXContent(dynamicUpdate), MAPPING_AUTO_UPDATE);

            // The passthrough node must still survive after the dynamic update
            Mapper passthrough = mapperService.mappingLookup().objectMappers().get("attributes");
            assertNotNull("passthrough node should survive dynamic update [" + indexMode + "]", passthrough);
            assertThat(passthrough, instanceOf(PassThroughObjectMapper.class));

            // Root-level aliases must work (aliases created by the passthrough)
            assertNotNull("env alias should exist after dynamic update [" + indexMode + "]", mapperService.fieldType("env"));
            assertNotNull("service alias should exist after dynamic update [" + indexMode + "]", mapperService.fieldType("service"));

            // The serialized mapping source must contain "type": "passthrough" for attributes
            CompressedXContent mappingSource = mapperService.documentMapper().mappingSource();
            Map<String, Object> sourceMap = XContentHelper.convertToMap(mappingSource.compressedReference(), true, XContentType.JSON).v2();
            // sourceMap is {"_doc": {"properties": {"attributes": {...}}}}
            // strip top-level type wrapper if present
            if (sourceMap.size() == 1 && sourceMap.containsKey("_doc")) {
                sourceMap = (Map<String, Object>) sourceMap.get("_doc");
            }
            Map<String, Object> properties = (Map<String, Object>) sourceMap.get("properties");
            assertNotNull("properties must exist in mapping source [" + indexMode + "]", properties);
            Map<String, Object> attributesNode = (Map<String, Object>) properties.get("attributes");
            assertNotNull("attributes must be in properties [" + indexMode + "]", attributesNode);
            assertEquals(
                "attributes.type must be 'passthrough' in serialized mapping [" + indexMode + "]",
                "passthrough",
                attributesNode.get("type")
            );
        }
    }

    /**
     * A passthrough whose name contains a dot (e.g. {@code "resource.attributes"}) must still
     * produce short-name aliases in columnar/logsdb_columnar mode.  For example, a field stored
     * as {@code "resource.attributes.env"} at root level must be reachable via the alias {@code "env"}.
     * This exercises the multi-segment prefix case in the prefix-based alias resolution path of
     * {@link FieldTypeLookup}.
     */
    public void testPassThroughWithDottedPrefixInColumnarMode() throws IOException {
        assumeTrue("columnar index mode requires snapshot build", IndexMode.COLUMNAR_FEATURE_FLAG.isEnabled());
        for (IndexMode indexMode : List.of(IndexMode.COLUMNAR, IndexMode.LOGSDB_COLUMNAR)) {
            Settings settings = Settings.builder().put(IndexSettings.MODE.getKey(), indexMode.getName()).build();
            MapperService mapperService = createMapperService(settings, mapping(b -> {
                // Dotted passthrough name — valid in subobjects:false mode
                b.startObject("resource.attributes").field("type", "passthrough").field("priority", "1").field("dynamic", true);
                {
                    b.startObject("properties");
                    b.startObject("env").field("type", "keyword").endObject();
                    b.endObject();
                }
                b.endObject();
            }));

            // The concrete dotted path must be reachable
            assertNotNull(
                "resource.attributes.env should be a concrete field [" + indexMode + "]",
                mapperService.fieldType("resource.attributes.env")
            );

            // The root-level alias "env" must resolve via the dotted passthrough prefix
            assertNotNull(
                "root alias 'env' should exist via dotted passthrough [" + indexMode + "]",
                mapperService.fieldType("env")
            );
        }
    }
}
