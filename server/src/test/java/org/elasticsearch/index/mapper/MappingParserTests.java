/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.Version;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.index.similarity.SimilarityService;
import org.elasticsearch.indices.IndicesModule;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.xcontent.XContentBuilder;
import org.hamcrest.CoreMatchers;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Supplier;

public class MappingParserTests extends MapperServiceTestCase {

    private static MappingParser createMappingParser(Settings settings) {
        ScriptService scriptService = new ScriptService(settings, Collections.emptyMap(), Collections.emptyMap(), () -> 1L);
        IndexSettings indexSettings = createIndexSettings(Version.CURRENT, settings);
        IndexAnalyzers indexAnalyzers = createIndexAnalyzers();
        SimilarityService similarityService = new SimilarityService(indexSettings, scriptService, Collections.emptyMap());
        MapperRegistry mapperRegistry = new IndicesModule(Collections.emptyList()).getMapperRegistry();
        Supplier<MappingParserContext> parserContextSupplier = () -> new MappingParserContext(
            similarityService::getSimilarity,
            type -> mapperRegistry.getMapperParser(type, indexSettings.getIndexVersionCreated()),
            mapperRegistry.getRuntimeFieldParsers()::get,
            indexSettings.getIndexVersionCreated(),
            () -> { throw new UnsupportedOperationException(); },
            null,
            scriptService,
            indexAnalyzers,
            indexSettings,
            indexSettings.getMode().idFieldMapperWithoutFieldData()
        );
        Map<String, MetadataFieldMapper.TypeParser> metadataMapperParsers = mapperRegistry.getMetadataMapperParsers(
            indexSettings.getIndexVersionCreated()
        );
        Map<Class<? extends MetadataFieldMapper>, MetadataFieldMapper> metadataMappers = new LinkedHashMap<>();
        metadataMapperParsers.values().stream().map(parser -> parser.getDefault(parserContextSupplier.get())).forEach(m -> {
            if (m != null) {
                metadataMappers.put(m.getClass(), m);
            }
        });
        return new MappingParser(
            parserContextSupplier,
            metadataMapperParsers,
            () -> metadataMappers,
            type -> MapperService.SINGLE_MAPPING_NAME
        );
    }

    public void testFieldNameWithDotsDisallowed() throws Exception {
        XContentBuilder builder = mapping(b -> {
            b.startObject("foo.bar").field("type", "text").endObject();
            b.startObject("foo.baz").field("type", "keyword").endObject();
        });
        Mapping mapping = createMappingParser(Settings.EMPTY).parse("_doc", new CompressedXContent(BytesReference.bytes(builder)));

        Mapper object = mapping.getRoot().getMapper("foo");
        assertThat(object, CoreMatchers.instanceOf(ObjectMapper.class));
        ObjectMapper objectMapper = (ObjectMapper) object;
        assertNotNull(objectMapper.getMapper("bar"));
        assertNotNull(objectMapper.getMapper("baz"));
    }

    public void testFieldNameWithDeepDots() throws Exception {
        XContentBuilder builder = mapping(b -> {
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
        });
        Mapping mapping = createMappingParser(Settings.EMPTY).parse("_doc", new CompressedXContent(BytesReference.bytes(builder)));
        MappingLookup mappingLookup = MappingLookup.fromMapping(mapping);
        assertNotNull(mappingLookup.getMapper("foo.bar"));
        assertNotNull(mappingLookup.getMapper("foo.baz.deep.field"));
        assertNotNull(mappingLookup.objectMappers().get("foo"));
    }

    public void testFieldNameWithDotPrefixDisallowed() throws IOException {
        XContentBuilder builder = mapping(b -> {
            b.startObject("foo").field("type", "text").endObject();
            b.startObject("foo.baz").field("type", "keyword").endObject();
        });
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> createMappingParser(Settings.EMPTY).parse("_doc", new CompressedXContent(BytesReference.bytes(builder)))
        );
        assertTrue(e.getMessage(), e.getMessage().contains("mapper [foo] cannot be changed from type [text] to [ObjectMapper]"));
    }

    public void testMultiFieldsWithFieldAlias() throws IOException {
        XContentBuilder builder = mapping(b -> {
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
        });
        MapperParsingException e = expectThrows(
            MapperParsingException.class,
            () -> createMappingParser(Settings.EMPTY).parse("_doc", new CompressedXContent(BytesReference.bytes(builder)))
        );
        assertEquals("Type [alias] cannot be used in multi field", e.getMessage());
    }

    public void testBadMetadataMapper() throws IOException {
        XContentBuilder builder = topMapping(b -> { b.field(RoutingFieldMapper.NAME, "required"); });
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> createMappingParser(Settings.EMPTY).parse("_doc", new CompressedXContent(BytesReference.bytes(builder)))
        );
        assertEquals("[_routing] config must be an object", e.getMessage());
    }

    public void testMergeSubfieldWhileParsing() throws Exception {
        /*
        If we are parsing mappings that hold the definition of the same field twice, the two are merged together. This can happen when
        mappings have the same field specified using the object notation as well as the dot notation, as well as when applying index
        templates, in which case the two definitions may come from separate index templates that end up in the same map (through
        XContentHelper#mergeDefaults, see MetadataCreateIndexService#parseV1Mappings).
        We had a bug (https://github.com/elastic/elasticsearch/issues/88573) triggered by this scenario that caused the merged leaf fields
        to get the wrong path (missing the first portion).
         */
        String mappingAsString = """
            {
               "_doc": {
                 "properties": {
                   "obj": {
                     "properties": {
                       "source": {
                         "properties": {
                           "geo": {
                             "properties": {
                               "location": {
                                 "type": "geo_point"
                               }
                             }
                           }
                         }
                       }
                     }
                   },
                   "obj.source.geo.location" : {
                     "type": "geo_point"
                   }
                 }
               }
            }
            """;
        Mapping mapping = createMappingParser(Settings.EMPTY).parse("_doc", new CompressedXContent(mappingAsString));
        assertEquals(1, mapping.getRoot().mappers.size());
        Mapper object = mapping.getRoot().getMapper("obj");
        assertThat(object, CoreMatchers.instanceOf(ObjectMapper.class));
        assertEquals("obj", object.simpleName());
        assertEquals("obj", object.name());
        ObjectMapper objectMapper = (ObjectMapper) object;
        assertEquals(1, objectMapper.mappers.size());
        object = objectMapper.getMapper("source");
        assertThat(object, CoreMatchers.instanceOf(ObjectMapper.class));
        assertEquals("source", object.simpleName());
        assertEquals("obj.source", object.name());
        objectMapper = (ObjectMapper) object;
        assertEquals(1, objectMapper.mappers.size());
        object = objectMapper.getMapper("geo");
        assertThat(object, CoreMatchers.instanceOf(ObjectMapper.class));
        assertEquals("geo", object.simpleName());
        assertEquals("obj.source.geo", object.name());
        objectMapper = (ObjectMapper) object;
        assertEquals(1, objectMapper.mappers.size());
        Mapper location = objectMapper.getMapper("location");
        assertThat(location, CoreMatchers.instanceOf(GeoPointFieldMapper.class));
        GeoPointFieldMapper geoPointFieldMapper = (GeoPointFieldMapper) location;
        assertEquals("obj.source.geo.location", geoPointFieldMapper.name());
        assertEquals("location", geoPointFieldMapper.simpleName());
        assertEquals("obj.source.geo.location", geoPointFieldMapper.mappedFieldType.name());
    }
}
