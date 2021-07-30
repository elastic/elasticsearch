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
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.index.similarity.SimilarityService;
import org.elasticsearch.indices.IndicesModule;
import org.elasticsearch.script.ScriptService;
import org.hamcrest.CoreMatchers;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Supplier;

public class MappingParserTests extends MapperServiceTestCase {

    private static MappingParser createMappingParser(Settings settings) {
        ScriptService scriptService = new ScriptService(settings, Collections.emptyMap(), Collections.emptyMap());
        IndexSettings indexSettings = createIndexSettings(Version.CURRENT, settings);
        IndexAnalyzers indexAnalyzers = createIndexAnalyzers();
        SimilarityService similarityService = new SimilarityService(indexSettings, scriptService, Collections.emptyMap());
        MapperRegistry mapperRegistry = new IndicesModule(Collections.emptyList()).getMapperRegistry();
        Supplier<MappingParserContext> parserContextSupplier =
            () -> new MappingParserContext(similarityService::getSimilarity, mapperRegistry.getMapperParsers()::get,
                mapperRegistry.getRuntimeFieldParsers()::get, indexSettings.getIndexVersionCreated(),
                () -> { throw new UnsupportedOperationException(); }, null,
                scriptService, indexAnalyzers, indexSettings, () -> false);
        Map<String, MetadataFieldMapper.TypeParser> metadataMapperParsers =
            mapperRegistry.getMetadataMapperParsers(indexSettings.getIndexVersionCreated());
        Map<Class<? extends MetadataFieldMapper>, MetadataFieldMapper> metadataMappers = new LinkedHashMap<>();
        metadataMapperParsers.values().stream().map(parser -> parser.getDefault(parserContextSupplier.get()))
            .forEach(m -> metadataMappers.put(m.getClass(), m));
        return new MappingParser(parserContextSupplier, metadataMapperParsers,
            () -> metadataMappers, type -> MapperService.SINGLE_MAPPING_NAME);
    }

    public void testFieldNameWithDots() throws Exception {
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

    public void testFieldNameWithDotsConflict() throws IOException {
        XContentBuilder builder = mapping(b -> {
            b.startObject("foo").field("type", "text").endObject();
            b.startObject("foo.baz").field("type", "keyword").endObject();
        });
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
            () -> createMappingParser(Settings.EMPTY).parse("_doc", new CompressedXContent(BytesReference.bytes(builder))));
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
        MapperParsingException e = expectThrows(MapperParsingException.class,
            () -> createMappingParser(Settings.EMPTY).parse("_doc", new CompressedXContent(BytesReference.bytes(builder))));
        assertEquals("Type [alias] cannot be used in multi field", e.getMessage());
    }
}
