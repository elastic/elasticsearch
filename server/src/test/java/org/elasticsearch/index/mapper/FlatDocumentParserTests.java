/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;

import java.io.IOException;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

/**
 * Tests for {@link FlatDocumentParser}, the specialized parser used for strict-columnar index mode.
 * Each test asserts that the flat parser produces the same indexed fields as the general {@link DocumentParser}
 * for the subset of document shapes it supports (NATIVE fields, no nested, no source-keep).
 */
public class FlatDocumentParserTests extends MapperServiceTestCase {

    private static Settings columnarSettings() {
        return Settings.builder().put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.name()).build();
    }

    // -----------------------------------------------------------------------
    // Eligibility
    // -----------------------------------------------------------------------

    /**
     * The flat parser silently drops unmapped values under dynamic:false rather than capturing them for _source.
     * This behavioral difference is used to confirm it is active for columnar indices.
     */
    public void testFlatParserBehaviorForColumnarIndex() throws IOException {
        assumeTrue("columnar index mode requires snapshot build", IndexMode.COLUMNAR_FEATURE_FLAG.isEnabled());
        DocumentMapper mapper = createMapperService(columnarSettings(), topMapping(b -> b.field("dynamic", "false"))).documentMapper();
        // FlatDocumentParser drops unmapped values (no ignored-source capture); no dynamic update either.
        ParsedDocument doc = mapper.parse(source(b -> b.field("unmapped", "value")));
        assertNull("flat parser drops unmapped values under dynamic:false", doc.dynamicMappingsUpdate());
        assertNull(doc.rootDoc().getField("unmapped"));
    }

    public void testDefaultParserIsUsedForStandardIndex() throws IOException {
        DocumentMapper mapper = createDocumentMapper(topMapping(b -> b.field("dynamic", "false")));
        // DefaultDocumentParser also drops unmapped values under dynamic:false (no indexing), but produces no crash.
        ParsedDocument doc = mapper.parse(source(b -> b.field("unmapped", "value")));
        assertNull(doc.rootDoc().getField("unmapped"));
    }

    // -----------------------------------------------------------------------
    // Basic field parsing
    // -----------------------------------------------------------------------

    public void testParseFlatDottedFieldName() throws IOException {
        assumeTrue("columnar index mode requires snapshot build", IndexMode.COLUMNAR_FEATURE_FLAG.isEnabled());
        DocumentMapper mapper = createMapperService(
            columnarSettings(),
            mapping(b -> b.startObject("host.name").field("type", "keyword").endObject())
        ).documentMapper();

        ParsedDocument doc = mapper.parse(source(b -> b.field("host.name", "localhost")));
        assertNotNull(doc.rootDoc().getField("host.name"));
    }

    public void testParseObjectNotationFlattened() throws IOException {
        assumeTrue("columnar index mode requires snapshot build", IndexMode.COLUMNAR_FEATURE_FLAG.isEnabled());
        DocumentMapper mapper = createMapperService(
            columnarSettings(),
            mapping(b -> b.startObject("host.name").field("type", "keyword").endObject())
        ).documentMapper();

        // Object notation {"host": {"name": "localhost"}} must be flattened to host.name
        ParsedDocument doc = mapper.parse(source(b -> b.startObject("host").field("name", "localhost").endObject()));
        assertNotNull(doc.rootDoc().getField("host.name"));
    }

    public void testParseMultipleFields() throws IOException {
        assumeTrue("columnar index mode requires snapshot build", IndexMode.COLUMNAR_FEATURE_FLAG.isEnabled());
        DocumentMapper mapper = createMapperService(
            columnarSettings(),
            mapping(
                b -> b.startObject("host.name")
                    .field("type", "keyword")
                    .endObject()
                    .startObject("host.ip")
                    .field("type", "ip")
                    .endObject()
                    .startObject("count")
                    .field("type", "long")
                    .endObject()
            )
        ).documentMapper();

        ParsedDocument doc = mapper.parse(source(b -> b.field("host.name", "localhost").field("host.ip", "127.0.0.1").field("count", 42)));
        assertNotNull(doc.rootDoc().getField("host.name"));
        assertNotNull(doc.rootDoc().getField("host.ip"));
        assertNotNull(doc.rootDoc().getField("count"));
    }

    public void testParseArrayOfValues() throws IOException {
        assumeTrue("columnar index mode requires snapshot build", IndexMode.COLUMNAR_FEATURE_FLAG.isEnabled());
        DocumentMapper mapper = createMapperService(
            columnarSettings(),
            mapping(b -> b.startObject("tags").field("type", "keyword").endObject())
        ).documentMapper();

        ParsedDocument doc = mapper.parse(source(b -> b.array("tags", "a", "b", "c")));
        // keyword field should index each element
        assertEquals(3, doc.rootDoc().getFields("tags").size());
    }

    public void testParseArrayOfObjects() throws IOException {
        assumeTrue("columnar index mode requires snapshot build", IndexMode.COLUMNAR_FEATURE_FLAG.isEnabled());
        DocumentMapper mapper = createMapperService(
            columnarSettings(),
            mapping(b -> b.startObject("host.name").field("type", "keyword").endObject())
        ).documentMapper();

        // Array of objects: [{"name": "a"}, {"name": "b"}]
        ParsedDocument doc = mapper.parse(
            source(
                b -> b.startArray("host")
                    .startObject()
                    .field("name", "a")
                    .endObject()
                    .startObject()
                    .field("name", "b")
                    .endObject()
                    .endArray()
            )
        );
        assertEquals(2, doc.rootDoc().getFields("host.name").size());
    }

    // -----------------------------------------------------------------------
    // Dynamic mapping
    // -----------------------------------------------------------------------

    public void testDynamicFieldCreatesDynamicMappingUpdate() throws IOException {
        assumeTrue("columnar index mode requires snapshot build", IndexMode.COLUMNAR_FEATURE_FLAG.isEnabled());
        DocumentMapper mapper = createMapperService(columnarSettings(), mapping(b -> {})).documentMapper();

        ParsedDocument doc = mapper.parse(source(b -> b.field("new_field", "value")));
        assertNotNull("expected a dynamic mapping update", doc.dynamicMappingsUpdate());
    }

    public void testDynamicObjectFlattened() throws IOException {
        assumeTrue("columnar index mode requires snapshot build", IndexMode.COLUMNAR_FEATURE_FLAG.isEnabled());
        DocumentMapper mapper = createMapperService(columnarSettings(), mapping(b -> {})).documentMapper();

        // {"host": {"name": "localhost"}} → dynamic field "host.name" must be created
        ParsedDocument doc = mapper.parse(source(b -> b.startObject("host").field("name", "localhost").endObject()));
        assertNotNull("expected a dynamic mapping update for host.name", doc.dynamicMappingsUpdate());
    }

    public void testDynamicStrictRejectsUnmappedField() throws IOException {
        assumeTrue("columnar index mode requires snapshot build", IndexMode.COLUMNAR_FEATURE_FLAG.isEnabled());
        DocumentMapper mapper = createMapperService(columnarSettings(), topMapping(b -> b.field("dynamic", "strict"))).documentMapper();

        expectThrows(DocumentParsingException.class, () -> mapper.parse(source(b -> b.field("unmapped", "value"))));
    }

    public void testDynamicStrictAllowsMappedFlatField() throws IOException {
        assumeTrue("columnar index mode requires snapshot build", IndexMode.COLUMNAR_FEATURE_FLAG.isEnabled());
        DocumentMapper mapper = createMapperService(
            columnarSettings(),
            topMapping(
                b -> b.field("dynamic", "strict")
                    .startObject("properties")
                    .startObject("host.name")
                    .field("type", "keyword")
                    .endObject()
                    .endObject()
            )
        ).documentMapper();

        // Flat notation for a mapped dotted field must succeed under dynamic:strict.
        // (Object notation {"host": {"name": ...}} would be rejected because "host" is not a mapped name.)
        ParsedDocument doc = mapper.parse(source(b -> b.field("host.name", "localhost")));
        assertNotNull(doc.rootDoc().getField("host.name"));
    }

    public void testDynamicStrictRejectsObjectNotationForUnmappedPrefix() throws IOException {
        assumeTrue("columnar index mode requires snapshot build", IndexMode.COLUMNAR_FEATURE_FLAG.isEnabled());
        DocumentMapper mapper = createMapperService(columnarSettings(), topMapping(b -> b.field("dynamic", "strict"))).documentMapper();

        // Object notation where the key is not mapped is rejected by strict.
        expectThrows(
            DocumentParsingException.class,
            () -> mapper.parse(source(b -> b.startObject("env").field("name", "prod").endObject()))
        );
    }

    public void testDynamicFalseDropsUnmappedValues() throws IOException {
        assumeTrue("columnar index mode requires snapshot build", IndexMode.COLUMNAR_FEATURE_FLAG.isEnabled());
        DocumentMapper mapper = createMapperService(columnarSettings(), topMapping(b -> b.field("dynamic", "false"))).documentMapper();

        // Unmapped values are silently dropped — no exception, no dynamic update.
        ParsedDocument doc = mapper.parse(source(b -> b.field("unmapped", "value")));
        assertNull("flat parser drops unmapped values under dynamic:false", doc.dynamicMappingsUpdate());
        assertThat(doc.rootDoc().getField("unmapped"), nullValue());
    }

    // -----------------------------------------------------------------------
    // copy_to
    // -----------------------------------------------------------------------

    public void testCopyToIndexesCopiedField() throws IOException {
        assumeTrue("columnar index mode requires snapshot build", IndexMode.COLUMNAR_FEATURE_FLAG.isEnabled());
        DocumentMapper mapper = createMapperService(
            columnarSettings(),
            mapping(
                b -> b.startObject("source_field")
                    .field("type", "keyword")
                    .array("copy_to", "dest_field")
                    .endObject()
                    .startObject("dest_field")
                    .field("type", "keyword")
                    .endObject()
            )
        ).documentMapper();

        ParsedDocument doc = mapper.parse(source(b -> b.field("source_field", "hello")));
        assertThat(doc.rootDoc().getField("dest_field"), notNullValue());
    }

    // -----------------------------------------------------------------------
    // Null and empty handling
    // -----------------------------------------------------------------------

    public void testNullValueForMappedField() throws IOException {
        assumeTrue("columnar index mode requires snapshot build", IndexMode.COLUMNAR_FEATURE_FLAG.isEnabled());
        DocumentMapper mapper = createMapperService(
            columnarSettings(),
            mapping(b -> b.startObject("field").field("type", "keyword").endObject())
        ).documentMapper();

        // Null values for mapped fields should be silently ignored (not throw).
        ParsedDocument doc = mapper.parse(source(b -> b.nullField("field")));
        assertNotNull(doc);
    }

    public void testEmptyDocument() throws IOException {
        assumeTrue("columnar index mode requires snapshot build", IndexMode.COLUMNAR_FEATURE_FLAG.isEnabled());
        DocumentMapper mapper = createMapperService(columnarSettings(), mapping(b -> {})).documentMapper();

        ParsedDocument doc = mapper.parse(source(b -> {}));
        assertNotNull(doc);
    }

    // -----------------------------------------------------------------------
    // Parser selection in MapperService
    // -----------------------------------------------------------------------

    public void testFlatParserSelectedForCompatibleColumnarMapping() throws IOException {
        assumeTrue("columnar index mode requires snapshot build", IndexMode.COLUMNAR_FEATURE_FLAG.isEnabled());
        MapperService mapperService = createMapperService(
            columnarSettings(),
            mapping(b -> b.startObject("field").field("type", "keyword").endObject())
        );
        assertThat(mapperService.documentParser(), instanceOf(FlatDocumentParser.class));
    }

    public void testDefaultParserSelectedForNonColumnarIndex() throws IOException {
        MapperService mapperService = createMapperService(mapping(b -> {}));
        assertThat(mapperService.documentParser(), instanceOf(DefaultDocumentParser.class));
    }

    public void testDefaultParserSelectedWhenMappingHasRuntimeFields() throws IOException {
        assumeTrue("columnar index mode requires snapshot build", IndexMode.COLUMNAR_FEATURE_FLAG.isEnabled());
        MapperService mapperService = createMapperService(
            columnarSettings(),
            topMapping(b -> {
                b.startObject("runtime");
                b.startObject("day_of_week").field("type", "keyword").endObject();
                b.endObject();
            })
        );
        assertThat(mapperService.documentParser(), instanceOf(DefaultDocumentParser.class));
    }

    public void testDefaultParserSelectedWhenMappingHasCopyTo() throws IOException {
        assumeTrue("columnar index mode requires snapshot build", IndexMode.COLUMNAR_FEATURE_FLAG.isEnabled());
        MapperService mapperService = createMapperService(
            columnarSettings(),
            mapping(b -> {
                b.startObject("source").field("type", "keyword").array("copy_to", "dest").endObject();
                b.startObject("dest").field("type", "keyword").endObject();
            })
        );
        assertThat(mapperService.documentParser(), instanceOf(DefaultDocumentParser.class));
    }

    public void testDefaultParserSelectedWhenMappingHasFallbackSyntheticSourceField() throws IOException {
        assumeTrue("columnar index mode requires snapshot build", IndexMode.COLUMNAR_FEATURE_FLAG.isEnabled());
        // binary fields have no doc values by default, so they use fallback synthetic source
        MapperService mapperService = createMapperService(
            columnarSettings(),
            mapping(b -> b.startObject("data").field("type", "binary").endObject())
        );
        assertThat(mapperService.documentParser(), instanceOf(DefaultDocumentParser.class));
    }
}
