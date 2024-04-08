/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.MapperService.MergeReason;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class RootObjectMapperTests extends MapperServiceTestCase {

    public void testNumericDetection() throws Exception {
        MergeReason reason = randomFrom(MergeReason.MAPPING_UPDATE, MergeReason.INDEX_TEMPLATE);
        String mapping = Strings.toString(
            XContentFactory.jsonBuilder()
                .startObject()
                .startObject(MapperService.SINGLE_MAPPING_NAME)
                .field("numeric_detection", false)
                .endObject()
                .endObject()
        );
        MapperService mapperService = createMapperService(mapping);
        assertEquals(mapping, mapperService.documentMapper().mappingSource().toString());

        // update with a different explicit value
        String mapping2 = Strings.toString(
            XContentFactory.jsonBuilder()
                .startObject()
                .startObject(MapperService.SINGLE_MAPPING_NAME)
                .field("numeric_detection", true)
                .endObject()
                .endObject()
        );
        merge(mapperService, reason, mapping2);
        assertEquals(mapping2, mapperService.documentMapper().mappingSource().toString());

        // update with an implicit value: no change
        String mapping3 = Strings.toString(
            XContentFactory.jsonBuilder().startObject().startObject(MapperService.SINGLE_MAPPING_NAME).endObject().endObject()
        );
        merge(mapperService, reason, mapping3);
        assertEquals(mapping2, mapperService.documentMapper().mappingSource().toString());
    }

    public void testDateDetection() throws Exception {
        MergeReason reason = randomFrom(MergeReason.MAPPING_UPDATE, MergeReason.INDEX_TEMPLATE);
        String mapping = Strings.toString(
            XContentFactory.jsonBuilder()
                .startObject()
                .startObject(MapperService.SINGLE_MAPPING_NAME)
                .field("date_detection", true)
                .endObject()
                .endObject()
        );
        MapperService mapperService = createMapperService(mapping);
        assertEquals(mapping, mapperService.documentMapper().mappingSource().toString());

        // update with a different explicit value
        String mapping2 = Strings.toString(
            XContentFactory.jsonBuilder()
                .startObject()
                .startObject(MapperService.SINGLE_MAPPING_NAME)
                .field("date_detection", false)
                .endObject()
                .endObject()
        );
        merge(mapperService, reason, mapping2);
        assertEquals(mapping2, mapperService.documentMapper().mappingSource().toString());

        // update with an implicit value: no change
        String mapping3 = Strings.toString(
            XContentFactory.jsonBuilder().startObject().startObject(MapperService.SINGLE_MAPPING_NAME).endObject().endObject()
        );
        merge(mapperService, reason, mapping3);
        assertEquals(mapping2, mapperService.documentMapper().mappingSource().toString());
    }

    public void testDateFormatters() throws Exception {
        MergeReason reason = randomFrom(MergeReason.MAPPING_UPDATE, MergeReason.INDEX_TEMPLATE);
        String mapping = Strings.toString(
            XContentFactory.jsonBuilder()
                .startObject()
                .startObject(MapperService.SINGLE_MAPPING_NAME)
                .field("dynamic_date_formats", Collections.singletonList("yyyy-MM-dd"))
                .endObject()
                .endObject()
        );
        MapperService mapperService = createMapperService(mapping);
        assertEquals(mapping, mapperService.documentMapper().mappingSource().toString());

        // no update if formatters are not set explicitly
        String mapping2 = Strings.toString(
            XContentFactory.jsonBuilder().startObject().startObject(MapperService.SINGLE_MAPPING_NAME).endObject().endObject()
        );
        merge(mapperService, reason, mapping2);
        assertEquals(mapping, mapperService.documentMapper().mappingSource().toString());

        String mapping3 = Strings.toString(
            XContentFactory.jsonBuilder()
                .startObject()
                .startObject(MapperService.SINGLE_MAPPING_NAME)
                .field("dynamic_date_formats", Collections.emptyList())
                .endObject()
                .endObject()
        );
        merge(mapperService, reason, mapping3);
        assertEquals(mapping3, mapperService.documentMapper().mappingSource().toString());
    }

    public void testIllegalFormatField() throws Exception {
        String dynamicMapping = Strings.toString(
            XContentFactory.jsonBuilder()
                .startObject()
                .startObject(MapperService.SINGLE_MAPPING_NAME)
                .startArray("dynamic_date_formats")
                .startArray()
                .value("test_format")
                .endArray()
                .endArray()
                .endObject()
                .endObject()
        );
        String mapping = Strings.toString(
            XContentFactory.jsonBuilder()
                .startObject()
                .startObject(MapperService.SINGLE_MAPPING_NAME)
                .startArray("date_formats")
                .startArray()
                .value("test_format")
                .endArray()
                .endArray()
                .endObject()
                .endObject()
        );
        for (String m : Arrays.asList(mapping, dynamicMapping)) {
            MapperParsingException e = expectThrows(MapperParsingException.class, () -> createMapperService(m));
            assertEquals("Failed to parse mapping: Invalid format: [[test_format]]: expected string value", e.getMessage());
        }
    }

    public void testRuntimeSection() throws IOException {
        String mapping = Strings.toString(runtimeMapping(builder -> {
            builder.startObject("field1").field("type", "double").endObject();
            builder.startObject("field2").field("type", "date").endObject();
            builder.startObject("field3").field("type", "ip").endObject();
        }));
        MapperService mapperService = createMapperService(mapping);
        assertEquals(mapping, mapperService.documentMapper().mappingSource().toString());
        assertEquals(3, mapperService.documentMapper().mapping().getRoot().getTotalFieldsCount());
    }

    public void testRuntimeSectionRejectedUpdate() throws IOException {
        MapperService mapperService;
        {
            XContentBuilder builder = XContentFactory.jsonBuilder().startObject().startObject("_doc");
            builder.startObject("runtime");
            builder.startObject("field").field("type", "long").endObject();
            builder.endObject();
            builder.startObject("properties");
            builder.startObject("concrete").field("type", "keyword").endObject();
            builder.endObject();
            builder.endObject().endObject();
            mapperService = createMapperService(builder);
            assertEquals(Strings.toString(builder), mapperService.documentMapper().mappingSource().toString());
            MappedFieldType concrete = mapperService.fieldType("concrete");
            assertThat(concrete, instanceOf(KeywordFieldMapper.KeywordFieldType.class));
            MappedFieldType field = mapperService.fieldType("field");
            assertThat(field, instanceOf(LongScriptFieldType.class));
        }
        {
            XContentBuilder builder = XContentFactory.jsonBuilder().startObject().startObject("_doc");
            builder.startObject("runtime");
            builder.startObject("another_field").field("type", "geo_point").endObject();
            builder.endObject();
            builder.startObject("properties");
            // try changing the type of the existing concrete field, so that merge fails
            builder.startObject("concrete").field("type", "text").endObject();
            builder.endObject();
            builder.endObject().endObject();

            expectThrows(IllegalArgumentException.class, () -> merge(mapperService, builder));

            // make sure that the whole rejected update, including changes to runtime fields, has not been applied
            MappedFieldType concrete = mapperService.fieldType("concrete");
            assertThat(concrete, instanceOf(KeywordFieldMapper.KeywordFieldType.class));
            MappedFieldType field = mapperService.fieldType("field");
            assertThat(field, instanceOf(LongScriptFieldType.class));
            assertNull(mapperService.fieldType("another_field"));
            assertEquals("""
                {"_doc":{"runtime":{"field":{"type":"long"}},\
                "properties":{"concrete":{"type":"keyword"}}}}""", Strings.toString(mapperService.documentMapper().mapping().getRoot()));
        }
    }

    public void testRuntimeSectionMerge() throws IOException {
        MapperService mapperService;
        {
            String mapping = Strings.toString(fieldMapping(b -> b.field("type", "keyword")));
            mapperService = createMapperService(mapping);
            assertEquals(mapping, mapperService.documentMapper().mappingSource().toString());
            MappedFieldType field = mapperService.fieldType("field");
            assertThat(field, instanceOf(KeywordFieldMapper.KeywordFieldType.class));
        }
        LongScriptFieldType field2;
        {
            String mapping = Strings.toString(runtimeMapping(builder -> {
                builder.startObject("field").field("type", "keyword").endObject();
                builder.startObject("field2").field("type", "long").endObject();
            }));
            merge(mapperService, mapping);
            // field overrides now the concrete field already defined
            KeywordScriptFieldType field = (KeywordScriptFieldType) mapperService.fieldType("field");
            assertEquals(KeywordFieldMapper.CONTENT_TYPE, field.typeName());
            field2 = (LongScriptFieldType) mapperService.fieldType("field2");
            assertEquals(NumberFieldMapper.NumberType.LONG.typeName(), field2.typeName());
        }
        {
            String mapping = Strings.toString(
                runtimeMapping(
                    // the existing runtime field gets updated
                    builder -> builder.startObject("field").field("type", "double").endObject()
                )
            );
            merge(mapperService, mapping);
            DoubleScriptFieldType field = (DoubleScriptFieldType) mapperService.fieldType("field");
            assertEquals(NumberFieldMapper.NumberType.DOUBLE.typeName(), field.typeName());
            LongScriptFieldType field2Updated = (LongScriptFieldType) mapperService.fieldType("field2");
            assertSame(field2, field2Updated);
        }
        {
            String mapping = Strings.toString(mapping(builder -> builder.startObject("concrete").field("type", "keyword").endObject()));
            merge(mapperService, mapping);
            DoubleScriptFieldType field = (DoubleScriptFieldType) mapperService.fieldType("field");
            assertEquals(NumberFieldMapper.NumberType.DOUBLE.typeName(), field.typeName());
            LongScriptFieldType field2Updated = (LongScriptFieldType) mapperService.fieldType("field2");
            assertSame(field2, field2Updated);
            MappedFieldType concrete = mapperService.fieldType("concrete");
            assertThat(concrete, instanceOf(KeywordFieldMapper.KeywordFieldType.class));
        }
        {
            String mapping = Strings.toString(runtimeMapping(builder -> builder.startObject("field3").field("type", "date").endObject()));
            merge(mapperService, mapping);
            assertEquals(XContentHelper.stripWhitespace("""
                {
                  "_doc": {
                    "runtime": {
                      "field": {
                        "type": "double"
                      },
                      "field2": {
                        "type": "long"
                      },
                      "field3": {
                        "type": "date"
                      }
                    },
                    "properties": {
                      "concrete": {
                        "type": "keyword"
                      },
                      "field": {
                        "type": "keyword"
                      }
                    }
                  }
                }"""), mapperService.documentMapper().mappingSource().toString());
        }
        {
            // remove a runtime field
            String mapping = Strings.toString(runtimeMapping(builder -> builder.nullField("field3")));
            merge(mapperService, mapping);
            assertEquals(XContentHelper.stripWhitespace("""
                {
                  "_doc": {
                    "runtime": {
                      "field": {
                        "type": "double"
                      },
                      "field2": {
                        "type": "long"
                      }
                    },
                    "properties": {
                      "concrete": {
                        "type": "keyword"
                      },
                      "field": {
                        "type": "keyword"
                      }
                    }
                  }
                }"""), mapperService.documentMapper().mappingSource().toString());
        }
    }

    public void testRuntimeSectionNonRuntimeType() throws IOException {
        XContentBuilder mapping = runtimeFieldMapping(builder -> builder.field("type", "unknown"));
        MapperParsingException e = expectThrows(MapperParsingException.class, () -> createMapperService(mapping));
        assertEquals("Failed to parse mapping: No handler for type [unknown] declared on runtime field [field]", e.getMessage());
    }

    public void testRuntimeSectionHandlerNotFound() throws IOException {
        XContentBuilder mapping = runtimeFieldMapping(builder -> builder.field("type", "unknown"));
        MapperParsingException e = expectThrows(MapperParsingException.class, () -> createMapperService(mapping));
        assertEquals("Failed to parse mapping: No handler for type [unknown] declared on runtime field [field]", e.getMessage());
    }

    public void testRuntimeSectionMissingType() throws IOException {
        XContentBuilder mapping = runtimeFieldMapping(builder -> {});
        MapperParsingException e = expectThrows(MapperParsingException.class, () -> createMapperService(mapping));
        assertEquals("Failed to parse mapping: No type specified for runtime field [field]", e.getMessage());
    }

    public void testRuntimeSectionWrongFormat() throws IOException {
        XContentBuilder mapping = runtimeMapping(builder -> builder.field("field", 123));
        MapperParsingException e = expectThrows(MapperParsingException.class, () -> createMapperService(mapping));
        assertEquals(
            "Failed to parse mapping: Expected map for runtime field [field] definition but got a java.lang.Integer",
            e.getMessage()
        );
    }

    public void testRuntimeSectionRemainingField() throws IOException {
        XContentBuilder mapping = runtimeFieldMapping(builder -> builder.field("type", "keyword").field("unsupported", "value"));
        MapperParsingException e = expectThrows(MapperParsingException.class, () -> createMapperService(mapping));
        assertEquals("Failed to parse mapping: unknown parameter [unsupported] on runtime field [field] of type [keyword]", e.getMessage());
    }

    public void testPassThroughObjectWithAliases() throws IOException {
        MapperService mapperService = createMapperService(mapping(b -> {
            b.startObject("labels").field("type", "passthrough");
            {
                b.startObject("properties");
                b.startObject("dim").field("type", "keyword").endObject();
                b.endObject();
            }
            b.endObject();
        }));
        assertThat(mapperService.mappingLookup().getMapper("dim"), instanceOf(FieldAliasMapper.class));
        assertThat(mapperService.mappingLookup().getMapper("labels.dim"), instanceOf(KeywordFieldMapper.class));
    }

    public void testPassThroughObjectNested() throws IOException {
        MapperService mapperService = createMapperService(mapping(b -> {
            b.startObject("resource").field("type", "object");
            {
                b.startObject("properties");
                {
                    b.startObject("attributes").field("type", "passthrough");
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
            b.startObject("attributes").field("type", "passthrough");
            {
                b.startObject("properties");
                b.startObject("another.dim").field("type", "keyword").endObject();
                b.endObject();
            }
            b.endObject();
        }));
        assertThat(mapperService.mappingLookup().getMapper("dim"), instanceOf(FieldAliasMapper.class));
        assertThat(mapperService.mappingLookup().getMapper("resource.attributes.dim"), instanceOf(KeywordFieldMapper.class));
        assertThat(mapperService.mappingLookup().objectMappers().get("another").getMapper("dim"), instanceOf(FieldAliasMapper.class));
        assertThat(mapperService.mappingLookup().getMapper("attributes.another.dim"), instanceOf(KeywordFieldMapper.class));
    }

    public void testPassThroughObjectNestedWithDuplicateNames() throws IOException {
        MapperService mapperService = createMapperService(mapping(b -> {
            b.startObject("resource").field("type", "object");
            {
                b.startObject("properties");
                {
                    b.startObject("attributes").field("type", "passthrough");
                    {
                        b.startObject("properties");
                        b.startObject("dim").field("type", "keyword").endObject();
                        b.startObject("more.attributes.another.dimA").field("type", "keyword").endObject();
                        b.startObject("more.attributes.another.dimB").field("type", "keyword").endObject();
                        b.endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endObject();
            b.startObject("attributes").field("type", "passthrough");
            {
                b.startObject("properties");
                b.startObject("another.dim").field("type", "keyword").endObject();
                b.startObject("more.attributes.another.dimC").field("type", "keyword").endObject();
                b.startObject("more.attributes.another.dimD").field("type", "keyword").endObject();
                b.endObject();
            }
            b.endObject();
        }));

        assertThat(mapperService.mappingLookup().getMapper("dim"), instanceOf(FieldAliasMapper.class));
        assertThat(mapperService.mappingLookup().getMapper("resource.attributes.dim"), instanceOf(KeywordFieldMapper.class));
        assertThat(
            mapperService.mappingLookup().objectMappers().get("more.attributes.another").getMapper("dimA"),
            instanceOf(FieldAliasMapper.class)
        );
        assertThat(
            mapperService.mappingLookup().getMapper("resource.attributes.more.attributes.another.dimA"),
            instanceOf(KeywordFieldMapper.class)
        );
        assertThat(
            mapperService.mappingLookup().objectMappers().get("more.attributes.another").getMapper("dimB"),
            instanceOf(FieldAliasMapper.class)
        );
        assertThat(
            mapperService.mappingLookup().getMapper("resource.attributes.more.attributes.another.dimB"),
            instanceOf(KeywordFieldMapper.class)
        );

        assertThat(mapperService.mappingLookup().objectMappers().get("another").getMapper("dim"), instanceOf(FieldAliasMapper.class));
        assertThat(mapperService.mappingLookup().getMapper("attributes.another.dim"), instanceOf(KeywordFieldMapper.class));
        assertThat(
            mapperService.mappingLookup().objectMappers().get("more.attributes.another").getMapper("dimC"),
            instanceOf(FieldAliasMapper.class)
        );
        assertThat(
            mapperService.mappingLookup().getMapper("attributes.more.attributes.another.dimC"),
            instanceOf(KeywordFieldMapper.class)
        );
        assertThat(
            mapperService.mappingLookup().objectMappers().get("more.attributes.another").getMapper("dimD"),
            instanceOf(FieldAliasMapper.class)
        );
        assertThat(
            mapperService.mappingLookup().getMapper("attributes.more.attributes.another.dimD"),
            instanceOf(KeywordFieldMapper.class)
        );
    }

    public void testPassThroughObjectNestedWithConflictingNames() throws IOException {
        MapperParsingException e = expectThrows(MapperParsingException.class, () -> createMapperService(mapping(b -> {
            b.startObject("resource").field("type", "object");
            {
                b.startObject("properties");
                {
                    b.startObject("attributes").field("type", "passthrough");
                    {
                        b.startObject("properties");
                        b.startObject("dim").field("type", "keyword").endObject();
                        b.startObject("resource.attributes.another.dim").field("type", "keyword").endObject();
                        b.endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endObject();
        })));
        assertEquals(
            "Failed to parse mapping: Conflicting objects created during alias generation for pass-through field: [resource]",
            e.getMessage()
        );
    }

    public void testAliasMappersCreatesAlias() throws Exception {
        var context = MapperBuilderContext.root(false, false);
        Map<String, Mapper> aliases = new RootObjectMapper.Builder("root", Explicit.EXPLICIT_FALSE).getAliasMappers(
            Map.of(
                "labels",
                new PassThroughObjectMapper(
                    "labels",
                    "labels",
                    Explicit.EXPLICIT_TRUE,
                    ObjectMapper.Dynamic.FALSE,
                    Map.of("host", new KeywordFieldMapper.Builder("host", IndexVersion.current()).build(context)),
                    Explicit.EXPLICIT_FALSE
                )
            ),
            context
        );
        assertEquals(1, aliases.size());
        assertThat(aliases.get("host"), instanceOf(FieldAliasMapper.class));
    }

    public void testAliasMappersCreatesAliasNested() throws Exception {
        var context = MapperBuilderContext.root(false, false);
        Map<String, Mapper> aliases = new RootObjectMapper.Builder("root", Explicit.EXPLICIT_FALSE).getAliasMappers(
            Map.of(
                "outer",
                new ObjectMapper(
                    "outer",
                    "outer",
                    Explicit.EXPLICIT_TRUE,
                    Explicit.EXPLICIT_TRUE,
                    ObjectMapper.Dynamic.FALSE,
                    Map.of(
                        "inner",
                        new PassThroughObjectMapper(
                            "inner",
                            "outer.inner",
                            Explicit.EXPLICIT_TRUE,
                            ObjectMapper.Dynamic.FALSE,
                            Map.of("host", new KeywordFieldMapper.Builder("host", IndexVersion.current()).build(context)),
                            Explicit.EXPLICIT_FALSE
                        )
                    )
                )
            ),
            context
        );
        assertEquals(1, aliases.size());
        assertThat(aliases.get("host"), instanceOf(FieldAliasMapper.class));
    }

    public void testAliasMappersExitsInDeepNesting() throws Exception {
        var context = MapperBuilderContext.root(false, false);
        Map<String, Mapper> aliases = new HashMap<>();
        var objectIntermediates = new HashMap<String, ObjectMapper.Builder>(1);
        var objectIntermediatesFullPath = new HashMap<String, ObjectMapper.Builder>(1);
        new RootObjectMapper.Builder("root", Explicit.EXPLICIT_FALSE).getAliasMappers(
            Map.of(
                "labels",
                new PassThroughObjectMapper(
                    "labels",
                    "labels",
                    Explicit.EXPLICIT_TRUE,
                    ObjectMapper.Dynamic.FALSE,
                    Map.of("host", new KeywordFieldMapper.Builder("host", IndexVersion.current()).build(context)),
                    Explicit.EXPLICIT_FALSE
                )
            ),
            Map.of(),
            aliases,
            objectIntermediates,
            objectIntermediatesFullPath,
            context,
            1_000_000
        );
        assertTrue(aliases.isEmpty());
    }

    public void testAliasMappersCreatesNoAliasForRegularObject() throws Exception {
        var context = MapperBuilderContext.root(false, false);
        Map<String, Mapper> aliases = new RootObjectMapper.Builder("root", Explicit.EXPLICIT_FALSE).getAliasMappers(
            Map.of(
                "labels",
                new ObjectMapper(
                    "labels",
                    "labels",
                    Explicit.EXPLICIT_TRUE,
                    Explicit.EXPLICIT_FALSE,
                    ObjectMapper.Dynamic.FALSE,
                    Map.of("host", new KeywordFieldMapper.Builder("host", IndexVersion.current()).build(context))
                )
            ),
            context
        );
        assertTrue(aliases.isEmpty());
    }

    public void testAliasMappersConflictingField() throws Exception {
        var context = MapperBuilderContext.root(false, false);
        Map<String, Mapper> aliases = new RootObjectMapper.Builder("root", Explicit.EXPLICIT_FALSE).getAliasMappers(
            Map.of(
                "labels",
                new PassThroughObjectMapper(
                    "labels",
                    "labels",
                    Explicit.EXPLICIT_TRUE,
                    ObjectMapper.Dynamic.FALSE,
                    Map.of("host", new KeywordFieldMapper.Builder("host", IndexVersion.current()).build(context)),
                    Explicit.EXPLICIT_FALSE
                ),
                "host",
                new KeywordFieldMapper.Builder("host", IndexVersion.current()).build(context)
            ),
            context
        );
        assertTrue(aliases.isEmpty());
    }

    public void testEmptyType() throws Exception {
        String mapping = Strings.toString(
            XContentFactory.jsonBuilder()
                .startObject()
                .startObject("")
                .startObject("properties")
                .startObject("name")
                .field("type", "text")
                .endObject()
                .endObject()
                .endObject()
                .endObject()
        );

        // Empty name not allowed in index created after 5.0
        Exception e = expectThrows(MapperParsingException.class, () -> createMapperService(mapping));
        assertThat(e.getMessage(), containsString("type cannot be an empty string"));
    }

    public void testWithoutMappers() throws IOException {
        RootObjectMapper shallowRoot = createRootObjectMapperWithAllParametersSet(b -> {}, b -> {});
        RootObjectMapper root = createRootObjectMapperWithAllParametersSet(b -> {
            b.startObject("keyword");
            {
                b.field("type", "keyword");
            }
            b.endObject();
        }, b -> {
            b.startObject("runtime");
            b.startObject("field").field("type", "keyword").endObject();
            b.endObject();
        });
        assertThat(root.withoutMappers().toString(), equalTo(shallowRoot.toString()));
    }

    private RootObjectMapper createRootObjectMapperWithAllParametersSet(
        CheckedConsumer<XContentBuilder, IOException> buildProperties,
        CheckedConsumer<XContentBuilder, IOException> buildRuntimeFields
    ) throws IOException {
        DocumentMapper mapper = createDocumentMapper(topMapping(b -> {
            b.field("enabled", false);
            b.field("subobjects", false);
            b.field("dynamic", false);
            b.field("date_detection", false);
            b.field("numeric_detection", false);
            b.field("dynamic_date_formats", Collections.singletonList("yyyy-MM-dd"));
            b.startArray("dynamic_templates");
            {
                b.startObject();
                {
                    b.startObject("my_template");
                    {
                        b.startObject("mapping").field("type", "keyword").endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endArray();
            b.startObject("properties");
            buildProperties.accept(b);
            b.endObject();
        }));
        return mapper.mapping().getRoot();
    }

}
