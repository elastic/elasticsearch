/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.Version;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.mapper.MapperService.MergeReason;
import org.elasticsearch.runtimefields.mapper.DoubleScriptFieldType;
import org.elasticsearch.runtimefields.mapper.KeywordScriptFieldType;
import org.elasticsearch.runtimefields.mapper.LongScriptFieldType;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

import static org.elasticsearch.test.VersionUtils.randomVersionBetween;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;

public class RootObjectMapperTests extends MapperServiceTestCase {

    public void testNumericDetection() throws Exception {
        MergeReason reason = randomFrom(MergeReason.MAPPING_UPDATE, MergeReason.INDEX_TEMPLATE);
        String mapping = Strings.toString(XContentFactory.jsonBuilder()
                .startObject()
                    .startObject(MapperService.SINGLE_MAPPING_NAME)
                        .field("numeric_detection", false)
                    .endObject()
                .endObject());
        MapperService mapperService = createMapperService(MapperService.SINGLE_MAPPING_NAME, mapping);
        assertEquals(mapping, mapperService.documentMapper().mappingSource().toString());

        // update with a different explicit value
        String mapping2 = Strings.toString(XContentFactory.jsonBuilder()
                .startObject()
                .startObject(MapperService.SINGLE_MAPPING_NAME)
                    .field("numeric_detection", true)
                .endObject()
            .endObject());
        merge(mapperService, reason, mapping2);
        assertEquals(mapping2, mapperService.documentMapper().mappingSource().toString());

        // update with an implicit value: no change
        String mapping3 = Strings.toString(XContentFactory.jsonBuilder()
                .startObject()
                .startObject(MapperService.SINGLE_MAPPING_NAME)
                .endObject()
            .endObject());
        merge(mapperService, reason, mapping3);
        assertEquals(mapping2, mapperService.documentMapper().mappingSource().toString());
    }

    public void testDateDetection() throws Exception {
        MergeReason reason = randomFrom(MergeReason.MAPPING_UPDATE, MergeReason.INDEX_TEMPLATE);
        String mapping = Strings.toString(XContentFactory.jsonBuilder()
                .startObject()
                    .startObject(MapperService.SINGLE_MAPPING_NAME)
                        .field("date_detection", true)
                    .endObject()
                .endObject());
        MapperService mapperService = createMapperService(MapperService.SINGLE_MAPPING_NAME, mapping);
        assertEquals(mapping, mapperService.documentMapper().mappingSource().toString());

        // update with a different explicit value
        String mapping2 = Strings.toString(XContentFactory.jsonBuilder()
                .startObject()
                .startObject(MapperService.SINGLE_MAPPING_NAME)
                    .field("date_detection", false)
                .endObject()
            .endObject());
        merge(mapperService, reason, mapping2);
        assertEquals(mapping2, mapperService.documentMapper().mappingSource().toString());

        // update with an implicit value: no change
        String mapping3 = Strings.toString(XContentFactory.jsonBuilder()
                .startObject()
                .startObject(MapperService.SINGLE_MAPPING_NAME)
                .endObject()
            .endObject());
        merge(mapperService, reason, mapping3);
        assertEquals(mapping2, mapperService.documentMapper().mappingSource().toString());
    }

    public void testDateFormatters() throws Exception {
        MergeReason reason = randomFrom(MergeReason.MAPPING_UPDATE, MergeReason.INDEX_TEMPLATE);
        String mapping = Strings.toString(XContentFactory.jsonBuilder()
                .startObject()
                    .startObject(MapperService.SINGLE_MAPPING_NAME)
                        .field("dynamic_date_formats", Collections.singletonList("yyyy-MM-dd"))
                    .endObject()
                .endObject());
        MapperService mapperService = createMapperService(MapperService.SINGLE_MAPPING_NAME, mapping);
        assertEquals(mapping, mapperService.documentMapper().mappingSource().toString());

        // no update if formatters are not set explicitly
        String mapping2 = Strings.toString(XContentFactory.jsonBuilder()
                .startObject()
                .startObject(MapperService.SINGLE_MAPPING_NAME)
                .endObject()
            .endObject());
        merge(mapperService, reason, mapping2);
        assertEquals(mapping, mapperService.documentMapper().mappingSource().toString());

        String mapping3 = Strings.toString(XContentFactory.jsonBuilder()
                .startObject()
                .startObject(MapperService.SINGLE_MAPPING_NAME)
                    .field("dynamic_date_formats", Collections.emptyList())
                .endObject()
            .endObject());
        merge(mapperService, reason, mapping3);
        assertEquals(mapping3, mapperService.documentMapper().mappingSource().toString());
    }

    public void testDynamicTemplates() throws Exception {
        String mapping = Strings.toString(XContentFactory.jsonBuilder()
                .startObject()
                    .startObject(MapperService.SINGLE_MAPPING_NAME)
                        .startArray("dynamic_templates")
                            .startObject()
                                .startObject("my_template")
                                    .field("match_mapping_type", "string")
                                    .startObject("mapping")
                                        .field("type", "keyword")
                                    .endObject()
                                .endObject()
                            .endObject()
                        .endArray()
                    .endObject()
                .endObject());
        MapperService mapperService = createMapperService(MapperService.SINGLE_MAPPING_NAME, mapping);
        assertEquals(mapping, mapperService.documentMapper().mappingSource().toString());

        // no update if templates are not set explicitly
        String mapping2 = Strings.toString(XContentFactory.jsonBuilder()
                .startObject()
                .startObject(MapperService.SINGLE_MAPPING_NAME)
                .endObject()
            .endObject());
        merge(MapperService.SINGLE_MAPPING_NAME, mapperService, mapping2);
        assertEquals(mapping, mapperService.documentMapper().mappingSource().toString());

        String mapping3 = Strings.toString(XContentFactory.jsonBuilder()
                .startObject()
                .startObject(MapperService.SINGLE_MAPPING_NAME)
                    .field("dynamic_templates", Collections.emptyList())
                .endObject()
            .endObject());
        merge(MapperService.SINGLE_MAPPING_NAME, mapperService, mapping3);
        assertEquals(mapping3, mapperService.documentMapper().mappingSource().toString());
    }

    public void testDynamicTemplatesForIndexTemplate() throws IOException {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject()
            .startArray("dynamic_templates")
                .startObject()
                    .startObject("first_template")
                        .field("path_match", "first")
                        .startObject("mapping")
                            .field("type", "keyword")
                        .endObject()
                    .endObject()
                .endObject()
                .startObject()
                    .startObject("second_template")
                        .field("path_match", "second")
                        .startObject("mapping")
                            .field("type", "keyword")
                        .endObject()
                    .endObject()
                .endObject()
            .endArray()
        .endObject());
        MapperService mapperService = createMapperService(Version.CURRENT, Settings.EMPTY, () -> true);
        mapperService.merge(MapperService.SINGLE_MAPPING_NAME, new CompressedXContent(mapping), MergeReason.INDEX_TEMPLATE);

        // There should be no update if templates are not set.
        mapping = Strings.toString(XContentFactory.jsonBuilder().startObject()
            .startObject("properties")
                .startObject("field")
                    .field("type", "integer")
                .endObject()
            .endObject()
        .endObject());
        DocumentMapper mapper = mapperService.merge(MapperService.SINGLE_MAPPING_NAME,
            new CompressedXContent(mapping), MergeReason.INDEX_TEMPLATE);

        DynamicTemplate[] templates = mapper.mapping().getRoot().dynamicTemplates();
        assertEquals(2, templates.length);
        assertEquals("first_template", templates[0].name());
        assertEquals("first", templates[0].pathMatch());
        assertEquals("second_template", templates[1].name());
        assertEquals("second", templates[1].pathMatch());

        // Dynamic templates should be appended and deduplicated.
        mapping = Strings.toString(XContentFactory.jsonBuilder().startObject()
            .startArray("dynamic_templates")
                .startObject()
                    .startObject("third_template")
                        .field("path_match", "third")
                        .startObject("mapping")
                            .field("type", "integer")
                        .endObject()
                    .endObject()
                .endObject()
                .startObject()
                    .startObject("second_template")
                        .field("path_match", "second_updated")
                        .startObject("mapping")
                            .field("type", "double")
                        .endObject()
                    .endObject()
                .endObject()
            .endArray()
        .endObject());
        mapper = mapperService.merge(MapperService.SINGLE_MAPPING_NAME, new CompressedXContent(mapping), MergeReason.INDEX_TEMPLATE);

        templates = mapper.mapping().getRoot().dynamicTemplates();
        assertEquals(3, templates.length);
        assertEquals("first_template", templates[0].name());
        assertEquals("first", templates[0].pathMatch());
        assertEquals("second_template", templates[1].name());
        assertEquals("second_updated", templates[1].pathMatch());
        assertEquals("third_template", templates[2].name());
        assertEquals("third", templates[2].pathMatch());
    }

    public void testIllegalFormatField() throws Exception {
        String dynamicMapping = Strings.toString(XContentFactory.jsonBuilder()
            .startObject()
                .startObject(MapperService.SINGLE_MAPPING_NAME)
                    .startArray("dynamic_date_formats")
                        .startArray().value("test_format").endArray()
                    .endArray()
                .endObject()
            .endObject());
        String mapping = Strings.toString(XContentFactory.jsonBuilder()
            .startObject()
                .startObject(MapperService.SINGLE_MAPPING_NAME)
                    .startArray("date_formats")
                        .startArray().value("test_format").endArray()
                    .endArray()
                .endObject()
            .endObject());
        for (String m : Arrays.asList(mapping, dynamicMapping)) {
            MapperParsingException e = expectThrows(MapperParsingException.class,
                () -> createMapperService(MapperService.SINGLE_MAPPING_NAME, m));
            assertEquals("Failed to parse mapping [_doc]: Invalid format: [[test_format]]: expected string value", e.getMessage());
        }
    }

    public void testIllegalDynamicTemplates() throws Exception {
        String mapping = Strings.toString(XContentFactory.jsonBuilder()
                .startObject()
                .startObject(MapperService.SINGLE_MAPPING_NAME)
                    .startObject("dynamic_templates")
                    .endObject()
                .endObject()
            .endObject());

        MapperParsingException e = expectThrows(MapperParsingException.class,
            () -> createMapperService(MapperService.SINGLE_MAPPING_NAME, mapping));
        assertEquals("Failed to parse mapping [_doc]: Dynamic template syntax error. An array of named objects is expected.",
            e.getMessage());
    }

    public void testIllegalDynamicTemplateUnknownFieldType() throws Exception {
        XContentBuilder mapping = XContentFactory.jsonBuilder();
        mapping.startObject();
        {
            mapping.startObject(MapperService.SINGLE_MAPPING_NAME);
            mapping.startArray("dynamic_templates");
            {
                mapping.startObject();
                mapping.startObject("my_template");
                mapping.field("match_mapping_type", "string");
                mapping.startObject("mapping");
                mapping.field("type", "string");
                mapping.endObject();
                mapping.endObject();
                mapping.endObject();
            }
            mapping.endArray();
            mapping.endObject();
        }
        mapping.endObject();
        MapperService mapperService = createMapperService(mapping);
        assertThat(mapperService.documentMapper().mappingSource().toString(), containsString("\"type\":\"string\""));
        assertWarnings("dynamic template [my_template] has invalid content [{\"match_mapping_type\":\"string\",\"mapping\":{\"type\":" +
            "\"string\"}}], attempted to validate it with the following match_mapping_type: [string], " +
            "caused by [No mapper found for type [string]]");
    }

    public void testIllegalDynamicTemplateUnknownRuntimeFieldType() throws Exception {
        XContentBuilder mapping = XContentFactory.jsonBuilder();
        mapping.startObject();
        {
            mapping.startObject(MapperService.SINGLE_MAPPING_NAME);
            mapping.startArray("dynamic_templates");
            {
                mapping.startObject();
                mapping.startObject("my_template");
                mapping.field("match_mapping_type", "string");
                mapping.startObject("runtime");
                mapping.field("type", "unknown");
                mapping.endObject();
                mapping.endObject();
                mapping.endObject();
            }
            mapping.endArray();
            mapping.endObject();
        }
        mapping.endObject();
        createMapperService(mapping);
        assertWarnings("dynamic template [my_template] has invalid content [" +
            "{\"match_mapping_type\":\"string\",\"runtime\":{\"type\":\"unknown\"}}], " +
            "attempted to validate it with the following match_mapping_type: [string], " +
            "caused by [No runtime field found for type [unknown]]");
    }

    public void testIllegalDynamicTemplateUnknownAttribute() throws Exception {
        XContentBuilder mapping = XContentFactory.jsonBuilder();
        mapping.startObject();
        {
            mapping.startObject(MapperService.SINGLE_MAPPING_NAME);
            mapping.startArray("dynamic_templates");
            {
                mapping.startObject();
                mapping.startObject("my_template");
                mapping.field("match_mapping_type", "string");
                mapping.startObject("mapping");
                mapping.field("type", "keyword");
                mapping.field("foo", "bar");
                mapping.endObject();
                mapping.endObject();
                mapping.endObject();
            }
            mapping.endArray();
            mapping.endObject();
        }
        mapping.endObject();

        MapperService mapperService = createMapperService(mapping);
        assertThat(mapperService.documentMapper().mappingSource().toString(), containsString("\"foo\":\"bar\""));
        assertWarnings("dynamic template [my_template] has invalid content [{\"match_mapping_type\":\"string\",\"mapping\":{" +
            "\"foo\":\"bar\",\"type\":\"keyword\"}}], " +
            "attempted to validate it with the following match_mapping_type: [string], " +
            "caused by [unknown parameter [foo] on mapper [__dynamic__my_template] of type [keyword]]");
    }

    public void testIllegalDynamicTemplateUnknownAttributeRuntime() throws Exception {
        XContentBuilder mapping = XContentFactory.jsonBuilder();
        mapping.startObject();
        {
            mapping.startObject(MapperService.SINGLE_MAPPING_NAME);
            mapping.startArray("dynamic_templates");
            {
                mapping.startObject();
                mapping.startObject("my_template");
                mapping.field("match_mapping_type", "string");
                mapping.startObject("runtime");
                mapping.field("type", "keyword");
                mapping.field("foo", "bar");
                mapping.endObject();
                mapping.endObject();
                mapping.endObject();
            }
            mapping.endArray();
            mapping.endObject();
        }
        mapping.endObject();

        createMapperService(mapping);
        assertWarnings("dynamic template [my_template] has invalid content [" +
            "{\"match_mapping_type\":\"string\",\"runtime\":{\"foo\":\"bar\",\"type\":\"keyword\"}}], " +
            "attempted to validate it with the following match_mapping_type: [string], " +
            "caused by [unknown parameter [foo] on mapper [__dynamic__my_template] of type [keyword]]");
    }

    public void testIllegalDynamicTemplateInvalidAttribute() throws Exception {
        XContentBuilder mapping = XContentFactory.jsonBuilder();
        mapping.startObject();
        {
            mapping.startObject(MapperService.SINGLE_MAPPING_NAME);
            mapping.startArray("dynamic_templates");
            {
                mapping.startObject();
                mapping.startObject("my_template");
                mapping.field("match_mapping_type", "string");
                mapping.startObject("mapping");
                mapping.field("type", "text");
                mapping.field("analyzer", "foobar");
                mapping.endObject();
                mapping.endObject();
                mapping.endObject();
            }
            mapping.endArray();
            mapping.endObject();
        }
        mapping.endObject();

        MapperService mapperService = createMapperService(mapping);
        assertThat(mapperService.documentMapper().mappingSource().toString(), containsString("\"analyzer\":\"foobar\""));
        assertWarnings("dynamic template [my_template] has invalid content [{\"match_mapping_type\":\"string\",\"mapping\":{" +
            "\"analyzer\":\"foobar\",\"type\":\"text\"}}], attempted to validate it with the following match_mapping_type: [string], " +
            "caused by [analyzer [foobar] has not been configured in mappings]");
    }

    public void testIllegalDynamicTemplateNoMappingType() throws Exception {
        MapperService mapperService;
        {
            XContentBuilder mapping = XContentFactory.jsonBuilder();
            mapping.startObject();
            {
                mapping.startObject(MapperService.SINGLE_MAPPING_NAME);
                mapping.startArray("dynamic_templates");
                {
                    mapping.startObject();
                    mapping.startObject("my_template");
                    if (randomBoolean()) {
                        mapping.field("match_mapping_type", "*");
                    } else {
                        mapping.field("match", "string_*");
                    }
                    mapping.startObject("mapping");
                    mapping.field("type", "{dynamic_type}");
                    mapping.field("index_phrases", true);
                    mapping.endObject();
                    mapping.endObject();
                    mapping.endObject();
                }
                mapping.endArray();
                mapping.endObject();
            }
            mapping.endObject();
            mapperService = createMapperService(mapping);
            assertThat(mapperService.documentMapper().mappingSource().toString(), containsString("\"index_phrases\":true"));
        }
        {
            boolean useMatchMappingType = randomBoolean();
            XContentBuilder mapping = XContentFactory.jsonBuilder();
            mapping.startObject();
            {
                mapping.startObject(MapperService.SINGLE_MAPPING_NAME);
                mapping.startArray("dynamic_templates");
                {
                    mapping.startObject();
                    mapping.startObject("my_template");
                    if (useMatchMappingType) {
                        mapping.field("match_mapping_type", "*");
                    } else {
                        mapping.field("match", "string_*");
                    }
                    mapping.startObject("mapping");
                    mapping.field("type", "{dynamic_type}");
                    mapping.field("foo", "bar");
                    mapping.endObject();
                    mapping.endObject();
                    mapping.endObject();
                }
                mapping.endArray();
                mapping.endObject();
            }
            mapping.endObject();

            merge(mapperService, mapping);
            assertThat(mapperService.documentMapper().mappingSource().toString(), containsString("\"foo\":\"bar\""));
            if (useMatchMappingType) {
                assertWarnings("dynamic template [my_template] has invalid content [{\"match_mapping_type\":\"*\",\"mapping\":{" +
                    "\"foo\":\"bar\",\"type\":\"{dynamic_type}\"}}], " +
                    "attempted to validate it with the following match_mapping_type: " +
                    "[object, string, long, double, boolean, date, binary], " +
                    "caused by [unknown parameter [foo] on mapper [__dynamic__my_template] of type [binary]]");
            } else {
                assertWarnings("dynamic template [my_template] has invalid content [{\"match\":\"string_*\",\"mapping\":{" +
                    "\"foo\":\"bar\",\"type\":\"{dynamic_type}\"}}], " +
                    "attempted to validate it with the following match_mapping_type: " +
                    "[object, string, long, double, boolean, date, binary], " +
                    "caused by [unknown parameter [foo] on mapper [__dynamic__my_template] of type [binary]]");
            }
        }
    }

    public void testIllegalDynamicTemplateNoMappingTypeRuntime() throws Exception {
        XContentBuilder mapping = XContentFactory.jsonBuilder();
        String matchError;
        mapping.startObject();
        {
            mapping.startObject(MapperService.SINGLE_MAPPING_NAME);
            mapping.startArray("dynamic_templates");
            {
                mapping.startObject();
                mapping.startObject("my_template");
                if (randomBoolean()) {
                    mapping.field("match_mapping_type", "*");
                    matchError = "\"match_mapping_type\":\"*\"";
                } else {
                    mapping.field("match", "string_*");
                    matchError = "\"match\":\"string_*\"";
                }
                mapping.startObject("runtime");
                mapping.field("type", "{dynamic_type}");
                mapping.field("foo", "bar");
                mapping.endObject();
                mapping.endObject();
                mapping.endObject();
            }
            mapping.endArray();
            mapping.endObject();
        }
        mapping.endObject();

        createMapperService(mapping);
        String expected = "dynamic template [my_template] has invalid content [{" + matchError +
            ",\"runtime\":{\"foo\":\"bar\",\"type\":\"{dynamic_type}\"}}], " +
            "attempted to validate it with the following match_mapping_type: [string, long, double, boolean, date], " +
            "caused by [unknown parameter [foo] on mapper [__dynamic__my_template] of type [date]]";
        assertWarnings(expected);
    }

    public void testIllegalDynamicTemplatePre7Dot7Index() throws Exception {
        XContentBuilder mapping = XContentFactory.jsonBuilder();
        mapping.startObject();
        {
            mapping.startObject(MapperService.SINGLE_MAPPING_NAME);
            mapping.startArray("dynamic_templates");
            {
                mapping.startObject();
                mapping.startObject("my_template");
                mapping.field("match_mapping_type", "string");
                mapping.startObject("mapping");
                mapping.field("type", "string");
                mapping.endObject();
                mapping.endObject();
                mapping.endObject();
            }
            mapping.endArray();
            mapping.endObject();
        }
        mapping.endObject();

        Version createdVersion = randomVersionBetween(random(), Version.V_7_0_0, Version.V_7_6_0);
        MapperService mapperService = createMapperService(createdVersion, mapping);
        assertThat(mapperService.documentMapper().mappingSource().toString(), containsString("\"type\":\"string\""));
    }

    public void testRuntimeSection() throws IOException {
        String mapping = Strings.toString(runtimeMapping(builder -> {
            builder.startObject("field1").field("type", "double").endObject();
            builder.startObject("field2").field("type", "date").endObject();
            builder.startObject("field3").field("type", "ip").endObject();
        }));
        MapperService mapperService = createMapperService(MapperService.SINGLE_MAPPING_NAME, mapping);
        assertEquals(mapping, mapperService.documentMapper().mappingSource().toString());
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
            //try changing the type of the existing concrete field, so that merge fails
            builder.startObject("concrete").field("type", "text").endObject();
            builder.endObject();
            builder.endObject().endObject();

            expectThrows(IllegalArgumentException.class, () -> merge(mapperService, builder));

            //make sure that the whole rejected update, including changes to runtime fields, has not been applied
            MappedFieldType concrete = mapperService.fieldType("concrete");
            assertThat(concrete, instanceOf(KeywordFieldMapper.KeywordFieldType.class));
            MappedFieldType field = mapperService.fieldType("field");
            assertThat(field, instanceOf(LongScriptFieldType.class));
            assertNull(mapperService.fieldType("another_field"));
            assertEquals("{\"_doc\":{\"runtime\":{\"field\":{\"type\":\"long\"}},\"properties\":{\"concrete\":{\"type\":\"keyword\"}}}}",
                Strings.toString(mapperService.documentMapper().mapping().getRoot()));
        }
    }

    public void testRuntimeSectionMerge() throws IOException {
        MapperService mapperService;
        {
            String mapping = Strings.toString(fieldMapping(b -> b.field("type", "keyword")));
            mapperService = createMapperService(MapperService.SINGLE_MAPPING_NAME, mapping);
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
            merge(MapperService.SINGLE_MAPPING_NAME, mapperService, mapping);
            //field overrides now the concrete field already defined
            KeywordScriptFieldType field = (KeywordScriptFieldType)mapperService.fieldType("field");
            assertEquals(KeywordFieldMapper.CONTENT_TYPE, field.typeName());
            field2 = (LongScriptFieldType)mapperService.fieldType("field2");
            assertEquals(NumberFieldMapper.NumberType.LONG.typeName(), field2.typeName());
        }
        {
            String mapping = Strings.toString(runtimeMapping(
                //the existing runtime field gets updated
                builder -> builder.startObject("field").field("type", "double").endObject()));
            merge(MapperService.SINGLE_MAPPING_NAME, mapperService, mapping);
            DoubleScriptFieldType field = (DoubleScriptFieldType)mapperService.fieldType("field");
            assertEquals(NumberFieldMapper.NumberType.DOUBLE.typeName(), field.typeName());
            LongScriptFieldType field2Updated = (LongScriptFieldType)mapperService.fieldType("field2");
            assertSame(field2, field2Updated);
        }
        {
            String mapping = Strings.toString(mapping(builder -> builder.startObject("concrete").field("type", "keyword").endObject()));
            merge(MapperService.SINGLE_MAPPING_NAME, mapperService, mapping);
            DoubleScriptFieldType field = (DoubleScriptFieldType)mapperService.fieldType("field");
            assertEquals(NumberFieldMapper.NumberType.DOUBLE.typeName(), field.typeName());
            LongScriptFieldType field2Updated = (LongScriptFieldType)mapperService.fieldType("field2");
            assertSame(field2, field2Updated);
            MappedFieldType concrete = mapperService.fieldType("concrete");
            assertThat(concrete, instanceOf(KeywordFieldMapper.KeywordFieldType.class));
        }
        {
            String mapping = Strings.toString(runtimeMapping(
                builder -> builder.startObject("field3").field("type", "date").endObject()));
            merge(MapperService.SINGLE_MAPPING_NAME, mapperService, mapping);
            assertEquals("{\"_doc\":" +
                    "{\"runtime\":{" +
                    "\"field\":{\"type\":\"double\"}," +
                    "\"field2\":{\"type\":\"long\"}," +
                    "\"field3\":{\"type\":\"date\"}}," +
                    "\"properties\":{" +
                    "\"concrete\":{\"type\":\"keyword\"}," +
                    "\"field\":{\"type\":\"keyword\"}}}}",
                mapperService.documentMapper().mappingSource().toString());
        }
        {
            //remove a runtime field
            String mapping = Strings.toString(runtimeMapping(
                builder -> builder.nullField("field3")));
            merge(MapperService.SINGLE_MAPPING_NAME, mapperService, mapping);
            assertEquals("{\"_doc\":" +
                    "{\"runtime\":{" +
                    "\"field\":{\"type\":\"double\"}," +
                    "\"field2\":{\"type\":\"long\"}}," +
                    "\"properties\":{" +
                    "\"concrete\":{\"type\":\"keyword\"}," +
                    "\"field\":{\"type\":\"keyword\"}}}}",
                mapperService.documentMapper().mappingSource().toString());
        }
    }

    public void testRuntimeSectionNonRuntimeType() throws IOException {
        XContentBuilder mapping = runtimeFieldMapping(builder -> builder.field("type", "unknown"));
        MapperParsingException e = expectThrows(MapperParsingException.class, () -> createMapperService(mapping));
        assertEquals("Failed to parse mapping [_doc]: No handler for type [unknown] declared on runtime field [field]", e.getMessage());
    }

    public void testRuntimeSectionHandlerNotFound() throws IOException {
        XContentBuilder mapping = runtimeFieldMapping(builder -> builder.field("type", "unknown"));
        MapperParsingException e = expectThrows(MapperParsingException.class, () -> createMapperService(mapping));
        assertEquals("Failed to parse mapping [_doc]: No handler for type [unknown] declared on runtime field [field]", e.getMessage());
    }

    public void testRuntimeSectionMissingType() throws IOException {
        XContentBuilder mapping = runtimeFieldMapping(builder -> {});
        MapperParsingException e = expectThrows(MapperParsingException.class, () -> createMapperService(mapping));
        assertEquals("Failed to parse mapping [_doc]: No type specified for runtime field [field]", e.getMessage());
    }

    public void testRuntimeSectionWrongFormat() throws IOException {
        XContentBuilder mapping = runtimeMapping(builder -> builder.field("field", 123));
        MapperParsingException e = expectThrows(MapperParsingException.class, () -> createMapperService(mapping));
        assertEquals("Failed to parse mapping [_doc]: Expected map for runtime field [field] definition but got a java.lang.Integer",
            e.getMessage());
    }

    public void testRuntimeSectionRemainingField() throws IOException {
        XContentBuilder mapping = runtimeFieldMapping(builder -> builder.field("type", "keyword").field("unsupported", "value"));
        MapperParsingException e = expectThrows(MapperParsingException.class, () -> createMapperService(mapping));
        assertEquals("Failed to parse mapping [_doc]: unknown parameter [unsupported] on mapper [field] of type [keyword]", e.getMessage());
    }
}
