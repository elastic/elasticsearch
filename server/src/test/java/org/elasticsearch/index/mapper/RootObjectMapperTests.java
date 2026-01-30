/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.index.mapper.MapperService.MergeReason;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

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
        assertEquals(
            "Failed to parse mapping: The mapper type [unknown] declared on runtime field [field] does not exist."
                + " It might have been created within a future version or requires a plugin to be installed. Check the documentation.",
            e.getMessage()
        );
    }

    public void testRuntimeSectionHandlerNotFound() throws IOException {
        XContentBuilder mapping = runtimeFieldMapping(builder -> builder.field("type", "unknown"));
        MapperParsingException e = expectThrows(MapperParsingException.class, () -> createMapperService(mapping));
        assertEquals(
            "Failed to parse mapping: The mapper type [unknown] declared on runtime field [field] does not exist."
                + " It might have been created within a future version or requires a plugin to be installed. Check the documentation.",
            e.getMessage()
        );
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

    public void testWithRootObjectMapperNamespaceValidator() throws Exception {
        String errorMessage = "error 1234";
        String disallowed = "_project";
        RootObjectMapperNamespaceValidator validator = new TestRootObjectMapperNamespaceValidator(disallowed, errorMessage);

        String notNested = """
            {
                "_doc": {
                    "properties": {
                        "<FIELD_NAME>": {
                            "type": "<TYPE>"
                        }
                    }
                }
            }""";

        // _project should fail, regardless of type
        {
            String json = notNested.replace("<FIELD_NAME>", disallowed);

            String keyword = json.replace("<TYPE>", "keyword");
            Exception e = expectThrows(IllegalArgumentException.class, () -> createMapperServiceWithNamespaceValidator(keyword, validator));
            assertThat(e.getMessage(), equalTo(errorMessage));

            String text = json.replace("<TYPE>", "text");
            e = expectThrows(IllegalArgumentException.class, () -> createMapperServiceWithNamespaceValidator(text, validator));
            assertThat(e.getMessage(), equalTo(errorMessage));

            String object = json.replace("<TYPE>", "object");
            e = expectThrows(IllegalArgumentException.class, () -> createMapperServiceWithNamespaceValidator(object, validator));
            assertThat(e.getMessage(), equalTo(errorMessage));
        }

        // _project.subfield should fail
        {
            String json = notNested.replace("<FIELD_NAME>", disallowed + ".subfield")
                .replace("<TYPE>", randomFrom("text", "keyword", "object"));
            Exception e = expectThrows(IllegalArgumentException.class, () -> createMapperServiceWithNamespaceValidator(json, validator));
            assertThat(e.getMessage(), equalTo(errorMessage));
        }

        // _projectx should pass
        {
            String json = notNested.replace("<FIELD_NAME>", disallowed + "x").replace("<TYPE>", randomFrom("text", "keyword", "object"));
            MapperService mapperService = createMapperServiceWithNamespaceValidator(json, validator);
            assertNotNull(mapperService);
        }

        // _project_subfield should pass
        {
            String json = notNested.replace("<FIELD_NAME>", disallowed + "_subfield");
            json = json.replace("<TYPE>", randomFrom("text", "keyword", "object"));
            MapperService mapperService = createMapperServiceWithNamespaceValidator(json, validator);
            assertNotNull(mapperService);
        }

        // _projectx.subfield should pass
        {
            String json = notNested.replace("<FIELD_NAME>", disallowed + "x.subfield");
            json = json.replace("<TYPE>", randomFrom("text", "keyword", "object"));
            MapperService mapperService = createMapperServiceWithNamespaceValidator(json, validator);
            assertNotNull(mapperService);
        }

        String nested = """
            {
              "_doc": {
                "properties": {
                  "<FIELD_NAME1>": {
                    "type": "object",
                    "properties": {
                      "<FIELD_NAME1>": {
                        "type": "keyword"
                      }
                    }
                  }
                }
              }
            }""";

        // nested _project { my_field } should fail
        {
            String json = nested.replace("<FIELD_NAME1>", disallowed).replace("<FIELD_NAME2>", "my_field");
            Exception e = expectThrows(IllegalArgumentException.class, () -> createMapperServiceWithNamespaceValidator(json, validator));
            assertThat(e.getMessage(), equalTo(errorMessage));
        }

        // nested my_field { _project } should succeed
        {
            String json = nested.replace("<FIELD_NAME1>", "my_field").replace("<FIELD_NAME2>", disallowed);
            MapperService mapperService = createMapperServiceWithNamespaceValidator(json, validator);
            assertNotNull(mapperService);
        }

        // nested _projectx { _project } should succeed
        {
            String json = nested.replace("<FIELD_NAME1>", disallowed + "x").replace("<FIELD_NAME2>", disallowed);
            MapperService mapperService = createMapperServiceWithNamespaceValidator(json, validator);
            assertNotNull(mapperService);
        }
    }

    public void testSubobjectsWithRootObjectMapperNamespaceValidator() throws Exception {
        String errorMessage = "error 1234";
        String disallowed = "_project";
        RootObjectMapperNamespaceValidator validator = new TestRootObjectMapperNamespaceValidator(disallowed, errorMessage);

        // test with subobjects setting
        String withSubobjects = """
            {
                "_doc": {
                    "subobjects": "<SUBOBJECTS_SETTING>",
                    "properties": {
                        "<FIELD_NAME>": {
                            "type": "object",
                            "properties": {
                                "my_field": {
                                    "type": "keyword"
                                }
                            }
                        }
                    }
                }
            }""";

        final String[] validSubojectsValues = new String[] { "false", "true" };
        {
            String json = withSubobjects.replace("<SUBOBJECTS_SETTING>", "false").replace("<FIELD_NAME>", "_project");
            Exception e = expectThrows(IllegalArgumentException.class, () -> createMapperServiceWithNamespaceValidator(json, validator));
            assertThat(e.getMessage(), equalTo(errorMessage));
        }
        {
            String json = withSubobjects.replace("<SUBOBJECTS_SETTING>", "true").replace("<FIELD_NAME>", "_project");
            Exception e = expectThrows(IllegalArgumentException.class, () -> createMapperServiceWithNamespaceValidator(json, validator));
            assertThat(e.getMessage(), equalTo(errorMessage));
        }
        {
            String json = withSubobjects.replace("<SUBOBJECTS_SETTING>", randomFrom(validSubojectsValues))
                .replace("<FIELD_NAME>", "_project.foo");
            Exception e = expectThrows(IllegalArgumentException.class, () -> createMapperServiceWithNamespaceValidator(json, validator));
            assertThat(e.getMessage(), equalTo(errorMessage));
        }
        {
            String json = withSubobjects.replace("<SUBOBJECTS_SETTING>", randomFrom(validSubojectsValues))
                .replace("<FIELD_NAME>", "project.foo");
            MapperService mapperService = createMapperServiceWithNamespaceValidator(json, validator);
            assertNotNull(mapperService);
        }
    }

    public void testRuntimeFieldInMappingWithNamespaceValidator() throws IOException {
        String errorMessage = "error 1234";
        String disallowed = "_project";
        RootObjectMapperNamespaceValidator validator = new TestRootObjectMapperNamespaceValidator(disallowed, errorMessage);

        // ensure that things close to the disallowed fields that are allowed
        {
            String mapping = Strings.toString(runtimeMapping(builder -> {
                builder.startObject(disallowed + "_x").field("type", "ip").endObject();
                builder.startObject(disallowed + "x").field("type", "date").endObject();
                builder.startObject("field1." + disallowed).field("type", "double").endObject();
            }));
            MapperService mapperService = createMapperServiceWithNamespaceValidator(mapping, validator);
            assertEquals(mapping, mapperService.documentMapper().mappingSource().toString());
            assertEquals(3, mapperService.documentMapper().mapping().getRoot().getTotalFieldsCount());
        }

        // _project is rejected
        {
            String mapping = Strings.toString(runtimeMapping(builder -> {
                builder.startObject("field1").field("type", "double").endObject();
                builder.startObject(disallowed).field("type", "date").endObject();
                builder.startObject("field3").field("type", "ip").endObject();
            }));
            Exception e = expectThrows(MapperParsingException.class, () -> createMapperServiceWithNamespaceValidator(mapping, validator));
            Throwable cause = ExceptionsHelper.unwrap(e, IllegalArgumentException.class);
            assertNotNull(cause);
            assertThat(cause.getMessage(), equalTo(errorMessage));
        }

        // _project.my_sub_field is rejected
        {
            String mapping = Strings.toString(runtimeMapping(builder -> {
                builder.startObject("field1").field("type", "double").endObject();
                builder.startObject(disallowed + ".my_sub_field").field("type", "keyword").endObject();
                builder.startObject("field3").field("type", "ip").endObject();
            }));
            Exception e = expectThrows(MapperParsingException.class, () -> createMapperServiceWithNamespaceValidator(mapping, validator));
            Throwable cause = ExceptionsHelper.unwrap(e, IllegalArgumentException.class);
            assertNotNull(cause);
            assertThat(cause.getMessage(), equalTo(errorMessage));
        }
    }

    public void testSyntheticSourceKeepAllThrows() throws IOException {
        String mapping = Strings.toString(
            XContentFactory.jsonBuilder()
                .startObject()
                .startObject(MapperService.SINGLE_MAPPING_NAME)
                .field("synthetic_source_keep", "all")
                .endObject()
                .endObject()
        );
        Exception e = expectThrows(MapperParsingException.class, () -> createMapperService(mapping));
        assertThat(e.getMessage(), containsString("root object can't be configured with [synthetic_source_keep:all]"));
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

    static class TestRootObjectMapperNamespaceValidator implements RootObjectMapperNamespaceValidator {
        private final String disallowed;
        private final String errorMessage;

        TestRootObjectMapperNamespaceValidator(String disallowedNamespace, String errorMessage) {
            this.disallowed = disallowedNamespace;
            this.errorMessage = errorMessage;
        }

        @Override
        public void validateNamespace(ObjectMapper.Subobjects subobjects, String name) {
            if (name.equals(disallowed)) {
                throw new IllegalArgumentException(errorMessage);
            } else if (subobjects != ObjectMapper.Subobjects.ENABLED) {
                // name here will be something like _project.my_field, rather than just _project
                if (name.startsWith(disallowed + ".")) {
                    throw new IllegalArgumentException(errorMessage);
                }
            }
        }
    }
}
