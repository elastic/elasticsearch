/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.common.Strings;
import org.elasticsearch.index.mapper.DynamicTemplate.XContentFieldType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class DynamicTemplateParseTests extends ESTestCase {

    public void testMappingTypeTypeNotSet() {
        Map<String, Object> templateDef = new HashMap<>();
        templateDef.put("match_mapping_type", "string");
        templateDef.put("mapping", Collections.emptyMap());
        DynamicTemplate template = DynamicTemplate.parse("my_template", templateDef);
        // when type is not set, the provided dynamic type is returned
        assertEquals("input", template.mappingType("input"));
    }

    public void testMappingTypeTypeNotSetRuntime() {
        Map<String, Object> templateDef = new HashMap<>();
        templateDef.put("match_mapping_type", "string");
        templateDef.put("runtime", Collections.emptyMap());
        DynamicTemplate template = DynamicTemplate.parse("my_template", templateDef);
        // when type is not set, the provided dynamic type is returned
        assertEquals("input", template.mappingType("input"));
    }

    public void testMappingType() {
        Map<String, Object> templateDef = new HashMap<>();
        templateDef.put("match_mapping_type", "string");
        templateDef.put("mapping", Collections.singletonMap("type", "type_set"));
        DynamicTemplate template = DynamicTemplate.parse("my_template", templateDef);
        // when type is set, the set type is returned
        assertEquals("type_set", template.mappingType("input"));
    }

    public void testMappingTypeRuntime() {
        Map<String, Object> templateDef = new HashMap<>();
        templateDef.put("match_mapping_type", "string");
        templateDef.put("runtime", Collections.singletonMap("type", "type_set"));
        DynamicTemplate template = DynamicTemplate.parse("my_template", templateDef);
        // when type is set, the set type is returned
        assertEquals("type_set", template.mappingType("input"));
    }

    public void testMappingTypeDynamicTypeReplace() {
        Map<String, Object> templateDef = new HashMap<>();
        templateDef.put("match_mapping_type", "string");
        templateDef.put("mapping", Collections.singletonMap("type", "type_set_{dynamic_type}_{dynamicType}"));
        DynamicTemplate template = DynamicTemplate.parse("my_template", templateDef);
        // when type is set, the set type is returned
        assertEquals("type_set_input_input", template.mappingType("input"));
    }

    public void testMappingTypeDynamicTypeReplaceRuntime() {
        Map<String, Object> templateDef = new HashMap<>();
        templateDef.put("match_mapping_type", "string");
        templateDef.put("runtime", Collections.singletonMap("type", "type_set_{dynamic_type}_{dynamicType}"));
        DynamicTemplate template = DynamicTemplate.parse("my_template", templateDef);
        // when type is set, the set type is returned
        assertEquals("type_set_input_input", template.mappingType("input"));
    }

    public void testMappingForName() throws IOException {
        Map<String, Object> templateDef = new HashMap<>();
        templateDef.put("match_mapping_type", "string");
        templateDef.put("mapping", Map.of("field1_{name}", "{dynamic_type}", "test", List.of("field2_{name}_{dynamicType}")));
        DynamicTemplate template = DynamicTemplate.parse("my_template", templateDef);
        Map<String, Object> stringObjectMap = template.mappingForName("my_name", "my_type");
        assertEquals(
            """
                {"field1_my_name":"my_type","test":["field2_my_name_my_type"]}""",
            Strings.toString(JsonXContent.contentBuilder().map(stringObjectMap))
        );
    }

    public void testMappingForNameRuntime() throws IOException {
        Map<String, Object> templateDef = new HashMap<>();
        templateDef.put("match_mapping_type", "string");
        templateDef.put("runtime", Map.of("field1_{name}", "{dynamic_type}", "test", List.of("field2_{name}_{dynamicType}")));
        DynamicTemplate template = DynamicTemplate.parse("my_template", templateDef);
        Map<String, Object> stringObjectMap = template.mappingForName("my_name", "my_type");
        assertEquals(
            """
                {"field1_my_name":"my_type","test":["field2_my_name_my_type"]}""",
            Strings.toString(JsonXContent.contentBuilder().map(stringObjectMap))
        );
    }

    public void testParseUnknownParam() {
        Map<String, Object> templateDef = new HashMap<>();
        templateDef.put("match_mapping_type", "string");
        templateDef.put("mapping", Collections.singletonMap("store", true));
        templateDef.put("random_param", "random_value");

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> DynamicTemplate.parse("my_template", templateDef));
        assertEquals("Illegal dynamic template parameter: [random_param]", e.getMessage());
    }

    public void testParseUnknownMatchType() {
        Map<String, Object> templateDef2 = new HashMap<>();
        templateDef2.put("match_mapping_type", "text");
        templateDef2.put("mapping", Collections.singletonMap("store", true));
        // if a wrong match type is specified, we ignore the template
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> DynamicTemplate.parse("my_template", templateDef2));
        assertEquals(
            "No field type matched on [text], possible values are [object, string, long, double, boolean, date, binary]",
            e.getMessage()
        );
    }

    public void testParseInvalidRegex() {
        for (String param : new String[] { "path_match", "match", "path_unmatch", "unmatch" }) {
            Map<String, Object> templateDef = new HashMap<>();
            templateDef.put("match", "foo");
            templateDef.put(param, "*a");
            templateDef.put("match_pattern", "regex");
            templateDef.put("mapping", Collections.singletonMap("store", true));
            MapperParsingException e = expectThrows(MapperParsingException.class, () -> DynamicTemplate.parse("my_template", templateDef));
            assertEquals("Pattern [*a] of type [regex] is invalid. Cannot create dynamic template [my_template].", e.getMessage());
        }
    }

    public void testParseMappingAndRuntime() {
        for (String param : new String[] { "path_match", "match", "path_unmatch", "unmatch" }) {
            Map<String, Object> templateDef = new HashMap<>();
            templateDef.put("match", "foo");
            templateDef.put(param, "*a");
            templateDef.put("match_pattern", "regex");
            templateDef.put("mapping", Collections.emptyMap());
            templateDef.put("runtime", Collections.emptyMap());
            MapperParsingException e = expectThrows(MapperParsingException.class, () -> DynamicTemplate.parse("my_template", templateDef));
            assertEquals("mapping and runtime cannot be both specified in the same dynamic template [my_template]", e.getMessage());
        }
    }

    public void testParseMissingMapping() {
        for (String param : new String[] { "path_match", "match", "path_unmatch", "unmatch" }) {
            Map<String, Object> templateDef = new HashMap<>();
            templateDef.put("match", "foo");
            templateDef.put(param, "*a");
            templateDef.put("match_pattern", "regex");
            MapperParsingException e = expectThrows(MapperParsingException.class, () -> DynamicTemplate.parse("my_template", templateDef));
            assertEquals("template [my_template] must have either mapping or runtime set", e.getMessage());
        }
    }

    public void testMatchAllTemplate() {
        Map<String, Object> templateDef = new HashMap<>();
        templateDef.put("match_mapping_type", "*");
        templateDef.put("mapping", Collections.singletonMap("store", true));
        DynamicTemplate template = DynamicTemplate.parse("my_template", templateDef);
        assertTrue(template.match(null, "a.b", "b", randomFrom(XContentFieldType.values())));
        assertFalse(template.isRuntimeMapping());
    }

    public void testMatchAllTemplateRuntime() {
        Map<String, Object> templateDef = new HashMap<>();
        templateDef.put("match_mapping_type", "*");
        templateDef.put("runtime", Collections.emptyMap());
        DynamicTemplate template = DynamicTemplate.parse("my_template", templateDef);
        assertTrue(template.isRuntimeMapping());
        assertTrue(template.match(null, "a.b", "b", XContentFieldType.BOOLEAN));
        assertTrue(template.match(null, "a.b", "b", XContentFieldType.DATE));
        assertTrue(template.match(null, "a.b", "b", XContentFieldType.STRING));
        assertTrue(template.match(null, "a.b", "b", XContentFieldType.DOUBLE));
        assertTrue(template.match(null, "a.b", "b", XContentFieldType.LONG));
        assertFalse(template.match(null, "a.b", "b", XContentFieldType.OBJECT));
        assertFalse(template.match(null, "a.b", "b", XContentFieldType.BINARY));
    }

    public void testMatchAllTypesTemplateRuntime() {
        Map<String, Object> templateDef = new HashMap<>();
        templateDef.put("match", "b");
        templateDef.put("runtime", Collections.emptyMap());
        DynamicTemplate template = DynamicTemplate.parse("my_template", templateDef);
        assertTrue(template.isRuntimeMapping());
        assertTrue(template.match(null, "a.b", "b", XContentFieldType.BOOLEAN));
        assertTrue(template.match(null, "a.b", "b", XContentFieldType.DATE));
        assertTrue(template.match(null, "a.b", "b", XContentFieldType.STRING));
        assertTrue(template.match(null, "a.b", "b", XContentFieldType.DOUBLE));
        assertTrue(template.match(null, "a.b", "b", XContentFieldType.LONG));
        assertFalse(template.match(null, "a.b", "b", XContentFieldType.OBJECT));
        assertFalse(template.match(null, "a.b", "b", XContentFieldType.BINARY));
    }

    public void testMatchAllTypesTemplateRuntimeWithListOfMatches() {
        Map<String, Object> templateDef = new HashMap<>();
        templateDef.put("match", List.of("b", "c*"));
        templateDef.put("runtime", Collections.emptyMap());
        DynamicTemplate template = DynamicTemplate.parse("my_template", templateDef);
        assertTrue(template.isRuntimeMapping());
        assertTrue(template.match(null, "a.b", "b", XContentFieldType.BOOLEAN));
        assertTrue(template.match(null, "a.b", "b", XContentFieldType.DATE));
        assertTrue(template.match(null, "a.b", "b", XContentFieldType.STRING));
        assertTrue(template.match(null, "a.b", "b", XContentFieldType.DOUBLE));
        assertTrue(template.match(null, "a.b", "b", XContentFieldType.LONG));
        assertFalse(template.match(null, "a.b", "b", XContentFieldType.OBJECT));
        assertFalse(template.match(null, "a.b", "b", XContentFieldType.BINARY));

        assertTrue(template.match(null, null, "cx", XContentFieldType.BOOLEAN));
        assertTrue(template.match(null, null, "cx", XContentFieldType.DATE));
        assertTrue(template.match(null, null, "cx", XContentFieldType.STRING));
        assertTrue(template.match(null, null, "cx", XContentFieldType.DOUBLE));
        assertTrue(template.match(null, null, "cx", XContentFieldType.LONG));
        assertFalse(template.match(null, null, "cx", XContentFieldType.OBJECT));
        assertFalse(template.match(null, null, "cx", XContentFieldType.BINARY));
    }

    public void testMatchTypeTemplate() {
        Map<String, Object> templateDef = new HashMap<>();
        templateDef.put("match_mapping_type", "string");
        templateDef.put("mapping", Collections.singletonMap("store", true));
        DynamicTemplate template = DynamicTemplate.parse("my_template", templateDef);
        assertTrue(template.match(null, "a.b", "b", XContentFieldType.STRING));
        assertFalse(template.match(null, "a.b", "b", XContentFieldType.BOOLEAN));
        assertFalse(template.isRuntimeMapping());
    }

    public void testMatchTypeTemplateRuntime() {
        List<XContentFieldType> runtimeFieldTypes = Arrays.stream(XContentFieldType.values())
            .filter(XContentFieldType::supportsRuntimeField)
            .toList();
        for (XContentFieldType runtimeFieldType : runtimeFieldTypes) {
            Map<String, Object> templateDef = new HashMap<>();
            templateDef.put("match_mapping_type", runtimeFieldType.name().toLowerCase(Locale.ROOT));
            templateDef.put("runtime", Collections.emptyMap());
            DynamicTemplate template = DynamicTemplate.parse("my_template", templateDef);
            assertTrue(template.isRuntimeMapping());
            for (XContentFieldType xContentFieldType : XContentFieldType.values()) {
                if (xContentFieldType == runtimeFieldType) {
                    assertTrue(template.match(null, "a.b", "b", xContentFieldType));
                } else {
                    assertFalse(template.match(null, "a.b", "b", xContentFieldType));
                }
            }
        }
    }

    public void testMatchTypeTemplateRuntimeUnsupported() {
        List<XContentFieldType> xContentFieldTypes = Arrays.stream(XContentFieldType.values())
            .filter(xContentFieldType -> xContentFieldType.supportsRuntimeField() == false)
            .toList();
        for (XContentFieldType xContentFieldType : xContentFieldTypes) {
            String fieldType = xContentFieldType.name().toLowerCase(Locale.ROOT);
            Map<String, Object> templateDef = new HashMap<>();
            templateDef.put("match_mapping_type", fieldType);
            templateDef.put("runtime", Collections.emptyMap());
            MapperParsingException e = expectThrows(MapperParsingException.class, () -> DynamicTemplate.parse("my_template", templateDef));
            assertEquals(
                "Dynamic template [my_template] defines a runtime field but type [" + fieldType + "] is not supported as runtime field",
                e.getMessage()
            );
        }
    }

    public void testSupportedMatchMappingTypesRuntime() {
        // binary and object are not supported as runtime fields
        List<String> nonSupported = Arrays.asList("binary", "object");
        for (String type : nonSupported) {
            Map<String, Object> templateDef = new HashMap<>();
            templateDef.put("match_mapping_type", type);
            templateDef.put("runtime", Collections.emptyMap());
            MapperParsingException e = expectThrows(MapperParsingException.class, () -> DynamicTemplate.parse("my_template", templateDef));
            assertEquals(
                "Dynamic template [my_template] defines a runtime field but type [" + type + "] is not supported as runtime field",
                e.getMessage()
            );
        }
        XContentFieldType[] supported = Arrays.stream(XContentFieldType.values())
            .filter(XContentFieldType::supportsRuntimeField)
            .toArray(XContentFieldType[]::new);
        for (XContentFieldType type : supported) {
            Map<String, Object> templateDef = new HashMap<>();
            templateDef.put("match_mapping_type", type);
            templateDef.put("runtime", Collections.emptyMap());
            assertNotNull(DynamicTemplate.parse("my_template", templateDef));
        }
    }

    public void testSerialization() throws Exception {
        // type-based template
        Map<String, Object> templateDef = new HashMap<>();
        templateDef.put("match_mapping_type", "string");
        templateDef.put("mapping", Collections.singletonMap("store", true));
        DynamicTemplate template = DynamicTemplate.parse("my_template", templateDef);
        XContentBuilder builder = JsonXContent.contentBuilder();
        template.toXContent(builder, ToXContent.EMPTY_PARAMS);
        assertEquals("""
            {"match_mapping_type":"string","mapping":{"store":true}}""", Strings.toString(builder));

        // name-based template
        templateDef = new HashMap<>();
        if (randomBoolean()) {
            templateDef.put("match_mapping_type", "*");
        }
        templateDef.put("match", "*name");
        templateDef.put("unmatch", "first_name");
        templateDef.put("mapping", Collections.singletonMap("store", true));
        template = DynamicTemplate.parse("my_template", templateDef);
        builder = JsonXContent.contentBuilder();
        template.toXContent(builder, ToXContent.EMPTY_PARAMS);
        assertEquals("""
            {"match":"*name","unmatch":"first_name","mapping":{"store":true}}""", Strings.toString(builder));

        // name-based template with array of match patterns
        templateDef = new HashMap<>();
        if (randomBoolean()) {
            templateDef.put("match_mapping_type", "*");
        }
        templateDef.put("match", List.of("*name", "user*"));
        templateDef.put("unmatch", "first_name");
        templateDef.put("mapping", Collections.singletonMap("store", true));
        template = DynamicTemplate.parse("my_template", templateDef);
        builder = JsonXContent.contentBuilder();
        template.toXContent(builder, ToXContent.EMPTY_PARAMS);
        assertEquals("""
            {"match":["*name","user*"],"unmatch":"first_name","mapping":{"store":true}}""", Strings.toString(builder));

        // path-based template
        templateDef = new HashMap<>();
        templateDef.put("path_match", "*name");
        templateDef.put("path_unmatch", "first_name");
        if (randomBoolean()) {
            templateDef.put("match_mapping_type", "*");
        }
        templateDef.put("mapping", Collections.singletonMap("store", true));
        template = DynamicTemplate.parse("my_template", templateDef);
        builder = JsonXContent.contentBuilder();
        template.toXContent(builder, ToXContent.EMPTY_PARAMS);
        assertEquals("""
            {"path_match":"*name","path_unmatch":"first_name","mapping":{"store":true}}""", Strings.toString(builder));

        // path-based template with single-entry array - still serializes as single string, rather than list
        templateDef = new HashMap<>();
        templateDef.put("path_match", List.of("*name"));
        templateDef.put("path_unmatch", List.of("first_name"));
        if (randomBoolean()) {
            templateDef.put("match_mapping_type", "*");
        }
        templateDef.put("mapping", Collections.singletonMap("store", true));
        template = DynamicTemplate.parse("my_template", templateDef);
        builder = JsonXContent.contentBuilder();
        template.toXContent(builder, ToXContent.EMPTY_PARAMS);
        assertEquals("""
            {"path_match":"*name","path_unmatch":"first_name","mapping":{"store":true}}""", Strings.toString(builder));

        // path-based template with multi-entry array - now serializes as list
        templateDef = new HashMap<>();
        templateDef.put("path_match", List.of("*name", "user*"));
        templateDef.put("path_unmatch", List.of("first_name", "username"));
        if (randomBoolean()) {
            templateDef.put("match_mapping_type", "*");
        }
        templateDef.put("mapping", Collections.singletonMap("store", true));
        template = DynamicTemplate.parse("my_template", templateDef);
        builder = JsonXContent.contentBuilder();
        template.toXContent(builder, ToXContent.EMPTY_PARAMS);
        assertEquals(
            """
                {"path_match":["*name","user*"],"path_unmatch":["first_name","username"],"mapping":{"store":true}}""",
            Strings.toString(builder)
        );

        // regex matching
        templateDef = new HashMap<>();
        templateDef.put("match", "^a$");
        templateDef.put("match_pattern", "regex");
        if (randomBoolean()) {
            templateDef.put("match_mapping_type", "*");
        }
        templateDef.put("mapping", Collections.singletonMap("store", true));
        template = DynamicTemplate.parse("my_template", templateDef);
        builder = JsonXContent.contentBuilder();
        template.toXContent(builder, ToXContent.EMPTY_PARAMS);
        assertEquals("""
            {"match":"^a$","match_pattern":"regex","mapping":{"store":true}}""", Strings.toString(builder));

        // empty condition
        templateDef = new HashMap<>();
        templateDef.put("mapping", Collections.singletonMap("store", true));
        template = DynamicTemplate.parse("my_template", templateDef);
        builder = JsonXContent.contentBuilder();
        template.toXContent(builder, ToXContent.EMPTY_PARAMS);
        assertThat(Strings.toString(builder), equalTo("""
            {"mapping":{"store":true}}"""));
    }

    public void testSerializationRuntimeMappings() throws Exception {
        // type-based template
        Map<String, Object> templateDef = new HashMap<>();
        templateDef.put("match_mapping_type", "string");
        templateDef.put("runtime", Collections.emptyMap());
        DynamicTemplate template = DynamicTemplate.parse("my_template", templateDef);
        XContentBuilder builder = JsonXContent.contentBuilder();
        template.toXContent(builder, ToXContent.EMPTY_PARAMS);
        assertEquals("""
            {"match_mapping_type":"string","runtime":{}}""", Strings.toString(builder));

        // name-based template
        templateDef = new HashMap<>();
        templateDef.put("match", "*name");
        templateDef.put("unmatch", "first_name");
        if (randomBoolean()) {
            templateDef.put("match_mapping_type", "*");
        }
        templateDef.put("runtime", Collections.singletonMap("type", "new_type"));
        template = DynamicTemplate.parse("my_template", templateDef);
        builder = JsonXContent.contentBuilder();
        template.toXContent(builder, ToXContent.EMPTY_PARAMS);
        assertEquals("""
            {"match":"*name","unmatch":"first_name","runtime":{"type":"new_type"}}""", Strings.toString(builder));

        // path-based template
        templateDef = new HashMap<>();
        templateDef.put("path_match", "*name");
        templateDef.put("path_unmatch", "first_name");
        if (randomBoolean()) {
            templateDef.put("match_mapping_type", "*");
        }
        templateDef.put("runtime", Collections.emptyMap());
        template = DynamicTemplate.parse("my_template", templateDef);
        builder = JsonXContent.contentBuilder();
        template.toXContent(builder, ToXContent.EMPTY_PARAMS);
        assertEquals("""
            {"path_match":"*name","path_unmatch":"first_name","runtime":{}}""", Strings.toString(builder));

        // regex matching
        templateDef = new HashMap<>();
        templateDef.put("match", "^a$");
        if (randomBoolean()) {
            templateDef.put("match_mapping_type", "*");
        }
        templateDef.put("match_pattern", "regex");
        templateDef.put("runtime", Collections.emptyMap());
        template = DynamicTemplate.parse("my_template", templateDef);
        builder = JsonXContent.contentBuilder();
        template.toXContent(builder, ToXContent.EMPTY_PARAMS);
        assertEquals("""
            {"match":"^a$","match_pattern":"regex","runtime":{}}""", Strings.toString(builder));
    }

    public void testMatchTemplateName() throws Exception {
        // match_mapping_type
        {
            Map<String, Object> templateDef = new HashMap<>();
            templateDef.put("match_mapping_type", "string");
            if (randomBoolean()) {
                templateDef.put("runtime", Collections.emptyMap());
            } else {
                templateDef.put("mapping", Map.of());
            }
            DynamicTemplate template = DynamicTemplate.parse("my_template", templateDef);
            assertTrue(template.match("my_template", "a", "a.b", randomFrom(XContentFieldType.values())));
            assertFalse(template.match("not_template_name", "a", "a.b", XContentFieldType.BOOLEAN));

            assertTrue(template.match(null, "a", "a.b", XContentFieldType.STRING));
            assertFalse(template.match(null, "a", "a.b", XContentFieldType.BOOLEAN));
        }
        // match name
        {
            Map<String, Object> templateDef = new HashMap<>();
            templateDef.put("match", "foo*");
            templateDef.put("mapping", Map.of());
            DynamicTemplate template = DynamicTemplate.parse("my_template", templateDef);
            assertTrue(template.match("my_template", "foo.bar", "foo", randomFrom(XContentFieldType.values())));
            assertTrue(template.match(null, "foo.bar", "foo", randomFrom(XContentFieldType.values())));
            assertFalse(template.match("not_template_name", "foo.bar", "foo", randomFrom(XContentFieldType.values())));
            assertTrue(template.match("my_template", "foo.bar", "not_match_name", randomFrom(XContentFieldType.values())));
            assertFalse(template.match(null, "foo.bar", "not_match_name", randomFrom(XContentFieldType.values())));
        }
        // match name with array of patterns
        {
            Map<String, Object> templateDef = new HashMap<>();
            templateDef.put("match", List.of("baz*", "*quux", "*wibble*"));
            templateDef.put("mapping", Map.of());
            DynamicTemplate template = DynamicTemplate.parse("my_template", templateDef);
            assertTrue(template.match("my_template", "foo.bar", "foo", randomFrom(XContentFieldType.values())));
            // won't match because fieldName doesn't match
            assertFalse(template.match(null, "foo.bar", "foo", randomFrom(XContentFieldType.values())));
            assertTrue(template.match(null, null, "bazzy", randomFrom(XContentFieldType.values())));
            assertTrue(template.match(null, null, "myquux", randomFrom(XContentFieldType.values())));
            assertTrue(template.match(null, null, "foo.wibble_bar", randomFrom(XContentFieldType.values())));
            assertFalse(template.match("not_template_name", "foo.bar", "foo", randomFrom(XContentFieldType.values())));
            assertTrue(template.match("my_template", "foo.bar", "not_match_name", randomFrom(XContentFieldType.values())));
            assertFalse(template.match(null, "foo.bar", "not_match_name", randomFrom(XContentFieldType.values())));
        }
        // match name with number rather than string (is allowed)
        {
            Map<String, Object> templateDef = new HashMap<>();
            templateDef.put("match", 12);
            templateDef.put("mapping", Map.of());
            DynamicTemplate template = DynamicTemplate.parse("my_template", templateDef);
            assertTrue(template.match("my_template", "foo.bar", "foo", randomFrom(XContentFieldType.values())));
            assertTrue(template.match(null, null, "12", randomFrom(XContentFieldType.values())));
            assertFalse(template.match("not_template_name", null, "12", randomFrom(XContentFieldType.values())));
            assertTrue(template.match("my_template", "foo.bar", "not_match_name", randomFrom(XContentFieldType.values())));
            assertFalse(template.match(null, "foo.bar", "not_match_name", randomFrom(XContentFieldType.values())));
        }
        // match name with array of patterns with non-strings (not allowed)
        {
            Map<String, Object> templateDef = new HashMap<>();
            templateDef.put("match", List.of("baz*", "*quux", 12));
            templateDef.put("mapping", Map.of());
            MapperParsingException e = expectThrows(MapperParsingException.class, () -> DynamicTemplate.parse("my_template", templateDef));
            assertEquals("[match] values must either be a string or list of strings, but was [[baz*, *quux, 12]]", e.getMessage());
        }
        // no match condition
        {
            Map<String, Object> templateDef = new HashMap<>();
            if (randomBoolean()) {
                templateDef.put("runtime", Collections.emptyMap());
            } else {
                templateDef.put("mapping", Map.of());
            }
            DynamicTemplate template = DynamicTemplate.parse("my_template", templateDef);
            assertTrue(template.match("my_template", "foo.bar", "bar", randomFrom(XContentFieldType.values())));
            assertFalse(template.match(null, "foo.bar", "foo", randomFrom(XContentFieldType.values())));
            assertFalse(template.match("not_template_name", "foo.bar", "bar", randomFrom(XContentFieldType.values())));
            assertTrue(template.match("my_template", "foo.bar", "bar", randomFrom(XContentFieldType.values())));
        }
    }
}
