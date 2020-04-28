/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ql.type;

import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.xpack.ql.type.DataTypes.CONSTANT_KEYWORD;
import static org.elasticsearch.xpack.ql.type.DataTypes.DATETIME;
import static org.elasticsearch.xpack.ql.type.DataTypes.INTEGER;
import static org.elasticsearch.xpack.ql.type.DataTypes.KEYWORD;
import static org.elasticsearch.xpack.ql.type.DataTypes.NESTED;
import static org.elasticsearch.xpack.ql.type.DataTypes.OBJECT;
import static org.elasticsearch.xpack.ql.type.DataTypes.TEXT;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class TypesTests extends ESTestCase {

    public void testNullMap() {
        Map<String, EsField> fromEs = Types.fromEs(DefaultDataTypeRegistry.INSTANCE, null);
        assertThat(fromEs.isEmpty(), is(true));
    }

    public void testEmptyMap() {
        Map<String, EsField> fromEs = Types.fromEs(DefaultDataTypeRegistry.INSTANCE, emptyMap());
        assertThat(fromEs.isEmpty(), is(true));
    }

    public void testBasicMapping() {
        Map<String, EsField> mapping = loadMapping("mapping-basic.json");
        assertThat(mapping.size(), is(7));
        assertThat(mapping.get("emp_no").getDataType(), is(INTEGER));
        assertThat(mapping.get("first_name"), instanceOf(TextEsField.class));
        assertThat(mapping.get("last_name").getDataType(), is(TEXT));
        assertThat(mapping.get("gender").getDataType(), is(KEYWORD));
        assertThat(mapping.get("salary").getDataType(), is(INTEGER));
        assertThat(mapping.get("_meta_field").getDataType(), is(KEYWORD));
    }

    public void testDefaultStringMapping() {
        Map<String, EsField> mapping = loadMapping("mapping-default-string.json");

        assertThat(mapping.size(), is(1));
        assertThat(mapping.get("dep_no").getDataType(), is(TEXT));
    }

    public void testTextField() {
        Map<String, EsField> mapping = loadMapping("mapping-text.json");

        assertThat(mapping.size(), is(1));
        EsField type = mapping.get("full_name");
        assertThat(type, instanceOf(TextEsField.class));
        assertThat(type.isAggregatable(), is(false));
        TextEsField ttype = (TextEsField) type;
        assertThat(ttype.isAggregatable(), is(false));
    }

    public void testKeywordField() {
        Map<String, EsField> mapping = loadMapping("mapping-keyword.json");

        assertThat(mapping.size(), is(1));
        EsField field = mapping.get("full_name");
        assertThat(field, instanceOf(KeywordEsField.class));
        assertThat(field.isAggregatable(), is(true));
    }

    public void testDateField() {
        Map<String, EsField> mapping = loadMapping("mapping-date.json");

        assertThat(mapping.size(), is(1));
        EsField field = mapping.get("date");
        assertThat(field.getDataType(), is(DATETIME));
        assertThat(field.isAggregatable(), is(true));
    }

    public void testDateNoFormat() {
        Map<String, EsField> mapping = loadMapping("mapping-date-no-format.json");

        assertThat(mapping.size(), is(1));
        EsField field = mapping.get("date");
        assertThat(field.getDataType(), is(DATETIME));
        assertThat(field.isAggregatable(), is(true));
        DateEsField dfield = (DateEsField) field;
    }

    public void testDateMulti() {
        Map<String, EsField> mapping = loadMapping("mapping-date-multi.json");

        assertThat(mapping.size(), is(1));
        EsField field = mapping.get("date");
        assertThat(field.getDataType(), is(DATETIME));
        assertThat(field.isAggregatable(), is(true));
        DateEsField dfield = (DateEsField) field;
    }

    public void testDocValueField() {
        Map<String, EsField> mapping = loadMapping("mapping-docvalues.json");

        assertThat(mapping.size(), is(1));
        EsField field = mapping.get("session_id");
        assertThat(field, instanceOf(KeywordEsField.class));
        //assertThat(field.getPrecision(), is(15));
        assertThat(field.isAggregatable(), is(false));
    }

    public void testDottedField() {
        Map<String, EsField> mapping = loadMapping("mapping-object.json");

        assertThat(mapping.size(), is(2));
        EsField field = mapping.get("manager");
        assertThat(DataTypes.isPrimitive(field.getDataType()), is(false));
        assertThat(field.getDataType(), is(OBJECT));
        Map<String, EsField> children = field.getProperties();
        assertThat(children.size(), is(2));
        EsField names = children.get("name");
        children = names.getProperties();
        assertThat(children.size(), is(2));
        assertThat(children.get("first").getDataType(), is(TEXT));
    }

    public void testMultiField() {
        Map<String, EsField> mapping = loadMapping("mapping-multi-field.json");

        assertThat(mapping.size(), is(1));
        EsField field = mapping.get("text");
        assertThat(DataTypes.isPrimitive(field.getDataType()), is(true));
        assertThat(field.getDataType(), is(TEXT));
        Map<String, EsField> fields = field.getProperties();
        assertThat(fields.size(), is(3));
        assertThat(fields.get("raw").getDataType(), is(KEYWORD));
        assertThat(fields.get("english").getDataType(), is(TEXT));
        assertThat(fields.get("constant").getDataType(), is(CONSTANT_KEYWORD));
    }

    public void testMultiFieldTooManyOptions() {
        Map<String, EsField> mapping = loadMapping("mapping-multi-field.json");

        assertThat(mapping.size(), is(1));
        EsField field = mapping.get("text");
        assertThat(DataTypes.isPrimitive(field.getDataType()), is(true));
        assertThat(field, instanceOf(TextEsField.class));
        Map<String, EsField> fields = field.getProperties();
        assertThat(fields.size(), is(3));
        assertThat(fields.get("raw").getDataType(), is(KEYWORD));
        assertThat(fields.get("english").getDataType(), is(TEXT));
        assertThat(fields.get("constant").getDataType(), is(CONSTANT_KEYWORD));
    }

    public void testNestedDoc() {
        Map<String, EsField> mapping = loadMapping("mapping-nested.json");

        assertThat(mapping.size(), is(1));
        EsField field = mapping.get("dep");
        assertThat(DataTypes.isPrimitive(field.getDataType()), is(false));
        assertThat(field.getDataType(), is(NESTED));
        Map<String, EsField> children = field.getProperties();
        assertThat(children.size(), is(4));
        assertThat(children.get("dep_name").getDataType(), is(TEXT));
        assertThat(children.get("start_date").getDataType(), is(DATETIME));
    }

    public void testIpField() {
        Map<String, EsField> mapping = loadMapping("mapping-ip.json");
        assertThat(mapping.size(), is(1));
        EsField dt = mapping.get("ip_addr");
        assertThat(dt.getDataType().typeName(), is("ip"));
    }

    public void testConstantKeywordField() {
        Map<String, EsField> mapping = loadMapping("mapping-constant-keyword.json");
        assertThat(mapping.size(), is(1));
        EsField dt = mapping.get("full_name");
        assertThat(dt.getDataType().typeName(), is("constant_keyword"));
    }

    public void testUnsupportedTypes() {
        Map<String, EsField> mapping = loadMapping("mapping-unsupported.json");
        EsField dt = mapping.get("range");
        assertThat(dt.getDataType().typeName(), is("unsupported"));
        dt = mapping.get("time_frame");
        assertThat(dt.getDataType().typeName(), is("unsupported"));
        dt = mapping.get("flat");
        assertThat(dt.getDataType().typeName(), is("unsupported"));
    }

    public static Map<String, EsField> loadMapping(String name) {
        return loadMapping(DefaultDataTypeRegistry.INSTANCE, name, null);
    }

    public static Map<String, EsField> loadMapping(String name, boolean ordered) {
        return loadMapping(DefaultDataTypeRegistry.INSTANCE, name, ordered);
    }

    public static Map<String, EsField> loadMapping(DataTypeRegistry registry, String name) {
        return loadMapping(registry, name, null);
    }

    public static Map<String, EsField> loadMapping(DataTypeRegistry registry, String name, Boolean ordered) {
        InputStream stream = TypesTests.class.getResourceAsStream("/" + name);
        assertNotNull("Could not find mapping resource:" + name, stream);
        return loadMapping(registry, stream, ordered);
    }

    public static Map<String, EsField> loadMapping(DataTypeRegistry registry, InputStream stream, Boolean ordered) {
        boolean order = ordered != null ? ordered.booleanValue() : randomBoolean();
        try (InputStream in = stream) {
            return Types.fromEs(registry, XContentHelper.convertToMap(JsonXContent.jsonXContent, in, order));
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }
}