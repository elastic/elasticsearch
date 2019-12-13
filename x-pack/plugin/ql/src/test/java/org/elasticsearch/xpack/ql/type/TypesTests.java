/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ql.type;

import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.ESTestCase;

import java.io.InputStream;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.xpack.ql.type.DataType.DATETIME;
import static org.elasticsearch.xpack.ql.type.DataType.INTEGER;
import static org.elasticsearch.xpack.ql.type.DataType.KEYWORD;
import static org.elasticsearch.xpack.ql.type.DataType.NESTED;
import static org.elasticsearch.xpack.ql.type.DataType.OBJECT;
import static org.elasticsearch.xpack.ql.type.DataType.TEXT;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class TypesTests extends ESTestCase {

    public void testNullMap() {
        Map<String, EsField> fromEs = Types.fromEs(null);
        assertThat(fromEs.isEmpty(), is(true));
    }

    public void testEmptyMap() {
        Map<String, EsField> fromEs = Types.fromEs(emptyMap());
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
        assertThat(type.getPrecision(), is(Integer.MAX_VALUE));
        assertThat(ttype.isAggregatable(), is(false));
    }

    public void testKeywordField() {
        Map<String, EsField> mapping = loadMapping("mapping-keyword.json");

        assertThat(mapping.size(), is(1));
        EsField field = mapping.get("full_name");
        assertThat(field, instanceOf(KeywordEsField.class));
        assertThat(field.isAggregatable(), is(true));
        assertThat(field.getPrecision(), is(256));
    }

    public void testDateField() {
        Map<String, EsField> mapping = loadMapping("mapping-date.json");

        assertThat(mapping.size(), is(1));
        EsField field = mapping.get("date");
        assertThat(field.getDataType(), is(DATETIME));
        assertThat(field.isAggregatable(), is(true));
        assertThat(field.getPrecision(), is(3));
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
        assertThat(field.getPrecision(), is(15));
        assertThat(field.isAggregatable(), is(false));
    }

    public void testDottedField() {
        Map<String, EsField> mapping = loadMapping("mapping-object.json");

        assertThat(mapping.size(), is(2));
        EsField field = mapping.get("manager");
        assertThat(field.getDataType().isPrimitive(), is(false));
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
        assertThat(field.getDataType().isPrimitive(), is(true));
        assertThat(field.getDataType(), is(TEXT));
        Map<String, EsField> fields = field.getProperties();
        assertThat(fields.size(), is(2));
        assertThat(fields.get("raw").getDataType(), is(KEYWORD));
        assertThat(fields.get("english").getDataType(), is(TEXT));
    }

    public void testMultiFieldTooManyOptions() {
        Map<String, EsField> mapping = loadMapping("mapping-multi-field.json");

        assertThat(mapping.size(), is(1));
        EsField field = mapping.get("text");
        assertThat(field.getDataType().isPrimitive(), is(true));
        assertThat(field, instanceOf(TextEsField.class));
        Map<String, EsField> fields = field.getProperties();
        assertThat(fields.size(), is(2));
        assertThat(fields.get("raw").getDataType(), is(KEYWORD));
        assertThat(fields.get("english").getDataType(), is(TEXT));
    }

    public void testNestedDoc() {
        Map<String, EsField> mapping = loadMapping("mapping-nested.json");

        assertThat(mapping.size(), is(1));
        EsField field = mapping.get("dep");
        assertThat(field.getDataType().isPrimitive(), is(false));
        assertThat(field.getDataType(), is(NESTED));
        Map<String, EsField> children = field.getProperties();
        assertThat(children.size(), is(4));
        assertThat(children.get("dep_name").getDataType(), is(TEXT));
        assertThat(children.get("start_date").getDataType(), is(DATETIME));
    }

    public void testGeoField() {
        Map<String, EsField> mapping = loadMapping("mapping-geo.json");
        assertThat(mapping.size(), is(2));
        EsField gp = mapping.get("location");
        assertThat(gp.getDataType().typeName(), is("geo_point"));
        EsField gs = mapping.get("site");
        assertThat(gs.getDataType().typeName(), is("geo_shape"));
    }

    public void testIpField() {
        Map<String, EsField> mapping = loadMapping("mapping-ip.json");
        assertThat(mapping.size(), is(1));
        EsField dt = mapping.get("ip_addr");
        assertThat(dt.getDataType().typeName(), is("ip"));
    }

    public void testUnsupportedTypes() {
        Map<String, EsField> mapping = loadMapping("mapping-unsupported.json");
        EsField dt = mapping.get("range");
        assertThat(dt.getDataType().typeName(), is("unsupported"));
    }

    public static Map<String, EsField> loadMapping(String name) {
        InputStream stream = TypesTests.class.getResourceAsStream("/" + name);
        assertNotNull("Could not find mapping resource:" + name, stream);
        return Types.fromEs(XContentHelper.convertToMap(JsonXContent.jsonXContent, stream, randomBoolean()));
    }

    public static Map<String, EsField> loadMapping(String name, boolean ordered) {
        InputStream stream = TypesTests.class.getResourceAsStream("/" + name);
        assertNotNull("Could not find mapping resource:" + name, stream);
        return Types.fromEs(XContentHelper.convertToMap(JsonXContent.jsonXContent, stream, ordered));
    }
}
