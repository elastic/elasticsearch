/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.type;

import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.ESTestCase;

import java.io.InputStream;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.xpack.sql.type.DataTypes.DATE;
import static org.elasticsearch.xpack.sql.type.DataTypes.INTEGER;
import static org.elasticsearch.xpack.sql.type.DataTypes.KEYWORD;
import static org.elasticsearch.xpack.sql.type.DataTypes.TEXT;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class TypesTests extends ESTestCase {

    public void testNullMap() throws Exception {
        Map<String, DataType> fromEs = Types.fromEs(null);
        assertThat(fromEs.isEmpty(), is(true));
    }

    public void testEmptyMap() throws Exception {
        Map<String, DataType> fromEs = Types.fromEs(emptyMap());
        assertThat(fromEs.isEmpty(), is(true));
    }

    public void testBasicMapping() throws Exception {
        Map<String, DataType> mapping = loadMapping("mapping-basic.json");
        assertThat(mapping.size(), is(6));
        assertThat(mapping.get("emp_no"), is(INTEGER));
        assertThat(mapping.get("first_name"), instanceOf(TextType.class));
        assertThat(mapping.get("last_name"), is(TEXT));
        assertThat(mapping.get("gender"), is(KEYWORD));
        assertThat(mapping.get("salary"), is(INTEGER));
    }

    public void testDefaultStringMapping() throws Exception {
        Map<String, DataType> mapping = loadMapping("mapping-default-string.json");

        assertThat(mapping.size(), is(1));
        assertThat(mapping.get("dep_no").same(TEXT), is(true));
    }

    public void testTextField() throws Exception {
        Map<String, DataType> mapping = loadMapping("mapping-text.json");

        assertThat(mapping.size(), is(1));
        DataType type = mapping.get("full_name");
        assertThat(type, instanceOf(TextType.class));
        assertThat(type.hasDocValues(), is(false));
        TextType ttype = (TextType) type;
        assertThat(type.precision(), is(Integer.MAX_VALUE));
        assertThat(ttype.hasFieldData(), is(false));
    }

    public void testKeywordField() throws Exception {
        Map<String, DataType> mapping = loadMapping("mapping-keyword.json");

        assertThat(mapping.size(), is(1));
        DataType type = mapping.get("full_name");
        assertThat(type, instanceOf(KeywordType.class));
        assertThat(type.hasDocValues(), is(true));
        assertThat(type.precision(), is(256));
    }

    public void testDateField() throws Exception {
        Map<String, DataType> mapping = loadMapping("mapping-date.json");

        assertThat(mapping.size(), is(1));
        DataType type = mapping.get("date");
        assertThat(type, is(DATE));
        assertThat(type.hasDocValues(), is(true));
        assertThat(type.precision(), is(19));

        DateType dtype = (DateType) type;
        List<String> formats = dtype.formats();
        assertThat(formats, hasSize(3));
    }

    public void testDateNoFormat() throws Exception {
        Map<String, DataType> mapping = loadMapping("mapping-date-no-format.json");

        assertThat(mapping.size(), is(1));
        DataType type = mapping.get("date");
        assertThat(type, is(DATE));
        assertThat(type.hasDocValues(), is(true));
        DateType dtype = (DateType) type;
        // default types
        assertThat(dtype.formats(), hasSize(2));
    }

    public void testDateMulti() throws Exception {
        Map<String, DataType> mapping = loadMapping("mapping-date-multi.json");

        assertThat(mapping.size(), is(1));
        DataType type = mapping.get("date");
        assertThat(type, is(DATE));
        assertThat(type.hasDocValues(), is(true));
        DateType dtype = (DateType) type;
        // default types
        assertThat(dtype.formats(), hasSize(1));
    }

    public void testDocValueField() throws Exception {
        Map<String, DataType> mapping = loadMapping("mapping-docvalues.json");

        assertThat(mapping.size(), is(1));
        DataType type = mapping.get("session_id");
        assertThat(type, instanceOf(KeywordType.class));
        assertThat(type.precision(), is(15));
        assertThat(type.hasDocValues(), is(false));
    }

    public void testDottedField() throws Exception {
        Map<String, DataType> mapping = loadMapping("mapping-object.json");

        assertThat(mapping.size(), is(2));
        DataType type = mapping.get("manager");
        assertThat(type.isPrimitive(), is(false));
        assertThat(type, instanceOf(ObjectType.class));
        ObjectType ot = (ObjectType) type;
        Map<String, DataType> children = ot.properties();
        assertThat(children.size(), is(2));
        DataType names = children.get("name");
        children = ((ObjectType) names).properties();
        assertThat(children.size(), is(2));
        assertThat(children.get("first"), is(TEXT));
    }

    public void testMultiField() throws Exception {
        Map<String, DataType> mapping = loadMapping("mapping-multi-field.json");

        assertThat(mapping.size(), is(1));
        DataType type = mapping.get("text");
        assertThat(type.isPrimitive(), is(true));
        assertThat(type, instanceOf(TextType.class));
        TextType tt = (TextType) type;
        Map<String, DataType> fields = tt.fields();
        assertThat(fields.size(), is(2));
        assertThat(fields.get("raw"), is(KEYWORD));
        assertThat(fields.get("english"), is(TEXT));
    }

    public void testMultiFieldTooManyOptions() throws Exception {
        Map<String, DataType> mapping = loadMapping("mapping-multi-field.json");

        assertThat(mapping.size(), is(1));
        DataType type = mapping.get("text");
        assertThat(type.isPrimitive(), is(true));
        assertThat(type, instanceOf(TextType.class));
        TextType tt = (TextType) type;
        Map<String, DataType> fields = tt.fields();
        assertThat(fields.size(), is(2));
        assertThat(fields.get("raw"), is(KEYWORD));
        assertThat(fields.get("english"), is(TEXT));
    }

    public void testNestedDoc() throws Exception {
        Map<String, DataType> mapping = loadMapping("mapping-nested.json");

        assertThat(mapping.size(), is(1));
        DataType type = mapping.get("dep");
        assertThat(type.isPrimitive(), is(false));
        assertThat(type, instanceOf(NestedType.class));
        NestedType ot = (NestedType) type;
        Map<String, DataType> children = ot.properties();
        assertThat(children.size(), is(4));
        assertThat(children.get("dep_name"), is(TEXT));
        assertThat(children.get("start_date"), is(DATE));
    }

    public void testGeoField() throws Exception {
        Map<String, DataType> mapping = loadMapping("mapping-geo.json");
        DataType dt = mapping.get("location");
        assertThat(dt.esName(), is("geo_point"));
    }

    public void testUnsupportedTypes() throws Exception {
        Map<String, DataType> mapping = loadMapping("mapping-unsupported.json");
        DataType dt = mapping.get("range");
        assertThat(dt.esName(), is("integer_range"));
    }

    public static Map<String, DataType> loadMapping(String name) {
        InputStream stream = TypesTests.class.getResourceAsStream("/" + name);
        assertNotNull("Could not find mapping resource:" + name, stream);
        return Types.fromEs(XContentHelper.convertToMap(JsonXContent.jsonXContent, stream, randomBoolean()));
    }
}