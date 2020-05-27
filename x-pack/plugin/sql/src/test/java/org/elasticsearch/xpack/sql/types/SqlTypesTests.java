/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.types;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ql.type.EsField;
import org.elasticsearch.xpack.ql.type.KeywordEsField;
import org.elasticsearch.xpack.ql.type.TextEsField;
import org.elasticsearch.xpack.ql.type.TypesTests;
import org.elasticsearch.xpack.sql.type.SqlDataTypeRegistry;
import org.elasticsearch.xpack.sql.type.SqlDataTypes;

import java.util.Map;

import static org.elasticsearch.xpack.ql.type.DataTypes.DATETIME;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class SqlTypesTests extends ESTestCase {


    public void testGeoField() {
        Map<String, EsField> mapping = loadMapping("mapping-geo.json");
        assertThat(mapping.size(), is(2));
        EsField gp = mapping.get("location");
        assertThat(gp.getDataType().typeName(), is("geo_point"));
        EsField gs = mapping.get("site");
        assertThat(gs.getDataType().typeName(), is("geo_shape"));
    }


    public void testTextField() {
        Map<String, EsField> mapping = loadMapping("mapping-text.json");
        assertThat(mapping.size(), is(1));
        EsField type = mapping.get("full_name");
        assertThat(type, instanceOf(TextEsField.class));
        assertThat(type.isAggregatable(), is(false));
        TextEsField ttype = (TextEsField) type;
        assertThat(SqlDataTypes.defaultPrecision(ttype.getDataType()), is(32766));
        assertThat(ttype.isAggregatable(), is(false));
    }

    public void testKeywordField() {
        Map<String, EsField> mapping = loadMapping("mapping-keyword.json");

        assertThat(mapping.size(), is(1));
        EsField field = mapping.get("full_name");
        assertThat(field, instanceOf(KeywordEsField.class));
        assertThat(field.isAggregatable(), is(true));
        assertThat(((KeywordEsField) field).getPrecision(), is(256));
    }

    public void testDateField() {
        Map<String, EsField> mapping = loadMapping("mapping-date.json");

        assertThat(mapping.size(), is(1));
        EsField field = mapping.get("date");
        assertThat(field.getDataType(), is(DATETIME));
        assertThat(field.isAggregatable(), is(true));
        assertThat(SqlDataTypes.defaultPrecision(field.getDataType()), is(3));
    }

    public void testDocValueField() {
        Map<String, EsField> mapping = loadMapping("mapping-docvalues.json");

        assertThat(mapping.size(), is(1));
        EsField field = mapping.get("session_id");
        assertThat(field, instanceOf(KeywordEsField.class));
        assertThat(((KeywordEsField) field).getPrecision(), is(15));
        assertThat(field.isAggregatable(), is(false));
    }

    public static Map<String, EsField> loadMapping(String name) {
        return TypesTests.loadMapping(SqlDataTypeRegistry.INSTANCE, name);
    }

    public static Map<String, EsField> loadMapping(String name, boolean ordered) {
        return TypesTests.loadMapping(SqlDataTypeRegistry.INSTANCE, name, ordered);
    }
}