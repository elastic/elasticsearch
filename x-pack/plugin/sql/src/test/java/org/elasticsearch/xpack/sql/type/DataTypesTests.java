/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.type;

import org.elasticsearch.test.ESTestCase;

import static org.elasticsearch.xpack.sql.type.DataType.DATE;
import static org.elasticsearch.xpack.sql.type.DataType.FLOAT;
import static org.elasticsearch.xpack.sql.type.DataType.KEYWORD;
import static org.elasticsearch.xpack.sql.type.DataType.LONG;
import static org.elasticsearch.xpack.sql.type.DataTypes.metaSqlDataType;
import static org.elasticsearch.xpack.sql.type.DataTypes.metaSqlDateTimeSub;
import static org.elasticsearch.xpack.sql.type.DataTypes.metaSqlMaximumScale;
import static org.elasticsearch.xpack.sql.type.DataTypes.metaSqlMinimumScale;
import static org.elasticsearch.xpack.sql.type.DataTypes.metaSqlRadix;

public class DataTypesTests extends ESTestCase {

    public void testMetaDataType() {
        assertEquals(Integer.valueOf(9), metaSqlDataType(DATE));
        DataType t = randomDataTypeNoDate();
        assertEquals(t.jdbcType.getVendorTypeNumber(), metaSqlDataType(t));
    }

    public void testMetaDateTypeSub() {
        assertEquals(Integer.valueOf(3), metaSqlDateTimeSub(DATE));
        assertEquals(Integer.valueOf(0), metaSqlDateTimeSub(randomDataTypeNoDate()));
    }

    public void testMetaMinimumScale() {
        assertEquals(Short.valueOf((short) 3), metaSqlMinimumScale(DATE));
        assertEquals(Short.valueOf((short) 0), metaSqlMinimumScale(LONG));
        assertEquals(Short.valueOf((short) 0), metaSqlMinimumScale(FLOAT));
        assertNull(metaSqlMinimumScale(KEYWORD));
    }

    public void testMetaMaximumScale() {
        assertEquals(Short.valueOf((short) 3), metaSqlMaximumScale(DATE));
        assertEquals(Short.valueOf((short) 0), metaSqlMaximumScale(LONG));
        assertEquals(Short.valueOf((short) FLOAT.defaultPrecision), metaSqlMaximumScale(FLOAT));
        assertNull(metaSqlMaximumScale(KEYWORD));
    }

    public void testMetaRadix() {
        assertNull(metaSqlRadix(DATE));
        assertNull(metaSqlRadix(KEYWORD));
        assertEquals(Integer.valueOf(10), metaSqlRadix(LONG));
        assertEquals(Integer.valueOf(2), metaSqlRadix(FLOAT));
    }

    private DataType randomDataTypeNoDate() {
        return randomValueOtherThan(DataType.DATE, () -> randomFrom(DataType.values()));
    }
}

