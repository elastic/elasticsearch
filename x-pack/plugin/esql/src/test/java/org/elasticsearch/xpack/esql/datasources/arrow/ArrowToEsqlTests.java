/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.arrow;

import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.util.List;

public class ArrowToEsqlTests extends ESTestCase {

    /**
     * Regression test: forField on a LIST field used to call getChildren().get(1), which throws
     * IndexOutOfBoundsException because a ListVector field has exactly one child (the element type)
     * at index 0.
     */
    public void testForFieldOnListDoesNotThrow() {
        Field elementField = new Field("element", FieldType.nullable(Types.MinorType.FLOAT4.getType()), null);
        Field listField = new Field("myList", FieldType.nullable(new ArrowType.List()), List.of(elementField));

        ArrowToEsql mapping = ArrowToEsql.forField(listField);

        assertNotNull(mapping);
        assertEquals(DataType.FLOAT, mapping.dataType());
    }

    public void testForFieldOnNestedList() {
        Field innerElement = new Field("element", FieldType.nullable(Types.MinorType.BIGINT.getType()), null);
        Field innerList = new Field("inner", FieldType.nullable(new ArrowType.List()), List.of(innerElement));
        Field outerList = new Field("outer", FieldType.nullable(new ArrowType.List()), List.of(innerList));

        // Nested LIST[LIST[BIGINT]] — forField recurses through both LIST levels
        ArrowToEsql mapping = ArrowToEsql.forField(outerList);

        assertNotNull(mapping);
        assertEquals(DataType.LONG, mapping.dataType());
    }

    public void testForFieldOnUnsupportedTypeReturnsNull() {
        Field field = new Field("f", FieldType.nullable(Types.MinorType.STRUCT.getType()), null);
        assertNull(ArrowToEsql.forField(field));
    }
}
