/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet.parquetrs;

import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Nullability;

import java.util.List;

public class ParquetRsFormatReaderTests extends ESTestCase {

    /**
     * Regression: when an Arrow schema is imported from native code via FFI, attribute nullability must reflect Arrow's
     * per-field {@code isNullable()} flag. Defaulting to non-nullable would mislead planner rules (e.g. {@code COALESCE}
     * simplification, {@code IS NULL}/{@code IS NOT NULL} rewriting, {@code FoldNull}) into dropping legitimate null
     * rows for nullable columns.
     */
    public void testArrowSchemaToAttributesPropagatesNullability() {
        Schema schema = new Schema(
            List.of(
                new Field("req_id", FieldType.notNullable(new ArrowType.Int(64, true)), null),
                new Field("opt_name", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("req_score", FieldType.notNullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null),
                new Field("opt_active", FieldType.nullable(new ArrowType.Bool()), null)
            )
        );

        List<Attribute> attributes = ParquetRsFormatReader.arrowSchemaToAttributes(schema);
        assertEquals(4, attributes.size());

        assertEquals("req_id", attributes.get(0).name());
        assertEquals(Nullability.FALSE, attributes.get(0).nullable());

        assertEquals("opt_name", attributes.get(1).name());
        assertEquals(Nullability.TRUE, attributes.get(1).nullable());

        assertEquals("req_score", attributes.get(2).name());
        assertEquals(Nullability.FALSE, attributes.get(2).nullable());

        assertEquals("opt_active", attributes.get(3).name());
        assertEquals(Nullability.TRUE, attributes.get(3).nullable());
    }
}
