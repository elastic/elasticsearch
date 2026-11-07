/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.grpc;

import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.referenceAttribute;
import static org.hamcrest.Matchers.containsString;

public class SatelliteSchemaCheckTests extends ESTestCase {

    private static final List<Attribute> EXPECTED = List.of(
        referenceAttribute("i", DataType.INTEGER),
        referenceAttribute("l", DataType.LONG),
        referenceAttribute("d", DataType.DOUBLE),
        referenceAttribute("b", DataType.BOOLEAN),
        referenceAttribute("k", DataType.KEYWORD),
        referenceAttribute("t", DataType.DATETIME),
        referenceAttribute("n", DataType.DATE_NANOS)
    );

    private static Field field(String name, ArrowType type) {
        return new Field(name, FieldType.nullable(type), null);
    }

    private static List<Field> strictFields() {
        return List.of(
            field("i", new ArrowType.Int(32, true)),
            field("l", new ArrowType.Int(64, true)),
            field("d", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
            field("b", ArrowType.Bool.INSTANCE),
            field("k", ArrowType.Utf8.INSTANCE),
            field("t", new ArrowType.Timestamp(TimeUnit.MILLISECOND, "UTC")),
            field("n", new ArrowType.Timestamp(TimeUnit.NANOSECOND, "UTC"))
        );
    }

    public void testStrictSchemaPasses() {
        SatelliteSchemaCheck.verify(new Schema(strictFields()), EXPECTED, "test");
    }

    public void testListOfStrictTypeIsTheElementType() {
        Field list = new Field("i", FieldType.nullable(ArrowType.List.INSTANCE), List.of(field("item", new ArrowType.Int(32, true))));
        SatelliteSchemaCheck.verify(new Schema(List.of(list)), List.of(referenceAttribute("i", DataType.INTEGER)), "test");
    }

    public void testWrongArrowTypeIsRejected() {
        Field wrong = field("i", new ArrowType.Int(64, true)); // a LongBlock would flow where the plan expects an INTEGER one
        Exception e = expectThrows(
            IllegalStateException.class,
            () -> SatelliteSchemaCheck.verify(new Schema(List.of(wrong)), List.of(referenceAttribute("i", DataType.INTEGER)), "test")
        );
        assertThat(e.getMessage(), containsString("field [i] has Arrow type"));
    }

    public void testNonUtcOrOtherUnitTimestampIsRejected() {
        for (ArrowType wrong : List.of(
            new ArrowType.Timestamp(TimeUnit.MICROSECOND, "UTC"),
            new ArrowType.Timestamp(TimeUnit.MILLISECOND, "America/New_York"),
            new ArrowType.Timestamp(TimeUnit.MILLISECOND, null)
        )) {
            expectThrows(
                IllegalStateException.class,
                () -> SatelliteSchemaCheck.verify(
                    new Schema(List.of(field("t", wrong))),
                    List.of(referenceAttribute("t", DataType.DATETIME)),
                    "test"
                )
            );
        }
    }

    public void testNonNullableFieldIsRejected() {
        Field notNull = new Field("i", FieldType.notNullable(new ArrowType.Int(32, true)), null);
        Exception e = expectThrows(
            IllegalStateException.class,
            () -> SatelliteSchemaCheck.verify(new Schema(List.of(notNull)), List.of(referenceAttribute("i", DataType.INTEGER)), "test")
        );
        assertThat(e.getMessage(), containsString("nullable"));
    }

    public void testExtraMissingOrRenamedColumnIsRejected() {
        List<Attribute> one = List.of(referenceAttribute("i", DataType.INTEGER));
        expectThrows(
            IllegalStateException.class,
            () -> SatelliteSchemaCheck.verify(
                new Schema(List.of(field("i", new ArrowType.Int(32, true)), field("x", ArrowType.Bool.INSTANCE))),
                one,
                "t"
            )
        );
        expectThrows(IllegalStateException.class, () -> SatelliteSchemaCheck.verify(new Schema(List.of()), one, "t"));
        expectThrows(
            IllegalStateException.class,
            () -> SatelliteSchemaCheck.verify(new Schema(List.of(field("j", new ArrowType.Int(32, true)))), one, "t")
        );
    }

    public void testMetadataIsRejected() {
        Schema withMetadata = new Schema(List.of(field("i", new ArrowType.Int(32, true))), Map.of("pandas", "{}"));
        expectThrows(
            IllegalStateException.class,
            () -> SatelliteSchemaCheck.verify(withMetadata, List.of(referenceAttribute("i", DataType.INTEGER)), "test")
        );
    }

    public void testIsStrictMatchesTheProtocolTypes() {
        for (Field f : strictFields()) {
            assertTrue(f.getName(), SatelliteSchemaCheck.isStrict(f));
        }
        assertFalse(SatelliteSchemaCheck.isStrict(field("x", new ArrowType.Int(16, true))));
        assertFalse(SatelliteSchemaCheck.isStrict(field("x", new ArrowType.Int(32, false))));
        assertFalse(SatelliteSchemaCheck.isStrict(field("x", new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE))));
        assertFalse(SatelliteSchemaCheck.isStrict(field("x", new ArrowType.Timestamp(TimeUnit.MICROSECOND, "UTC"))));
        assertFalse(SatelliteSchemaCheck.isStrict(field("x", new ArrowType.Timestamp(TimeUnit.MILLISECOND, "Europe/Rome"))));
        assertTrue(
            SatelliteSchemaCheck.isStrict(
                new Field("x", FieldType.nullable(ArrowType.List.INSTANCE), List.of(field("item", ArrowType.Utf8.INSTANCE)))
            )
        );
    }
}
