/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.multivalue;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.geo.GeometryTestUtils;
import org.elasticsearch.geo.ShapeTestUtils;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.randomLiteral;
import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.CARTESIAN;
import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.GEO;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class MvSliceTests extends AbstractScalarFunctionTestCase {
    public MvSliceTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();
        booleans(suppliers);
        ints(suppliers);
        longs(suppliers);
        doubles(suppliers);
        bytesRefs(suppliers);
        return parameterSuppliersFromTypedData(
            anyNullIsNull(
                suppliers,
                (nullPosition, nullValueDataType, original) -> nullPosition == 0 && nullValueDataType == DataType.NULL
                    ? DataType.NULL
                    : original.expectedType(),
                (nullPosition, nullData, original) -> original
            )
        );
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new MvSlice(source, args.get(0), args.get(1), args.size() > 2 ? args.get(2) : null);
    }

    private static void booleans(List<TestCaseSupplier> suppliers) {
        // Positive
        suppliers.add(new TestCaseSupplier(List.of(DataType.BOOLEAN, DataType.INTEGER, DataType.INTEGER), () -> {
            List<Boolean> field = randomList(1, 10, () -> randomBoolean());
            int length = field.size();
            int start = randomIntBetween(0, length - 1);
            int end = randomIntBetween(start, length - 1);
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(field, DataType.BOOLEAN, "field"),
                    new TestCaseSupplier.TypedData(start, DataType.INTEGER, "start"),
                    new TestCaseSupplier.TypedData(end, DataType.INTEGER, "end")
                ),
                "MvSliceBooleanEvaluator[field=Attribute[channel=0], start=Attribute[channel=1], end=Attribute[channel=2]]",
                DataType.BOOLEAN,
                equalTo(start == end ? field.get(start) : field.subList(start, end + 1))
            );
        }));
        // Positive Start IndexOutofBound
        suppliers.add(new TestCaseSupplier(List.of(DataType.BOOLEAN, DataType.INTEGER, DataType.INTEGER), () -> {
            List<Boolean> field = randomList(1, 10, () -> randomBoolean());
            int length = field.size();
            int start = randomIntBetween(length, length + 1);
            int end = randomIntBetween(start, length + 10);
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(field, DataType.BOOLEAN, "field"),
                    new TestCaseSupplier.TypedData(start, DataType.INTEGER, "start"),
                    new TestCaseSupplier.TypedData(end, DataType.INTEGER, "end")
                ),
                "MvSliceBooleanEvaluator[field=Attribute[channel=0], start=Attribute[channel=1], end=Attribute[channel=2]]",
                DataType.BOOLEAN,
                nullValue()
            );
        }));
        // Positive End IndexOutofBound
        suppliers.add(new TestCaseSupplier(List.of(DataType.BOOLEAN, DataType.INTEGER, DataType.INTEGER), () -> {
            List<Boolean> field = randomList(1, 10, () -> randomBoolean());
            int length = field.size();
            int start = randomIntBetween(0, length - 1);
            int end = randomIntBetween(length, length + 10);
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(field, DataType.BOOLEAN, "field"),
                    new TestCaseSupplier.TypedData(start, DataType.INTEGER, "start"),
                    new TestCaseSupplier.TypedData(end, DataType.INTEGER, "end")
                ),
                "MvSliceBooleanEvaluator[field=Attribute[channel=0], start=Attribute[channel=1], end=Attribute[channel=2]]",
                DataType.BOOLEAN,
                equalTo(start == length - 1 ? field.get(start) : field.subList(start, length))
            );
        }));
        // Negative
        suppliers.add(new TestCaseSupplier(List.of(DataType.BOOLEAN, DataType.INTEGER, DataType.INTEGER), () -> {
            List<Boolean> field = randomList(1, 10, () -> randomBoolean());
            int length = field.size();
            int start = randomIntBetween(0 - length, -1);
            int end = randomIntBetween(start, -1);
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(field, DataType.BOOLEAN, "field"),
                    new TestCaseSupplier.TypedData(start, DataType.INTEGER, "start"),
                    new TestCaseSupplier.TypedData(end, DataType.INTEGER, "end")
                ),
                "MvSliceBooleanEvaluator[field=Attribute[channel=0], start=Attribute[channel=1], end=Attribute[channel=2]]",
                DataType.BOOLEAN,
                equalTo(start == end ? field.get(start + length) : field.subList(start + length, end + 1 + length))
            );
        }));
    }

    private static void ints(List<TestCaseSupplier> suppliers) {
        suppliers.add(new TestCaseSupplier(List.of(DataType.INTEGER, DataType.INTEGER, DataType.INTEGER), () -> {
            List<Integer> field = randomList(1, 10, () -> randomInt());
            int length = field.size();
            int start = randomIntBetween(0, length - 1);
            int end = randomIntBetween(start, length - 1);
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(field, DataType.INTEGER, "field"),
                    new TestCaseSupplier.TypedData(start, DataType.INTEGER, "start"),
                    new TestCaseSupplier.TypedData(end, DataType.INTEGER, "end")
                ),
                "MvSliceIntEvaluator[field=Attribute[channel=0], start=Attribute[channel=1], end=Attribute[channel=2]]",
                DataType.INTEGER,
                equalTo(start == end ? field.get(start) : field.subList(start, end + 1))
            );
        }));
    }

    private static void longs(List<TestCaseSupplier> suppliers) {
        suppliers.add(new TestCaseSupplier(List.of(DataType.LONG, DataType.INTEGER, DataType.INTEGER), () -> {
            List<Long> field = randomList(1, 10, () -> randomLong());
            int length = field.size();
            int start = randomIntBetween(0, length - 1);
            int end = randomIntBetween(start, length - 1);
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(field, DataType.LONG, "field"),
                    new TestCaseSupplier.TypedData(start, DataType.INTEGER, "start"),
                    new TestCaseSupplier.TypedData(end, DataType.INTEGER, "end")
                ),
                "MvSliceLongEvaluator[field=Attribute[channel=0], start=Attribute[channel=1], end=Attribute[channel=2]]",
                DataType.LONG,
                equalTo(start == end ? field.get(start) : field.subList(start, end + 1))
            );
        }));

        suppliers.add(new TestCaseSupplier(List.of(DataType.DATETIME, DataType.INTEGER, DataType.INTEGER), () -> {
            List<Long> field = randomList(1, 10, () -> randomLong());
            int length = field.size();
            int start = randomIntBetween(0, length - 1);
            int end = randomIntBetween(start, length - 1);
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(field, DataType.DATETIME, "field"),
                    new TestCaseSupplier.TypedData(start, DataType.INTEGER, "start"),
                    new TestCaseSupplier.TypedData(end, DataType.INTEGER, "end")
                ),
                "MvSliceLongEvaluator[field=Attribute[channel=0], start=Attribute[channel=1], end=Attribute[channel=2]]",
                DataType.DATETIME,
                equalTo(start == end ? field.get(start) : field.subList(start, end + 1))
            );
        }));

        suppliers.add(new TestCaseSupplier(List.of(DataType.DATE_NANOS, DataType.INTEGER, DataType.INTEGER), () -> {
            List<Long> field = randomList(1, 10, () -> randomLong());
            int length = field.size();
            int start = randomIntBetween(0, length - 1);
            int end = randomIntBetween(start, length - 1);
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(field, DataType.DATE_NANOS, "field"),
                    new TestCaseSupplier.TypedData(start, DataType.INTEGER, "start"),
                    new TestCaseSupplier.TypedData(end, DataType.INTEGER, "end")
                ),
                "MvSliceLongEvaluator[field=Attribute[channel=0], start=Attribute[channel=1], end=Attribute[channel=2]]",
                DataType.DATE_NANOS,
                equalTo(start == end ? field.get(start) : field.subList(start, end + 1))
            );
        }));
    }

    private static void doubles(List<TestCaseSupplier> suppliers) {
        suppliers.add(new TestCaseSupplier(List.of(DataType.DOUBLE, DataType.INTEGER, DataType.INTEGER), () -> {
            List<Double> field = randomList(1, 10, () -> randomDouble());
            int length = field.size();
            int start = randomIntBetween(0, length - 1);
            int end = randomIntBetween(start, length - 1);
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(field, DataType.DOUBLE, "field"),
                    new TestCaseSupplier.TypedData(start, DataType.INTEGER, "start"),
                    new TestCaseSupplier.TypedData(end, DataType.INTEGER, "end")
                ),
                "MvSliceDoubleEvaluator[field=Attribute[channel=0], start=Attribute[channel=1], end=Attribute[channel=2]]",
                DataType.DOUBLE,
                equalTo(start == end ? field.get(start) : field.subList(start, end + 1))
            );
        }));
    }

    private static void bytesRefs(List<TestCaseSupplier> suppliers) {
        suppliers.add(new TestCaseSupplier(List.of(DataType.KEYWORD, DataType.INTEGER, DataType.INTEGER), () -> {
            List<Object> field = randomList(1, 10, () -> randomLiteral(DataType.KEYWORD).value());
            int length = field.size();
            int start = randomIntBetween(0, length - 1);
            int end = randomIntBetween(start, length - 1);
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(field, DataType.KEYWORD, "field"),
                    new TestCaseSupplier.TypedData(start, DataType.INTEGER, "start"),
                    new TestCaseSupplier.TypedData(end, DataType.INTEGER, "end")
                ),
                "MvSliceBytesRefEvaluator[field=Attribute[channel=0], start=Attribute[channel=1], end=Attribute[channel=2]]",
                DataType.KEYWORD,
                equalTo(start == end ? field.get(start) : field.subList(start, end + 1))
            );
        }));

        suppliers.add(new TestCaseSupplier(List.of(DataType.TEXT, DataType.INTEGER, DataType.INTEGER), () -> {
            List<Object> field = randomList(1, 10, () -> randomLiteral(DataType.TEXT).value());
            int length = field.size();
            int start = randomIntBetween(0, length - 1);
            int end = randomIntBetween(start, length - 1);
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(field, DataType.TEXT, "field"),
                    new TestCaseSupplier.TypedData(start, DataType.INTEGER, "start"),
                    new TestCaseSupplier.TypedData(end, DataType.INTEGER, "end")
                ),
                "MvSliceBytesRefEvaluator[field=Attribute[channel=0], start=Attribute[channel=1], end=Attribute[channel=2]]",
                DataType.TEXT,
                equalTo(start == end ? field.get(start) : field.subList(start, end + 1))
            );
        }));

        suppliers.add(new TestCaseSupplier(List.of(DataType.IP, DataType.INTEGER, DataType.INTEGER), () -> {
            List<Object> field = randomList(1, 10, () -> randomLiteral(DataType.IP).value());
            int length = field.size();
            int start = randomIntBetween(0, length - 1);
            int end = randomIntBetween(start, length - 1);
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(field, DataType.IP, "field"),
                    new TestCaseSupplier.TypedData(start, DataType.INTEGER, "start"),
                    new TestCaseSupplier.TypedData(end, DataType.INTEGER, "end")
                ),
                "MvSliceBytesRefEvaluator[field=Attribute[channel=0], start=Attribute[channel=1], end=Attribute[channel=2]]",
                DataType.IP,
                equalTo(start == end ? field.get(start) : field.subList(start, end + 1))
            );
        }));

        suppliers.add(new TestCaseSupplier(List.of(DataType.VERSION, DataType.INTEGER, DataType.INTEGER), () -> {
            List<Object> field = randomList(1, 10, () -> randomLiteral(DataType.VERSION).value());
            int length = field.size();
            int start = randomIntBetween(0, length - 1);
            int end = randomIntBetween(start, length - 1);
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(field, DataType.VERSION, "field"),
                    new TestCaseSupplier.TypedData(start, DataType.INTEGER, "start"),
                    new TestCaseSupplier.TypedData(end, DataType.INTEGER, "end")
                ),
                "MvSliceBytesRefEvaluator[field=Attribute[channel=0], start=Attribute[channel=1], end=Attribute[channel=2]]",
                DataType.VERSION,
                equalTo(start == end ? field.get(start) : field.subList(start, end + 1))
            );
        }));

        suppliers.add(new TestCaseSupplier(List.of(DataType.GEO_POINT, DataType.INTEGER, DataType.INTEGER), () -> {
            List<Object> field = randomList(1, 5, () -> new BytesRef(GEO.asWkt(GeometryTestUtils.randomPoint())));
            int length = field.size();
            int start = randomIntBetween(0, length - 1);
            int end = randomIntBetween(start, length - 1);
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(field, DataType.GEO_POINT, "field"),
                    new TestCaseSupplier.TypedData(start, DataType.INTEGER, "start"),
                    new TestCaseSupplier.TypedData(end, DataType.INTEGER, "end")
                ),
                "MvSliceBytesRefEvaluator[field=Attribute[channel=0], start=Attribute[channel=1], end=Attribute[channel=2]]",
                DataType.GEO_POINT,
                equalTo(start == end ? field.get(start) : field.subList(start, end + 1))
            );
        }));

        suppliers.add(new TestCaseSupplier(List.of(DataType.CARTESIAN_POINT, DataType.INTEGER, DataType.INTEGER), () -> {
            List<Object> field = randomList(1, 5, () -> new BytesRef(CARTESIAN.asWkt(ShapeTestUtils.randomPoint())));
            int length = field.size();
            int start = randomIntBetween(0, length - 1);
            int end = randomIntBetween(start, length - 1);
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(field, DataType.CARTESIAN_POINT, "field"),
                    new TestCaseSupplier.TypedData(start, DataType.INTEGER, "start"),
                    new TestCaseSupplier.TypedData(end, DataType.INTEGER, "end")
                ),
                "MvSliceBytesRefEvaluator[field=Attribute[channel=0], start=Attribute[channel=1], end=Attribute[channel=2]]",
                DataType.CARTESIAN_POINT,
                equalTo(start == end ? field.get(start) : field.subList(start, end + 1))
            );
        }));

        suppliers.add(new TestCaseSupplier(List.of(DataType.GEO_SHAPE, DataType.INTEGER, DataType.INTEGER), () -> {
            var field = randomList(1, 5, () -> new BytesRef(GEO.asWkt(GeometryTestUtils.randomGeometry(randomBoolean(), 500))));
            int length = field.size();
            int start = randomIntBetween(0, length - 1);
            int end = randomIntBetween(start, length - 1);
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(field, DataType.GEO_SHAPE, "field"),
                    new TestCaseSupplier.TypedData(start, DataType.INTEGER, "start"),
                    new TestCaseSupplier.TypedData(end, DataType.INTEGER, "end")
                ),
                "MvSliceBytesRefEvaluator[field=Attribute[channel=0], start=Attribute[channel=1], end=Attribute[channel=2]]",
                DataType.GEO_SHAPE,
                equalTo(start == end ? field.get(start) : field.subList(start, end + 1))
            );
        }));

        suppliers.add(new TestCaseSupplier(List.of(DataType.CARTESIAN_SHAPE, DataType.INTEGER, DataType.INTEGER), () -> {
            var field = randomList(1, 5, () -> new BytesRef(CARTESIAN.asWkt(GeometryTestUtils.randomGeometry(randomBoolean(), 500))));
            int length = field.size();
            int start = randomIntBetween(0, length - 1);
            int end = randomIntBetween(start, length - 1);
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(field, DataType.CARTESIAN_SHAPE, "field"),
                    new TestCaseSupplier.TypedData(start, DataType.INTEGER, "start"),
                    new TestCaseSupplier.TypedData(end, DataType.INTEGER, "end")
                ),
                "MvSliceBytesRefEvaluator[field=Attribute[channel=0], start=Attribute[channel=1], end=Attribute[channel=2]]",
                DataType.CARTESIAN_SHAPE,
                equalTo(start == end ? field.get(start) : field.subList(start, end + 1))
            );
        }));
    }
}
