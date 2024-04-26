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
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.elasticsearch.xpack.esql.expression.function.scalar.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.type.EsqlDataTypes;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.ql.util.SpatialCoordinateTypes.CARTESIAN;
import static org.elasticsearch.xpack.ql.util.SpatialCoordinateTypes.GEO;
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
        return parameterSuppliersFromTypedData(suppliers);
    }

    @Override
    protected DataType expectedType(List<DataType> argTypes) {
        return argTypes.get(0);
    }

    @Override
    protected List<ArgumentSpec> argSpec() {
        return List.of(required(representableTypes()), required(integers()), optional(integers()));
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new MvSlice(source, args.get(0), args.get(1), args.size() > 2 ? args.get(2) : null);
    }

    private static void booleans(List<TestCaseSupplier> suppliers) {
        // Positive
        suppliers.add(new TestCaseSupplier(List.of(DataTypes.BOOLEAN, DataTypes.INTEGER, DataTypes.INTEGER), () -> {
            List<Boolean> field = randomList(1, 10, () -> randomBoolean());
            int length = field.size();
            int start = randomIntBetween(0, length - 1);
            int end = randomIntBetween(start, length - 1);
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(field, DataTypes.BOOLEAN, "field"),
                    new TestCaseSupplier.TypedData(start, DataTypes.INTEGER, "start"),
                    new TestCaseSupplier.TypedData(end, DataTypes.INTEGER, "end")
                ),
                "MvSliceBooleanEvaluator[field=Attribute[channel=0], start=Attribute[channel=1], end=Attribute[channel=2]]",
                DataTypes.BOOLEAN,
                equalTo(start == end ? field.get(start) : field.subList(start, end + 1))
            );
        }));
        // Positive Start IndexOutofBound
        suppliers.add(new TestCaseSupplier(List.of(DataTypes.BOOLEAN, DataTypes.INTEGER, DataTypes.INTEGER), () -> {
            List<Boolean> field = randomList(1, 10, () -> randomBoolean());
            int length = field.size();
            int start = randomIntBetween(length, length + 1);
            int end = randomIntBetween(start, length + 10);
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(field, DataTypes.BOOLEAN, "field"),
                    new TestCaseSupplier.TypedData(start, DataTypes.INTEGER, "start"),
                    new TestCaseSupplier.TypedData(end, DataTypes.INTEGER, "end")
                ),
                "MvSliceBooleanEvaluator[field=Attribute[channel=0], start=Attribute[channel=1], end=Attribute[channel=2]]",
                DataTypes.BOOLEAN,
                nullValue()
            );
        }));
        // Positive End IndexOutofBound
        suppliers.add(new TestCaseSupplier(List.of(DataTypes.BOOLEAN, DataTypes.INTEGER, DataTypes.INTEGER), () -> {
            List<Boolean> field = randomList(1, 10, () -> randomBoolean());
            int length = field.size();
            int start = randomIntBetween(0, length - 1);
            int end = randomIntBetween(length, length + 10);
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(field, DataTypes.BOOLEAN, "field"),
                    new TestCaseSupplier.TypedData(start, DataTypes.INTEGER, "start"),
                    new TestCaseSupplier.TypedData(end, DataTypes.INTEGER, "end")
                ),
                "MvSliceBooleanEvaluator[field=Attribute[channel=0], start=Attribute[channel=1], end=Attribute[channel=2]]",
                DataTypes.BOOLEAN,
                equalTo(start == length - 1 ? field.get(start) : field.subList(start, length))
            );
        }));
        // Negative
        suppliers.add(new TestCaseSupplier(List.of(DataTypes.BOOLEAN, DataTypes.INTEGER, DataTypes.INTEGER), () -> {
            List<Boolean> field = randomList(1, 10, () -> randomBoolean());
            int length = field.size();
            int start = randomIntBetween(0 - length, -1);
            int end = randomIntBetween(start, -1);
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(field, DataTypes.BOOLEAN, "field"),
                    new TestCaseSupplier.TypedData(start, DataTypes.INTEGER, "start"),
                    new TestCaseSupplier.TypedData(end, DataTypes.INTEGER, "end")
                ),
                "MvSliceBooleanEvaluator[field=Attribute[channel=0], start=Attribute[channel=1], end=Attribute[channel=2]]",
                DataTypes.BOOLEAN,
                equalTo(start == end ? field.get(start + length) : field.subList(start + length, end + 1 + length))
            );
        }));
    }

    private static void ints(List<TestCaseSupplier> suppliers) {
        suppliers.add(new TestCaseSupplier(List.of(DataTypes.INTEGER, DataTypes.INTEGER, DataTypes.INTEGER), () -> {
            List<Integer> field = randomList(1, 10, () -> randomInt());
            int length = field.size();
            int start = randomIntBetween(0, length - 1);
            int end = randomIntBetween(start, length - 1);
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(field, DataTypes.INTEGER, "field"),
                    new TestCaseSupplier.TypedData(start, DataTypes.INTEGER, "start"),
                    new TestCaseSupplier.TypedData(end, DataTypes.INTEGER, "end")
                ),
                "MvSliceIntEvaluator[field=Attribute[channel=0], start=Attribute[channel=1], end=Attribute[channel=2]]",
                DataTypes.INTEGER,
                equalTo(start == end ? field.get(start) : field.subList(start, end + 1))
            );
        }));
    }

    private static void longs(List<TestCaseSupplier> suppliers) {
        suppliers.add(new TestCaseSupplier(List.of(DataTypes.LONG, DataTypes.INTEGER, DataTypes.INTEGER), () -> {
            List<Long> field = randomList(1, 10, () -> randomLong());
            int length = field.size();
            int start = randomIntBetween(0, length - 1);
            int end = randomIntBetween(start, length - 1);
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(field, DataTypes.LONG, "field"),
                    new TestCaseSupplier.TypedData(start, DataTypes.INTEGER, "start"),
                    new TestCaseSupplier.TypedData(end, DataTypes.INTEGER, "end")
                ),
                "MvSliceLongEvaluator[field=Attribute[channel=0], start=Attribute[channel=1], end=Attribute[channel=2]]",
                DataTypes.LONG,
                equalTo(start == end ? field.get(start) : field.subList(start, end + 1))
            );
        }));

        suppliers.add(new TestCaseSupplier(List.of(DataTypes.DATETIME, DataTypes.INTEGER, DataTypes.INTEGER), () -> {
            List<Long> field = randomList(1, 10, () -> randomLong());
            int length = field.size();
            int start = randomIntBetween(0, length - 1);
            int end = randomIntBetween(start, length - 1);
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(field, DataTypes.DATETIME, "field"),
                    new TestCaseSupplier.TypedData(start, DataTypes.INTEGER, "start"),
                    new TestCaseSupplier.TypedData(end, DataTypes.INTEGER, "end")
                ),
                "MvSliceLongEvaluator[field=Attribute[channel=0], start=Attribute[channel=1], end=Attribute[channel=2]]",
                DataTypes.DATETIME,
                equalTo(start == end ? field.get(start) : field.subList(start, end + 1))
            );
        }));
    }

    private static void doubles(List<TestCaseSupplier> suppliers) {
        suppliers.add(new TestCaseSupplier(List.of(DataTypes.DOUBLE, DataTypes.INTEGER, DataTypes.INTEGER), () -> {
            List<Double> field = randomList(1, 10, () -> randomDouble());
            int length = field.size();
            int start = randomIntBetween(0, length - 1);
            int end = randomIntBetween(start, length - 1);
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(field, DataTypes.DOUBLE, "field"),
                    new TestCaseSupplier.TypedData(start, DataTypes.INTEGER, "start"),
                    new TestCaseSupplier.TypedData(end, DataTypes.INTEGER, "end")
                ),
                "MvSliceDoubleEvaluator[field=Attribute[channel=0], start=Attribute[channel=1], end=Attribute[channel=2]]",
                DataTypes.DOUBLE,
                equalTo(start == end ? field.get(start) : field.subList(start, end + 1))
            );
        }));
    }

    private static void bytesRefs(List<TestCaseSupplier> suppliers) {
        suppliers.add(new TestCaseSupplier(List.of(DataTypes.KEYWORD, DataTypes.INTEGER, DataTypes.INTEGER), () -> {
            List<Object> field = randomList(1, 10, () -> randomLiteral(DataTypes.KEYWORD).value());
            int length = field.size();
            int start = randomIntBetween(0, length - 1);
            int end = randomIntBetween(start, length - 1);
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(field, DataTypes.KEYWORD, "field"),
                    new TestCaseSupplier.TypedData(start, DataTypes.INTEGER, "start"),
                    new TestCaseSupplier.TypedData(end, DataTypes.INTEGER, "end")
                ),
                "MvSliceBytesRefEvaluator[field=Attribute[channel=0], start=Attribute[channel=1], end=Attribute[channel=2]]",
                DataTypes.KEYWORD,
                equalTo(start == end ? field.get(start) : field.subList(start, end + 1))
            );
        }));

        suppliers.add(new TestCaseSupplier(List.of(DataTypes.TEXT, DataTypes.INTEGER, DataTypes.INTEGER), () -> {
            List<Object> field = randomList(1, 10, () -> randomLiteral(DataTypes.TEXT).value());
            int length = field.size();
            int start = randomIntBetween(0, length - 1);
            int end = randomIntBetween(start, length - 1);
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(field, DataTypes.TEXT, "field"),
                    new TestCaseSupplier.TypedData(start, DataTypes.INTEGER, "start"),
                    new TestCaseSupplier.TypedData(end, DataTypes.INTEGER, "end")
                ),
                "MvSliceBytesRefEvaluator[field=Attribute[channel=0], start=Attribute[channel=1], end=Attribute[channel=2]]",
                DataTypes.TEXT,
                equalTo(start == end ? field.get(start) : field.subList(start, end + 1))
            );
        }));

        suppliers.add(new TestCaseSupplier(List.of(DataTypes.IP, DataTypes.INTEGER, DataTypes.INTEGER), () -> {
            List<Object> field = randomList(1, 10, () -> randomLiteral(DataTypes.IP).value());
            int length = field.size();
            int start = randomIntBetween(0, length - 1);
            int end = randomIntBetween(start, length - 1);
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(field, DataTypes.IP, "field"),
                    new TestCaseSupplier.TypedData(start, DataTypes.INTEGER, "start"),
                    new TestCaseSupplier.TypedData(end, DataTypes.INTEGER, "end")
                ),
                "MvSliceBytesRefEvaluator[field=Attribute[channel=0], start=Attribute[channel=1], end=Attribute[channel=2]]",
                DataTypes.IP,
                equalTo(start == end ? field.get(start) : field.subList(start, end + 1))
            );
        }));

        suppliers.add(new TestCaseSupplier(List.of(DataTypes.VERSION, DataTypes.INTEGER, DataTypes.INTEGER), () -> {
            List<Object> field = randomList(1, 10, () -> randomLiteral(DataTypes.VERSION).value());
            int length = field.size();
            int start = randomIntBetween(0, length - 1);
            int end = randomIntBetween(start, length - 1);
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(field, DataTypes.VERSION, "field"),
                    new TestCaseSupplier.TypedData(start, DataTypes.INTEGER, "start"),
                    new TestCaseSupplier.TypedData(end, DataTypes.INTEGER, "end")
                ),
                "MvSliceBytesRefEvaluator[field=Attribute[channel=0], start=Attribute[channel=1], end=Attribute[channel=2]]",
                DataTypes.VERSION,
                equalTo(start == end ? field.get(start) : field.subList(start, end + 1))
            );
        }));

        suppliers.add(new TestCaseSupplier(List.of(EsqlDataTypes.GEO_POINT, DataTypes.INTEGER, DataTypes.INTEGER), () -> {
            List<Object> field = randomList(1, 10, () -> new BytesRef(GEO.asWkt(GeometryTestUtils.randomPoint())));
            int length = field.size();
            int start = randomIntBetween(0, length - 1);
            int end = randomIntBetween(start, length - 1);
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(field, EsqlDataTypes.GEO_POINT, "field"),
                    new TestCaseSupplier.TypedData(start, DataTypes.INTEGER, "start"),
                    new TestCaseSupplier.TypedData(end, DataTypes.INTEGER, "end")
                ),
                "MvSliceBytesRefEvaluator[field=Attribute[channel=0], start=Attribute[channel=1], end=Attribute[channel=2]]",
                EsqlDataTypes.GEO_POINT,
                equalTo(start == end ? field.get(start) : field.subList(start, end + 1))
            );
        }));

        suppliers.add(new TestCaseSupplier(List.of(EsqlDataTypes.CARTESIAN_POINT, DataTypes.INTEGER, DataTypes.INTEGER), () -> {
            List<Object> field = randomList(1, 10, () -> new BytesRef(CARTESIAN.asWkt(ShapeTestUtils.randomPoint())));
            int length = field.size();
            int start = randomIntBetween(0, length - 1);
            int end = randomIntBetween(start, length - 1);
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(field, EsqlDataTypes.CARTESIAN_POINT, "field"),
                    new TestCaseSupplier.TypedData(start, DataTypes.INTEGER, "start"),
                    new TestCaseSupplier.TypedData(end, DataTypes.INTEGER, "end")
                ),
                "MvSliceBytesRefEvaluator[field=Attribute[channel=0], start=Attribute[channel=1], end=Attribute[channel=2]]",
                EsqlDataTypes.CARTESIAN_POINT,
                equalTo(start == end ? field.get(start) : field.subList(start, end + 1))
            );
        }));

        suppliers.add(new TestCaseSupplier(List.of(EsqlDataTypes.GEO_SHAPE, DataTypes.INTEGER, DataTypes.INTEGER), () -> {
            List<Object> field = randomList(1, 10, () -> new BytesRef(GEO.asWkt(GeometryTestUtils.randomGeometry(randomBoolean()))));
            int length = field.size();
            int start = randomIntBetween(0, length - 1);
            int end = randomIntBetween(start, length - 1);
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(field, EsqlDataTypes.GEO_SHAPE, "field"),
                    new TestCaseSupplier.TypedData(start, DataTypes.INTEGER, "start"),
                    new TestCaseSupplier.TypedData(end, DataTypes.INTEGER, "end")
                ),
                "MvSliceBytesRefEvaluator[field=Attribute[channel=0], start=Attribute[channel=1], end=Attribute[channel=2]]",
                EsqlDataTypes.GEO_SHAPE,
                equalTo(start == end ? field.get(start) : field.subList(start, end + 1))
            );
        }));

        suppliers.add(new TestCaseSupplier(List.of(EsqlDataTypes.CARTESIAN_SHAPE, DataTypes.INTEGER, DataTypes.INTEGER), () -> {
            List<Object> field = randomList(1, 10, () -> new BytesRef(CARTESIAN.asWkt(ShapeTestUtils.randomGeometry(randomBoolean()))));
            int length = field.size();
            int start = randomIntBetween(0, length - 1);
            int end = randomIntBetween(start, length - 1);
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(field, EsqlDataTypes.CARTESIAN_SHAPE, "field"),
                    new TestCaseSupplier.TypedData(start, DataTypes.INTEGER, "start"),
                    new TestCaseSupplier.TypedData(end, DataTypes.INTEGER, "end")
                ),
                "MvSliceBytesRefEvaluator[field=Attribute[channel=0], start=Attribute[channel=1], end=Attribute[channel=2]]",
                EsqlDataTypes.CARTESIAN_SHAPE,
                equalTo(start == end ? field.get(start) : field.subList(start, end + 1))
            );
        }));
    }
}
