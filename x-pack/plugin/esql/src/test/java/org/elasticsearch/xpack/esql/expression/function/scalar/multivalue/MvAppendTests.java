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
import org.elasticsearch.xpack.esql.core.type.DataTypes;
import org.elasticsearch.xpack.esql.expression.function.AbstractFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.CARTESIAN;
import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.GEO;
import static org.hamcrest.Matchers.equalTo;

public class MvAppendTests extends AbstractFunctionTestCase {
    public MvAppendTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
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
        nulls(suppliers);
        return parameterSuppliersFromTypedData(suppliers);
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new MvAppend(source, args.get(0), args.get(1));
    }

    private static void booleans(List<TestCaseSupplier> suppliers) {
        suppliers.add(new TestCaseSupplier(List.of(DataTypes.BOOLEAN, DataTypes.BOOLEAN), () -> {
            List<Boolean> field1 = randomList(1, 10, () -> randomBoolean());
            List<Boolean> field2 = randomList(1, 10, () -> randomBoolean());
            var result = new ArrayList<>(field1);
            result.addAll(field2);
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(field1, DataTypes.BOOLEAN, "field1"),
                    new TestCaseSupplier.TypedData(field2, DataTypes.BOOLEAN, "field2")
                ),
                "MvAppendBooleanEvaluator[field1=Attribute[channel=0], field2=Attribute[channel=1]]",
                DataTypes.BOOLEAN,
                equalTo(result)
            );
        }));
    }

    private static void ints(List<TestCaseSupplier> suppliers) {
        suppliers.add(new TestCaseSupplier(List.of(DataTypes.INTEGER, DataTypes.INTEGER), () -> {
            List<Integer> field1 = randomList(1, 10, () -> randomInt());
            List<Integer> field2 = randomList(1, 10, () -> randomInt());
            var result = new ArrayList<>(field1);
            result.addAll(field2);
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(field1, DataTypes.INTEGER, "field1"),
                    new TestCaseSupplier.TypedData(field2, DataTypes.INTEGER, "field2")
                ),
                "MvAppendIntEvaluator[field1=Attribute[channel=0], field2=Attribute[channel=1]]",
                DataTypes.INTEGER,
                equalTo(result)
            );
        }));
    }

    private static void longs(List<TestCaseSupplier> suppliers) {
        suppliers.add(new TestCaseSupplier(List.of(DataTypes.LONG, DataTypes.LONG), () -> {
            List<Long> field1 = randomList(1, 10, () -> randomLong());
            List<Long> field2 = randomList(1, 10, () -> randomLong());
            var result = new ArrayList<>(field1);
            result.addAll(field2);
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(field1, DataTypes.LONG, "field1"),
                    new TestCaseSupplier.TypedData(field2, DataTypes.LONG, "field2")
                ),
                "MvAppendLongEvaluator[field1=Attribute[channel=0], field2=Attribute[channel=1]]",
                DataTypes.LONG,
                equalTo(result)
            );
        }));

        suppliers.add(new TestCaseSupplier(List.of(DataTypes.DATETIME, DataTypes.DATETIME), () -> {
            List<Long> field1 = randomList(1, 10, () -> randomLong());
            List<Long> field2 = randomList(1, 10, () -> randomLong());
            var result = new ArrayList<>(field1);
            result.addAll(field2);
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(field1, DataTypes.DATETIME, "field1"),
                    new TestCaseSupplier.TypedData(field2, DataTypes.DATETIME, "field2")
                ),
                "MvAppendLongEvaluator[field1=Attribute[channel=0], field2=Attribute[channel=1]]",
                DataTypes.DATETIME,
                equalTo(result)
            );
        }));
    }

    private static void doubles(List<TestCaseSupplier> suppliers) {
        suppliers.add(new TestCaseSupplier(List.of(DataTypes.DOUBLE, DataTypes.DOUBLE), () -> {
            List<Double> field1 = randomList(1, 10, () -> randomDouble());
            List<Double> field2 = randomList(1, 10, () -> randomDouble());
            var result = new ArrayList<>(field1);
            result.addAll(field2);
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(field1, DataTypes.DOUBLE, "field1"),
                    new TestCaseSupplier.TypedData(field2, DataTypes.DOUBLE, "field2")
                ),
                "MvAppendDoubleEvaluator[field1=Attribute[channel=0], field2=Attribute[channel=1]]",
                DataTypes.DOUBLE,
                equalTo(result)
            );
        }));
    }

    private static void bytesRefs(List<TestCaseSupplier> suppliers) {
        suppliers.add(new TestCaseSupplier(List.of(DataTypes.KEYWORD, DataTypes.KEYWORD), () -> {
            List<Object> field1 = randomList(1, 10, () -> randomLiteral(DataTypes.KEYWORD).value());
            List<Object> field2 = randomList(1, 10, () -> randomLiteral(DataTypes.KEYWORD).value());
            var result = new ArrayList<>(field1);
            result.addAll(field2);
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(field1, DataTypes.KEYWORD, "field1"),
                    new TestCaseSupplier.TypedData(field2, DataTypes.KEYWORD, "field2")
                ),
                "MvAppendBytesRefEvaluator[field1=Attribute[channel=0], field2=Attribute[channel=1]]",
                DataTypes.KEYWORD,
                equalTo(result)
            );
        }));

        suppliers.add(new TestCaseSupplier(List.of(DataTypes.TEXT, DataTypes.TEXT), () -> {
            List<Object> field1 = randomList(1, 10, () -> randomLiteral(DataTypes.TEXT).value());
            List<Object> field2 = randomList(1, 10, () -> randomLiteral(DataTypes.TEXT).value());
            var result = new ArrayList<>(field1);
            result.addAll(field2);
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(field1, DataTypes.TEXT, "field1"),
                    new TestCaseSupplier.TypedData(field2, DataTypes.TEXT, "field2")
                ),
                "MvAppendBytesRefEvaluator[field1=Attribute[channel=0], field2=Attribute[channel=1]]",
                DataTypes.TEXT,
                equalTo(result)
            );
        }));

        suppliers.add(new TestCaseSupplier(List.of(DataTypes.IP, DataTypes.IP), () -> {
            List<Object> field1 = randomList(1, 10, () -> randomLiteral(DataTypes.IP).value());
            List<Object> field2 = randomList(1, 10, () -> randomLiteral(DataTypes.IP).value());
            var result = new ArrayList<>(field1);
            result.addAll(field2);
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(field1, DataTypes.IP, "field"),
                    new TestCaseSupplier.TypedData(field2, DataTypes.IP, "field")
                ),
                "MvAppendBytesRefEvaluator[field1=Attribute[channel=0], field2=Attribute[channel=1]]",
                DataTypes.IP,
                equalTo(result)
            );
        }));

        suppliers.add(new TestCaseSupplier(List.of(DataTypes.VERSION, DataTypes.VERSION), () -> {
            List<Object> field1 = randomList(1, 10, () -> randomLiteral(DataTypes.VERSION).value());
            List<Object> field2 = randomList(1, 10, () -> randomLiteral(DataTypes.VERSION).value());
            var result = new ArrayList<>(field1);
            result.addAll(field2);
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(field1, DataTypes.VERSION, "field"),
                    new TestCaseSupplier.TypedData(field2, DataTypes.VERSION, "field")
                ),
                "MvAppendBytesRefEvaluator[field1=Attribute[channel=0], field2=Attribute[channel=1]]",
                DataTypes.VERSION,
                equalTo(result)
            );
        }));

        suppliers.add(new TestCaseSupplier(List.of(DataTypes.GEO_POINT, DataTypes.GEO_POINT), () -> {
            List<Object> field1 = randomList(1, 10, () -> new BytesRef(GEO.asWkt(GeometryTestUtils.randomPoint())));
            List<Object> field2 = randomList(1, 10, () -> new BytesRef(GEO.asWkt(GeometryTestUtils.randomPoint())));
            var result = new ArrayList<>(field1);
            result.addAll(field2);
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(field1, DataTypes.GEO_POINT, "field1"),
                    new TestCaseSupplier.TypedData(field2, DataTypes.GEO_POINT, "field2")
                ),
                "MvAppendBytesRefEvaluator[field1=Attribute[channel=0], field2=Attribute[channel=1]]",
                DataTypes.GEO_POINT,
                equalTo(result)
            );
        }));

        suppliers.add(new TestCaseSupplier(List.of(DataTypes.CARTESIAN_POINT, DataTypes.CARTESIAN_POINT), () -> {
            List<Object> field1 = randomList(1, 10, () -> new BytesRef(CARTESIAN.asWkt(ShapeTestUtils.randomPoint())));
            List<Object> field2 = randomList(1, 10, () -> new BytesRef(CARTESIAN.asWkt(ShapeTestUtils.randomPoint())));
            var result = new ArrayList<>(field1);
            result.addAll(field2);
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(field1, DataTypes.CARTESIAN_POINT, "field1"),
                    new TestCaseSupplier.TypedData(field2, DataTypes.CARTESIAN_POINT, "field2")
                ),
                "MvAppendBytesRefEvaluator[field1=Attribute[channel=0], field2=Attribute[channel=1]]",
                DataTypes.CARTESIAN_POINT,
                equalTo(result)
            );
        }));

        suppliers.add(new TestCaseSupplier(List.of(DataTypes.GEO_SHAPE, DataTypes.GEO_SHAPE), () -> {
            List<Object> field1 = randomList(1, 10, () -> new BytesRef(GEO.asWkt(GeometryTestUtils.randomGeometry(randomBoolean()))));
            List<Object> field2 = randomList(1, 10, () -> new BytesRef(GEO.asWkt(GeometryTestUtils.randomGeometry(randomBoolean()))));
            var result = new ArrayList<>(field1);
            result.addAll(field2);
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(field1, DataTypes.GEO_SHAPE, "field1"),
                    new TestCaseSupplier.TypedData(field2, DataTypes.GEO_SHAPE, "field2")
                ),
                "MvAppendBytesRefEvaluator[field1=Attribute[channel=0], field2=Attribute[channel=1]]",
                DataTypes.GEO_SHAPE,
                equalTo(result)
            );
        }));

        suppliers.add(new TestCaseSupplier(List.of(DataTypes.CARTESIAN_SHAPE, DataTypes.CARTESIAN_SHAPE), () -> {
            List<Object> field1 = randomList(1, 10, () -> new BytesRef(CARTESIAN.asWkt(ShapeTestUtils.randomGeometry(randomBoolean()))));
            List<Object> field2 = randomList(1, 10, () -> new BytesRef(CARTESIAN.asWkt(ShapeTestUtils.randomGeometry(randomBoolean()))));
            var result = new ArrayList<>(field1);
            result.addAll(field2);
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(field1, DataTypes.CARTESIAN_SHAPE, "field1"),
                    new TestCaseSupplier.TypedData(field2, DataTypes.CARTESIAN_SHAPE, "field2")
                ),
                "MvAppendBytesRefEvaluator[field1=Attribute[channel=0], field2=Attribute[channel=1]]",
                DataTypes.CARTESIAN_SHAPE,
                equalTo(result)
            );
        }));
    }

    private static void nulls(List<TestCaseSupplier> suppliers) {
        suppliers.add(new TestCaseSupplier(List.of(DataTypes.INTEGER, DataTypes.INTEGER), () -> {
            List<Integer> field2 = randomList(2, 10, () -> randomInt());
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(null, DataTypes.INTEGER, "field1"),
                    new TestCaseSupplier.TypedData(field2, DataTypes.INTEGER, "field2")
                ),
                "MvAppendIntEvaluator[field1=Attribute[channel=0], field2=Attribute[channel=1]]",
                DataTypes.INTEGER,
                equalTo(null)
            );
        }));
        suppliers.add(new TestCaseSupplier(List.of(DataTypes.INTEGER, DataTypes.INTEGER), () -> {
            List<Integer> field1 = randomList(2, 10, () -> randomInt());
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(field1, DataTypes.INTEGER, "field1"),
                    new TestCaseSupplier.TypedData(null, DataTypes.INTEGER, "field2")
                ),
                "MvAppendIntEvaluator[field1=Attribute[channel=0], field2=Attribute[channel=1]]",
                DataTypes.INTEGER,
                equalTo(null)
            );
        }));
    }
}
