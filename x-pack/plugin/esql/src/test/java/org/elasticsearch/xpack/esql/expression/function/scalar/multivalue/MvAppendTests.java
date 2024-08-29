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

public class MvAppendTests extends AbstractScalarFunctionTestCase {
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
        suppliers.add(new TestCaseSupplier(List.of(DataType.BOOLEAN, DataType.BOOLEAN), () -> {
            List<Boolean> field1 = randomList(1, 10, () -> randomBoolean());
            List<Boolean> field2 = randomList(1, 10, () -> randomBoolean());
            var result = new ArrayList<>(field1);
            result.addAll(field2);
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(field1, DataType.BOOLEAN, "field1"),
                    new TestCaseSupplier.TypedData(field2, DataType.BOOLEAN, "field2")
                ),
                "MvAppendBooleanEvaluator[field1=Attribute[channel=0], field2=Attribute[channel=1]]",
                DataType.BOOLEAN,
                equalTo(result)
            );
        }));
    }

    private static void ints(List<TestCaseSupplier> suppliers) {
        suppliers.add(new TestCaseSupplier(List.of(DataType.INTEGER, DataType.INTEGER), () -> {
            List<Integer> field1 = randomList(1, 10, () -> randomInt());
            List<Integer> field2 = randomList(1, 10, () -> randomInt());
            var result = new ArrayList<>(field1);
            result.addAll(field2);
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(field1, DataType.INTEGER, "field1"),
                    new TestCaseSupplier.TypedData(field2, DataType.INTEGER, "field2")
                ),
                "MvAppendIntEvaluator[field1=Attribute[channel=0], field2=Attribute[channel=1]]",
                DataType.INTEGER,
                equalTo(result)
            );
        }));
    }

    private static void longs(List<TestCaseSupplier> suppliers) {
        suppliers.add(new TestCaseSupplier(List.of(DataType.LONG, DataType.LONG), () -> {
            List<Long> field1 = randomList(1, 10, () -> randomLong());
            List<Long> field2 = randomList(1, 10, () -> randomLong());
            var result = new ArrayList<>(field1);
            result.addAll(field2);
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(field1, DataType.LONG, "field1"),
                    new TestCaseSupplier.TypedData(field2, DataType.LONG, "field2")
                ),
                "MvAppendLongEvaluator[field1=Attribute[channel=0], field2=Attribute[channel=1]]",
                DataType.LONG,
                equalTo(result)
            );
        }));

        suppliers.add(new TestCaseSupplier(List.of(DataType.DATETIME, DataType.DATETIME), () -> {
            List<Long> field1 = randomList(1, 10, () -> randomLong());
            List<Long> field2 = randomList(1, 10, () -> randomLong());
            var result = new ArrayList<>(field1);
            result.addAll(field2);
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(field1, DataType.DATETIME, "field1"),
                    new TestCaseSupplier.TypedData(field2, DataType.DATETIME, "field2")
                ),
                "MvAppendLongEvaluator[field1=Attribute[channel=0], field2=Attribute[channel=1]]",
                DataType.DATETIME,
                equalTo(result)
            );
        }));
    }

    private static void doubles(List<TestCaseSupplier> suppliers) {
        suppliers.add(new TestCaseSupplier(List.of(DataType.DOUBLE, DataType.DOUBLE), () -> {
            List<Double> field1 = randomList(1, 10, () -> randomDouble());
            List<Double> field2 = randomList(1, 10, () -> randomDouble());
            var result = new ArrayList<>(field1);
            result.addAll(field2);
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(field1, DataType.DOUBLE, "field1"),
                    new TestCaseSupplier.TypedData(field2, DataType.DOUBLE, "field2")
                ),
                "MvAppendDoubleEvaluator[field1=Attribute[channel=0], field2=Attribute[channel=1]]",
                DataType.DOUBLE,
                equalTo(result)
            );
        }));
    }

    private static void bytesRefs(List<TestCaseSupplier> suppliers) {
        suppliers.add(new TestCaseSupplier(List.of(DataType.KEYWORD, DataType.KEYWORD), () -> {
            List<Object> field1 = randomList(1, 10, () -> randomLiteral(DataType.KEYWORD).value());
            List<Object> field2 = randomList(1, 10, () -> randomLiteral(DataType.KEYWORD).value());
            var result = new ArrayList<>(field1);
            result.addAll(field2);
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(field1, DataType.KEYWORD, "field1"),
                    new TestCaseSupplier.TypedData(field2, DataType.KEYWORD, "field2")
                ),
                "MvAppendBytesRefEvaluator[field1=Attribute[channel=0], field2=Attribute[channel=1]]",
                DataType.KEYWORD,
                equalTo(result)
            );
        }));

        suppliers.add(new TestCaseSupplier(List.of(DataType.TEXT, DataType.TEXT), () -> {
            List<Object> field1 = randomList(1, 10, () -> randomLiteral(DataType.TEXT).value());
            List<Object> field2 = randomList(1, 10, () -> randomLiteral(DataType.TEXT).value());
            var result = new ArrayList<>(field1);
            result.addAll(field2);
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(field1, DataType.TEXT, "field1"),
                    new TestCaseSupplier.TypedData(field2, DataType.TEXT, "field2")
                ),
                "MvAppendBytesRefEvaluator[field1=Attribute[channel=0], field2=Attribute[channel=1]]",
                DataType.TEXT,
                equalTo(result)
            );
        }));

        suppliers.add(new TestCaseSupplier(List.of(DataType.IP, DataType.IP), () -> {
            List<Object> field1 = randomList(1, 10, () -> randomLiteral(DataType.IP).value());
            List<Object> field2 = randomList(1, 10, () -> randomLiteral(DataType.IP).value());
            var result = new ArrayList<>(field1);
            result.addAll(field2);
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(field1, DataType.IP, "field"),
                    new TestCaseSupplier.TypedData(field2, DataType.IP, "field")
                ),
                "MvAppendBytesRefEvaluator[field1=Attribute[channel=0], field2=Attribute[channel=1]]",
                DataType.IP,
                equalTo(result)
            );
        }));

        suppliers.add(new TestCaseSupplier(List.of(DataType.VERSION, DataType.VERSION), () -> {
            List<Object> field1 = randomList(1, 10, () -> randomLiteral(DataType.VERSION).value());
            List<Object> field2 = randomList(1, 10, () -> randomLiteral(DataType.VERSION).value());
            var result = new ArrayList<>(field1);
            result.addAll(field2);
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(field1, DataType.VERSION, "field"),
                    new TestCaseSupplier.TypedData(field2, DataType.VERSION, "field")
                ),
                "MvAppendBytesRefEvaluator[field1=Attribute[channel=0], field2=Attribute[channel=1]]",
                DataType.VERSION,
                equalTo(result)
            );
        }));

        suppliers.add(new TestCaseSupplier(List.of(DataType.GEO_POINT, DataType.GEO_POINT), () -> {
            List<Object> field1 = randomList(1, 10, () -> new BytesRef(GEO.asWkt(GeometryTestUtils.randomPoint())));
            List<Object> field2 = randomList(1, 10, () -> new BytesRef(GEO.asWkt(GeometryTestUtils.randomPoint())));
            var result = new ArrayList<>(field1);
            result.addAll(field2);
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(field1, DataType.GEO_POINT, "field1"),
                    new TestCaseSupplier.TypedData(field2, DataType.GEO_POINT, "field2")
                ),
                "MvAppendBytesRefEvaluator[field1=Attribute[channel=0], field2=Attribute[channel=1]]",
                DataType.GEO_POINT,
                equalTo(result)
            );
        }));

        suppliers.add(new TestCaseSupplier(List.of(DataType.CARTESIAN_POINT, DataType.CARTESIAN_POINT), () -> {
            List<Object> field1 = randomList(1, 10, () -> new BytesRef(CARTESIAN.asWkt(ShapeTestUtils.randomPoint())));
            List<Object> field2 = randomList(1, 10, () -> new BytesRef(CARTESIAN.asWkt(ShapeTestUtils.randomPoint())));
            var result = new ArrayList<>(field1);
            result.addAll(field2);
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(field1, DataType.CARTESIAN_POINT, "field1"),
                    new TestCaseSupplier.TypedData(field2, DataType.CARTESIAN_POINT, "field2")
                ),
                "MvAppendBytesRefEvaluator[field1=Attribute[channel=0], field2=Attribute[channel=1]]",
                DataType.CARTESIAN_POINT,
                equalTo(result)
            );
        }));

        suppliers.add(new TestCaseSupplier(List.of(DataType.GEO_SHAPE, DataType.GEO_SHAPE), () -> {
            var field1 = randomList(1, 3, () -> new BytesRef(GEO.asWkt(GeometryTestUtils.randomGeometry(randomBoolean(), 500))));
            var field2 = randomList(1, 3, () -> new BytesRef(GEO.asWkt(GeometryTestUtils.randomGeometry(randomBoolean(), 500))));
            var result = new ArrayList<>(field1);
            result.addAll(field2);
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(field1, DataType.GEO_SHAPE, "field1"),
                    new TestCaseSupplier.TypedData(field2, DataType.GEO_SHAPE, "field2")
                ),
                "MvAppendBytesRefEvaluator[field1=Attribute[channel=0], field2=Attribute[channel=1]]",
                DataType.GEO_SHAPE,
                equalTo(result)
            );
        }));

        suppliers.add(new TestCaseSupplier(List.of(DataType.CARTESIAN_SHAPE, DataType.CARTESIAN_SHAPE), () -> {
            var field1 = randomList(1, 3, () -> new BytesRef(CARTESIAN.asWkt(ShapeTestUtils.randomGeometry(randomBoolean(), 500))));
            var field2 = randomList(1, 3, () -> new BytesRef(CARTESIAN.asWkt(ShapeTestUtils.randomGeometry(randomBoolean(), 500))));
            var result = new ArrayList<>(field1);
            result.addAll(field2);
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(field1, DataType.CARTESIAN_SHAPE, "field1"),
                    new TestCaseSupplier.TypedData(field2, DataType.CARTESIAN_SHAPE, "field2")
                ),
                "MvAppendBytesRefEvaluator[field1=Attribute[channel=0], field2=Attribute[channel=1]]",
                DataType.CARTESIAN_SHAPE,
                equalTo(result)
            );
        }));
    }

    private static void nulls(List<TestCaseSupplier> suppliers) {
        suppliers.add(new TestCaseSupplier(List.of(DataType.INTEGER, DataType.INTEGER), () -> {
            List<Integer> field2 = randomList(2, 10, () -> randomInt());
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(null, DataType.INTEGER, "field1"),
                    new TestCaseSupplier.TypedData(field2, DataType.INTEGER, "field2")
                ),
                "MvAppendIntEvaluator[field1=Attribute[channel=0], field2=Attribute[channel=1]]",
                DataType.INTEGER,
                equalTo(null)
            );
        }));
        suppliers.add(new TestCaseSupplier(List.of(DataType.INTEGER, DataType.INTEGER), () -> {
            List<Integer> field1 = randomList(2, 10, () -> randomInt());
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(field1, DataType.INTEGER, "field1"),
                    new TestCaseSupplier.TypedData(null, DataType.INTEGER, "field2")
                ),
                "MvAppendIntEvaluator[field1=Attribute[channel=0], field2=Attribute[channel=1]]",
                DataType.INTEGER,
                equalTo(null)
            );
        }));
    }
}
