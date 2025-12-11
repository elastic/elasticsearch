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
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.hamcrest.Matcher;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.randomLiteral;
import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.CARTESIAN;
import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.GEO;
import static org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvSliceTests.randomGrid;
import static org.hamcrest.Matchers.equalTo;

public class MvIntersectTests extends AbstractScalarFunctionTestCase {

    public MvIntersectTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
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
        return parameterSuppliersFromTypedData(anyNullIsNull(true, suppliers));
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new MvIntersect(source, args.get(0), args.get(1));
    }

    private static <T> Matcher<?> matchResult(HashSet<T> result) {
        if (result == null || result.isEmpty()) {
            return equalTo(null);
        }

        if (result.size() > 1) {
            return equalTo(new ArrayList<>(result));
        }

        return equalTo(result.stream().findFirst().get());
    }

    private static void booleans(List<TestCaseSupplier> suppliers) {
        suppliers.add(new TestCaseSupplier(List.of(DataType.BOOLEAN, DataType.BOOLEAN), () -> {
            List<Boolean> field1 = randomList(1, 10, () -> randomBoolean());
            List<Boolean> field2 = randomList(1, 10, () -> randomBoolean());
            var result = new LinkedHashSet<>(field1);
            result.retainAll(field2);
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(field1, DataType.BOOLEAN, "field1"),
                    new TestCaseSupplier.TypedData(field2, DataType.BOOLEAN, "field2")
                ),
                "MvIntersectBooleanEvaluator[field1=Attribute[channel=0], field2=Attribute[channel=1]]",
                DataType.BOOLEAN,
                matchResult(result)
            );
        }));
    }

    private static void ints(List<TestCaseSupplier> suppliers) {
        suppliers.add(new TestCaseSupplier(List.of(DataType.INTEGER, DataType.INTEGER), () -> {
            List<Integer> field1 = randomList(1, 10, () -> randomIntBetween(1, 10));
            List<Integer> field2 = randomList(1, 10, () -> randomIntBetween(1, 10));
            var result = new LinkedHashSet<>(field1);
            result.retainAll(field2);
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(field1, DataType.INTEGER, "field1"),
                    new TestCaseSupplier.TypedData(field2, DataType.INTEGER, "field2")
                ),
                "MvIntersectIntEvaluator[field1=Attribute[channel=0], field2=Attribute[channel=1]]",
                DataType.INTEGER,
                matchResult(result)
            );
        }));
    }

    private static void longs(List<TestCaseSupplier> suppliers) {
        addLongTestCase(suppliers, DataType.LONG, ESTestCase::randomLong);
        addLongTestCase(suppliers, DataType.DATETIME, ESTestCase::randomLong);
        addLongTestCase(suppliers, DataType.DATE_NANOS, ESTestCase::randomNonNegativeLong);
        for (DataType gridType : new DataType[] { DataType.GEOHASH, DataType.GEOTILE, DataType.GEOHEX }) {
            addLongTestCase(suppliers, gridType, () -> randomGrid(gridType));
        }

        suppliers.add(new TestCaseSupplier(List.of(DataType.UNSIGNED_LONG, DataType.UNSIGNED_LONG), () -> {
            List<Long> field1 = randomList(1, 10, ESTestCase::randomLong);
            List<Long> field2 = randomList(1, 10, ESTestCase::randomLong);
            var result = new LinkedHashSet<>(field1);
            result.retainAll(field2);

            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(field1, DataType.UNSIGNED_LONG, "field1"),
                    new TestCaseSupplier.TypedData(field2, DataType.UNSIGNED_LONG, "field2")
                ),
                "MvIntersectLongEvaluator[field1=Attribute[channel=0], field2=Attribute[channel=1]]",
                DataType.UNSIGNED_LONG,
                matchResult(result)
            );
        }));
    }

    private static void addLongTestCase(List<TestCaseSupplier> suppliers, DataType dataType, Supplier<Long> longSupplier) {
        suppliers.add(new TestCaseSupplier(List.of(dataType, dataType), () -> {
            List<Long> field1 = randomList(1, 10, longSupplier);
            List<Long> field2 = randomList(1, 10, longSupplier);
            var result = new LinkedHashSet<>(field1);
            result.retainAll(field2);

            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(field1, dataType, "field1"),
                    new TestCaseSupplier.TypedData(field2, dataType, "field2")
                ),
                "MvIntersectLongEvaluator[field1=Attribute[channel=0], field2=Attribute[channel=1]]",
                dataType,
                matchResult(result)
            );
        }));
    }

    private static void doubles(List<TestCaseSupplier> suppliers) {
        suppliers.add(new TestCaseSupplier(List.of(DataType.DOUBLE, DataType.DOUBLE), () -> {
            List<Double> field1 = randomList(1, 10, () -> randomDouble());
            List<Double> field2 = randomList(1, 10, () -> randomDouble());
            var result = new LinkedHashSet<>(field1);
            result.retainAll(field2);

            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(field1, DataType.DOUBLE, "field1"),
                    new TestCaseSupplier.TypedData(field2, DataType.DOUBLE, "field2")
                ),
                "MvIntersectDoubleEvaluator[field1=Attribute[channel=0], field2=Attribute[channel=1]]",
                DataType.DOUBLE,
                matchResult(result)
            );
        }));
    }

    private static void bytesRefs(List<TestCaseSupplier> suppliers) {
        for (DataType lhs : new DataType[] { DataType.KEYWORD, DataType.TEXT }) {
            for (DataType rhs : new DataType[] { DataType.KEYWORD, DataType.TEXT }) {
                suppliers.add(new TestCaseSupplier(List.of(lhs, rhs), () -> {
                    List<Object> field1 = randomList(1, 10, () -> randomLiteral(lhs).value());
                    List<Object> field2 = randomList(1, 10, () -> randomLiteral(rhs).value());
                    var result = new LinkedHashSet<>(field1);
                    result.retainAll(field2);

                    return new TestCaseSupplier.TestCase(
                        List.of(
                            new TestCaseSupplier.TypedData(field1, lhs, "field1"),
                            new TestCaseSupplier.TypedData(field2, rhs, "field2")
                        ),
                        "MvIntersectBytesRefEvaluator[field1=Attribute[channel=0], field2=Attribute[channel=1]]",
                        DataType.KEYWORD,
                        matchResult(result)
                    );
                }));
            }
        }
        suppliers.add(new TestCaseSupplier(List.of(DataType.IP, DataType.IP), () -> {
            List<Object> field1 = randomList(1, 10, () -> randomLiteral(DataType.IP).value());
            List<Object> field2 = randomList(1, 10, () -> randomLiteral(DataType.IP).value());
            var result = new LinkedHashSet<>(field1);
            result.retainAll(field2);

            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(field1, DataType.IP, "field"),
                    new TestCaseSupplier.TypedData(field2, DataType.IP, "field")
                ),
                "MvIntersectBytesRefEvaluator[field1=Attribute[channel=0], field2=Attribute[channel=1]]",
                DataType.IP,
                matchResult(result)
            );
        }));

        suppliers.add(new TestCaseSupplier(List.of(DataType.VERSION, DataType.VERSION), () -> {
            List<Object> field1 = randomList(1, 10, () -> randomLiteral(DataType.VERSION).value());
            List<Object> field2 = randomList(1, 10, () -> randomLiteral(DataType.VERSION).value());
            var result = new LinkedHashSet<>(field1);
            result.retainAll(field2);

            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(field1, DataType.VERSION, "field"),
                    new TestCaseSupplier.TypedData(field2, DataType.VERSION, "field")
                ),
                "MvIntersectBytesRefEvaluator[field1=Attribute[channel=0], field2=Attribute[channel=1]]",
                DataType.VERSION,
                matchResult(result)
            );
        }));

        suppliers.add(new TestCaseSupplier(List.of(DataType.GEO_POINT, DataType.GEO_POINT), () -> {
            List<Object> field1 = randomList(1, 10, () -> new BytesRef(GEO.asWkt(GeometryTestUtils.randomPoint())));
            List<Object> field2 = randomList(1, 10, () -> new BytesRef(GEO.asWkt(GeometryTestUtils.randomPoint())));
            var result = new LinkedHashSet<>(field1);
            result.retainAll(field2);

            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(field1, DataType.GEO_POINT, "field1"),
                    new TestCaseSupplier.TypedData(field2, DataType.GEO_POINT, "field2")
                ),
                "MvIntersectBytesRefEvaluator[field1=Attribute[channel=0], field2=Attribute[channel=1]]",
                DataType.GEO_POINT,
                matchResult(result)
            );
        }));

        suppliers.add(new TestCaseSupplier(List.of(DataType.CARTESIAN_POINT, DataType.CARTESIAN_POINT), () -> {
            List<Object> field1 = randomList(1, 10, () -> new BytesRef(CARTESIAN.asWkt(ShapeTestUtils.randomPoint())));
            List<Object> field2 = randomList(1, 10, () -> new BytesRef(CARTESIAN.asWkt(ShapeTestUtils.randomPoint())));
            var result = new LinkedHashSet<>(field1);
            result.retainAll(field2);

            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(field1, DataType.CARTESIAN_POINT, "field1"),
                    new TestCaseSupplier.TypedData(field2, DataType.CARTESIAN_POINT, "field2")
                ),
                "MvIntersectBytesRefEvaluator[field1=Attribute[channel=0], field2=Attribute[channel=1]]",
                DataType.CARTESIAN_POINT,
                matchResult(result)
            );
        }));

        suppliers.add(new TestCaseSupplier(List.of(DataType.GEO_SHAPE, DataType.GEO_SHAPE), () -> {
            var field1 = randomList(1, 3, () -> new BytesRef(GEO.asWkt(GeometryTestUtils.randomGeometry(randomBoolean(), 500))));
            var field2 = randomList(1, 3, () -> new BytesRef(GEO.asWkt(GeometryTestUtils.randomGeometry(randomBoolean(), 500))));
            var result = new LinkedHashSet<>(field1);
            result.retainAll(field2);

            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(field1, DataType.GEO_SHAPE, "field1"),
                    new TestCaseSupplier.TypedData(field2, DataType.GEO_SHAPE, "field2")
                ),
                "MvIntersectBytesRefEvaluator[field1=Attribute[channel=0], field2=Attribute[channel=1]]",
                DataType.GEO_SHAPE,
                matchResult(result)
            );
        }));

        suppliers.add(new TestCaseSupplier(List.of(DataType.CARTESIAN_SHAPE, DataType.CARTESIAN_SHAPE), () -> {
            var field1 = randomList(1, 3, () -> new BytesRef(CARTESIAN.asWkt(ShapeTestUtils.randomGeometry(randomBoolean(), 500))));
            var field2 = randomList(1, 3, () -> new BytesRef(CARTESIAN.asWkt(ShapeTestUtils.randomGeometry(randomBoolean(), 500))));
            var result = new LinkedHashSet<>(field1);
            result.retainAll(field2);

            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(field1, DataType.CARTESIAN_SHAPE, "field1"),
                    new TestCaseSupplier.TypedData(field2, DataType.CARTESIAN_SHAPE, "field2")
                ),
                "MvIntersectBytesRefEvaluator[field1=Attribute[channel=0], field2=Attribute[channel=1]]",
                DataType.CARTESIAN_SHAPE,
                matchResult(result)
            );
        }));
    }
}
