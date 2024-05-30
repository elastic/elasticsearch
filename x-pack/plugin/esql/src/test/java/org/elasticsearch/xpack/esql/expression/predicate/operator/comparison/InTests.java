/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.predicate.operator.comparison;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.geo.GeometryTestUtils;
import org.elasticsearch.geo.ShapeTestUtils;
import org.elasticsearch.xpack.esql.core.TestUtils;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.DataTypes;
import org.elasticsearch.xpack.esql.expression.function.AbstractFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.esql.core.expression.Literal.NULL;
import static org.elasticsearch.xpack.esql.core.tree.Source.EMPTY;
import static org.elasticsearch.xpack.esql.core.type.DataTypes.CARTESIAN_POINT;
import static org.elasticsearch.xpack.esql.core.type.DataTypes.CARTESIAN_SHAPE;
import static org.elasticsearch.xpack.esql.core.type.DataTypes.GEO_POINT;
import static org.elasticsearch.xpack.esql.core.type.DataTypes.GEO_SHAPE;
import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.CARTESIAN;
import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.GEO;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.matchesPattern;

public class InTests extends AbstractFunctionTestCase {
    public InTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    private static final Literal ONE = L(1);
    private static final Literal TWO = L(2);
    private static final Literal THREE = L(3);

    public void testInWithContainedValue() {
        In in = new In(EMPTY, TWO, Arrays.asList(ONE, TWO, THREE));
        assertTrue(in.fold());
    }

    public void testInWithNotContainedValue() {
        In in = new In(EMPTY, THREE, Arrays.asList(ONE, TWO));
        assertFalse(in.fold());
    }

    public void testHandleNullOnLeftValue() {
        In in = new In(EMPTY, NULL, Arrays.asList(ONE, TWO, THREE));
        assertNull(in.fold());
        in = new In(EMPTY, NULL, Arrays.asList(ONE, NULL, THREE));
        assertNull(in.fold());

    }

    public void testHandleNullsOnRightValue() {
        In in = new In(EMPTY, THREE, Arrays.asList(ONE, NULL, THREE));
        assertTrue(in.fold());
        in = new In(EMPTY, ONE, Arrays.asList(TWO, NULL, THREE));
        assertNull(in.fold());
    }

    private static Literal L(Object value) {
        return TestUtils.of(EMPTY, value);
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();
        for (int i : new int[] { 1, 3 }) {
            booleans(suppliers, i);
            numerics(suppliers, i);
            bytesRefs(suppliers, i);
        }
        return parameterSuppliersFromTypedData(suppliers);
    }

    private static void booleans(List<TestCaseSupplier> suppliers, int items) {
        suppliers.add(new TestCaseSupplier("boolean", List.of(DataTypes.BOOLEAN, DataTypes.BOOLEAN), () -> {
            List<Boolean> inlist = randomList(items, items, () -> randomBoolean());
            boolean field = randomBoolean();
            List<TestCaseSupplier.TypedData> args = new ArrayList<>(inlist.size() + 1);
            for (Boolean i : inlist) {
                args.add(new TestCaseSupplier.TypedData(i, DataTypes.BOOLEAN, "inlist" + i));
            }
            args.add(new TestCaseSupplier.TypedData(field, DataTypes.BOOLEAN, "field"));
            return new TestCaseSupplier.TestCase(
                args,
                matchesPattern("InExpressionEvaluator\\[EqualsBoolsEvaluator.*\\]"),
                DataTypes.BOOLEAN,
                equalTo(inlist.contains(field))
            );
        }));
    }

    private static void numerics(List<TestCaseSupplier> suppliers, int items) {
        suppliers.add(new TestCaseSupplier("integer", List.of(DataTypes.INTEGER, DataTypes.INTEGER), () -> {
            List<Integer> inlist = randomList(items, items, () -> randomInt());
            int field = inlist.get(inlist.size() - 1);
            List<TestCaseSupplier.TypedData> args = new ArrayList<>(inlist.size() + 1);
            for (Integer i : inlist) {
                args.add(new TestCaseSupplier.TypedData(i, DataTypes.INTEGER, "inlist" + i));
            }
            args.add(new TestCaseSupplier.TypedData(field, DataTypes.INTEGER, "field"));
            return new TestCaseSupplier.TestCase(
                args,
                matchesPattern("InExpressionEvaluator\\[EqualsIntsEvaluator.*\\]"),
                DataTypes.BOOLEAN,
                equalTo(inlist.contains(field))
            );
        }));

        suppliers.add(new TestCaseSupplier("long", List.of(DataTypes.LONG, DataTypes.LONG), () -> {
            List<Long> inlist = randomList(items, items, () -> randomLong());
            long field = randomLong();
            List<TestCaseSupplier.TypedData> args = new ArrayList<>(inlist.size() + 1);
            for (Long i : inlist) {
                args.add(new TestCaseSupplier.TypedData(i, DataTypes.LONG, "inlist" + i));
            }
            args.add(new TestCaseSupplier.TypedData(field, DataTypes.LONG, "field"));
            return new TestCaseSupplier.TestCase(
                args,
                matchesPattern("InExpressionEvaluator\\[EqualsLongsEvaluator.*\\]"),
                DataTypes.BOOLEAN,
                equalTo(inlist.contains(field))
            );
        }));

        suppliers.add(new TestCaseSupplier("double", List.of(DataTypes.DOUBLE, DataTypes.DOUBLE), () -> {
            List<Double> inlist = randomList(items, items, () -> randomDouble());
            double field = inlist.get(0);
            List<TestCaseSupplier.TypedData> args = new ArrayList<>(inlist.size() + 1);
            for (Double i : inlist) {
                args.add(new TestCaseSupplier.TypedData(i, DataTypes.DOUBLE, "inlist" + i));
            }
            args.add(new TestCaseSupplier.TypedData(field, DataTypes.DOUBLE, "field"));
            return new TestCaseSupplier.TestCase(
                args,
                matchesPattern("InExpressionEvaluator\\[EqualsDoublesEvaluator.*\\]"),
                DataTypes.BOOLEAN,
                equalTo(inlist.contains(field))
            );
        }));
    }

    private static void bytesRefs(List<TestCaseSupplier> suppliers, int items) {
        suppliers.add(new TestCaseSupplier("keyword", List.of(DataTypes.KEYWORD, DataTypes.KEYWORD), () -> {
            List<Object> inlist = randomList(items, items, () -> randomLiteral(DataTypes.KEYWORD).value());
            Object field = inlist.get(inlist.size() - 1);
            List<TestCaseSupplier.TypedData> args = new ArrayList<>(inlist.size() + 1);
            for (Object i : inlist) {
                args.add(new TestCaseSupplier.TypedData(i, DataTypes.KEYWORD, "inlist" + i));
            }
            args.add(new TestCaseSupplier.TypedData(field, DataTypes.KEYWORD, "field"));
            return new TestCaseSupplier.TestCase(
                args,
                matchesPattern("InExpressionEvaluator\\[EqualsKeywordsEvaluator.*\\]"),
                DataTypes.BOOLEAN,
                equalTo(inlist.contains(field))
            );
        }));

        suppliers.add(new TestCaseSupplier("text", List.of(DataTypes.TEXT, DataTypes.TEXT), () -> {
            List<Object> inlist = randomList(items, items, () -> randomLiteral(DataTypes.TEXT).value());
            Object field = inlist.get(0);
            List<TestCaseSupplier.TypedData> args = new ArrayList<>(inlist.size() + 1);
            for (Object i : inlist) {
                args.add(new TestCaseSupplier.TypedData(i, DataTypes.TEXT, "inlist" + i));
            }
            args.add(new TestCaseSupplier.TypedData(field, DataTypes.TEXT, "field"));
            return new TestCaseSupplier.TestCase(
                args,
                matchesPattern("InExpressionEvaluator\\[EqualsKeywordsEvaluator.*\\]"),
                DataTypes.BOOLEAN,
                equalTo(inlist.contains(field))
            );
        }));

        for (DataType type1 : new DataType[] { DataTypes.KEYWORD, DataTypes.TEXT }) {
            for (DataType type2 : new DataType[] { DataTypes.KEYWORD, DataTypes.TEXT }) {
                if (type1 == type2 || items > 1) continue;
                suppliers.add(new TestCaseSupplier(type1 + " " + type2, List.of(type1, type2), () -> {
                    List<Object> inlist = randomList(items, items, () -> randomLiteral(type1).value());
                    Object field = randomLiteral(type2).value();
                    List<TestCaseSupplier.TypedData> args = new ArrayList<>(inlist.size() + 1);
                    for (Object i : inlist) {
                        args.add(new TestCaseSupplier.TypedData(i, type1, "inlist" + i));
                    }
                    args.add(new TestCaseSupplier.TypedData(field, type2, "field"));
                    return new TestCaseSupplier.TestCase(
                        args,
                        matchesPattern("InExpressionEvaluator\\[EqualsKeywordsEvaluator.*\\]"),
                        DataTypes.BOOLEAN,
                        equalTo(inlist.contains(field))
                    );
                }));
            }
        }
        suppliers.add(new TestCaseSupplier("ip", List.of(DataTypes.IP, DataTypes.IP), () -> {
            List<Object> inlist = randomList(items, items, () -> randomLiteral(DataTypes.IP).value());
            Object field = randomLiteral(DataTypes.IP).value();
            List<TestCaseSupplier.TypedData> args = new ArrayList<>(inlist.size() + 1);
            for (Object i : inlist) {
                args.add(new TestCaseSupplier.TypedData(i, DataTypes.IP, "inlist" + i));
            }
            args.add(new TestCaseSupplier.TypedData(field, DataTypes.IP, "field"));
            return new TestCaseSupplier.TestCase(
                args,
                matchesPattern("InExpressionEvaluator\\[EqualsKeywordsEvaluator.*\\]"),
                DataTypes.BOOLEAN,
                equalTo(inlist.contains(field))
            );
        }));

        suppliers.add(new TestCaseSupplier("version", List.of(DataTypes.VERSION, DataTypes.VERSION), () -> {
            List<Object> inlist = randomList(items, items, () -> randomLiteral(DataTypes.VERSION).value());
            Object field = randomLiteral(DataTypes.VERSION).value();
            List<TestCaseSupplier.TypedData> args = new ArrayList<>(inlist.size() + 1);
            for (Object i : inlist) {
                args.add(new TestCaseSupplier.TypedData(i, DataTypes.VERSION, "inlist" + i));
            }
            args.add(new TestCaseSupplier.TypedData(field, DataTypes.VERSION, "field"));
            return new TestCaseSupplier.TestCase(
                args,
                matchesPattern("InExpressionEvaluator\\[EqualsKeywordsEvaluator.*\\]"),
                DataTypes.BOOLEAN,
                equalTo(inlist.contains(field))
            );
        }));

        suppliers.add(new TestCaseSupplier("geo_point", List.of(GEO_POINT, GEO_POINT), () -> {
            List<Object> inlist = randomList(items, items, () -> new BytesRef(GEO.asWkt(GeometryTestUtils.randomPoint())));
            Object field = inlist.get(0);
            List<TestCaseSupplier.TypedData> args = new ArrayList<>(inlist.size() + 1);
            for (Object i : inlist) {
                args.add(new TestCaseSupplier.TypedData(i, GEO_POINT, "inlist" + i));
            }
            args.add(new TestCaseSupplier.TypedData(field, GEO_POINT, "field"));
            return new TestCaseSupplier.TestCase(
                args,
                matchesPattern("InExpressionEvaluator\\[EqualsGeometriesEvaluator.*\\]"),
                DataTypes.BOOLEAN,
                equalTo(inlist.contains(field))
            );
        }));

        suppliers.add(new TestCaseSupplier("geo_shape", List.of(GEO_SHAPE, GEO_SHAPE), () -> {
            List<Object> inlist = randomList(
                items,
                items,
                () -> new BytesRef(GEO.asWkt(GeometryTestUtils.randomGeometry(randomBoolean())))
            );
            Object field = inlist.get(inlist.size() - 1);
            List<TestCaseSupplier.TypedData> args = new ArrayList<>(inlist.size() + 1);
            for (Object i : inlist) {
                args.add(new TestCaseSupplier.TypedData(i, GEO_SHAPE, "inlist" + i));
            }
            args.add(new TestCaseSupplier.TypedData(field, GEO_SHAPE, "field"));
            return new TestCaseSupplier.TestCase(
                args,
                matchesPattern("InExpressionEvaluator\\[EqualsGeometriesEvaluator.*\\]"),
                DataTypes.BOOLEAN,
                equalTo(inlist.contains(field))
            );
        }));

        suppliers.add(new TestCaseSupplier("cartesian_point", List.of(CARTESIAN_POINT, CARTESIAN_POINT), () -> {
            List<Object> inlist = randomList(items, items, () -> new BytesRef(CARTESIAN.asWkt(ShapeTestUtils.randomPoint())));
            Object field = new BytesRef(CARTESIAN.asWkt(ShapeTestUtils.randomPoint()));
            List<TestCaseSupplier.TypedData> args = new ArrayList<>(inlist.size() + 1);
            for (Object i : inlist) {
                args.add(new TestCaseSupplier.TypedData(i, CARTESIAN_POINT, "inlist" + i));
            }
            args.add(new TestCaseSupplier.TypedData(field, CARTESIAN_POINT, "field"));
            return new TestCaseSupplier.TestCase(
                args,
                matchesPattern("InExpressionEvaluator\\[EqualsGeometriesEvaluator.*\\]"),
                DataTypes.BOOLEAN,
                equalTo(inlist.contains(field))
            );
        }));

        suppliers.add(new TestCaseSupplier("cartesian_shape", List.of(CARTESIAN_SHAPE, CARTESIAN_SHAPE), () -> {
            List<Object> inlist = randomList(
                items,
                items,
                () -> new BytesRef(CARTESIAN.asWkt(ShapeTestUtils.randomGeometry(randomBoolean())))
            );
            Object field = new BytesRef(CARTESIAN.asWkt(ShapeTestUtils.randomGeometry(randomBoolean())));
            List<TestCaseSupplier.TypedData> args = new ArrayList<>(inlist.size() + 1);
            for (Object i : inlist) {
                args.add(new TestCaseSupplier.TypedData(i, CARTESIAN_SHAPE, "inlist" + i));
            }
            args.add(new TestCaseSupplier.TypedData(field, CARTESIAN_SHAPE, "field"));
            return new TestCaseSupplier.TestCase(
                args,
                matchesPattern("InExpressionEvaluator\\[EqualsGeometriesEvaluator.*\\]"),
                DataTypes.BOOLEAN,
                equalTo(inlist.contains(field))
            );
        }));
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new In(source, args.get(args.size() - 1), args.subList(0, args.size() - 1));
    }

    @Override
    public void testSimpleWithNulls() {
        assumeFalse("test case is invalid", false);
    }

    @Override
    public void testFactoryToString() {
        assumeFalse("test case is invalid", false);
    }
}
