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
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.of;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.randomLiteral;
import static org.elasticsearch.xpack.esql.core.expression.Literal.NULL;
import static org.elasticsearch.xpack.esql.core.tree.Source.EMPTY;
import static org.elasticsearch.xpack.esql.core.type.DataType.CARTESIAN_POINT;
import static org.elasticsearch.xpack.esql.core.type.DataType.CARTESIAN_SHAPE;
import static org.elasticsearch.xpack.esql.core.type.DataType.GEO_POINT;
import static org.elasticsearch.xpack.esql.core.type.DataType.GEO_SHAPE;
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
        assertTrue((Boolean) in.fold());
    }

    public void testInWithNotContainedValue() {
        In in = new In(EMPTY, THREE, Arrays.asList(ONE, TWO));
        assertFalse((Boolean) in.fold());
    }

    public void testHandleNullOnLeftValue() {
        In in = new In(EMPTY, NULL, Arrays.asList(ONE, TWO, THREE));
        assertNull(in.fold());
        in = new In(EMPTY, NULL, Arrays.asList(ONE, NULL, THREE));
        assertNull(in.fold());

    }

    public void testHandleNullsOnRightValue() {
        In in = new In(EMPTY, THREE, Arrays.asList(ONE, NULL, THREE));
        assertTrue((Boolean) in.fold());
        in = new In(EMPTY, ONE, Arrays.asList(TWO, NULL, THREE));
        assertNull(in.fold());
    }

    private static Literal L(Object value) {
        return of(EMPTY, value);
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
        suppliers.add(new TestCaseSupplier("boolean", List.of(DataType.BOOLEAN, DataType.BOOLEAN), () -> {
            List<Boolean> inlist = randomList(items, items, () -> randomBoolean());
            boolean field = randomBoolean();
            List<TestCaseSupplier.TypedData> args = new ArrayList<>(inlist.size() + 1);
            for (Boolean i : inlist) {
                args.add(new TestCaseSupplier.TypedData(i, DataType.BOOLEAN, "inlist" + i));
            }
            args.add(new TestCaseSupplier.TypedData(field, DataType.BOOLEAN, "field"));
            return new TestCaseSupplier.TestCase(
                args,
                matchesPattern("InBooleanEvaluator.*"),
                DataType.BOOLEAN,
                equalTo(inlist.contains(field))
            );
        }));
    }

    private static void numerics(List<TestCaseSupplier> suppliers, int items) {
        suppliers.add(new TestCaseSupplier("integer", List.of(DataType.INTEGER, DataType.INTEGER), () -> {
            List<Integer> inlist = randomList(items, items, () -> randomInt());
            int field = inlist.get(inlist.size() - 1);
            List<TestCaseSupplier.TypedData> args = new ArrayList<>(inlist.size() + 1);
            for (Integer i : inlist) {
                args.add(new TestCaseSupplier.TypedData(i, DataType.INTEGER, "inlist" + i));
            }
            args.add(new TestCaseSupplier.TypedData(field, DataType.INTEGER, "field"));
            return new TestCaseSupplier.TestCase(
                args,
                matchesPattern("InIntEvaluator.*"),
                DataType.BOOLEAN,
                equalTo(inlist.contains(field))
            );
        }));

        suppliers.add(new TestCaseSupplier("long", List.of(DataType.LONG, DataType.LONG), () -> {
            List<Long> inlist = randomList(items, items, () -> randomLong());
            long field = randomLong();
            List<TestCaseSupplier.TypedData> args = new ArrayList<>(inlist.size() + 1);
            for (Long i : inlist) {
                args.add(new TestCaseSupplier.TypedData(i, DataType.LONG, "inlist" + i));
            }
            args.add(new TestCaseSupplier.TypedData(field, DataType.LONG, "field"));
            return new TestCaseSupplier.TestCase(
                args,
                matchesPattern("InLongEvaluator.*"),
                DataType.BOOLEAN,
                equalTo(inlist.contains(field))
            );
        }));

        suppliers.add(new TestCaseSupplier("double", List.of(DataType.DOUBLE, DataType.DOUBLE), () -> {
            List<Double> inlist = randomList(items, items, () -> randomDouble());
            double field = inlist.get(0);
            List<TestCaseSupplier.TypedData> args = new ArrayList<>(inlist.size() + 1);
            for (Double i : inlist) {
                args.add(new TestCaseSupplier.TypedData(i, DataType.DOUBLE, "inlist" + i));
            }
            args.add(new TestCaseSupplier.TypedData(field, DataType.DOUBLE, "field"));
            return new TestCaseSupplier.TestCase(
                args,
                matchesPattern("InDoubleEvaluator.*"),
                DataType.BOOLEAN,
                equalTo(inlist.contains(field))
            );
        }));
    }

    private static void bytesRefs(List<TestCaseSupplier> suppliers, int items) {
        suppliers.add(new TestCaseSupplier("keyword", List.of(DataType.KEYWORD, DataType.KEYWORD), () -> {
            List<Object> inlist = randomList(items, items, () -> randomLiteral(DataType.KEYWORD).value());
            Object field = inlist.get(inlist.size() - 1);
            List<TestCaseSupplier.TypedData> args = new ArrayList<>(inlist.size() + 1);
            for (Object i : inlist) {
                args.add(new TestCaseSupplier.TypedData(i, DataType.KEYWORD, "inlist" + i));
            }
            args.add(new TestCaseSupplier.TypedData(field, DataType.KEYWORD, "field"));
            return new TestCaseSupplier.TestCase(
                args,
                matchesPattern("InBytesRefEvaluator.*"),
                DataType.BOOLEAN,
                equalTo(inlist.contains(field))
            );
        }));

        suppliers.add(new TestCaseSupplier("text", List.of(DataType.TEXT, DataType.TEXT), () -> {
            List<Object> inlist = randomList(items, items, () -> randomLiteral(DataType.TEXT).value());
            Object field = inlist.get(0);
            List<TestCaseSupplier.TypedData> args = new ArrayList<>(inlist.size() + 1);
            for (Object i : inlist) {
                args.add(new TestCaseSupplier.TypedData(i, DataType.TEXT, "inlist" + i));
            }
            args.add(new TestCaseSupplier.TypedData(field, DataType.TEXT, "field"));
            return new TestCaseSupplier.TestCase(
                args,
                matchesPattern("InBytesRefEvaluator.*"),
                DataType.BOOLEAN,
                equalTo(inlist.contains(field))
            );
        }));

        for (DataType type1 : new DataType[] { DataType.KEYWORD, DataType.TEXT }) {
            for (DataType type2 : new DataType[] { DataType.KEYWORD, DataType.TEXT }) {
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
                        matchesPattern("InBytesRefEvaluator.*"),
                        DataType.BOOLEAN,
                        equalTo(inlist.contains(field))
                    );
                }));
            }
        }
        suppliers.add(new TestCaseSupplier("ip", List.of(DataType.IP, DataType.IP), () -> {
            List<Object> inlist = randomList(items, items, () -> randomLiteral(DataType.IP).value());
            Object field = randomLiteral(DataType.IP).value();
            List<TestCaseSupplier.TypedData> args = new ArrayList<>(inlist.size() + 1);
            for (Object i : inlist) {
                args.add(new TestCaseSupplier.TypedData(i, DataType.IP, "inlist" + i));
            }
            args.add(new TestCaseSupplier.TypedData(field, DataType.IP, "field"));
            return new TestCaseSupplier.TestCase(
                args,
                matchesPattern("InBytesRefEvaluator.*"),
                DataType.BOOLEAN,
                equalTo(inlist.contains(field))
            );
        }));

        suppliers.add(new TestCaseSupplier("version", List.of(DataType.VERSION, DataType.VERSION), () -> {
            List<Object> inlist = randomList(items, items, () -> randomLiteral(DataType.VERSION).value());
            Object field = randomLiteral(DataType.VERSION).value();
            List<TestCaseSupplier.TypedData> args = new ArrayList<>(inlist.size() + 1);
            for (Object i : inlist) {
                args.add(new TestCaseSupplier.TypedData(i, DataType.VERSION, "inlist" + i));
            }
            args.add(new TestCaseSupplier.TypedData(field, DataType.VERSION, "field"));
            return new TestCaseSupplier.TestCase(
                args,
                matchesPattern("InBytesRefEvaluator.*"),
                DataType.BOOLEAN,
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
                matchesPattern("InBytesRefEvaluator.*"),
                DataType.BOOLEAN,
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
                matchesPattern("InBytesRefEvaluator.*"),
                DataType.BOOLEAN,
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
                matchesPattern("InBytesRefEvaluator.*"),
                DataType.BOOLEAN,
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
                matchesPattern("InBytesRefEvaluator.*"),
                DataType.BOOLEAN,
                equalTo(inlist.contains(field))
            );
        }));
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new In(source, args.get(args.size() - 1), args.subList(0, args.size() - 1));
    }
}
