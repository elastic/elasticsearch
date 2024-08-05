/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.multivalue;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.geo.GeometryTestUtils;
import org.elasticsearch.geo.ShapeTestUtils;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.NumericUtils;
import org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.hamcrest.Matcher;

import java.math.BigInteger;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.CARTESIAN;
import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.GEO;

public abstract class AbstractMultivalueFunctionTestCase extends AbstractScalarFunctionTestCase {
    /**
     * Build many test cases with {@code boolean} values.
     */
    protected static void booleans(
        List<TestCaseSupplier> cases,
        String name,
        String evaluatorName,
        BiFunction<Integer, Stream<Boolean>, Matcher<Object>> matcher
    ) {
        booleans(cases, name, evaluatorName, DataType.BOOLEAN, matcher);
    }

    /**
     * Build many test cases with {@code boolean} values.
     */
    protected static void booleans(
        List<TestCaseSupplier> cases,
        String name,
        String evaluatorName,
        DataType expectedDataType,
        BiFunction<Integer, Stream<Boolean>, Matcher<Object>> matcher
    ) {
        cases.add(
            new TestCaseSupplier(
                name + "(false)",
                List.of(DataType.BOOLEAN),
                () -> new TestCaseSupplier.TestCase(
                    List.of(new TestCaseSupplier.TypedData(List.of(false), DataType.BOOLEAN, "field")),
                    evaluatorName + "[field=Attribute[channel=0]]",
                    expectedDataType,
                    matcher.apply(1, Stream.of(false))
                )
            )
        );
        cases.add(
            new TestCaseSupplier(
                name + "(true)",
                List.of(DataType.BOOLEAN),
                () -> new TestCaseSupplier.TestCase(
                    List.of(new TestCaseSupplier.TypedData(List.of(true), DataType.BOOLEAN, "field")),
                    evaluatorName + "[field=Attribute[channel=0]]",
                    expectedDataType,
                    matcher.apply(1, Stream.of(true))
                )
            )
        );
        for (Block.MvOrdering ordering : Block.MvOrdering.values()) {
            cases.add(new TestCaseSupplier(name + "(<booleans>) " + ordering, List.of(DataType.BOOLEAN), () -> {
                List<Boolean> mvData = randomList(2, 100, ESTestCase::randomBoolean);
                putInOrder(mvData, ordering);
                return new TestCaseSupplier.TestCase(
                    List.of(new TestCaseSupplier.TypedData(mvData, DataType.BOOLEAN, "field")),
                    evaluatorName + "[field=Attribute[channel=0]]",
                    expectedDataType,
                    matcher.apply(mvData.size(), mvData.stream())
                );
            }));
        }
    }

    /**
     * Build many test cases with {@link BytesRef} values.
     */
    protected static void bytesRefs(
        List<TestCaseSupplier> cases,
        String name,
        String evaluatorName,
        BiFunction<Integer, Stream<BytesRef>, Matcher<Object>> matcher
    ) {
        bytesRefs(cases, name, evaluatorName, t -> t, matcher);
    }

    /**
     * Build many test cases with {@link BytesRef} values.
     */
    protected static void bytesRefs(
        List<TestCaseSupplier> cases,
        String name,
        String evaluatorName,
        Function<DataType, DataType> expectedDataType,
        BiFunction<Integer, Stream<BytesRef>, Matcher<Object>> matcher
    ) {
        for (DataType type : new DataType[] { DataType.KEYWORD, DataType.TEXT, DataType.IP, DataType.VERSION }) {
            if (type != DataType.IP) {
                cases.add(
                    new TestCaseSupplier(
                        name + "(empty " + type.typeName() + ")",
                        List.of(type),
                        () -> new TestCaseSupplier.TestCase(
                            List.of(new TestCaseSupplier.TypedData(List.of(new BytesRef("")), type, "field")),
                            evaluatorName + "[field=Attribute[channel=0]]",
                            expectedDataType.apply(type),
                            matcher.apply(1, Stream.of(new BytesRef("")))
                        )
                    )
                );
            }
            cases.add(new TestCaseSupplier(name + "(" + type.typeName() + ")", List.of(type), () -> {
                BytesRef data = (BytesRef) randomLiteral(type).value();
                return new TestCaseSupplier.TestCase(
                    List.of(new TestCaseSupplier.TypedData(List.of(data), type, "field")),
                    evaluatorName + "[field=Attribute[channel=0]]",
                    expectedDataType.apply(type),
                    matcher.apply(1, Stream.of(data))
                );
            }));
            for (Block.MvOrdering ordering : Block.MvOrdering.values()) {
                cases.add(new TestCaseSupplier(name + "(<" + type.typeName() + "s>) " + ordering, List.of(type), () -> {
                    List<BytesRef> mvData = randomList(1, 100, () -> (BytesRef) randomLiteral(type).value());
                    putInOrder(mvData, ordering);
                    return new TestCaseSupplier.TestCase(
                        List.of(new TestCaseSupplier.TypedData(mvData, type, "field")),
                        evaluatorName + "[field=Attribute[channel=0]]",
                        expectedDataType.apply(type),
                        matcher.apply(mvData.size(), mvData.stream())
                    );
                }));
            }
        }
    }

    /**
     * Build many test cases with {@code double} values.
     */
    protected static void doubles(
        List<TestCaseSupplier> cases,
        String name,
        String evaluatorName,
        BiFunction<Integer, DoubleStream, Matcher<Object>> matcher
    ) {
        doubles(cases, name, evaluatorName, DataType.DOUBLE, matcher);
    }

    /**
     * Build many test cases with {@code double} values.
     */
    protected static void doubles(
        List<TestCaseSupplier> cases,
        String name,
        String evaluatorName,
        DataType expectedDataType,
        BiFunction<Integer, DoubleStream, Matcher<Object>> matcher
    ) {
        cases.add(
            new TestCaseSupplier(
                name + "(0.0)",
                List.of(DataType.DOUBLE),
                () -> new TestCaseSupplier.TestCase(
                    List.of(new TestCaseSupplier.TypedData(List.of(0.0), DataType.DOUBLE, "field")),
                    evaluatorName + "[field=Attribute[channel=0]]",
                    expectedDataType,
                    matcher.apply(1, DoubleStream.of(0.0))
                )
            )
        );
        cases.add(new TestCaseSupplier(name + "(double)", List.of(DataType.DOUBLE), () -> {
            double mvData = randomDouble();
            return new TestCaseSupplier.TestCase(
                List.of(new TestCaseSupplier.TypedData(List.of(mvData), DataType.DOUBLE, "field")),
                evaluatorName + "[field=Attribute[channel=0]]",
                expectedDataType,
                matcher.apply(1, DoubleStream.of(mvData))
            );
        }));
        for (Block.MvOrdering ordering : Block.MvOrdering.values()) {
            cases.add(new TestCaseSupplier(name + "(<double>) " + ordering, List.of(DataType.DOUBLE), () -> {
                List<Double> mvData = randomList(1, 100, ESTestCase::randomDouble);
                putInOrder(mvData, ordering);
                return new TestCaseSupplier.TestCase(
                    List.of(new TestCaseSupplier.TypedData(mvData, DataType.DOUBLE, "field")),
                    evaluatorName + "[field=Attribute[channel=0]]",
                    expectedDataType,
                    matcher.apply(mvData.size(), mvData.stream().mapToDouble(Double::doubleValue))
                );
            }));
        }
    }

    /**
     * Build many test cases with {@code int} values.
     */
    protected static void ints(
        List<TestCaseSupplier> cases,
        String name,
        String evaluatorName,
        BiFunction<Integer, IntStream, Matcher<Object>> matcher
    ) {
        ints(cases, name, evaluatorName, DataType.INTEGER, matcher);
    }

    /**
     * Build many test cases with {@code int} values.
     */
    protected static void ints(
        List<TestCaseSupplier> cases,
        String name,
        String evaluatorName,
        DataType expectedDataType,
        BiFunction<Integer, IntStream, Matcher<Object>> matcher
    ) {
        cases.add(
            new TestCaseSupplier(
                name + "(0)",
                List.of(DataType.INTEGER),
                () -> new TestCaseSupplier.TestCase(
                    List.of(new TestCaseSupplier.TypedData(List.of(0), DataType.INTEGER, "field")),
                    evaluatorName + "[field=Attribute[channel=0]]",
                    expectedDataType,
                    matcher.apply(1, IntStream.of(0))
                )
            )
        );
        cases.add(new TestCaseSupplier(name + "(int)", List.of(DataType.INTEGER), () -> {
            int data = randomInt();
            return new TestCaseSupplier.TestCase(
                List.of(new TestCaseSupplier.TypedData(List.of(data), DataType.INTEGER, "field")),
                evaluatorName + "[field=Attribute[channel=0]]",
                expectedDataType,
                matcher.apply(1, IntStream.of(data))
            );
        }));
        for (Block.MvOrdering ordering : Block.MvOrdering.values()) {
            cases.add(new TestCaseSupplier(name + "(<ints>) " + ordering, List.of(DataType.INTEGER), () -> {
                List<Integer> mvData = randomList(1, 100, ESTestCase::randomInt);
                putInOrder(mvData, ordering);
                return new TestCaseSupplier.TestCase(
                    List.of(new TestCaseSupplier.TypedData(mvData, DataType.INTEGER, "field")),
                    evaluatorName + "[field=Attribute[channel=0]]",
                    expectedDataType,
                    matcher.apply(mvData.size(), mvData.stream().mapToInt(Integer::intValue))
                );
            }));
        }
    }

    /**
     * Build many test cases with {@code long} values.
     */
    protected static void longs(
        List<TestCaseSupplier> cases,
        String name,
        String evaluatorName,
        BiFunction<Integer, LongStream, Matcher<Object>> matcher
    ) {
        longs(cases, name, evaluatorName, DataType.LONG, matcher);
    }

    /**
     * Build many test cases with {@code long} values.
     */
    protected static void longs(
        List<TestCaseSupplier> cases,
        String name,
        String evaluatorName,
        DataType expectedDataType,
        BiFunction<Integer, LongStream, Matcher<Object>> matcher
    ) {
        cases.add(
            new TestCaseSupplier(
                name + "(0L)",
                List.of(DataType.LONG),
                () -> new TestCaseSupplier.TestCase(
                    List.of(new TestCaseSupplier.TypedData(List.of(0L), DataType.LONG, "field")),
                    evaluatorName + "[field=Attribute[channel=0]]",
                    expectedDataType,
                    matcher.apply(1, LongStream.of(0L))
                )
            )
        );
        cases.add(new TestCaseSupplier(name + "(long)", List.of(DataType.LONG), () -> {
            long data = randomLong();
            return new TestCaseSupplier.TestCase(
                List.of(new TestCaseSupplier.TypedData(List.of(data), DataType.LONG, "field")),
                evaluatorName + "[field=Attribute[channel=0]]",
                expectedDataType,
                matcher.apply(1, LongStream.of(data))
            );
        }));
        for (Block.MvOrdering ordering : Block.MvOrdering.values()) {
            cases.add(new TestCaseSupplier(name + "(<longs>) " + ordering, List.of(DataType.LONG), () -> {
                List<Long> mvData = randomList(1, 100, ESTestCase::randomLong);
                putInOrder(mvData, ordering);
                return new TestCaseSupplier.TestCase(
                    List.of(new TestCaseSupplier.TypedData(mvData, DataType.LONG, "field")),
                    evaluatorName + "[field=Attribute[channel=0]]",
                    expectedDataType,
                    matcher.apply(mvData.size(), mvData.stream().mapToLong(Long::longValue))
                );
            }));
        }
    }

    /**
     * Build many test cases with {@code date} values.
     */
    protected static void dateTimes(
        List<TestCaseSupplier> cases,
        String name,
        String evaluatorName,
        BiFunction<Integer, LongStream, Matcher<Object>> matcher
    ) {
        dateTimes(cases, name, evaluatorName, DataType.DATETIME, matcher);
    }

    /**
     * Build many test cases with {@code date} values.
     */
    protected static void dateTimes(
        List<TestCaseSupplier> cases,
        String name,
        String evaluatorName,
        DataType expectedDataType,
        BiFunction<Integer, LongStream, Matcher<Object>> matcher
    ) {
        cases.add(
            new TestCaseSupplier(
                name + "(epoch)",
                List.of(DataType.DATETIME),
                () -> new TestCaseSupplier.TestCase(
                    List.of(new TestCaseSupplier.TypedData(List.of(0L), DataType.DATETIME, "field")),
                    evaluatorName + "[field=Attribute[channel=0]]",
                    expectedDataType,
                    matcher.apply(1, LongStream.of(0L))
                )
            )
        );
        cases.add(new TestCaseSupplier(name + "(date)", List.of(DataType.DATETIME), () -> {
            long data = randomLong();
            return new TestCaseSupplier.TestCase(
                List.of(new TestCaseSupplier.TypedData(List.of(data), DataType.DATETIME, "field")),
                evaluatorName + "[field=Attribute[channel=0]]",
                expectedDataType,
                matcher.apply(1, LongStream.of(data))
            );
        }));
        for (Block.MvOrdering ordering : Block.MvOrdering.values()) {
            cases.add(new TestCaseSupplier(name + "(<dates>) " + ordering, List.of(DataType.DATETIME), () -> {
                List<Long> mvData = randomList(1, 100, ESTestCase::randomLong);
                putInOrder(mvData, ordering);
                return new TestCaseSupplier.TestCase(
                    List.of(new TestCaseSupplier.TypedData(mvData, DataType.DATETIME, "field")),
                    evaluatorName + "[field=Attribute[channel=0]]",
                    expectedDataType,
                    matcher.apply(mvData.size(), mvData.stream().mapToLong(Long::longValue))
                );
            }));
        }
    }

    /**
     * Build many test cases with {@code geo_point} values.
     * This assumes that the function consumes {@code geo_point} values and produces {@code geo_point} values.
     */
    protected static void geoPoints(
        List<TestCaseSupplier> cases,
        String name,
        String evaluatorName,
        BiFunction<Integer, Stream<BytesRef>, Matcher<Object>> matcher
    ) {
        geoPoints(cases, name, evaluatorName, DataType.GEO_POINT, matcher);
    }

    /**
     * Build many test cases with {@code geo_point} values that are converted to another type.
     * This assumes that the function consumes {@code geo_point} values and produces another type.
     * For example, mv_count() can consume points and produce an integer count.
     */
    protected static void geoPoints(
        List<TestCaseSupplier> cases,
        String name,
        String evaluatorName,
        DataType expectedDataType,
        BiFunction<Integer, Stream<BytesRef>, Matcher<Object>> matcher
    ) {
        spatial(cases, name, evaluatorName, DataType.GEO_POINT, expectedDataType, GEO, GeometryTestUtils::randomPoint, matcher);
    }

    /**
     * Build many test cases with {@code cartesian_point} values.
     * This assumes that the function consumes {@code cartesian_point} values and produces {@code cartesian_point} values.
     */
    protected static void cartesianPoints(
        List<TestCaseSupplier> cases,
        String name,
        String evaluatorName,
        BiFunction<Integer, Stream<BytesRef>, Matcher<Object>> matcher
    ) {
        cartesianPoints(cases, name, evaluatorName, DataType.CARTESIAN_POINT, matcher);
    }

    /**
     * Build many test cases with {@code cartesian_point} values that are converted to another type.
     * This assumes that the function consumes {@code cartesian_point} values and produces another type.
     * For example, mv_count() can consume points and produce an integer count.
     */
    protected static void cartesianPoints(
        List<TestCaseSupplier> cases,
        String name,
        String evaluatorName,
        DataType expectedDataType,
        BiFunction<Integer, Stream<BytesRef>, Matcher<Object>> matcher
    ) {
        spatial(cases, name, evaluatorName, DataType.CARTESIAN_POINT, expectedDataType, CARTESIAN, ShapeTestUtils::randomPoint, matcher);
    }

    /**
     * Build many test cases with {@code geo_shape} values that are converted to another type.
     * This assumes that the function consumes {@code geo_shape} values and produces another type.
     * For example, mv_count() can consume geo_shapes and produce an integer count.
     */
    protected static void geoShape(
        List<TestCaseSupplier> cases,
        String name,
        String evaluatorName,
        DataType expectedDataType,
        BiFunction<Integer, Stream<BytesRef>, Matcher<Object>> matcher
    ) {
        spatial(
            cases,
            name,
            evaluatorName,
            DataType.GEO_SHAPE,
            expectedDataType,
            GEO,
            () -> rarely() ? GeometryTestUtils.randomGeometry(randomBoolean()) : GeometryTestUtils.randomPoint(),
            matcher
        );
    }

    /**
     * Build many test cases with {@code cartesian_shape} values that are converted to another type.
     * This assumes that the function consumes {@code cartesian_shape} values and produces another type.
     * For example, mv_count() can consume cartesian shapes and produce an integer count.
     */
    protected static void cartesianShape(
        List<TestCaseSupplier> cases,
        String name,
        String evaluatorName,
        DataType expectedDataType,
        BiFunction<Integer, Stream<BytesRef>, Matcher<Object>> matcher
    ) {
        spatial(
            cases,
            name,
            evaluatorName,
            DataType.CARTESIAN_SHAPE,
            expectedDataType,
            CARTESIAN,
            () -> rarely() ? ShapeTestUtils.randomGeometry(randomBoolean()) : ShapeTestUtils.randomPoint(),
            matcher
        );
    }

    /**
     * Build many test cases for spatial values
     */
    protected static void spatial(
        List<TestCaseSupplier> cases,
        String name,
        String evaluatorName,
        DataType dataType,
        DataType expectedDataType,
        SpatialCoordinateTypes spatial,
        Supplier<Geometry> randomGeometry,
        BiFunction<Integer, Stream<BytesRef>, Matcher<Object>> matcher
    ) {
        cases.add(new TestCaseSupplier(name + "(" + dataType.typeName() + ")", List.of(dataType), () -> {
            BytesRef wkb = spatial.asWkb(randomGeometry.get());
            return new TestCaseSupplier.TestCase(
                List.of(new TestCaseSupplier.TypedData(List.of(wkb), dataType, "field")),
                evaluatorName + "[field=Attribute[channel=0]]",
                expectedDataType,
                matcher.apply(1, Stream.of(wkb))
            );
        }));
        for (Block.MvOrdering ordering : Block.MvOrdering.values()) {
            cases.add(new TestCaseSupplier(name + "(<" + dataType.typeName() + "s>) " + ordering, List.of(dataType), () -> {
                List<BytesRef> mvData = randomList(1, 100, () -> spatial.asWkb(randomGeometry.get()));
                putInOrder(mvData, ordering);
                return new TestCaseSupplier.TestCase(
                    List.of(new TestCaseSupplier.TypedData(mvData, dataType, "field")),
                    evaluatorName + "[field=Attribute[channel=0]]",
                    expectedDataType,
                    matcher.apply(mvData.size(), mvData.stream())
                );
            }));
        }
    }

    /**
     * Build many test cases with unsigned {@code long} values.
     */
    protected static void unsignedLongs(
        List<TestCaseSupplier> cases,
        String name,
        String evaluatorName,
        BiFunction<Integer, Stream<BigInteger>, Matcher<Object>> matcher
    ) {
        unsignedLongs(cases, name, evaluatorName, DataType.UNSIGNED_LONG, matcher);
    }

    /**
     * Build many test cases with unsigned {@code long} values.
     */
    protected static void unsignedLongs(
        List<TestCaseSupplier> cases,
        String name,
        String evaluatorName,
        DataType expectedDataType,
        BiFunction<Integer, Stream<BigInteger>, Matcher<Object>> matcher
    ) {
        cases.add(
            new TestCaseSupplier(
                name + "(0UL)",
                List.of(DataType.UNSIGNED_LONG),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(
                            List.of(NumericUtils.asLongUnsigned(BigInteger.ZERO)),
                            DataType.UNSIGNED_LONG,
                            "field"
                        )
                    ),
                    evaluatorName + "[field=Attribute[channel=0]]",
                    expectedDataType,
                    matcher.apply(1, Stream.of(BigInteger.ZERO))
                )
            )
        );
        cases.add(new TestCaseSupplier(name + "(unsigned long)", List.of(DataType.UNSIGNED_LONG), () -> {
            long data = randomLong();
            return new TestCaseSupplier.TestCase(
                List.of(new TestCaseSupplier.TypedData(List.of(data), DataType.UNSIGNED_LONG, "field")),
                evaluatorName + "[field=Attribute[channel=0]]",
                expectedDataType,
                matcher.apply(1, Stream.of(NumericUtils.unsignedLongAsBigInteger(data)))
            );
        }));
        for (Block.MvOrdering ordering : Block.MvOrdering.values()) {
            cases.add(new TestCaseSupplier(name + "(<unsigned longs>) " + ordering, List.of(DataType.UNSIGNED_LONG), () -> {
                List<Long> mvData = randomList(1, 100, ESTestCase::randomLong);
                putInOrder(mvData, ordering);
                return new TestCaseSupplier.TestCase(
                    List.of(new TestCaseSupplier.TypedData(mvData, DataType.UNSIGNED_LONG, "field")),
                    evaluatorName + "[field=Attribute[channel=0]]",
                    expectedDataType,
                    matcher.apply(mvData.size(), mvData.stream().map(NumericUtils::unsignedLongAsBigInteger))
                );
            }));
        }
    }

    private static <T extends Comparable<T>> void putInOrder(List<T> mvData, Block.MvOrdering ordering) {
        switch (ordering) {
            case UNORDERED -> {
            }
            case DEDUPLICATED_UNORDERD -> {
                var dedup = new LinkedHashSet<>(mvData);
                mvData.clear();
                mvData.addAll(dedup);
            }
            case DEDUPLICATED_AND_SORTED_ASCENDING -> {
                var dedup = new HashSet<>(mvData);
                mvData.clear();
                mvData.addAll(dedup);
                Collections.sort(mvData);
            }
            case SORTED_ASCENDING -> {
                Collections.sort(mvData);
            }
            default -> throw new UnsupportedOperationException("unsupported ordering [" + ordering + "]");
        }
    }

    protected abstract Expression build(Source source, Expression field);

    protected DataType expectedType(List<DataType> argTypes) {
        return argTypes.get(0);
    }

    @Override
    protected final Expression build(Source source, List<Expression> args) {
        return build(source, args.get(0));
    }
}
