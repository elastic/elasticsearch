/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.multivalue;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.elasticsearch.xpack.esql.expression.function.scalar.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.elasticsearch.xpack.ql.util.NumericUtils;
import org.hamcrest.Matcher;

import java.math.BigInteger;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

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
        booleans(cases, name, evaluatorName, DataTypes.BOOLEAN, matcher);
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
                List.of(DataTypes.BOOLEAN),
                () -> new TestCaseSupplier.TestCase(
                    List.of(new TestCaseSupplier.TypedData(List.of(false), DataTypes.BOOLEAN, "field")),
                    evaluatorName + "[field=Attribute[channel=0]]",
                    expectedDataType,
                    matcher.apply(1, Stream.of(false))
                )
            )
        );
        cases.add(
            new TestCaseSupplier(
                name + "(true)",
                List.of(DataTypes.BOOLEAN),
                () -> new TestCaseSupplier.TestCase(
                    List.of(new TestCaseSupplier.TypedData(List.of(true), DataTypes.BOOLEAN, "field")),
                    evaluatorName + "[field=Attribute[channel=0]]",
                    expectedDataType,
                    matcher.apply(1, Stream.of(true))
                )
            )
        );
        for (Block.MvOrdering ordering : Block.MvOrdering.values()) {
            cases.add(new TestCaseSupplier(name + "(<booleans>) " + ordering, List.of(DataTypes.BOOLEAN), () -> {
                List<Boolean> mvData = randomList(2, 100, ESTestCase::randomBoolean);
                putInOrder(mvData, ordering);
                return new TestCaseSupplier.TestCase(
                    List.of(new TestCaseSupplier.TypedData(mvData, DataTypes.BOOLEAN, "field")),
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
        for (DataType type : new DataType[] { DataTypes.KEYWORD, DataTypes.TEXT, DataTypes.IP, DataTypes.VERSION }) {
            if (type != DataTypes.IP) {
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
        doubles(cases, name, evaluatorName, DataTypes.DOUBLE, matcher);
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
                List.of(DataTypes.DOUBLE),
                () -> new TestCaseSupplier.TestCase(
                    List.of(new TestCaseSupplier.TypedData(List.of(0.0), DataTypes.DOUBLE, "field")),
                    evaluatorName + "[field=Attribute[channel=0]]",
                    expectedDataType,
                    matcher.apply(1, DoubleStream.of(0.0))
                )
            )
        );
        cases.add(new TestCaseSupplier(name + "(double)", List.of(DataTypes.DOUBLE), () -> {
            double mvData = randomDouble();
            return new TestCaseSupplier.TestCase(
                List.of(new TestCaseSupplier.TypedData(List.of(mvData), DataTypes.DOUBLE, "field")),
                evaluatorName + "[field=Attribute[channel=0]]",
                expectedDataType,
                matcher.apply(1, DoubleStream.of(mvData))
            );
        }));
        for (Block.MvOrdering ordering : Block.MvOrdering.values()) {
            cases.add(new TestCaseSupplier(name + "(<double>) " + ordering, List.of(DataTypes.DOUBLE), () -> {
                List<Double> mvData = randomList(1, 100, ESTestCase::randomDouble);
                putInOrder(mvData, ordering);
                return new TestCaseSupplier.TestCase(
                    List.of(new TestCaseSupplier.TypedData(mvData, DataTypes.DOUBLE, "field")),
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
        ints(cases, name, evaluatorName, DataTypes.INTEGER, matcher);
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
                List.of(DataTypes.INTEGER),
                () -> new TestCaseSupplier.TestCase(
                    List.of(new TestCaseSupplier.TypedData(List.of(0), DataTypes.INTEGER, "field")),
                    evaluatorName + "[field=Attribute[channel=0]]",
                    expectedDataType,
                    matcher.apply(1, IntStream.of(0))
                )
            )
        );
        cases.add(new TestCaseSupplier(name + "(int)", List.of(DataTypes.INTEGER), () -> {
            int data = randomInt();
            return new TestCaseSupplier.TestCase(
                List.of(new TestCaseSupplier.TypedData(List.of(data), DataTypes.INTEGER, "field")),
                evaluatorName + "[field=Attribute[channel=0]]",
                expectedDataType,
                matcher.apply(1, IntStream.of(data))
            );
        }));
        for (Block.MvOrdering ordering : Block.MvOrdering.values()) {
            cases.add(new TestCaseSupplier(name + "(<ints>) " + ordering, List.of(DataTypes.INTEGER), () -> {
                List<Integer> mvData = randomList(1, 100, ESTestCase::randomInt);
                putInOrder(mvData, ordering);
                return new TestCaseSupplier.TestCase(
                    List.of(new TestCaseSupplier.TypedData(mvData, DataTypes.INTEGER, "field")),
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
        longs(cases, name, evaluatorName, DataTypes.LONG, matcher);
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
                List.of(DataTypes.LONG),
                () -> new TestCaseSupplier.TestCase(
                    List.of(new TestCaseSupplier.TypedData(List.of(0L), DataTypes.LONG, "field")),
                    evaluatorName + "[field=Attribute[channel=0]]",
                    expectedDataType,
                    matcher.apply(1, LongStream.of(0L))
                )
            )
        );
        cases.add(new TestCaseSupplier(name + "(long)", List.of(DataTypes.LONG), () -> {
            long data = randomLong();
            return new TestCaseSupplier.TestCase(
                List.of(new TestCaseSupplier.TypedData(List.of(data), DataTypes.LONG, "field")),
                evaluatorName + "[field=Attribute[channel=0]]",
                expectedDataType,
                matcher.apply(1, LongStream.of(data))
            );
        }));
        for (Block.MvOrdering ordering : Block.MvOrdering.values()) {
            cases.add(new TestCaseSupplier(name + "(<longs>) " + ordering, List.of(DataTypes.LONG), () -> {
                List<Long> mvData = randomList(1, 100, ESTestCase::randomLong);
                putInOrder(mvData, ordering);
                return new TestCaseSupplier.TestCase(
                    List.of(new TestCaseSupplier.TypedData(mvData, DataTypes.LONG, "field")),
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
        dateTimes(cases, name, evaluatorName, DataTypes.DATETIME, matcher);
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
                List.of(DataTypes.DATETIME),
                () -> new TestCaseSupplier.TestCase(
                    List.of(new TestCaseSupplier.TypedData(List.of(0L), DataTypes.DATETIME, "field")),
                    evaluatorName + "[field=Attribute[channel=0]]",
                    expectedDataType,
                    matcher.apply(1, LongStream.of(0L))
                )
            )
        );
        cases.add(new TestCaseSupplier(name + "(date)", List.of(DataTypes.DATETIME), () -> {
            long data = randomLong();
            return new TestCaseSupplier.TestCase(
                List.of(new TestCaseSupplier.TypedData(List.of(data), DataTypes.DATETIME, "field")),
                evaluatorName + "[field=Attribute[channel=0]]",
                expectedDataType,
                matcher.apply(1, LongStream.of(data))
            );
        }));
        for (Block.MvOrdering ordering : Block.MvOrdering.values()) {
            cases.add(new TestCaseSupplier(name + "(<dates>) " + ordering, List.of(DataTypes.DATETIME), () -> {
                List<Long> mvData = randomList(1, 100, ESTestCase::randomLong);
                putInOrder(mvData, ordering);
                return new TestCaseSupplier.TestCase(
                    List.of(new TestCaseSupplier.TypedData(mvData, DataTypes.DATETIME, "field")),
                    evaluatorName + "[field=Attribute[channel=0]]",
                    expectedDataType,
                    matcher.apply(mvData.size(), mvData.stream().mapToLong(Long::longValue))
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
        unsignedLongs(cases, name, evaluatorName, DataTypes.UNSIGNED_LONG, matcher);
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
                List.of(DataTypes.UNSIGNED_LONG),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(
                            List.of(NumericUtils.asLongUnsigned(BigInteger.ZERO)),
                            DataTypes.UNSIGNED_LONG,
                            "field"
                        )
                    ),
                    evaluatorName + "[field=Attribute[channel=0]]",
                    expectedDataType,
                    matcher.apply(1, Stream.of(BigInteger.ZERO))
                )
            )
        );
        cases.add(new TestCaseSupplier(name + "(unsigned long)", List.of(DataTypes.UNSIGNED_LONG), () -> {
            long data = randomLong();
            return new TestCaseSupplier.TestCase(
                List.of(new TestCaseSupplier.TypedData(List.of(data), DataTypes.UNSIGNED_LONG, "field")),
                evaluatorName + "[field=Attribute[channel=0]]",
                expectedDataType,
                matcher.apply(1, Stream.of(NumericUtils.unsignedLongAsBigInteger(data)))
            );
        }));
        for (Block.MvOrdering ordering : Block.MvOrdering.values()) {
            cases.add(new TestCaseSupplier(name + "(<unsigned longs>) " + ordering, List.of(DataTypes.UNSIGNED_LONG), () -> {
                List<Long> mvData = randomList(1, 100, ESTestCase::randomLong);
                putInOrder(mvData, ordering);
                return new TestCaseSupplier.TestCase(
                    List.of(new TestCaseSupplier.TypedData(mvData, DataTypes.UNSIGNED_LONG, "field")),
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
            default -> throw new UnsupportedOperationException("unsupported ordering [" + ordering + "]");
        }
    }

    protected abstract Expression build(Source source, Expression field);

    protected abstract DataType[] supportedTypes();

    @Override
    protected final List<AbstractScalarFunctionTestCase.ArgumentSpec> argSpec() {
        return List.of(required(supportedTypes()));
    }

    @Override
    protected DataType expectedType(List<DataType> argTypes) {
        return argTypes.get(0);
    }

    @Override
    protected final Expression build(Source source, List<Expression> args) {
        return build(source, args.get(0));
    }
}
