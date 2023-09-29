/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.multivalue;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.data.Vector;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.elasticsearch.xpack.esql.expression.function.scalar.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.planner.LocalExecutionPlanner;
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
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.elasticsearch.compute.data.BlockUtils.toJavaObject;
import static org.hamcrest.Matchers.equalTo;

public abstract class AbstractMultivalueFunctionTestCase extends AbstractScalarFunctionTestCase {
    /**
     * Build a test case with {@code boolean} values.
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
     * Build a test case with {@code boolean} values.
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
                () -> new TestCaseSupplier.TestCase(
                    List.of(new TestCaseSupplier.TypedData(List.of(true), DataTypes.BOOLEAN, "field")),
                    evaluatorName + "[field=Attribute[channel=0]]",
                    expectedDataType,
                    matcher.apply(1, Stream.of(true))
                )
            )
        );
        for (Block.MvOrdering ordering : Block.MvOrdering.values()) {
            cases.add(new TestCaseSupplier(name + "(<booleans>) " + ordering, () -> {
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
     * Build a test case with {@link BytesRef} values.
     */
    protected static void bytesRefs(
        List<TestCaseSupplier> cases,
        String name,
        String evaluatorName,
        BiFunction<Integer, Stream<BytesRef>, Matcher<Object>> matcher
    ) {
        bytesRefs(cases, name, evaluatorName, DataTypes.KEYWORD, matcher);
    }

    /**
     * Build a test case with {@link BytesRef} values.
     */
    protected static void bytesRefs(
        List<TestCaseSupplier> cases,
        String name,
        String evaluatorName,
        DataType expectedDataType,
        BiFunction<Integer, Stream<BytesRef>, Matcher<Object>> matcher
    ) {
        cases.add(
            new TestCaseSupplier(
                name + "(empty string)",
                () -> new TestCaseSupplier.TestCase(
                    List.of(new TestCaseSupplier.TypedData(List.of(new BytesRef("")), DataTypes.KEYWORD, "field")),
                    evaluatorName + "[field=Attribute[channel=0]]",
                    expectedDataType,
                    matcher.apply(1, Stream.of(new BytesRef("")))
                )
            )
        );
        cases.add(new TestCaseSupplier(name + "(BytesRef)", () -> {
            BytesRef data = new BytesRef(randomAlphaOfLength(10));
            return new TestCaseSupplier.TestCase(
                List.of(new TestCaseSupplier.TypedData(List.of(data), DataTypes.KEYWORD, "field")),
                evaluatorName + "[field=Attribute[channel=0]]",
                expectedDataType,
                matcher.apply(1, Stream.of(data))
            );
        }));
        for (Block.MvOrdering ordering : Block.MvOrdering.values()) {
            cases.add(new TestCaseSupplier(name + "(<BytesRefs>) " + ordering, () -> {
                List<BytesRef> mvData = randomList(1, 100, () -> new BytesRef(randomAlphaOfLength(10)));
                putInOrder(mvData, ordering);
                return new TestCaseSupplier.TestCase(
                    List.of(new TestCaseSupplier.TypedData(mvData, DataTypes.KEYWORD, "field")),
                    evaluatorName + "[field=Attribute[channel=0]]",
                    expectedDataType,
                    matcher.apply(mvData.size(), mvData.stream())
                );
            }));
        }
    }

    /**
     * Build a test case with {@code double} values.
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
     * Build a test case with {@code double} values.
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
                () -> new TestCaseSupplier.TestCase(
                    List.of(new TestCaseSupplier.TypedData(List.of(0.0), DataTypes.DOUBLE, "field")),
                    evaluatorName + "[field=Attribute[channel=0]]",
                    expectedDataType,
                    matcher.apply(1, DoubleStream.of(0.0))
                )
            )
        );
        cases.add(new TestCaseSupplier(name + "(double)", () -> {
            double mvData = randomDouble();
            return new TestCaseSupplier.TestCase(
                List.of(new TestCaseSupplier.TypedData(List.of(mvData), DataTypes.DOUBLE, "field")),
                evaluatorName + "[field=Attribute[channel=0]]",
                expectedDataType,
                matcher.apply(1, DoubleStream.of(mvData))
            );
        }));
        for (Block.MvOrdering ordering : Block.MvOrdering.values()) {
            cases.add(new TestCaseSupplier(name + "(<double>) " + ordering, () -> {
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
     * Build a test case with {@code int} values.
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
     * Build a test case with {@code int} values.
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
                () -> new TestCaseSupplier.TestCase(
                    List.of(new TestCaseSupplier.TypedData(List.of(0), DataTypes.INTEGER, "field")),
                    evaluatorName + "[field=Attribute[channel=0]]",
                    expectedDataType,
                    matcher.apply(1, IntStream.of(0))
                )
            )
        );
        cases.add(new TestCaseSupplier(name + "(int)", () -> {
            int data = randomInt();
            return new TestCaseSupplier.TestCase(
                List.of(new TestCaseSupplier.TypedData(List.of(data), DataTypes.INTEGER, "field")),
                evaluatorName + "[field=Attribute[channel=0]]",
                expectedDataType,
                matcher.apply(1, IntStream.of(data))
            );
        }));
        for (Block.MvOrdering ordering : Block.MvOrdering.values()) {
            cases.add(new TestCaseSupplier(name + "(<ints>) " + ordering, () -> {
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
     * Build a test case with {@code long} values.
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
     * Build a test case with {@code long} values.
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
                () -> new TestCaseSupplier.TestCase(
                    List.of(new TestCaseSupplier.TypedData(List.of(0L), DataTypes.LONG, "field")),
                    evaluatorName + "[field=Attribute[channel=0]]",
                    expectedDataType,
                    matcher.apply(1, LongStream.of(0L))
                )
            )
        );
        cases.add(new TestCaseSupplier(name + "(long)", () -> {
            long data = randomLong();
            return new TestCaseSupplier.TestCase(
                List.of(new TestCaseSupplier.TypedData(List.of(data), DataTypes.LONG, "field")),
                evaluatorName + "[field=Attribute[channel=0]]",
                expectedDataType,
                matcher.apply(1, LongStream.of(data))
            );
        }));
        for (Block.MvOrdering ordering : Block.MvOrdering.values()) {
            cases.add(new TestCaseSupplier(name + "(<longs>) " + ordering, () -> {
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
     * Build a test case with unsigned {@code long} values.
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
     * Build a test case with unsigned {@code long} values.
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
        cases.add(new TestCaseSupplier(name + "(unsigned long)", () -> {
            long data = randomLong();
            return new TestCaseSupplier.TestCase(
                List.of(new TestCaseSupplier.TypedData(List.of(data), DataTypes.UNSIGNED_LONG, "field")),
                evaluatorName + "[field=Attribute[channel=0]]",
                expectedDataType,
                matcher.apply(1, Stream.of(NumericUtils.unsignedLongAsBigInteger(data)))
            );
        }));
        for (Block.MvOrdering ordering : Block.MvOrdering.values()) {
            cases.add(new TestCaseSupplier(name + "(<unsigned longs>) " + ordering, () -> {
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

    /**
     * Tests a {@link Block} of values, all copied from the input pattern.
     * <p>
     *     Note that this'll sometimes be a {@link Vector} of values if the
     *     input pattern contained only a single value.
     * </p>
     */
    public final void testBlockWithoutNulls() {
        testBlock(false);
    }

    /**
     * Tests a {@link Block} of values, all copied from the input pattern with
     * some null values inserted between.
     */
    public final void testBlockWithNulls() {
        testBlock(true);
    }

    private void testBlock(boolean insertNulls) {
        int positions = between(1, 1024);
        TestCaseSupplier.TypedData data = testCase.getData().get(0);
        Block oneRowBlock = BlockUtils.fromListRow(testCase.getDataValues())[0];
        ElementType elementType = LocalExecutionPlanner.toElementType(data.type());
        Block.Builder builder = elementType.newBlockBuilder(positions);
        for (int p = 0; p < positions; p++) {
            if (insertNulls && randomBoolean()) {
                int nulls = between(1, 5);
                for (int n = 0; n < nulls; n++) {
                    builder.appendNull();
                }
            }
            builder.copyFrom(oneRowBlock, 0, 1);
        }
        Block input = builder.build();
        Block result = evaluator(buildFieldExpression(testCase)).get(driverContext()).eval(new Page(input));

        assertThat(result.getPositionCount(), equalTo(result.getPositionCount()));
        for (int p = 0; p < input.getPositionCount(); p++) {
            if (input.isNull(p)) {
                assertThat(result.isNull(p), equalTo(true));
                continue;
            }
            assertThat(result.isNull(p), equalTo(false));
            assertThat(toJavaObject(result, p), testCase.getMatcher());
        }
    }
}
