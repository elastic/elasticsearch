/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.nulls;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.evaluator.EvalMapper;
import org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.elasticsearch.xpack.esql.expression.function.scalar.VaragsTestCaseBuilder;
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.SpatialRelatesFunctionTestCase;
import org.elasticsearch.xpack.esql.planner.Layout;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.elasticsearch.compute.data.BlockUtils.toJavaObject;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.randomLiteral;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class CoalesceTests extends AbstractScalarFunctionTestCase {
    public CoalesceTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> noNullsSuppliers = new ArrayList<>();
        VaragsTestCaseBuilder builder = new VaragsTestCaseBuilder(type -> "Coalesce" + type + "Eager");
        builder.expectString(strings -> strings.filter(v -> v != null).findFirst());
        builder.expectLong(longs -> longs.filter(v -> v != null).findFirst());
        builder.expectInt(ints -> ints.filter(v -> v != null).findFirst());
        builder.expectBoolean(booleans -> booleans.filter(v -> v != null).findFirst());
        noNullsSuppliers.addAll(builder.suppliers());
        addSpatialCombinations(noNullsSuppliers);
        noNullsSuppliers.add(new TestCaseSupplier(List.of(DataType.IP, DataType.IP), () -> {
            var first = randomBoolean() ? null : EsqlDataTypeConverter.stringToIP(NetworkAddress.format(randomIp(true)));
            var second = EsqlDataTypeConverter.stringToIP(NetworkAddress.format(randomIp(true)));
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(first, DataType.IP, "first"),
                    new TestCaseSupplier.TypedData(second, DataType.IP, "second")
                ),
                "CoalesceBytesRefEagerEvaluator[values=[Attribute[channel=0], Attribute[channel=1]]]",
                DataType.IP,
                equalTo(first == null ? second : first)
            );
        }));
        noNullsSuppliers.add(new TestCaseSupplier(List.of(DataType.VERSION, DataType.VERSION), () -> {
            var first = randomBoolean()
                ? null
                : EsqlDataTypeConverter.stringToVersion(randomInt(10) + "." + randomInt(10) + "." + randomInt(10));
            var second = EsqlDataTypeConverter.stringToVersion(randomInt(10) + "." + randomInt(10) + "." + randomInt(10));
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(first, DataType.VERSION, "first"),
                    new TestCaseSupplier.TypedData(second, DataType.VERSION, "second")
                ),
                "CoalesceBytesRefEagerEvaluator[values=[Attribute[channel=0], Attribute[channel=1]]]",
                DataType.VERSION,
                equalTo(first == null ? second : first)
            );
        }));
        noNullsSuppliers.add(new TestCaseSupplier(List.of(DataType.DATETIME, DataType.DATETIME), () -> {
            Long firstDate = randomBoolean() ? null : ZonedDateTime.parse("2023-12-04T10:15:30Z").toInstant().toEpochMilli();
            Long secondDate = ZonedDateTime.parse("2023-12-05T10:45:00Z").toInstant().toEpochMilli();
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(firstDate, DataType.DATETIME, "first"),
                    new TestCaseSupplier.TypedData(secondDate, DataType.DATETIME, "second")
                ),
                "CoalesceLongEagerEvaluator[values=[Attribute[channel=0], Attribute[channel=1]]]",
                DataType.DATETIME,
                equalTo(firstDate == null ? secondDate : firstDate)
            );
        }));
        noNullsSuppliers.add(new TestCaseSupplier(List.of(DataType.DATE_NANOS, DataType.DATE_NANOS), () -> {
            Long firstDate = randomBoolean() ? null : randomNonNegativeLong();
            Long secondDate = randomNonNegativeLong();
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(firstDate, DataType.DATE_NANOS, "first"),
                    new TestCaseSupplier.TypedData(secondDate, DataType.DATE_NANOS, "second")
                ),
                "CoalesceLongEagerEvaluator[values=[Attribute[channel=0], Attribute[channel=1]]]",
                DataType.DATE_NANOS,
                equalTo(firstDate == null ? secondDate : firstDate)
            );
        }));

        List<TestCaseSupplier> suppliers = new ArrayList<>(noNullsSuppliers);
        for (TestCaseSupplier s : noNullsSuppliers) {
            for (int nullUpTo = 1; nullUpTo < s.types().size(); nullUpTo++) {
                int finalNullUpTo = nullUpTo;

                // Build cases with nulls up to a point
                suppliers.add(
                    new TestCaseSupplier(nullCaseName(s, nullUpTo, false), s.types(), () -> nullCase(s.get(), finalNullUpTo, false))
                );

                // Now do the same thing, but with the type forced to null
                List<DataType> types = new ArrayList<>(s.types());
                for (int i = 0; i < nullUpTo; i++) {
                    types.set(i, DataType.NULL);
                }
                suppliers.add(new TestCaseSupplier(nullCaseName(s, nullUpTo, true), types, () -> nullCase(s.get(), finalNullUpTo, true)));
            }
        }
        suppliers.add(
            new TestCaseSupplier(
                List.of(DataType.NULL, DataType.NULL),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(null, DataType.NULL, "first"),
                        new TestCaseSupplier.TypedData(null, DataType.NULL, "second")
                    ),
                    "ConstantNull",
                    DataType.NULL,
                    nullValue()
                )
            )
        );

        return parameterSuppliersFromTypedData(suppliers);
    }

    private static String nullCaseName(TestCaseSupplier supplier, int nullUpTo, boolean forceNullType) {
        StringBuilder name = new StringBuilder();
        for (int i = 0; i < nullUpTo; i++) {
            name.append("null <").append((forceNullType ? DataType.NULL : supplier.types().get(i)).typeName()).append(">, ");
        }
        for (int i = nullUpTo; i < supplier.types().size(); i++) {
            name.append('<').append(supplier.types().get(i).typeName()).append('>');
            if (i != supplier.types().size() - 1) {
                name.append(", ");
            }
        }
        return name.toString();
    }

    private static TestCaseSupplier.TestCase nullCase(TestCaseSupplier.TestCase delegate, int nullUpTo, boolean forceNullType) {
        List<TestCaseSupplier.TypedData> data = new ArrayList<>(delegate.getData());
        for (int i = 0; i < nullUpTo; i++) {
            data.set(i, new TestCaseSupplier.TypedData(null, forceNullType ? DataType.NULL : data.get(i).type(), data.get(i).name()));
        }
        Object expected = data.get(nullUpTo).data();
        if (expected instanceof List<?> l && l.size() == 1) {
            expected = l.get(0);
        }
        return new TestCaseSupplier.TestCase(data, delegate.evaluatorToString(), delegate.expectedType(), equalTo(expected));
    }

    protected static void addSpatialCombinations(List<TestCaseSupplier> suppliers) {
        for (DataType dataType : List.of(DataType.GEO_POINT, DataType.GEO_SHAPE, DataType.CARTESIAN_POINT, DataType.CARTESIAN_SHAPE)) {
            TestCaseSupplier.TypedDataSupplier leftDataSupplier = SpatialRelatesFunctionTestCase.testCaseSupplier(dataType, false);
            TestCaseSupplier.TypedDataSupplier rightDataSupplier = SpatialRelatesFunctionTestCase.testCaseSupplier(dataType, false);
            suppliers.add(
                TestCaseSupplier.testCaseSupplier(
                    leftDataSupplier,
                    rightDataSupplier,
                    (l, r) -> equalTo("CoalesceBytesRefEagerEvaluator[values=[Attribute[channel=0], Attribute[channel=1]]]"),
                    dataType,
                    (l, r) -> l
                )
            );
        }
    }

    @Override
    protected Coalesce build(Source source, List<Expression> args) {
        return new Coalesce(Source.EMPTY, args.get(0), args.subList(1, args.size()));
    }

    public void testCoalesceIsLazy() {
        List<Expression> sub = new ArrayList<>(testCase.getDataAsFields());
        FieldAttribute evil = new FieldAttribute(Source.EMPTY, "evil", new EsField("evil", sub.get(0).dataType(), Map.of(), true));
        sub.add(evil);
        Coalesce exp = build(Source.EMPTY, sub);
        Layout.Builder builder = new Layout.Builder();
        buildLayout(builder, exp);
        Layout layout = builder.build();
        EvaluatorMapper.ToEvaluator toEvaluator = new EvaluatorMapper.ToEvaluator() {
            @Override
            public EvalOperator.ExpressionEvaluator.Factory apply(Expression expression) {
                if (expression == evil) {
                    return dvrCtx -> new EvalOperator.ExpressionEvaluator() {
                        @Override
                        public Block eval(Page page) {
                            throw new AssertionError("shouldn't be called");
                        }

                        @Override
                        public void close() {}
                    };
                }
                return EvalMapper.toEvaluator(FoldContext.small(), expression, layout);
            }

            @Override
            public FoldContext foldCtx() {
                return FoldContext.small();
            }
        };
        try (
            EvalOperator.ExpressionEvaluator eval = exp.toEvaluator(toEvaluator).get(driverContext());
            Block block = eval.eval(row(testCase.getDataValues()))
        ) {
            assertThat(toJavaObject(block, 0), testCase.getMatcher());
        }
    }

    public void testCoalesceNullabilityIsUnknown() {
        assertThat(buildFieldExpression(testCase).nullable(), equalTo(Nullability.UNKNOWN));
    }

    public void testCoalesceKnownNullable() {
        List<Expression> sub = new ArrayList<>(testCase.getDataAsFields());
        sub.add(between(0, sub.size()), new Literal(Source.EMPTY, null, sub.get(0).dataType()));
        Coalesce exp = build(Source.EMPTY, testCase.getDataAsFields());
        // Still UNKNOWN - if it were TRUE then an optimizer would replace it with null
        assertThat(exp.nullable(), equalTo(Nullability.UNKNOWN));
    }

    public void testCoalesceNotNullable() {
        List<Expression> sub = new ArrayList<>(testCase.getDataAsFields());
        sub.add(between(0, sub.size()), randomLiteral(sub.get(sub.size() - 1).dataType()));
        Coalesce exp = build(Source.EMPTY, sub);
        // Known not to be nullable because it contains a non-null literal
        if (testCase.expectedType() == DataType.NULL) {
            assertThat(exp.nullable(), equalTo(Nullability.UNKNOWN));
        } else {
            assertThat(exp.nullable(), equalTo(Nullability.FALSE));
        }
    }

    /**
     * Inserts random non-null garbage <strong>around</strong> the expected data and runs COALESCE.
     * <p>
     *     This is important for catching the case where your value is null, but the rest of the block
     *     isn't null. An off-by-one error in the evaluators can break this in a way that the standard
     *     tests weren't catching and this does.
     * </p>
     */
    public void testEvaluateWithGarbage() {
        DriverContext context = driverContext();
        Expression expression = randomBoolean() ? buildDeepCopyOfFieldExpression(testCase) : buildFieldExpression(testCase);
        int positions = between(2, 1024);
        List<TestCaseSupplier.TypedData> data = testCase.getData();
        Page onePositionPage = row(testCase.getDataValues());
        Block[] blocks = new Block[Math.toIntExact(data.stream().filter(d -> d.isForceLiteral() == false).count())];
        int realPosition = between(0, positions - 1);
        try {
            int blocksIndex = 0;
            for (TestCaseSupplier.TypedData d : data) {
                blocks[blocksIndex] = blockWithRandomGarbage(
                    context.blockFactory(),
                    d.type(),
                    onePositionPage.getBlock(blocksIndex),
                    positions,
                    realPosition
                );
                blocksIndex++;
            }
            try (
                EvalOperator.ExpressionEvaluator eval = evaluator(expression).get(context);
                Block block = eval.eval(new Page(positions, blocks))
            ) {
                assertThat(block.getPositionCount(), is(positions));
                assertThat(toJavaObjectUnsignedLongAware(block, realPosition), testCase.getMatcher());
                assertThat("evaluates to tracked block", block.blockFactory(), sameInstance(context.blockFactory()));
            }
        } finally {
            Releasables.close(onePositionPage::releaseBlocks, Releasables.wrap(blocks));
        }
    }

    private Block blockWithRandomGarbage(
        BlockFactory blockFactory,
        DataType type,
        Block singlePositionBlock,
        int totalPositions,
        int insertLocation
    ) {
        try (Block.Builder builder = PlannerUtils.toElementType(type).newBlockBuilder(totalPositions, blockFactory)) {
            for (int p = 0; p < totalPositions; p++) {
                Block copyFrom = p == insertLocation
                    ? singlePositionBlock
                    : BlockUtils.constantBlock(TestBlockFactory.getNonBreakingInstance(), randomLiteral(type).value(), 1);
                builder.copyFrom(copyFrom, 0, 1);
            }
            return builder.build();
        }
    }
}
