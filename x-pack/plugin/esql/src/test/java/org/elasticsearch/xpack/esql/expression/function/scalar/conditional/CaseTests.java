/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.conditional;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expression.TypeResolution;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;

import java.math.BigInteger;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.elasticsearch.compute.data.BlockUtils.toJavaObject;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.randomLiteral;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class CaseTests extends AbstractScalarFunctionTestCase {

    public CaseTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    /**
     * Generate the test cases for this test
     */
    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        // TODO this needs lots of stuff flipped to parameters
        return parameterSuppliersFromTypedData(
            List.of(new TestCaseSupplier("keyword", List.of(DataType.BOOLEAN, DataType.KEYWORD, DataType.KEYWORD), () -> {
                List<TestCaseSupplier.TypedData> typedData = List.of(
                    new TestCaseSupplier.TypedData(true, DataType.BOOLEAN, "cond"),
                    new TestCaseSupplier.TypedData(new BytesRef("a"), DataType.KEYWORD, "a"),
                    new TestCaseSupplier.TypedData(new BytesRef("b"), DataType.KEYWORD, "b")
                );
                return new TestCaseSupplier.TestCase(
                    typedData,
                    "CaseEvaluator[resultType=BYTES_REF, conditions=[ConditionEvaluator[condition=Attribute[channel=0], "
                        + "value=Attribute[channel=1]]], elseVal=Attribute[channel=2]]",
                    DataType.KEYWORD,
                    equalTo(new BytesRef("a"))
                );
            }), new TestCaseSupplier("text", List.of(DataType.BOOLEAN, DataType.TEXT), () -> {
                List<TestCaseSupplier.TypedData> typedData = List.of(
                    new TestCaseSupplier.TypedData(false, DataType.BOOLEAN, "cond"),
                    new TestCaseSupplier.TypedData(new BytesRef("a"), DataType.TEXT, "trueValue")
                );
                return new TestCaseSupplier.TestCase(
                    typedData,
                    "CaseEvaluator[resultType=BYTES_REF, conditions=[ConditionEvaluator[condition=Attribute[channel=0], "
                        + "value=Attribute[channel=1]]], elseVal=LiteralsEvaluator[lit=null]]",
                    DataType.TEXT,
                    nullValue()
                );
            }), new TestCaseSupplier("boolean", List.of(DataType.BOOLEAN, DataType.BOOLEAN), () -> {
                List<TestCaseSupplier.TypedData> typedData = List.of(
                    new TestCaseSupplier.TypedData(false, DataType.BOOLEAN, "cond"),
                    new TestCaseSupplier.TypedData(false, DataType.BOOLEAN, "trueValue")
                );
                return new TestCaseSupplier.TestCase(
                    typedData,
                    "CaseEvaluator[resultType=BOOLEAN, conditions=[ConditionEvaluator[condition=Attribute[channel=0], "
                        + "value=Attribute[channel=1]]], elseVal=LiteralsEvaluator[lit=null]]",
                    DataType.BOOLEAN,
                    nullValue()
                );
            }), new TestCaseSupplier("date", List.of(DataType.BOOLEAN, DataType.DATETIME), () -> {
                long value = randomNonNegativeLong();
                List<TestCaseSupplier.TypedData> typedData = List.of(
                    new TestCaseSupplier.TypedData(true, DataType.BOOLEAN, "cond"),
                    new TestCaseSupplier.TypedData(value, DataType.DATETIME, "trueValue")
                );
                return new TestCaseSupplier.TestCase(
                    typedData,
                    "CaseEvaluator[resultType=LONG, conditions=[ConditionEvaluator[condition=Attribute[channel=0], "
                        + "value=Attribute[channel=1]]], elseVal=LiteralsEvaluator[lit=null]]",
                    DataType.DATETIME,
                    equalTo(value)
                );
            }), new TestCaseSupplier("double", List.of(DataType.BOOLEAN, DataType.DOUBLE), () -> {
                double value = randomDouble();
                List<TestCaseSupplier.TypedData> typedData = List.of(
                    new TestCaseSupplier.TypedData(true, DataType.BOOLEAN, "cond"),
                    new TestCaseSupplier.TypedData(value, DataType.DOUBLE, "trueValue")
                );
                return new TestCaseSupplier.TestCase(
                    typedData,
                    "CaseEvaluator[resultType=DOUBLE, conditions=[ConditionEvaluator[condition=Attribute[channel=0], "
                        + "value=Attribute[channel=1]]], elseVal=LiteralsEvaluator[lit=null]]",
                    DataType.DOUBLE,
                    equalTo(value)
                );
            }), new TestCaseSupplier("integer", List.of(DataType.BOOLEAN, DataType.INTEGER), () -> {
                int value = randomInt();
                List<TestCaseSupplier.TypedData> typedData = List.of(
                    new TestCaseSupplier.TypedData(false, DataType.BOOLEAN, "cond"),
                    new TestCaseSupplier.TypedData(value, DataType.INTEGER, "trueValue")
                );
                return new TestCaseSupplier.TestCase(
                    typedData,
                    "CaseEvaluator[resultType=INT, conditions=[ConditionEvaluator[condition=Attribute[channel=0], "
                        + "value=Attribute[channel=1]]], elseVal=LiteralsEvaluator[lit=null]]",
                    DataType.INTEGER,
                    nullValue()
                );
            }), new TestCaseSupplier("long", List.of(DataType.BOOLEAN, DataType.LONG), () -> {
                long value = randomLong();
                List<TestCaseSupplier.TypedData> typedData = List.of(
                    new TestCaseSupplier.TypedData(false, DataType.BOOLEAN, "cond"),
                    new TestCaseSupplier.TypedData(value, DataType.LONG, "trueValue")
                );
                return new TestCaseSupplier.TestCase(
                    typedData,
                    "CaseEvaluator[resultType=LONG, conditions=[ConditionEvaluator[condition=Attribute[channel=0], "
                        + "value=Attribute[channel=1]]], elseVal=LiteralsEvaluator[lit=null]]",
                    DataType.LONG,
                    nullValue()
                );
            }), new TestCaseSupplier("unsigned_long", List.of(DataType.BOOLEAN, DataType.UNSIGNED_LONG), () -> {
                BigInteger value = randomUnsignedLongBetween(BigInteger.ZERO, UNSIGNED_LONG_MAX);
                List<TestCaseSupplier.TypedData> typedData = List.of(
                    new TestCaseSupplier.TypedData(true, DataType.BOOLEAN, "cond"),
                    new TestCaseSupplier.TypedData(value, DataType.UNSIGNED_LONG, "trueValue")
                );
                return new TestCaseSupplier.TestCase(
                    typedData,
                    "CaseEvaluator[resultType=LONG, conditions=[ConditionEvaluator[condition=Attribute[channel=0], "
                        + "value=Attribute[channel=1]]], elseVal=LiteralsEvaluator[lit=null]]",
                    DataType.UNSIGNED_LONG,
                    equalTo(value)
                );
            }), new TestCaseSupplier("ip", List.of(DataType.BOOLEAN, DataType.IP), () -> {
                BytesRef value = (BytesRef) randomLiteral(DataType.IP).value();
                List<TestCaseSupplier.TypedData> typedData = List.of(
                    new TestCaseSupplier.TypedData(true, DataType.BOOLEAN, "cond"),
                    new TestCaseSupplier.TypedData(value, DataType.IP, "trueValue")
                );
                return new TestCaseSupplier.TestCase(
                    typedData,
                    "CaseEvaluator[resultType=BYTES_REF, conditions=[ConditionEvaluator[condition=Attribute[channel=0], "
                        + "value=Attribute[channel=1]]], elseVal=LiteralsEvaluator[lit=null]]",
                    DataType.IP,
                    equalTo(value)
                );
            }), new TestCaseSupplier("version", List.of(DataType.BOOLEAN, DataType.VERSION), () -> {
                BytesRef value = (BytesRef) randomLiteral(DataType.VERSION).value();
                List<TestCaseSupplier.TypedData> typedData = List.of(
                    new TestCaseSupplier.TypedData(false, DataType.BOOLEAN, "cond"),
                    new TestCaseSupplier.TypedData(value, DataType.VERSION, "trueValue")
                );
                return new TestCaseSupplier.TestCase(
                    typedData,
                    "CaseEvaluator[resultType=BYTES_REF, conditions=[ConditionEvaluator[condition=Attribute[channel=0], "
                        + "value=Attribute[channel=1]]], elseVal=LiteralsEvaluator[lit=null]]",
                    DataType.VERSION,
                    nullValue()
                );
            }), new TestCaseSupplier("cartesian_point", List.of(DataType.BOOLEAN, DataType.CARTESIAN_POINT), () -> {
                BytesRef value = (BytesRef) randomLiteral(DataType.CARTESIAN_POINT).value();
                List<TestCaseSupplier.TypedData> typedData = List.of(
                    new TestCaseSupplier.TypedData(false, DataType.BOOLEAN, "cond"),
                    new TestCaseSupplier.TypedData(value, DataType.CARTESIAN_POINT, "trueValue")
                );
                return new TestCaseSupplier.TestCase(
                    typedData,
                    "CaseEvaluator[resultType=BYTES_REF, conditions=[ConditionEvaluator[condition=Attribute[channel=0], "
                        + "value=Attribute[channel=1]]], elseVal=LiteralsEvaluator[lit=null]]",
                    DataType.CARTESIAN_POINT,
                    nullValue()
                );
            }), new TestCaseSupplier("geo_point", List.of(DataType.BOOLEAN, DataType.GEO_POINT), () -> {
                BytesRef value = (BytesRef) randomLiteral(DataType.GEO_POINT).value();
                List<TestCaseSupplier.TypedData> typedData = List.of(
                    new TestCaseSupplier.TypedData(true, DataType.BOOLEAN, "cond"),
                    new TestCaseSupplier.TypedData(value, DataType.GEO_POINT, "trueValue")
                );
                return new TestCaseSupplier.TestCase(
                    typedData,
                    "CaseEvaluator[resultType=BYTES_REF, conditions=[ConditionEvaluator[condition=Attribute[channel=0], "
                        + "value=Attribute[channel=1]]], elseVal=LiteralsEvaluator[lit=null]]",
                    DataType.GEO_POINT,
                    equalTo(value)
                );
            }))
        );
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new Case(Source.EMPTY, args.get(0), args.subList(1, args.size()));
    }

    public void testEvalCase() {
        testCase(caseExpr -> {
            DriverContext driverContext = driverContext();
            Page page = new Page(driverContext.blockFactory().newConstantIntBlockWith(0, 1));
            try (
                EvalOperator.ExpressionEvaluator eval = caseExpr.toEvaluator(child -> evaluator(child)).get(driverContext);
                Block block = eval.eval(page)
            ) {
                return toJavaObject(block, 0);
            } finally {
                page.releaseBlocks();
            }
        });
    }

    public void testFoldCase() {
        testCase(caseExpr -> {
            assertTrue(caseExpr.foldable());
            return caseExpr.fold();
        });
    }

    public void testCase(Function<Case, Object> toValue) {
        assertEquals(1, toValue.apply(caseExpr(true, 1)));
        assertNull(toValue.apply(caseExpr(false, 1)));
        assertEquals(2, toValue.apply(caseExpr(false, 1, 2)));
        assertEquals(1, toValue.apply(caseExpr(true, 1, true, 2)));
        assertEquals(2, toValue.apply(caseExpr(false, 1, true, 2)));
        assertNull(toValue.apply(caseExpr(false, 1, false, 2)));
        assertEquals(3, toValue.apply(caseExpr(false, 1, false, 2, 3)));
        assertNull(toValue.apply(caseExpr(true, null, 1)));
        assertEquals(1, toValue.apply(caseExpr(false, null, 1)));
        assertEquals(1, toValue.apply(caseExpr(false, field("ignored", DataType.INTEGER), 1)));
        assertEquals(1, toValue.apply(caseExpr(true, 1, field("ignored", DataType.INTEGER))));
    }

    public void testIgnoreLeadingNulls() {
        assertEquals(DataType.INTEGER, resolveType(false, null, 1));
        assertEquals(DataType.INTEGER, resolveType(false, null, false, null, false, 2, null));
        assertEquals(DataType.NULL, resolveType(false, null, null));
        assertEquals(DataType.BOOLEAN, resolveType(false, null, field("bool", DataType.BOOLEAN)));
    }

    public void testCaseWithInvalidCondition() {
        assertEquals("expected at least two arguments in [<case>] but got 1", resolveCase(1).message());
        assertEquals("first argument of [<case>] must be [boolean], found value [1] type [integer]", resolveCase(1, 2).message());
        assertEquals(
            "third argument of [<case>] must be [boolean], found value [3] type [integer]",
            resolveCase(true, 2, 3, 4, 5).message()
        );
    }

    public void testCaseWithIncompatibleTypes() {
        assertEquals("third argument of [<case>] must be [integer], found value [hi] type [keyword]", resolveCase(true, 1, "hi").message());
        assertEquals(
            "fourth argument of [<case>] must be [integer], found value [hi] type [keyword]",
            resolveCase(true, 1, false, "hi", 5).message()
        );
        assertEquals(
            "argument of [<case>] must be [integer], found value [hi] type [keyword]",
            resolveCase(true, 1, false, 2, true, 5, "hi").message()
        );
    }

    public void testCaseIsLazy() {
        Case caseExpr = caseExpr(true, 1, true, 2);
        DriverContext driveContext = driverContext();
        EvalOperator.ExpressionEvaluator evaluator = caseExpr.toEvaluator(child -> {
            Object value = child.fold();
            if (value != null && value.equals(2)) {
                return dvrCtx -> new EvalOperator.ExpressionEvaluator() {
                    @Override
                    public Block eval(Page page) {
                        fail("Unexpected evaluation of 4th argument");
                        return null;
                    }

                    @Override
                    public void close() {}
                };
            }
            return evaluator(child);
        }).get(driveContext);
        Page page = new Page(driveContext.blockFactory().newConstantIntBlockWith(0, 1));
        try (Block block = evaluator.eval(page)) {
            assertEquals(1, toJavaObject(block, 0));
        } finally {
            page.releaseBlocks();
        }
    }

    private static Case caseExpr(Object... args) {
        List<Expression> exps = Stream.of(args).<Expression>map(arg -> {
            if (arg instanceof Expression e) {
                return e;
            }
            return new Literal(Source.synthetic(arg == null ? "null" : arg.toString()), arg, DataType.fromJava(arg));
        }).toList();
        return new Case(Source.synthetic("<case>"), exps.get(0), exps.subList(1, exps.size()));
    }

    private static TypeResolution resolveCase(Object... args) {
        return caseExpr(args).resolveType();
    }

    private static DataType resolveType(Object... args) {
        return caseExpr(args).dataType();
    }
}
