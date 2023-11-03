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
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.xpack.esql.expression.function.AbstractFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.elasticsearch.xpack.esql.type.EsqlDataTypes;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Expression.TypeResolution;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.elasticsearch.compute.data.BlockUtils.toJavaObject;
import static org.hamcrest.Matchers.equalTo;

public class CaseTests extends AbstractFunctionTestCase {

    public CaseTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    /**
     * Generate the test cases for this test
     */
    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        return parameterSuppliersFromTypedData(List.of(new TestCaseSupplier("basics", () -> {
            List<TestCaseSupplier.TypedData> typedData = List.of(
                new TestCaseSupplier.TypedData(true, DataTypes.BOOLEAN, "cond"),
                new TestCaseSupplier.TypedData(new BytesRef("a"), DataTypes.KEYWORD, "a"),
                new TestCaseSupplier.TypedData(new BytesRef("b"), DataTypes.KEYWORD, "b")
            );
            return new TestCaseSupplier.TestCase(
                typedData,
                "CaseEvaluator[resultType=BYTES_REF, conditions=[ConditionEvaluator[condition=Attribute[channel=0], "
                    + "value=Attribute[channel=1]]], elseVal=Attribute[channel=2]]",
                DataTypes.KEYWORD,
                equalTo(new BytesRef("a"))
            );
        })));
    }

    @Override
    protected void assertSimpleWithNulls(List<Object> data, Block value, int nullBlock) {
        if (nullBlock == 0) {
            assertThat(toJavaObject(value, 0), equalTo(data.get(2)));
            return;
        }
        if (((Boolean) data.get(0)).booleanValue()) {
            if (nullBlock == 1) {
                super.assertSimpleWithNulls(data, value, nullBlock);
            } else {
                assertThat(toJavaObject(value, 0), equalTo(data.get(1)));
            }
            return;
        }
        if (nullBlock == 2) {
            super.assertSimpleWithNulls(data, value, nullBlock);
        } else {
            assertThat(toJavaObject(value, 0), equalTo(data.get(2)));
        }
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new Case(Source.EMPTY, args.get(0), args.subList(1, args.size()));
    }

    public void testEvalCase() {
        testCase(caseExpr -> {
            try (
                EvalOperator.ExpressionEvaluator eval = caseExpr.toEvaluator(child -> evaluator(child)).get(driverContext());
                Block.Ref ref = eval.eval(new Page(IntBlock.newConstantBlockWith(0, 1)))
            ) {
                return toJavaObject(ref.block(), 0);
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
        assertEquals(1, toValue.apply(caseExpr(false, field("ignored", DataTypes.INTEGER), 1)));
        assertEquals(1, toValue.apply(caseExpr(true, 1, field("ignored", DataTypes.INTEGER))));
    }

    public void testIgnoreLeadingNulls() {
        assertEquals(DataTypes.INTEGER, resolveType(false, null, 1));
        assertEquals(DataTypes.INTEGER, resolveType(false, null, false, null, false, 2, null));
        assertEquals(DataTypes.NULL, resolveType(false, null, null));
        assertEquals(DataTypes.BOOLEAN, resolveType(false, null, field("bool", DataTypes.BOOLEAN)));
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
        try (Block.Ref ref = caseExpr.toEvaluator(child -> {
            Object value = child.fold();
            if (value != null && value.equals(2)) {
                return dvrCtx -> new EvalOperator.ExpressionEvaluator() {
                    @Override
                    public Block.Ref eval(Page page) {
                        fail("Unexpected evaluation of 4th argument");
                        return null;
                    }

                    @Override
                    public void close() {}
                };
            }
            return evaluator(child);
        }).get(driverContext()).eval(new Page(IntBlock.newConstantBlockWith(0, 1)))) {
            assertEquals(1, toJavaObject(ref.block(), 0));
        }
    }

    private static Case caseExpr(Object... args) {
        List<Expression> exps = Stream.of(args).<Expression>map(arg -> {
            if (arg instanceof Expression e) {
                return e;
            }
            return new Literal(Source.synthetic(arg == null ? "null" : arg.toString()), arg, EsqlDataTypes.fromJava(arg));
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
