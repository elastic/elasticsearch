/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.conditional;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.xpack.esql.expression.function.AbstractFunctionTestCase;
import org.elasticsearch.xpack.esql.type.EsqlDataTypes;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Expression.TypeResolution;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.hamcrest.Matcher;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.elasticsearch.compute.data.BlockUtils.toJavaObject;
import static org.hamcrest.Matchers.equalTo;

public class CaseTests extends AbstractFunctionTestCase {
    @Override
    protected List<Object> simpleData() {
        return List.of(true, new BytesRef("a"), new BytesRef("b"));
    }

    @Override
    protected Expression expressionForSimpleData() {
        return new Case(
            Source.EMPTY,
            List.of(field("cond", DataTypes.BOOLEAN), field("a", DataTypes.KEYWORD), field("b", DataTypes.KEYWORD))
        );
    }

    @Override
    protected DataType expressionForSimpleDataType() {
        return DataTypes.KEYWORD;
    }

    @Override
    protected String expectedEvaluatorSimpleToString() {
        return "CaseEvaluator[resultType=BYTES_REF, "
            + "conditions=[ConditionEvaluator[condition=Attribute[channel=0], value=Attribute[channel=1]]], elseVal=Attribute[channel=2]]";
    }

    @Override
    protected Expression constantFoldable(List<Object> data) {
        return caseExpr(data.toArray());
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
    protected Matcher<Object> resultMatcher(List<Object> data) {
        for (int i = 0; i < data.size() - 1; i += 2) {
            Object cond = data.get(i);
            if (cond != null && ((Boolean) cond).booleanValue()) {
                return equalTo(data.get(i + 1));
            }
        }
        if (data.size() % 2 == 0) {
            return null;
        }
        return equalTo(data.get(data.size() - 1));
    }

    @Override
    protected Expression build(Source source, List<Literal> args) {
        return new Case(Source.EMPTY, args.stream().map(l -> (Expression) l).toList());
    }

    public void testEvalCase() {
        testCase(
            caseExpr -> toJavaObject(
                caseExpr.toEvaluator(child -> evaluator(child)).get().eval(new Page(IntBlock.newConstantBlockWith(0, 1))),
                0
            )
        );
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
        assertEquals("expected at least two arguments in [<case>] but got 0", resolveCase().message());
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
        assertEquals(1, toJavaObject(caseExpr.toEvaluator(child -> {
            Object value = child.fold();
            if (value != null && value.equals(2)) {
                return () -> page -> {
                    fail("Unexpected evaluation of 4th argument");
                    return null;
                };
            }
            return evaluator(child);
        }).get().eval(new Page(IntBlock.newConstantBlockWith(0, 1))), 0));
    }

    private static Case caseExpr(Object... args) {
        return new Case(Source.synthetic("<case>"), Stream.of(args).<Expression>map(arg -> {
            if (arg instanceof Expression e) {
                return e;
            }
            return new Literal(Source.synthetic(arg == null ? "null" : arg.toString()), arg, EsqlDataTypes.fromJava(arg));
        }).toList());
    }

    private static TypeResolution resolveCase(Object... args) {
        return caseExpr(args).resolveType();
    }

    private static DataType resolveType(Object... args) {
        return caseExpr(args).dataType();
    }
}
