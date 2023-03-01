/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.conditional;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.type.EsqlDataTypes;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Expression.TypeResolution;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;

import java.util.function.Function;
import java.util.stream.Stream;

import static org.elasticsearch.xpack.ql.type.DataTypes.INTEGER;
import static org.elasticsearch.xpack.ql.type.DataTypes.NULL;

public class CaseTests extends ESTestCase {
    public void testEvalCase() {
        testCase(caseExpr -> caseExpr.toEvaluator(child -> () -> (page, pos) -> child.fold()).get().computeRow(null, 0));
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
    }

    public void testIgnoreLeadingNulls() {
        assertEquals(INTEGER, resolveType(false, null, 1));
        assertEquals(INTEGER, resolveType(false, null, false, null, false, 2, null));
        assertEquals(NULL, resolveType(false, null, null));
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
    }

    public void testCaseIsLazy() {
        Case caseExpr = caseExpr(true, 1, true, 2);
        assertEquals(1, caseExpr.toEvaluator(child -> {
            Object value = child.fold();
            if (value.equals(2)) {
                return () -> (page, pos) -> {
                    fail("Unexpected evaluation of 4th argument");
                    return null;
                };
            } else {
                return () -> (page, pos) -> value;
            }
        }).get().computeRow(null, 0));
    }

    private static Case caseExpr(Object... args) {
        return new Case(
            Source.synthetic("<case>"),
            Stream.of(args)
                .<Expression>map(
                    arg -> new Literal(Source.synthetic(arg == null ? "null" : arg.toString()), arg, EsqlDataTypes.fromJava(arg))
                )
                .toList()
        );
    }

    private static TypeResolution resolveCase(Object... args) {
        return caseExpr(args).resolveType();
    }

    private static DataType resolveType(Object... args) {
        return caseExpr(args).dataType();
    }
}
