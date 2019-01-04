/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.sql.expression.FieldAttribute;
import org.elasticsearch.xpack.sql.expression.Literal;
import org.elasticsearch.xpack.sql.expression.predicate.operator.arithmetic.Add;
import org.elasticsearch.xpack.sql.expression.predicate.operator.arithmetic.Div;
import org.elasticsearch.xpack.sql.expression.predicate.operator.arithmetic.Mod;
import org.elasticsearch.xpack.sql.expression.predicate.operator.arithmetic.Mul;
import org.elasticsearch.xpack.sql.expression.predicate.operator.arithmetic.Neg;
import org.elasticsearch.xpack.sql.expression.predicate.operator.arithmetic.Sub;
import org.elasticsearch.xpack.sql.type.DataType;
import org.elasticsearch.xpack.sql.type.EsField;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.xpack.sql.tree.Source.EMPTY;

public class NamedExpressionTests extends ESTestCase {

    public void testArithmeticFunctionName() {
        Add add = new Add(EMPTY, l(5), l(2));
        assertEquals("5 + 2", add.name());

        Div div = new Div(EMPTY, l(5), l(2));
        assertEquals("5 / 2", div.name());

        Mod mod = new Mod(EMPTY, l(5), l(2));
        assertEquals("5 % 2", mod.name());

        Mul mul = new Mul(EMPTY, l(5), l(2));
        assertEquals("5 * 2", mul.name());

        Sub sub = new Sub(EMPTY, l(5), l(2));
        assertEquals("5 - 2", sub.name());

        Neg neg = new Neg(EMPTY, l(5));
        assertEquals("-5", neg.name());
    }

    public void testNameForArithmeticFunctionAppliedOnTableColumn() {
        FieldAttribute fa = new FieldAttribute(EMPTY, "myField", new EsField("myESField", DataType.INTEGER, emptyMap(), true));
        Add add = new Add(EMPTY, fa, l(10));
        assertEquals("(myField) + 10", add.name());
    }

    private static Literal l(Object value) {
        return Literal.of(EMPTY, value);
    }
}
