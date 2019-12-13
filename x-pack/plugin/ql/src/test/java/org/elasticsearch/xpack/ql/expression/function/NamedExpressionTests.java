/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ql.expression.function;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ql.expression.FieldAttribute;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.expression.predicate.operator.arithmetic.Add;
import org.elasticsearch.xpack.ql.expression.predicate.operator.arithmetic.Div;
import org.elasticsearch.xpack.ql.expression.predicate.operator.arithmetic.Mod;
import org.elasticsearch.xpack.ql.expression.predicate.operator.arithmetic.Mul;
import org.elasticsearch.xpack.ql.expression.predicate.operator.arithmetic.Neg;
import org.elasticsearch.xpack.ql.expression.predicate.operator.arithmetic.Sub;
import org.elasticsearch.xpack.ql.tree.Location;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.EsField;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.xpack.ql.tree.Source.EMPTY;

public class NamedExpressionTests extends ESTestCase {

    public void testArithmeticFunctionName() {
        String e = "5 +  2";
        Add add = new Add(s(e), l(5), l(2));
        assertEquals(e, add.sourceText());

        e = "5 /  2";
        Div div = new Div(s(e), l(5), l(2));
        assertEquals(e, div.sourceText());

        e = "5%2";
        Mod mod = new Mod(s(e), l(5), l(2));
        assertEquals(e, mod.sourceText());

        e = "5  *  2";
        Mul mul = new Mul(s(e), l(5), l(2));
        assertEquals(e, mul.sourceText());

        e = "5 -2";
        Sub sub = new Sub(s(e), l(5), l(2));
        assertEquals(e, sub.sourceText());

        e = " -  5";
        Neg neg = new Neg(s(e), l(5));
        assertEquals(e, neg.sourceText());
    }

    public void testNameForArithmeticFunctionAppliedOnTableColumn() {
        FieldAttribute fa = new FieldAttribute(EMPTY, "myField", new EsField("myESField", DataType.INTEGER, emptyMap(), true));
        String e = "myField  + 10";
        Add add = new Add(s(e), fa, l(10));
        assertEquals(e, add.sourceText());
    }

    private static Source s(String text) {
        return new Source(Location.EMPTY, text);
    }

    private static Literal l(Object value) {
        return Literal.of(EMPTY, value);
    }
}
