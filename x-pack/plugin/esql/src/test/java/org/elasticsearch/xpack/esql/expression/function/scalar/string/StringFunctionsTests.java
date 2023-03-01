/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.containsString;

public class StringFunctionsTests extends ESTestCase {
    public void testConcat() {
        assertEquals(new BytesRef("cats and"), processConcat(new BytesRef("cats"), new BytesRef(" and")));
        assertEquals(
            new BytesRef("cats and dogs"),
            processConcat(new BytesRef("cats"), new BytesRef(" "), new BytesRef("and"), new BytesRef(" "), new BytesRef("dogs"))
        );
        assertEquals(null, processConcat(new BytesRef("foo"), null));
        assertEquals(null, processConcat(null, new BytesRef("foo")));

        Concat c = concatWithLiterals(new BytesRef("cats"), new BytesRef(" and"));
        assertTrue(c.foldable());
        assertEquals(new BytesRef("cats and"), c.fold());

        c = concatWithLiterals(new BytesRef("cats"), new BytesRef(" "), new BytesRef("and"), new BytesRef(" "), new BytesRef("dogs"));
        assertTrue(c.foldable());
        assertEquals(new BytesRef("cats and dogs"), c.fold());
    }

    private Concat concatWithLiterals(Object... inputs) {
        if (inputs.length < 2) {
            throw new IllegalArgumentException("needs at least two");
        }
        List<Expression> values = Arrays.stream(inputs).map(i -> (Expression) new Literal(Source.EMPTY, i, DataTypes.KEYWORD)).toList();
        return new Concat(Source.EMPTY, values.get(0), values.subList(1, values.size()));
    }

    private BytesRef processConcat(Object... inputs) {
        Concat concat = concatWithLiterals(inputs);
        EvalOperator.ExpressionEvaluator eval = concat.toEvaluator(e -> () -> (page, position) -> ((Literal) e).value()).get();
        return (BytesRef) eval.computeRow(null, 0);
    }

    public void testLength() {
        assertEquals(Integer.valueOf(0), Length.process(new BytesRef("")));
        assertEquals(Integer.valueOf(1), Length.process(new BytesRef("a")));
        assertEquals(Integer.valueOf(1), Length.process(new BytesRef("☕"))); // 3 bytes, 1 code point
        assertEquals(Integer.valueOf(2), Length.process(new BytesRef("❗️"))); // 6 bytes, 2 code points
        assertEquals(Integer.valueOf(100), Length.process(new BytesRef(randomUnicodeOfCodepointLength(100))));
        assertEquals(Integer.valueOf(100), Length.process(new BytesRef(randomAlphaOfLength(100))));
        assertNull(Length.process(null));
    }

    public void testStartsWith() {
        assertEquals(true, StartsWith.process(new BytesRef("cat"), new BytesRef("cat")));
        assertEquals(true, StartsWith.process(new BytesRef("cat"), new BytesRef("ca")));
        assertEquals(true, StartsWith.process(new BytesRef("cat"), new BytesRef("c")));
        assertEquals(true, StartsWith.process(new BytesRef("cat"), new BytesRef("")));
        assertEquals(false, StartsWith.process(new BytesRef("cat"), new BytesRef("cata")));
        assertEquals(null, StartsWith.process(null, new BytesRef("cat")));
        assertEquals(null, StartsWith.process(new BytesRef("cat"), null));
        String s = randomUnicodeOfLength(10);
        assertEquals(true, StartsWith.process(new BytesRef(s), new BytesRef("")));
        assertEquals(true, StartsWith.process(new BytesRef(s), new BytesRef(s)));
        assertEquals(true, StartsWith.process(new BytesRef(s + randomUnicodeOfLength(2)), new BytesRef(s)));
        assertEquals(true, StartsWith.process(new BytesRef(s + randomAlphaOfLength(100)), new BytesRef(s)));

        Expression e = new StartsWith(
            Source.EMPTY,
            new Literal(Source.EMPTY, new BytesRef("ab"), DataTypes.KEYWORD),
            new Literal(Source.EMPTY, new BytesRef("a"), DataTypes.KEYWORD)
        );
        assertTrue(e.foldable());
        assertEquals(true, e.fold());
    }

    public void testSubstring() {
        assertEquals("a tiger", Substring.process(new BytesRef("a tiger"), 0, null));
        assertEquals("tiger", Substring.process(new BytesRef("a tiger"), 3, null));
        assertEquals("ger", Substring.process(new BytesRef("a tiger"), -3, null));

        assertEquals("tiger", Substring.process(new BytesRef("a tiger"), 3, 1000));
        assertEquals("ger", Substring.process(new BytesRef("a tiger"), -3, 1000));

        assertEquals("a tiger", Substring.process(new BytesRef("a tiger"), -300, null));
        assertEquals("a", Substring.process(new BytesRef("a tiger"), -300, 1));

        assertEquals("a t", Substring.process(new BytesRef("a tiger"), 1, 3));

        // test with a supplementary character
        final String s = "a\ud83c\udf09tiger";
        assert s.length() == 8 && s.codePointCount(0, s.length()) == 7;
        assertEquals("tiger", Substring.process(new BytesRef(s), 3, 1000));
        assertEquals("\ud83c\udf09tiger", Substring.process(new BytesRef(s), -6, 1000));

        assertNull(Substring.process(new BytesRef("a tiger"), null, null));
        assertNull(Substring.process(null, 1, 1));

        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> Substring.process(new BytesRef("a tiger"), 1, -1));
        assertThat(ex.getMessage(), containsString("Length parameter cannot be negative, found [-1]"));

        Expression e = new Substring(
            Source.EMPTY,
            new Literal(Source.EMPTY, new BytesRef("ab"), DataTypes.KEYWORD),
            new Literal(Source.EMPTY, 1, DataTypes.INTEGER),
            new Literal(Source.EMPTY, 1, DataTypes.INTEGER)
        );
        assertTrue(e.foldable());
        assertEquals("a", e.fold());
    }
}
