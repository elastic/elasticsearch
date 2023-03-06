/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataTypes;

import static org.hamcrest.Matchers.containsString;

public class StringFunctionsTests extends ESTestCase {
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
