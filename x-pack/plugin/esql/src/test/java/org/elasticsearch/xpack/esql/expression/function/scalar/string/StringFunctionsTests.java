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

public class StringFunctionsTests extends ESTestCase {

    public void testLength() {
        assertEquals(Integer.valueOf(0), Length.process(""));
        assertEquals(Integer.valueOf(1), Length.process("a"));
        assertEquals(Integer.valueOf(2), Length.process("❗️"));
        assertEquals(Integer.valueOf(100), Length.process(randomUnicodeOfLength(100)));
        assertEquals(Integer.valueOf(100), Length.process(randomAlphaOfLength(100)));
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
}
