/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.predicate.operator.comparison;

import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ql.TestUtils;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Literal;

import static org.elasticsearch.xpack.ql.tree.Source.EMPTY;

public class InsensitiveEqualsTests extends ESTestCase {

    public void testFold() {
        assertTrue(insensitiveEquals(l("foo"), l("foo")).fold());
        assertTrue(insensitiveEquals(l("Foo"), l("foo")).fold());
        assertTrue(insensitiveEquals(l("Foo"), l("fOO")).fold());
        assertTrue(insensitiveEquals(l("foo*"), l("foo*")).fold());
        assertTrue(insensitiveEquals(l("foo*"), l("FOO*")).fold());
        assertTrue(insensitiveEquals(l("foo?bar"), l("foo?bar")).fold());
        assertTrue(insensitiveEquals(l("foo?bar"), l("FOO?BAR")).fold());
        assertFalse(insensitiveEquals(l("Foo"), l("fo*")).fold());
        assertFalse(insensitiveEquals(l("Fox"), l("fo?")).fold());
        assertFalse(insensitiveEquals(l("Foo"), l("*OO")).fold());
        assertFalse(insensitiveEquals(l("BarFooBaz"), l("*O*")).fold());
        assertFalse(insensitiveEquals(l("BarFooBaz"), l("bar*baz")).fold());
        assertFalse(insensitiveEquals(l("foo"), l("*")).fold());

        assertFalse(insensitiveEquals(l("foo*bar"), l("foo\\*bar")).fold());
        assertFalse(insensitiveEquals(l("foo?"), l("foo\\?")).fold());
        assertFalse(insensitiveEquals(l("foo?bar"), l("foo\\?bar")).fold());
        assertFalse(insensitiveEquals(l(randomAlphaOfLength(10)), l("*")).fold());
        assertFalse(insensitiveEquals(l(randomAlphaOfLength(3)), l("???")).fold());

        assertFalse(insensitiveEquals(l("foo"), l("bar")).fold());
        assertFalse(insensitiveEquals(l("foo"), l("ba*")).fold());
        assertFalse(insensitiveEquals(l("foo"), l("*a*")).fold());
        assertFalse(insensitiveEquals(l(""), l("bar")).fold());
        assertFalse(insensitiveEquals(l("foo"), l("")).fold());
        assertFalse(insensitiveEquals(l(randomAlphaOfLength(3)), l("??")).fold());
        assertFalse(insensitiveEquals(l(randomAlphaOfLength(3)), l("????")).fold());

        assertNull(insensitiveEquals(l("foo"), Literal.NULL).fold());
        assertNull(insensitiveEquals(Literal.NULL, l("foo")).fold());
        assertNull(insensitiveEquals(Literal.NULL, Literal.NULL).fold());
    }

    public void testProcess() {
        assertTrue(InsensitiveEquals.process(BytesRefs.toBytesRef("foo"), BytesRefs.toBytesRef("foo")));
        assertTrue(InsensitiveEquals.process(BytesRefs.toBytesRef("Foo"), BytesRefs.toBytesRef("foo")));
        assertTrue(InsensitiveEquals.process(BytesRefs.toBytesRef("Foo"), BytesRefs.toBytesRef("fOO")));
        assertTrue(InsensitiveEquals.process(BytesRefs.toBytesRef("foo*"), BytesRefs.toBytesRef("foo*")));
        assertTrue(InsensitiveEquals.process(BytesRefs.toBytesRef("foo*"), BytesRefs.toBytesRef("FOO*")));
        assertTrue(InsensitiveEquals.process(BytesRefs.toBytesRef("foo?bar"), BytesRefs.toBytesRef("foo?bar")));
        assertTrue(InsensitiveEquals.process(BytesRefs.toBytesRef("foo?bar"), BytesRefs.toBytesRef("FOO?BAR")));
        assertFalse(InsensitiveEquals.process(BytesRefs.toBytesRef("Foo"), BytesRefs.toBytesRef("fo*")));
        assertFalse(InsensitiveEquals.process(BytesRefs.toBytesRef("Fox"), BytesRefs.toBytesRef("fo?")));
        assertFalse(InsensitiveEquals.process(BytesRefs.toBytesRef("Foo"), BytesRefs.toBytesRef("*OO")));
        assertFalse(InsensitiveEquals.process(BytesRefs.toBytesRef("BarFooBaz"), BytesRefs.toBytesRef("*O*")));
        assertFalse(InsensitiveEquals.process(BytesRefs.toBytesRef("BarFooBaz"), BytesRefs.toBytesRef("bar*baz")));
        assertFalse(InsensitiveEquals.process(BytesRefs.toBytesRef("foo"), BytesRefs.toBytesRef("*")));

        assertFalse(InsensitiveEquals.process(BytesRefs.toBytesRef("foo*bar"), BytesRefs.toBytesRef("foo\\*bar")));
        assertFalse(InsensitiveEquals.process(BytesRefs.toBytesRef("foo?"), BytesRefs.toBytesRef("foo\\?")));
        assertFalse(InsensitiveEquals.process(BytesRefs.toBytesRef("foo?bar"), BytesRefs.toBytesRef("foo\\?bar")));
        assertFalse(InsensitiveEquals.process(BytesRefs.toBytesRef(randomAlphaOfLength(10)), BytesRefs.toBytesRef("*")));
        assertFalse(InsensitiveEquals.process(BytesRefs.toBytesRef(randomAlphaOfLength(3)), BytesRefs.toBytesRef("???")));

        assertFalse(InsensitiveEquals.process(BytesRefs.toBytesRef("foo"), BytesRefs.toBytesRef("bar")));
        assertFalse(InsensitiveEquals.process(BytesRefs.toBytesRef("foo"), BytesRefs.toBytesRef("ba*")));
        assertFalse(InsensitiveEquals.process(BytesRefs.toBytesRef("foo"), BytesRefs.toBytesRef("*a*")));
        assertFalse(InsensitiveEquals.process(BytesRefs.toBytesRef(""), BytesRefs.toBytesRef("bar")));
        assertFalse(InsensitiveEquals.process(BytesRefs.toBytesRef("foo"), BytesRefs.toBytesRef("")));
        assertFalse(InsensitiveEquals.process(BytesRefs.toBytesRef(randomAlphaOfLength(3)), BytesRefs.toBytesRef("??")));
        assertFalse(InsensitiveEquals.process(BytesRefs.toBytesRef(randomAlphaOfLength(3)), BytesRefs.toBytesRef("????")));
    }

    protected InsensitiveEquals insensitiveEquals(Expression left, Expression right) {
        return new InsensitiveEquals(EMPTY, left, right);
    }

    private static Literal l(Object value) {
        return TestUtils.of(EMPTY, value);
    }
}
