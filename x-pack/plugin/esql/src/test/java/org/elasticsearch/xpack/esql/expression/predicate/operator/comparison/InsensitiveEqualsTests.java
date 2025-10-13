/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.predicate.operator.comparison;

import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.of;
import static org.elasticsearch.xpack.esql.core.tree.Source.EMPTY;

public class InsensitiveEqualsTests extends ESTestCase {

    public void testFold() {
        assertTrue(insensitiveEquals(l("foo"), l("foo")).fold(FoldContext.small()));
        assertTrue(insensitiveEquals(l("Foo"), l("foo")).fold(FoldContext.small()));
        assertTrue(insensitiveEquals(l("Foo"), l("fOO")).fold(FoldContext.small()));
        assertTrue(insensitiveEquals(l("foo*"), l("foo*")).fold(FoldContext.small()));
        assertTrue(insensitiveEquals(l("foo*"), l("FOO*")).fold(FoldContext.small()));
        assertTrue(insensitiveEquals(l("foo?bar"), l("foo?bar")).fold(FoldContext.small()));
        assertTrue(insensitiveEquals(l("foo?bar"), l("FOO?BAR")).fold(FoldContext.small()));
        assertTrue(insensitiveEquals(l(""), l("")).fold(FoldContext.small()));

        assertFalse(insensitiveEquals(l("Foo"), l("fo*")).fold(FoldContext.small()));
        assertFalse(insensitiveEquals(l("Fox"), l("fo?")).fold(FoldContext.small()));
        assertFalse(insensitiveEquals(l("Foo"), l("*OO")).fold(FoldContext.small()));
        assertFalse(insensitiveEquals(l("BarFooBaz"), l("*O*")).fold(FoldContext.small()));
        assertFalse(insensitiveEquals(l("BarFooBaz"), l("bar*baz")).fold(FoldContext.small()));
        assertFalse(insensitiveEquals(l("foo"), l("*")).fold(FoldContext.small()));

        assertFalse(insensitiveEquals(l("foo*bar"), l("foo\\*bar")).fold(FoldContext.small()));
        assertFalse(insensitiveEquals(l("foo?"), l("foo\\?")).fold(FoldContext.small()));
        assertFalse(insensitiveEquals(l("foo?bar"), l("foo\\?bar")).fold(FoldContext.small()));
        assertFalse(insensitiveEquals(l(randomAlphaOfLength(10)), l("*")).fold(FoldContext.small()));
        assertFalse(insensitiveEquals(l(randomAlphaOfLength(3)), l("???")).fold(FoldContext.small()));

        assertFalse(insensitiveEquals(l("foo"), l("bar")).fold(FoldContext.small()));
        assertFalse(insensitiveEquals(l("foo"), l("ba*")).fold(FoldContext.small()));
        assertFalse(insensitiveEquals(l("foo"), l("*a*")).fold(FoldContext.small()));
        assertFalse(insensitiveEquals(l(""), l("bar")).fold(FoldContext.small()));
        assertFalse(insensitiveEquals(l("foo"), l("")).fold(FoldContext.small()));
        assertFalse(insensitiveEquals(l(randomAlphaOfLength(3)), l("??")).fold(FoldContext.small()));
        assertFalse(insensitiveEquals(l(randomAlphaOfLength(3)), l("????")).fold(FoldContext.small()));

        assertNull(insensitiveEquals(l("foo"), Literal.NULL).fold(FoldContext.small()));
        assertNull(insensitiveEquals(Literal.NULL, l("foo")).fold(FoldContext.small()));
        assertNull(insensitiveEquals(Literal.NULL, Literal.NULL).fold(FoldContext.small()));
    }

    public void testProcess() {
        assertTrue(InsensitiveEquals.process(BytesRefs.toBytesRef("foo"), BytesRefs.toBytesRef("foo")));
        assertTrue(InsensitiveEquals.process(BytesRefs.toBytesRef("Foo"), BytesRefs.toBytesRef("foo")));
        assertTrue(InsensitiveEquals.process(BytesRefs.toBytesRef("Foo"), BytesRefs.toBytesRef("fOO")));
        assertTrue(InsensitiveEquals.process(BytesRefs.toBytesRef("foo*"), BytesRefs.toBytesRef("foo*")));
        assertTrue(InsensitiveEquals.process(BytesRefs.toBytesRef("foo*"), BytesRefs.toBytesRef("FOO*")));
        assertTrue(InsensitiveEquals.process(BytesRefs.toBytesRef("foo?bar"), BytesRefs.toBytesRef("foo?bar")));
        assertTrue(InsensitiveEquals.process(BytesRefs.toBytesRef("foo?bar"), BytesRefs.toBytesRef("FOO?BAR")));
        assertTrue(InsensitiveEquals.process(BytesRefs.toBytesRef(""), BytesRefs.toBytesRef("")));

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
        return of(EMPTY, value);
    }
}
