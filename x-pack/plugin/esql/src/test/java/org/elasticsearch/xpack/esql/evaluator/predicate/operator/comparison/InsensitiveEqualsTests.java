/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.evaluator.predicate.operator.comparison;

import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ql.TestUtils;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Literal;

import java.time.ZoneId;

import static org.elasticsearch.xpack.ql.tree.Source.EMPTY;

public class InsensitiveEqualsTests extends ESTestCase {

    public void testFold() {
        assertEquals(true, insensitiveEquals(l("foo"), l("foo")).fold());
        assertEquals(true, insensitiveEquals(l("Foo"), l("foo")).fold());
        assertEquals(true, insensitiveEquals(l("Foo"), l("fOO")).fold());
        assertEquals(true, insensitiveEquals(l("Foo"), l("fo*")).fold());
        assertEquals(true, insensitiveEquals(l("Foobar"), l("fo*")).fold());
        assertEquals(true, insensitiveEquals(l("Fox"), l("fo?")).fold());
        assertEquals(true, insensitiveEquals(l("Foo"), l("*OO")).fold());
        assertEquals(true, insensitiveEquals(l("BarFooBaz"), l("*O*")).fold());
        assertEquals(true, insensitiveEquals(l("BarFooBaz"), l("bar*baz")).fold());
        assertEquals(true, insensitiveEquals(l("foo"), l("*")).fold());
        assertEquals(true, insensitiveEquals(l("foo*"), l("foo\\*")).fold());
        assertEquals(true, insensitiveEquals(l("foo*bar"), l("foo\\*bar")).fold());
        assertEquals(true, insensitiveEquals(l("foo?"), l("foo\\?")).fold());
        assertEquals(true, insensitiveEquals(l("foo?bar"), l("foo\\?bar")).fold());
        assertEquals(true, insensitiveEquals(l(randomAlphaOfLength(10)), l("*")).fold());
        assertEquals(true, insensitiveEquals(l(randomAlphaOfLength(3)), l("???")).fold());

        assertEquals(false, insensitiveEquals(l("foo"), l("bar")).fold());
        assertEquals(false, insensitiveEquals(l("foo"), l("ba*")).fold());
        assertEquals(false, insensitiveEquals(l("foo"), l("*a*")).fold());
        assertEquals(false, insensitiveEquals(l(""), l("bar")).fold());
        assertEquals(false, insensitiveEquals(l("foo"), l("")).fold());
        assertEquals(false, insensitiveEquals(l(randomAlphaOfLength(3)), l("??")).fold());
        assertEquals(false, insensitiveEquals(l(randomAlphaOfLength(3)), l("????")).fold());

        assertNull(insensitiveEquals(l("foo"), Literal.NULL).fold());
        assertNull(insensitiveEquals(Literal.NULL, l("foo")).fold());
        assertNull(insensitiveEquals(Literal.NULL, Literal.NULL).fold());
    }

    public void testProcess() {
        assertEquals(true, InsensitiveEquals.process(BytesRefs.toBytesRef("foo"), BytesRefs.toBytesRef("foo")));
        assertEquals(true, InsensitiveEquals.process(BytesRefs.toBytesRef("Foo"), BytesRefs.toBytesRef("foo")));
        assertEquals(true, InsensitiveEquals.process(BytesRefs.toBytesRef("Foo"), BytesRefs.toBytesRef("fOO")));
        assertEquals(true, InsensitiveEquals.process(BytesRefs.toBytesRef("Foo"), BytesRefs.toBytesRef("fo*")));
        assertEquals(true, InsensitiveEquals.process(BytesRefs.toBytesRef("Foobar"), BytesRefs.toBytesRef("fo*")));
        assertEquals(true, InsensitiveEquals.process(BytesRefs.toBytesRef("Fox"), BytesRefs.toBytesRef("fo?")));
        assertEquals(true, InsensitiveEquals.process(BytesRefs.toBytesRef("Foo"), BytesRefs.toBytesRef("*OO")));
        assertEquals(true, InsensitiveEquals.process(BytesRefs.toBytesRef("BarFooBaz"), BytesRefs.toBytesRef("*O*")));
        assertEquals(true, InsensitiveEquals.process(BytesRefs.toBytesRef("BarFooBaz"), BytesRefs.toBytesRef("bar*baz")));
        assertEquals(true, InsensitiveEquals.process(BytesRefs.toBytesRef("foo"), BytesRefs.toBytesRef("*")));
        assertEquals(true, InsensitiveEquals.process(BytesRefs.toBytesRef("foo*"), BytesRefs.toBytesRef("foo\\*")));
        assertEquals(true, InsensitiveEquals.process(BytesRefs.toBytesRef("foo*bar"), BytesRefs.toBytesRef("foo\\*bar")));
        assertEquals(true, InsensitiveEquals.process(BytesRefs.toBytesRef("foo?"), BytesRefs.toBytesRef("foo\\?")));
        assertEquals(true, InsensitiveEquals.process(BytesRefs.toBytesRef("foo?bar"), BytesRefs.toBytesRef("foo\\?bar")));
        assertEquals(true, InsensitiveEquals.process(BytesRefs.toBytesRef(randomAlphaOfLength(10)), BytesRefs.toBytesRef("*")));
        assertEquals(true, InsensitiveEquals.process(BytesRefs.toBytesRef(randomAlphaOfLength(3)), BytesRefs.toBytesRef("???")));

        assertEquals(false, InsensitiveEquals.process(BytesRefs.toBytesRef("foo"), BytesRefs.toBytesRef("bar")));
        assertEquals(false, InsensitiveEquals.process(BytesRefs.toBytesRef("foo"), BytesRefs.toBytesRef("ba*")));
        assertEquals(false, InsensitiveEquals.process(BytesRefs.toBytesRef("foo"), BytesRefs.toBytesRef("*a*")));
        assertEquals(false, InsensitiveEquals.process(BytesRefs.toBytesRef(""), BytesRefs.toBytesRef("bar")));
        assertEquals(false, InsensitiveEquals.process(BytesRefs.toBytesRef("foo"), BytesRefs.toBytesRef("")));
        assertEquals(false, InsensitiveEquals.process(BytesRefs.toBytesRef(randomAlphaOfLength(3)), BytesRefs.toBytesRef("??")));
        assertEquals(false, InsensitiveEquals.process(BytesRefs.toBytesRef(randomAlphaOfLength(3)), BytesRefs.toBytesRef("????")));
    }

    protected InsensitiveEquals insensitiveEquals(Expression left, Expression right) {
        return new InsensitiveEquals(EMPTY, left, right, ZoneId.of("UTC"));
    }

    private static Literal l(Object value) {
        return TestUtils.of(EMPTY, value);
    }
}
