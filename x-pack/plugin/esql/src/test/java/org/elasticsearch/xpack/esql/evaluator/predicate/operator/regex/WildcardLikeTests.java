/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.evaluator.predicate.operator.regex;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ql.TestUtils;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.expression.predicate.regex.WildcardPattern;

import static org.elasticsearch.xpack.ql.tree.Source.EMPTY;

public class WildcardLikeTests extends ESTestCase {

    public void testFold() {
        assertTrue(wildcardLike(l("foo"), "foo", randomBoolean()).fold());

        assertFalse(wildcardLike(l("foo"), "FoO", false).fold());
        assertTrue(wildcardLike(l("foo"), "FoO", true).fold());

        assertTrue(wildcardLike(l("Foobar"), "Foo*", randomBoolean()).fold());
        assertFalse(wildcardLike(l("Foobar"), "foo*", false).fold());
        assertTrue(wildcardLike(l("Foobar"), "foo*", true).fold());

        assertTrue(wildcardLike(l("Foobar"), "Fooba?", randomBoolean()).fold());
        assertTrue(wildcardLike(l("Foobar"), "fOoBa?", true).fold());
        assertFalse(wildcardLike(l("Foobar"), "foob?", randomBoolean()).fold());
        assertFalse(wildcardLike(l("Foobar"), "fooba?", false).fold());
        assertFalse(wildcardLike(l("Foobar"), "foob?", randomBoolean()).fold());
        assertTrue(wildcardLike(l("Foobar"), "Foo?ar", randomBoolean()).fold());

        assertTrue(wildcardLike(l("Foobar"), "F*ar", randomBoolean()).fold());
        assertFalse(wildcardLike(l("Foobar"), "f*Ar", false).fold());

        assertTrue(wildcardLike(l("Foo*"), "Foo\\*", randomBoolean()).fold());
        assertTrue(wildcardLike(l("Foo*"), "fOo\\*", true).fold());
        assertFalse(wildcardLike(l("Foo*"), "fOo\\*", false).fold());

        assertTrue(wildcardLike(l(randomAlphaOfLength(3)), "*", randomBoolean()).fold());
        assertTrue(wildcardLike(l(randomAlphaOfLength(3)), "???", randomBoolean()).fold());
        assertFalse(wildcardLike(l(randomAlphaOfLength(3)), "??", randomBoolean()).fold());
    }

    protected WildcardLike wildcardLike(Expression left, String right, boolean caseInsensitive) {
        return new WildcardLike(EMPTY, left, new WildcardPattern(right, caseInsensitive));
    }

    private static Literal l(Object value) {
        return TestUtils.of(EMPTY, value);
    }
}
