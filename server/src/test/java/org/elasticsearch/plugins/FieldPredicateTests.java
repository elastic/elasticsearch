/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.plugins;

import org.elasticsearch.test.ESTestCase;

import java.util.function.Predicate;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;

public class FieldPredicateTests extends ESTestCase {

    public void testAcceptAll() {
        assertThat(FieldPredicate.ACCEPT_ALL.test(randomAlphaOfLengthBetween(0, 10)), is(true));
        String hash = randomAlphaOfLength(10);
        assertThat(FieldPredicate.ACCEPT_ALL.modifyHash(hash), equalTo(hash));
        assertThat(FieldPredicate.ACCEPT_ALL.ramBytesUsed(), equalTo(0L));
    }

    public void testAnd() {
        FieldPredicate and = new FieldPredicate.And(namedPredicate("first", f -> f.startsWith("a")), namedPredicate("second", f -> {
            throw new AssertionError("second predicate must not be consulted when the first one rejects");
        }));
        assertThat(and.test("bcd"), is(false));

        and = new FieldPredicate.And(namedPredicate("first", f -> f.startsWith("a")), namedPredicate("second", f -> f.endsWith("z")));
        assertThat(and.test("abz"), is(true));
        assertThat(and.test("abc"), is(false));
        assertThat(and.test("xyz"), is(false));
        assertThat(and.modifyHash("hash"), equalTo("hash:first:second"));
        assertThat(and.toString(), equalTo("first then second"));
        assertThat(and.ramBytesUsed(), greaterThan(0L));
    }

    public void testOr() {
        FieldPredicate or = new FieldPredicate.Or(namedPredicate("first", f -> f.startsWith("a")), namedPredicate("second", f -> {
            throw new AssertionError("second predicate must not be consulted when the first one matches");
        }));
        assertThat(or.test("abc"), is(true));

        or = new FieldPredicate.Or(namedPredicate("first", f -> f.startsWith("a")), namedPredicate("second", f -> f.endsWith("z")));
        assertThat(or.test("abc"), is(true));
        assertThat(or.test("xyz"), is(true));
        assertThat(or.test("abz"), is(true));
        assertThat(or.test("bcd"), is(false));
        assertThat(or.modifyHash("hash"), equalTo("hash:first:second"));
        assertThat(or.toString(), equalTo("first or second"));
        assertThat(or.ramBytesUsed(), greaterThan(0L));
    }

    private static FieldPredicate namedPredicate(String name, Predicate<String> predicate) {
        return new FieldPredicate() {
            @Override
            public boolean test(String field) {
                return predicate.test(field);
            }

            @Override
            public String modifyHash(String hash) {
                return hash + ":" + name;
            }

            @Override
            public long ramBytesUsed() {
                return 0;
            }

            @Override
            public String toString() {
                return name;
            }
        };
    }
}
