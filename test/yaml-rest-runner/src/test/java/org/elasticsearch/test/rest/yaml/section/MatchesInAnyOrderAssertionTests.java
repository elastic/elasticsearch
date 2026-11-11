/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.rest.yaml.section;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentLocation;

import java.util.List;

import static java.util.Collections.singletonMap;
import static org.hamcrest.Matchers.containsString;

public class MatchesInAnyOrderAssertionTests extends ESTestCase {

    public void testMatchesInAnyOrderStrings() {
        XContentLocation location = new XContentLocation(0, 0);
        MatchesInAnyOrderAssertion assertion = new MatchesInAnyOrderAssertion(location, "field", List.of("b", "a"));
        assertion.doAssert(List.of("a", "b"), List.of("b", "a"));
        assertion.doAssert(List.of("a", "b", "a"), List.of("a", "a", "b"));
    }

    public void testMatchesInAnyOrderNumbers() {
        XContentLocation location = new XContentLocation(0, 0);
        MatchesInAnyOrderAssertion assertion = new MatchesInAnyOrderAssertion(location, "field", List.of(2, 1));
        assertion.doAssert(List.of(1, 2), List.of(2, 1));
        assertion.doAssert(List.of(1.0, 2), List.of(2, 1));
    }

    public void testMatchesInAnyOrderMaps() {
        XContentLocation location = new XContentLocation(0, 0);
        MatchesInAnyOrderAssertion assertion = new MatchesInAnyOrderAssertion(
            location,
            "field",
            List.of(singletonMap("_id", "2"), singletonMap("_id", "1"))
        );
        assertion.doAssert(
            List.of(singletonMap("_id", "1"), singletonMap("_id", "2")),
            List.of(singletonMap("_id", "2"), singletonMap("_id", "1"))
        );
    }

    public void testMatchesInAnyOrderScalars() {
        XContentLocation location = new XContentLocation(0, 0);
        MatchesInAnyOrderAssertion assertion = new MatchesInAnyOrderAssertion(location, "field", "foo");
        assertion.doAssert("foo", "foo");
        assertion.doAssert(1, 1.0);
        expectThrows(AssertionError.class, () -> assertion.doAssert("bar", "foo"));
    }

    public void testMatchesInAnyOrderFailsWhenDifferent() {
        XContentLocation location = new XContentLocation(0, 0);
        MatchesInAnyOrderAssertion assertion = new MatchesInAnyOrderAssertion(location, "field", List.of("a", "b"));
        expectThrows(AssertionError.class, () -> assertion.doAssert(List.of("a", "c"), List.of("a", "b")));
    }

    public void testMatchesInAnyOrderFailsWhenActualIsNotAList() {
        XContentLocation location = new XContentLocation(0, 0);
        MatchesInAnyOrderAssertion assertion = new MatchesInAnyOrderAssertion(location, "field", List.of("a"));
        AssertionError error = expectThrows(AssertionError.class, () -> assertion.doAssert("not a list", List.of("a")));
        assertEquals("field [field] expected to be a list but was [not a list] of type [class java.lang.String]", error.getMessage());
    }

    public void testMatchesInAnyOrderFailsWhenExpectedIsNotAList() {
        XContentLocation location = new XContentLocation(0, 0);
        MatchesInAnyOrderAssertion assertion = new MatchesInAnyOrderAssertion(location, "field", "not a list");
        AssertionError error = expectThrows(AssertionError.class, () -> assertion.doAssert(List.of("a"), "not a list"));
        assertEquals(
            "field [field] expected value must be a list but was [not a list] of type [class java.lang.String]",
            error.getMessage()
        );
    }

    public void testMatchesInAnyOrderFailsWhenUnexpectedValuesRemain() {
        XContentLocation location = new XContentLocation(0, 0);
        MatchesInAnyOrderAssertion assertion = new MatchesInAnyOrderAssertion(location, "field", List.of("a"));
        AssertionError error = expectThrows(AssertionError.class, () -> assertion.doAssert(List.of("a", "b"), List.of("a")));
        assertEquals(
            "field [field] expected to contain the same values in any order but contained unexpected values [b] in [a, b]",
            error.getMessage()
        );
    }

    public void testMatchesInAnyOrderFailsWhenActualIsShorterThanExpected() {
        XContentLocation location = new XContentLocation(0, 0);
        MatchesInAnyOrderAssertion assertion = new MatchesInAnyOrderAssertion(location, "field", List.of("a", "b"));
        AssertionError error = expectThrows(AssertionError.class, () -> assertion.doAssert(List.of("a"), List.of("a", "b")));
        assertThat(
            error.getMessage(),
            containsString("field [field] expected to contain the same values in any order but no value matched [b]")
        );
    }
}
