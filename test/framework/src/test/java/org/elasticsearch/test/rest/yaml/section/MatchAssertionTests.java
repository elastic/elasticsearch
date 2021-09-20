/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.test.rest.yaml.section;

import org.elasticsearch.common.xcontent.XContentLocation;
import org.elasticsearch.test.ESTestCase;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.hamcrest.Matchers.containsString;

public class MatchAssertionTests extends ESTestCase  {

    public void testNull() {
        XContentLocation xContentLocation = new XContentLocation(0, 0);
        {
            MatchAssertion matchAssertion = new MatchAssertion(xContentLocation, "field", null);
            matchAssertion.doAssert(null, null);
            expectThrows(AssertionError.class, () -> matchAssertion.doAssert("non-null", null));
        }
        {
            MatchAssertion matchAssertion = new MatchAssertion(xContentLocation, "field", "non-null");
            expectThrows(AssertionError.class, () -> matchAssertion.doAssert(null, "non-null"));
        }
        {
            MatchAssertion matchAssertion = new MatchAssertion(xContentLocation, "field", "/exp/");
            expectThrows(AssertionError.class, () -> matchAssertion.doAssert(null, "/exp/"));
        }
    }

    public void testNullInMap() {
        XContentLocation xContentLocation = new XContentLocation(0, 0);
        MatchAssertion matchAssertion = new MatchAssertion(xContentLocation, "field", singletonMap("a", null));
        matchAssertion.doAssert(singletonMap("a", null), matchAssertion.getExpectedValue());
        AssertionError e = expectThrows(AssertionError.class, () ->
            matchAssertion.doAssert(emptyMap(), matchAssertion.getExpectedValue()));
        assertThat(e.getMessage(), containsString("Expected a map containing\na: expected null but was <missing>"));
    }
}
