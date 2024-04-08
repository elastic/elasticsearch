/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.rest.yaml.section;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentLocation;

public class ContainsAssertionTests extends ESTestCase {

    public void testStringContains() {
        XContentLocation location = new XContentLocation(0, 0);
        ContainsAssertion containsAssertion = new ContainsAssertion(location, "field", "part");
        containsAssertion.doAssert("partial match", "l m");
        expectThrows(AssertionError.class, () -> containsAssertion.doAssert("partial match", "foo"));
    }
}
