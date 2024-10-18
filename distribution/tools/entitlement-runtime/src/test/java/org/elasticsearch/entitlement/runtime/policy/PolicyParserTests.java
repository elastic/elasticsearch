/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.runtime.policy;

import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.List;

public class PolicyParserTests extends ESTestCase {

    public void testPolicyBuilder() throws IOException {
        Policy parsedPolicy = new PolicyParser(PolicyParserTests.class.getResourceAsStream("test-policy.yaml"), "test-policy.yaml")
            .parsePolicy();
        Policy builtPolicy = new Policy(
            "test-policy.yaml",
            List.of(new Scope("entitlement-module-name", List.of(new FileEntitlement("test/path/to/file", List.of("read", "write")))))
        );
        assertEquals(parsedPolicy, builtPolicy);
    }
}
