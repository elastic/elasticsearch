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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.elasticsearch.test.LambdaMatchers.transformedMatch;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class PolicyParserTests extends ESTestCase {

    public void testGetEntitlementClassName() {
        assertEquals("CreateClassloaderEntitlement", PolicyParser.getEntitlementClassName("create classloader"));
        assertEquals("CreateClassloaderEntitlement", PolicyParser.getEntitlementClassName("create_classloader"));
        assertEquals("CreateClassloaderEntitlement", PolicyParser.getEntitlementClassName("create-classloader"));
        assertEquals("MultiNameActionEntitlement", PolicyParser.getEntitlementClassName("multi name action"));
    }

    public void testPolicyBuilder() throws IOException {
        Policy parsedPolicy = new PolicyParser(PolicyParserTests.class.getResourceAsStream("test-policy.yaml"), "test-policy.yaml")
            .parsePolicy();
        Policy builtPolicy = new Policy(
            "test-policy.yaml",
            List.of(new Scope("entitlement-module-name", List.of(new FileEntitlement("test/path/to/file", List.of("read", "write")))))
        );
        assertEquals(parsedPolicy, builtPolicy);
    }

    public void testParseCreateClassloader() throws IOException {
        Policy parsedPolicy = new PolicyParser(new ByteArrayInputStream("""
            entitlement-module-name:
              - create_classloader
            """.getBytes(StandardCharsets.UTF_8)), "test-policy.yaml").parsePolicy();
        Policy builtPolicy = new Policy(
            "test-policy.yaml",
            List.of(new Scope("entitlement-module-name", List.of(new CreateClassloaderEntitlement())))
        );
        assertThat(
            parsedPolicy.scopes,
            contains(
                both(transformedMatch((Scope scope) -> scope.name, equalTo("entitlement-module-name"))).and(
                    transformedMatch(scope -> scope.entitlements, contains(instanceOf(CreateClassloaderEntitlement.class)))
                )
            )
        );
    }
}
