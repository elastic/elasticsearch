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

public class PolicyParserFailureTests extends ESTestCase {

    public void testParserSyntaxFailures() {
        PolicyParserException ppe = expectThrows(
            PolicyParserException.class,
            () -> new PolicyParser(new ByteArrayInputStream("[]".getBytes(StandardCharsets.UTF_8)), "test-failure-policy.yaml")
                .parsePolicy()
        );
        assertEquals("[1:1] policy parsing error for [test-failure-policy.yaml]: expected object <scope name>", ppe.getMessage());
    }

    public void testEntitlementDoesNotExist() throws IOException {
        PolicyParserException ppe = expectThrows(PolicyParserException.class, () -> new PolicyParser(new ByteArrayInputStream("""
            entitlement-module-name:
              entitlements:
                - does_not_exist: {}
            """.getBytes(StandardCharsets.UTF_8)), "test-failure-policy.yaml").parsePolicy());
        assertEquals(
            "[3:7] policy parsing error for [test-failure-policy.yaml] in scope [entitlement-module-name]: "
                + "unknown entitlement type [does_not_exist]",
            ppe.getMessage()
        );
    }

    public void testEntitlementMissingParameter() throws IOException {
        PolicyParserException ppe = expectThrows(PolicyParserException.class, () -> new PolicyParser(new ByteArrayInputStream("""
            entitlement-module-name:
              entitlements:
                - file: {}
            """.getBytes(StandardCharsets.UTF_8)), "test-failure-policy.yaml").parsePolicy());
        assertEquals(
            "[3:14] policy parsing error for [test-failure-policy.yaml] in scope [entitlement-module-name] "
                + "for entitlement type [file]: missing entitlement parameter [path]",
            ppe.getMessage()
        );

        ppe = expectThrows(PolicyParserException.class, () -> new PolicyParser(new ByteArrayInputStream("""
            entitlement-module-name:
              entitlements:
                - file:
                    path: test-path
            """.getBytes(StandardCharsets.UTF_8)), "test-failure-policy.yaml").parsePolicy());
        assertEquals(
            "[5:1] policy parsing error for [test-failure-policy.yaml] in scope [entitlement-module-name] "
                + "for entitlement type [file]: missing entitlement parameter [actions]",
            ppe.getMessage()
        );
    }

    public void testEntitlementExtraneousParameter() throws IOException {
        PolicyParserException ppe = expectThrows(PolicyParserException.class, () -> new PolicyParser(new ByteArrayInputStream("""
            entitlement-module-name:
              entitlements:
                - file:
                    path: test-path
                    actions:
                      - read
                    extra: test
            """.getBytes(StandardCharsets.UTF_8)), "test-failure-policy.yaml").parsePolicy());
        assertEquals(
            "[8:1] policy parsing error for [test-failure-policy.yaml] in scope [entitlement-module-name] "
                + "for entitlement type [file]: extraneous entitlement parameter(s) {extra=test}",
            ppe.getMessage()
        );
    }
}
