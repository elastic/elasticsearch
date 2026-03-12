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
import java.nio.charset.StandardCharsets;

public class PolicyParserFailureTests extends ESTestCase {

    public void testParserSyntaxFailures() {
        PolicyParserException ppe = expectThrows(
            PolicyParserException.class,
            () -> new PolicyParser(new ByteArrayInputStream("[]".getBytes(StandardCharsets.UTF_8)), "test-failure-policy.yaml", false)
                .parsePolicy()
        );
        assertEquals("[1:1] policy parsing error for [test-failure-policy.yaml]: expected object <scope name>", ppe.getMessage());
    }

    public void testEntitlementDoesNotExist() {
        PolicyParserException ppe = expectThrows(PolicyParserException.class, () -> new PolicyParser(new ByteArrayInputStream("""
            entitlement-module-name:
              - does_not_exist: {}
            """.getBytes(StandardCharsets.UTF_8)), "test-failure-policy.yaml", false).parsePolicy());
        assertEquals(
            "[2:5] policy parsing error for [test-failure-policy.yaml] in scope [entitlement-module-name]: "
                + "unknown entitlement type [does_not_exist]",
            ppe.getMessage()
        );
    }

    public void testEntitlementMissingParameter() {
        PolicyParserException ppe = expectThrows(PolicyParserException.class, () -> new PolicyParser(new ByteArrayInputStream("""
            entitlement-module-name:
              - files:
                  - path: test-path
            """.getBytes(StandardCharsets.UTF_8)), "test-failure-policy.yaml", false).parsePolicy());
        assertEquals(
            "[2:5] policy parsing error for [test-failure-policy.yaml] in scope [entitlement-module-name] "
                + "for entitlement type [files]: files entitlement must contain 'mode' for every listed file",
            ppe.getMessage()
        );
    }

    public void testEntitlementMissingDependentParameter() {
        PolicyParserException ppe = expectThrows(PolicyParserException.class, () -> new PolicyParser(new ByteArrayInputStream("""
            entitlement-module-name:
              - files:
                  - relative_path: test-path
                    mode: read
            """.getBytes(StandardCharsets.UTF_8)), "test-failure-policy.yaml", false).parsePolicy());
        assertEquals(
            "[2:5] policy parsing error for [test-failure-policy.yaml] in scope [entitlement-module-name] "
                + "for entitlement type [files]: files entitlement with a 'relative_path' must specify 'relative_to'",
            ppe.getMessage()
        );
    }

    public void testEntitlementMutuallyExclusiveParameters() {
        PolicyParserException ppe = expectThrows(PolicyParserException.class, () -> new PolicyParser(new ByteArrayInputStream("""
            entitlement-module-name:
              - files:
                  - relative_path: test-path
                    path: test-path
                    mode: read
            """.getBytes(StandardCharsets.UTF_8)), "test-failure-policy.yaml", false).parsePolicy());
        assertEquals(
            "[2:5] policy parsing error for [test-failure-policy.yaml] in scope [entitlement-module-name] "
                + "for entitlement type [files]: a files entitlement entry must contain one of "
                + "[path, relative_path, path_setting]",
            ppe.getMessage()
        );
    }

    public void testEntitlementAtLeastOneParameter() {
        PolicyParserException ppe = expectThrows(PolicyParserException.class, () -> new PolicyParser(new ByteArrayInputStream("""
            entitlement-module-name:
              - files:
                  - mode: read
            """.getBytes(StandardCharsets.UTF_8)), "test-failure-policy.yaml", false).parsePolicy());
        assertEquals(
            "[2:5] policy parsing error for [test-failure-policy.yaml] in scope [entitlement-module-name] "
                + "for entitlement type [files]: a files entitlement entry must contain one of "
                + "[path, relative_path, path_setting]",
            ppe.getMessage()
        );
    }

    public void testEntitlementExtraneousParameter() {
        PolicyParserException ppe = expectThrows(PolicyParserException.class, () -> new PolicyParser(new ByteArrayInputStream("""
            entitlement-module-name:
              - files:
                  - path: test-path
                    mode: read
                    extra: test
            """.getBytes(StandardCharsets.UTF_8)), "test-failure-policy.yaml", false).parsePolicy());
        assertEquals(
            "[2:5] policy parsing error for [test-failure-policy.yaml] in scope [entitlement-module-name] "
                + "for entitlement type [files]: unknown key(s) [{extra=test}] in a listed file for files entitlement",
            ppe.getMessage()
        );
    }

    public void testEntitlementIsNotForExternalPlugins() {
        PolicyParserException ppe = expectThrows(PolicyParserException.class, () -> new PolicyParser(new ByteArrayInputStream("""
            entitlement-module-name:
              - create_class_loader
            """.getBytes(StandardCharsets.UTF_8)), "test-failure-policy.yaml", true).parsePolicy());
        assertEquals(
            "[2:5] policy parsing error for [test-failure-policy.yaml]: entitlement type [create_class_loader] is allowed only on modules",
            ppe.getMessage()
        );
    }
}
