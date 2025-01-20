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
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;

public class PolicyParserTests extends ESTestCase {

    private static class TestWrongEntitlementName implements Entitlement {}

    public void testGetEntitlementTypeName() {
        assertEquals("create_class_loader", PolicyParser.getEntitlementTypeName(CreateClassLoaderEntitlement.class));

        var ex = expectThrows(IllegalArgumentException.class, () -> PolicyParser.getEntitlementTypeName(TestWrongEntitlementName.class));
        assertThat(
            ex.getMessage(),
            equalTo("TestWrongEntitlementName is not a valid Entitlement class name. A valid class name must end with 'Entitlement'")
        );
    }

    public void testPolicyBuilder() throws IOException {
        Policy parsedPolicy = new PolicyParser(PolicyParserTests.class.getResourceAsStream("test-policy.yaml"), "test-policy.yaml", false)
            .parsePolicy();
        Policy expected = new Policy(
            "test-policy.yaml",
            List.of(new Scope("entitlement-module-name", List.of(new FileEntitlement("test/path/to/file", List.of("read", "write")))))
        );
        assertEquals(expected, parsedPolicy);
    }

    public void testPolicyBuilderOnExternalPlugin() throws IOException {
        Policy parsedPolicy = new PolicyParser(PolicyParserTests.class.getResourceAsStream("test-policy.yaml"), "test-policy.yaml", true)
            .parsePolicy();
        Policy expected = new Policy(
            "test-policy.yaml",
            List.of(new Scope("entitlement-module-name", List.of(new FileEntitlement("test/path/to/file", List.of("read", "write")))))
        );
        assertEquals(expected, parsedPolicy);
    }

    public void testParseNetwork() throws IOException {
        Policy parsedPolicy = new PolicyParser(new ByteArrayInputStream("""
            entitlement-module-name:
              - network:
                  actions:
                    - listen
                    - accept
                    - connect
            """.getBytes(StandardCharsets.UTF_8)), "test-policy.yaml", false).parsePolicy();
        Policy expected = new Policy(
            "test-policy.yaml",
            List.of(new Scope("entitlement-module-name", List.of(new NetworkEntitlement(List.of("listen", "accept", "connect")))))
        );
        assertEquals(expected, parsedPolicy);
    }

    public void testParseWriteProperties() throws IOException {
        Policy parsedPolicy = new PolicyParser(new ByteArrayInputStream("""
            entitlement-module-name:
              - write_properties:
                  properties:
                    - es.property1
                    - es.property2
            """.getBytes(StandardCharsets.UTF_8)), "test-policy.yaml", false).parsePolicy();
        Policy expected = new Policy(
            "test-policy.yaml",
            List.of(new Scope("entitlement-module-name", List.of(new WritePropertiesEntitlement(Set.of("es.property1", "es.property2")))))
        );
        assertEquals(expected, parsedPolicy);
    }

    public void testParseWriteAllProperties() throws IOException {
        Policy parsedPolicy = new PolicyParser(new ByteArrayInputStream("""
            entitlement-module-name:
              - write_all_properties
            """.getBytes(StandardCharsets.UTF_8)), "test-policy.yaml", false).parsePolicy();
        Policy expected = new Policy(
            "test-policy.yaml",
            List.of(new Scope("entitlement-module-name", List.of(new WriteAllPropertiesEntitlement())))
        );
        assertEquals(expected, parsedPolicy);
    }

    public void testParseNetworkIllegalAction() throws IOException {
        var ex = expectThrows(PolicyParserException.class, () -> new PolicyParser(new ByteArrayInputStream("""
            entitlement-module-name:
              - network:
                  actions:
                    - listen
                    - doesnotexist
                    - connect
            """.getBytes(StandardCharsets.UTF_8)), "test-policy.yaml", false).parsePolicy());
        assertThat(
            ex.getMessage(),
            equalTo(
                "[2:5] policy parsing error for [test-policy.yaml] in scope [entitlement-module-name] for entitlement type [network]: "
                    + "unknown network action [doesnotexist]"
            )
        );
    }

    public void testParseCreateClassloader() throws IOException {
        Policy parsedPolicy = new PolicyParser(new ByteArrayInputStream("""
            entitlement-module-name:
              - create_class_loader
            """.getBytes(StandardCharsets.UTF_8)), "test-policy.yaml", false).parsePolicy();
        Policy expected = new Policy(
            "test-policy.yaml",
            List.of(new Scope("entitlement-module-name", List.of(new CreateClassLoaderEntitlement())))
        );
        assertEquals(expected, parsedPolicy);
    }

    public void testParseSetHttpsConnectionProperties() throws IOException {
        Policy parsedPolicy = new PolicyParser(new ByteArrayInputStream("""
            entitlement-module-name:
              - set_https_connection_properties
            """.getBytes(StandardCharsets.UTF_8)), "test-policy.yaml", true).parsePolicy();
        Policy expected = new Policy(
            "test-policy.yaml",
            List.of(new Scope("entitlement-module-name", List.of(new SetHttpsConnectionPropertiesEntitlement())))
        );
        assertEquals(expected, parsedPolicy);
    }
}
