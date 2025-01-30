/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.runtime.policy;

import org.elasticsearch.entitlement.runtime.policy.entitlements.CreateClassLoaderEntitlement;
import org.elasticsearch.entitlement.runtime.policy.entitlements.Entitlement;
import org.elasticsearch.entitlement.runtime.policy.entitlements.FileEntitlement;
import org.elasticsearch.entitlement.runtime.policy.entitlements.InboundNetworkEntitlement;
import org.elasticsearch.entitlement.runtime.policy.entitlements.LoadNativeLibrariesEntitlement;
import org.elasticsearch.entitlement.runtime.policy.entitlements.OutboundNetworkEntitlement;
import org.elasticsearch.entitlement.runtime.policy.entitlements.SetHttpsConnectionPropertiesEntitlement;
import org.elasticsearch.entitlement.runtime.policy.entitlements.WriteSystemPropertiesEntitlement;
import org.elasticsearch.test.ESTestCase;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;

public class PolicyParserTests extends ESTestCase {

    private static class TestWrongEntitlementName implements Entitlement {}

    public static class ManyConstructorsEntitlement implements Entitlement {
        @ExternalEntitlement
        public ManyConstructorsEntitlement(String s) {}

        @ExternalEntitlement
        public ManyConstructorsEntitlement(int i) {}
    }

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
            List.of(new Scope("entitlement-module-name", List.of(new FileEntitlement("test/path/to/file", "read_write"))))
        );
        assertEquals(expected, parsedPolicy);
    }

    public void testPolicyBuilderOnExternalPlugin() throws IOException {
        Policy parsedPolicy = new PolicyParser(PolicyParserTests.class.getResourceAsStream("test-policy.yaml"), "test-policy.yaml", true)
            .parsePolicy();
        Policy expected = new Policy(
            "test-policy.yaml",
            List.of(new Scope("entitlement-module-name", List.of(new FileEntitlement("test/path/to/file", "read_write"))))
        );
        assertEquals(expected, parsedPolicy);
    }

    public void testParseNetwork() throws IOException {
        Policy parsedPolicy = new PolicyParser(new ByteArrayInputStream("""
            entitlement-module-name:
              - inbound_network
            """.getBytes(StandardCharsets.UTF_8)), "test-policy.yaml", false).parsePolicy();
        Policy expected = new Policy(
            "test-policy.yaml",
            List.of(new Scope("entitlement-module-name", List.of(new InboundNetworkEntitlement())))
        );
        assertEquals(expected, parsedPolicy);

        parsedPolicy = new PolicyParser(new ByteArrayInputStream("""
            entitlement-module-name:
              - outbound_network
            """.getBytes(StandardCharsets.UTF_8)), "test-policy.yaml", false).parsePolicy();
        expected = new Policy("test-policy.yaml", List.of(new Scope("entitlement-module-name", List.of(new OutboundNetworkEntitlement()))));
        assertEquals(expected, parsedPolicy);

        parsedPolicy = new PolicyParser(new ByteArrayInputStream("""
            entitlement-module-name:
              - outbound_network
              - inbound_network
            """.getBytes(StandardCharsets.UTF_8)), "test-policy.yaml", false).parsePolicy();
        expected = new Policy(
            "test-policy.yaml",
            List.of(new Scope("entitlement-module-name", List.of(new OutboundNetworkEntitlement(), new InboundNetworkEntitlement())))
        );
        assertEquals(expected, parsedPolicy);
    }

    public void testParseWriteSystemProperties() throws IOException {
        Policy parsedPolicy = new PolicyParser(new ByteArrayInputStream("""
            entitlement-module-name:
              - write_system_properties:
                  properties:
                    - es.property1
                    - es.property2
            """.getBytes(StandardCharsets.UTF_8)), "test-policy.yaml", false).parsePolicy();
        Policy expected = new Policy(
            "test-policy.yaml",
            List.of(
                new Scope("entitlement-module-name", List.of(new WriteSystemPropertiesEntitlement(Set.of("es.property1", "es.property2"))))
            )
        );
        assertEquals(expected, parsedPolicy);
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

    public void testParseLoadNativeLibraries() throws IOException {
        Policy parsedPolicy = new PolicyParser(new ByteArrayInputStream("""
            entitlement-module-name:
              - load_native_libraries
            """.getBytes(StandardCharsets.UTF_8)), "test-policy.yaml", true).parsePolicy();
        Policy expected = new Policy(
            "test-policy.yaml",
            List.of(new Scope("entitlement-module-name", List.of(new LoadNativeLibrariesEntitlement())))
        );
        assertEquals(expected, parsedPolicy);
    }

    public void testMultipleConstructorsAnnotated() throws IOException {
        var parser = new PolicyParser(
            new ByteArrayInputStream("""
                entitlement-module-name:
                  - many_constructors
                """.getBytes(StandardCharsets.UTF_8)),
            "test-policy.yaml",
            true,
            Map.of("many_constructors", ManyConstructorsEntitlement.class)
        );

        var e = expectThrows(IllegalStateException.class, parser::parsePolicy);
        assertThat(
            e.getMessage(),
            equalTo(
                "entitlement class "
                    + "[org.elasticsearch.entitlement.runtime.policy.PolicyParserTests$ManyConstructorsEntitlement]"
                    + " has more than one constructor annotated with ExternalEntitlement"
            )
        );
    }
}
