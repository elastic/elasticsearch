/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.runtime.policy;

import org.elasticsearch.core.Strings;
import org.elasticsearch.entitlement.runtime.policy.entitlements.CreateClassLoaderEntitlement;
import org.elasticsearch.entitlement.runtime.policy.entitlements.Entitlement;
import org.elasticsearch.entitlement.runtime.policy.entitlements.FilesEntitlement;
import org.elasticsearch.entitlement.runtime.policy.entitlements.InboundNetworkEntitlement;
import org.elasticsearch.entitlement.runtime.policy.entitlements.LoadNativeLibrariesEntitlement;
import org.elasticsearch.entitlement.runtime.policy.entitlements.OutboundNetworkEntitlement;
import org.elasticsearch.entitlement.runtime.policy.entitlements.SetHttpsConnectionPropertiesEntitlement;
import org.elasticsearch.entitlement.runtime.policy.entitlements.WriteSystemPropertiesEntitlement;
import org.elasticsearch.test.ESTestCase;
import org.junit.BeforeClass;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;

public class PolicyParserTests extends ESTestCase {

    public static String TEST_ABSOLUTE_PATH_TO_FILE;

    @BeforeClass
    public static void beforeClass() throws IOException {
        TEST_ABSOLUTE_PATH_TO_FILE = createTempFile().toAbsolutePath().toString();
    }

    private static class TestWrongEntitlementName implements Entitlement {}

    public static class ManyConstructorsEntitlement implements Entitlement {
        @ExternalEntitlement
        public ManyConstructorsEntitlement(String s) {}

        @ExternalEntitlement
        public ManyConstructorsEntitlement(int i) {}
    }

    public static class ManyMethodsEntitlement implements Entitlement {
        @ExternalEntitlement
        public static ManyMethodsEntitlement create(String s) {
            return new ManyMethodsEntitlement();
        }

        @ExternalEntitlement
        public static ManyMethodsEntitlement create(int i) {
            return new ManyMethodsEntitlement();
        }
    }

    public static class ConstructorAndMethodEntitlement implements Entitlement {
        @ExternalEntitlement
        public static ConstructorAndMethodEntitlement create(String s) {
            return new ConstructorAndMethodEntitlement(s);
        }

        @ExternalEntitlement
        public ConstructorAndMethodEntitlement(String s) {}
    }

    public static class NonStaticMethodEntitlement implements Entitlement {
        @ExternalEntitlement
        public NonStaticMethodEntitlement create() {
            return new NonStaticMethodEntitlement();
        }
    }

    public void testBuildEntitlementNameFromClass() {
        assertEquals("create_class_loader", PolicyParser.buildEntitlementNameFromClass(CreateClassLoaderEntitlement.class));

        var ex = expectThrows(
            IllegalArgumentException.class,
            () -> PolicyParser.buildEntitlementNameFromClass(TestWrongEntitlementName.class)
        );
        assertThat(
            ex.getMessage(),
            equalTo("TestWrongEntitlementName is not a valid Entitlement class name. A valid class name must end with 'Entitlement'")
        );
    }

    private static InputStream createFilesTestPolicy() {
        return new ByteArrayInputStream(Strings.format("""
            entitlement-module-name:
              - files:
                - path: '%s'
                  mode: "read_write"
            """, TEST_ABSOLUTE_PATH_TO_FILE).getBytes(StandardCharsets.UTF_8));
    }

    public void testPolicyBuilder() throws IOException {
        Policy parsedPolicy = new PolicyParser(createFilesTestPolicy(), "test-policy.yaml", false).parsePolicy();
        Policy expected = new Policy(
            "test-policy.yaml",
            List.of(
                new Scope(
                    "entitlement-module-name",
                    List.of(FilesEntitlement.build(List.of(Map.of("path", TEST_ABSOLUTE_PATH_TO_FILE, "mode", "read_write"))))
                )
            )
        );
        assertEquals(expected, parsedPolicy);
    }

    public void testPolicyBuilderOnExternalPlugin() throws IOException {
        Policy parsedPolicy = new PolicyParser(createFilesTestPolicy(), "test-policy.yaml", true).parsePolicy();
        Policy expected = new Policy(
            "test-policy.yaml",
            List.of(
                new Scope(
                    "entitlement-module-name",
                    List.of(FilesEntitlement.build(List.of(Map.of("path", TEST_ABSOLUTE_PATH_TO_FILE, "mode", "read_write"))))
                )
            )
        );
        assertEquals(expected, parsedPolicy);
    }

    public void testParseFiles() throws IOException {
        Policy policyWithOnePath = new PolicyParser(createFilesTestPolicy(), "test-policy.yaml", false).parsePolicy();
        Policy expected = new Policy(
            "test-policy.yaml",
            List.of(
                new Scope(
                    "entitlement-module-name",
                    List.of(FilesEntitlement.build(List.of(Map.of("path", TEST_ABSOLUTE_PATH_TO_FILE, "mode", "read_write"))))
                )
            )
        );
        assertEquals(expected, policyWithOnePath);

        String testPathToReadDir = createTempDir().toAbsolutePath().toString();
        Policy policyWithTwoPaths = new PolicyParser(new ByteArrayInputStream(Strings.format("""
            entitlement-module-name:
              - files:
                - path: '%s'
                  mode: "read_write"
                - path: '%s'
                  mode: "read"
            """, TEST_ABSOLUTE_PATH_TO_FILE, testPathToReadDir).getBytes(StandardCharsets.UTF_8)), "test-policy.yaml", false).parsePolicy();
        expected = new Policy(
            "test-policy.yaml",
            List.of(
                new Scope(
                    "entitlement-module-name",
                    List.of(
                        FilesEntitlement.build(
                            List.of(
                                Map.of("path", TEST_ABSOLUTE_PATH_TO_FILE, "mode", "read_write"),
                                Map.of("path", testPathToReadDir, "mode", "read")
                            )
                        )
                    )
                )
            )
        );
        assertEquals(expected, policyWithTwoPaths);

        String relativePathToFile = Path.of("test/path/to/file").normalize().toString();
        String relativePathToDir = Path.of("test/path/to/read-dir/").normalize().toString();
        Policy policyWithMultiplePathsAndBaseDir = new PolicyParser(
            new ByteArrayInputStream(Strings.format("""
                entitlement-module-name:
                  - files:
                    - relative_path: '%s'
                      relative_to: "data"
                      mode: "read_write"
                    - relative_path: '%s'
                      relative_to: "config"
                      mode: "read"
                    - path: '%s'
                      mode: "read_write"
                    - path_setting: foo.bar
                      basedir_if_relative: config
                      mode: read
                """, relativePathToFile, relativePathToDir, TEST_ABSOLUTE_PATH_TO_FILE).getBytes(StandardCharsets.UTF_8)),
            "test-policy.yaml",
            false
        ).parsePolicy();
        expected = new Policy(
            "test-policy.yaml",
            List.of(
                new Scope(
                    "entitlement-module-name",
                    List.of(
                        FilesEntitlement.build(
                            List.of(
                                Map.of("relative_path", relativePathToFile, "mode", "read_write", "relative_to", "data"),
                                Map.of("relative_path", relativePathToDir, "mode", "read", "relative_to", "config"),
                                Map.of("path", TEST_ABSOLUTE_PATH_TO_FILE, "mode", "read_write"),
                                Map.of("path_setting", "foo.bar", "basedir_if_relative", "config", "mode", "read")
                            )
                        )
                    )
                )
            )
        );
        assertEquals(expected, policyWithMultiplePathsAndBaseDir);
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

    public void testVersionedPolicyParsing() throws IOException {
        var versionedPolicy = new ByteArrayInputStream("""
            versions:
              - x
            policy:
              entitlement-module-name:
                - load_native_libraries
              entitlement-module-name-2:
                - set_https_connection_properties
            """.getBytes(StandardCharsets.UTF_8));

        var policyParser = new PolicyParser(versionedPolicy, "test-policy.yaml", true);
        var parsedPolicy = policyParser.parseVersionedPolicy();

        Policy expectedPolicy = new Policy(
            "test-policy.yaml",
            List.of(
                new Scope("entitlement-module-name", List.of(new LoadNativeLibrariesEntitlement())),
                new Scope("entitlement-module-name-2", List.of(new SetHttpsConnectionPropertiesEntitlement()))
            )
        );
        assertEquals(expectedPolicy, parsedPolicy.policy());
        assertThat(parsedPolicy.versions(), contains("x"));
    }

    public void testVersionedPolicyParsingMultipleVersions() throws IOException {
        var versionedPolicy = new ByteArrayInputStream("""
            versions:
              - x
              - y
            policy:
              entitlement-module-name:
                - load_native_libraries
              entitlement-module-name-2:
                - set_https_connection_properties
            """.getBytes(StandardCharsets.UTF_8));

        var policyParser = new PolicyParser(versionedPolicy, "test-policy.yaml", true);
        var parsedPolicy = policyParser.parseVersionedPolicy();

        Policy expectedPolicy = new Policy(
            "test-policy.yaml",
            List.of(
                new Scope("entitlement-module-name", List.of(new LoadNativeLibrariesEntitlement())),
                new Scope("entitlement-module-name-2", List.of(new SetHttpsConnectionPropertiesEntitlement()))
            )
        );
        assertEquals(expectedPolicy, parsedPolicy.policy());
        assertThat(parsedPolicy.versions(), contains("x", "y"));
    }

    public void testVersionedPolicyParsingAnyFieldOrder() throws IOException {
        var versionedPolicy = new ByteArrayInputStream("""
            policy:
              entitlement-module-name:
                - load_native_libraries
              entitlement-module-name-2:
                - set_https_connection_properties
            versions:
              - x
              - y
            """.getBytes(StandardCharsets.UTF_8));

        var policyParser = new PolicyParser(versionedPolicy, "test-policy.yaml", true);
        var parsedPolicy = policyParser.parseVersionedPolicy();

        Policy expectedPolicy = new Policy(
            "test-policy.yaml",
            List.of(
                new Scope("entitlement-module-name", List.of(new LoadNativeLibrariesEntitlement())),
                new Scope("entitlement-module-name-2", List.of(new SetHttpsConnectionPropertiesEntitlement()))
            )
        );
        assertEquals(expectedPolicy, parsedPolicy.policy());
        assertThat(parsedPolicy.versions(), contains("x", "y"));
    }

    public void testVersionedPolicyParsingEmptyPolicy() throws IOException {
        var versionedPolicy = new ByteArrayInputStream("""
            versions:
              - x
              - y
            """.getBytes(StandardCharsets.UTF_8));

        var policyParser = new PolicyParser(versionedPolicy, "test-policy.yaml", true);
        var parsedPolicy = policyParser.parseVersionedPolicy();

        Policy expectedPolicy = new Policy("test-policy.yaml", List.of());
        assertEquals(expectedPolicy, parsedPolicy.policy());
        assertThat(parsedPolicy.versions(), contains("x", "y"));
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

    public void testMultipleMethodsAnnotated() throws IOException {
        var parser = new PolicyParser(new ByteArrayInputStream("""
            entitlement-module-name:
              - many_methods
            """.getBytes(StandardCharsets.UTF_8)), "test-policy.yaml", true, Map.of("many_methods", ManyMethodsEntitlement.class));

        var e = expectThrows(IllegalStateException.class, parser::parsePolicy);
        assertThat(
            e.getMessage(),
            equalTo(
                "entitlement class "
                    + "[org.elasticsearch.entitlement.runtime.policy.PolicyParserTests$ManyMethodsEntitlement]"
                    + " has more than one constructor and/or method annotated with ExternalEntitlement"
            )
        );
    }

    public void testConstructorAndMethodAnnotated() throws IOException {
        var parser = new PolicyParser(
            new ByteArrayInputStream("""
                entitlement-module-name:
                  - constructor_and_method
                """.getBytes(StandardCharsets.UTF_8)),
            "test-policy.yaml",
            true,
            Map.of("constructor_and_method", ConstructorAndMethodEntitlement.class)
        );

        var e = expectThrows(IllegalStateException.class, parser::parsePolicy);
        assertThat(
            e.getMessage(),
            equalTo(
                "entitlement class "
                    + "[org.elasticsearch.entitlement.runtime.policy.PolicyParserTests$ConstructorAndMethodEntitlement]"
                    + " has more than one constructor and/or method annotated with ExternalEntitlement"
            )
        );
    }

    public void testNonStaticMethodAnnotated() throws IOException {
        var parser = new PolicyParser(new ByteArrayInputStream("""
            entitlement-module-name:
              - non_static
            """.getBytes(StandardCharsets.UTF_8)), "test-policy.yaml", true, Map.of("non_static", NonStaticMethodEntitlement.class));

        var e = expectThrows(IllegalStateException.class, parser::parsePolicy);
        assertThat(
            e.getMessage(),
            equalTo(
                "entitlement class "
                    + "[org.elasticsearch.entitlement.runtime.policy.PolicyParserTests$NonStaticMethodEntitlement]"
                    + " has non-static method annotated with ExternalEntitlement"
            )
        );
    }
}
