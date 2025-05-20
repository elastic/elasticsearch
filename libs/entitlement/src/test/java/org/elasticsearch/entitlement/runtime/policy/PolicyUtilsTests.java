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
import org.elasticsearch.entitlement.runtime.policy.entitlements.FilesEntitlement;
import org.elasticsearch.entitlement.runtime.policy.entitlements.InboundNetworkEntitlement;
import org.elasticsearch.entitlement.runtime.policy.entitlements.LoadNativeLibrariesEntitlement;
import org.elasticsearch.entitlement.runtime.policy.entitlements.ManageThreadsEntitlement;
import org.elasticsearch.entitlement.runtime.policy.entitlements.OutboundNetworkEntitlement;
import org.elasticsearch.entitlement.runtime.policy.entitlements.SetHttpsConnectionPropertiesEntitlement;
import org.elasticsearch.entitlement.runtime.policy.entitlements.WriteAllSystemPropertiesEntitlement;
import org.elasticsearch.entitlement.runtime.policy.entitlements.WriteSystemPropertiesEntitlement;
import org.elasticsearch.test.ESTestCase;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Base64;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.entitlement.runtime.policy.entitlements.FilesEntitlement.SEPARATOR;
import static org.elasticsearch.test.LambdaMatchers.transformedMatch;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

@ESTestCase.WithoutSecurityManager
public class PolicyUtilsTests extends ESTestCase {

    public void testCreatePluginPolicyWithPatch() {

        var policyPatch = """
            versions:
              - 9.0.0
              - 9.0.0-SNAPSHOT
            policy:
              entitlement-module-name:
                - load_native_libraries
              entitlement-module-name-2:
                - set_https_connection_properties
            """;
        var base64EncodedPolicy = new String(
            Base64.getEncoder().encode(policyPatch.getBytes(StandardCharsets.UTF_8)),
            StandardCharsets.UTF_8
        );
        final Policy expectedPolicy = new Policy(
            "test-plugin",
            List.of(
                new Scope("entitlement-module-name", List.of(new LoadNativeLibrariesEntitlement())),
                new Scope("entitlement-module-name-2", List.of(new SetHttpsConnectionPropertiesEntitlement()))
            )
        );

        var policy = PolicyUtils.parseEncodedPolicyIfExists(
            base64EncodedPolicy,
            "9.0.0",
            true,
            "test-plugin",
            Set.of("entitlement-module-name", "entitlement-module-name-2")
        );

        assertThat(policy, equalTo(expectedPolicy));
    }

    public void testCreatePluginPolicyWithPatchAnyVersion() {

        var policyPatch = """
            policy:
              entitlement-module-name:
                - load_native_libraries
              entitlement-module-name-2:
                - set_https_connection_properties
            """;
        var base64EncodedPolicy = new String(
            Base64.getEncoder().encode(policyPatch.getBytes(StandardCharsets.UTF_8)),
            StandardCharsets.UTF_8
        );

        final Policy expectedPolicy = new Policy(
            "test-plugin",
            List.of(
                new Scope("entitlement-module-name", List.of(new LoadNativeLibrariesEntitlement())),
                new Scope("entitlement-module-name-2", List.of(new SetHttpsConnectionPropertiesEntitlement()))
            )
        );

        var policy = PolicyUtils.parseEncodedPolicyIfExists(
            base64EncodedPolicy,
            "abcdef",
            true,
            "test-plugin",
            Set.of("entitlement-module-name", "entitlement-module-name-2")
        );

        assertThat(policy, equalTo(expectedPolicy));
    }

    public void testNoPatchWithVersionMismatch() {

        var policyPatch = """
            versions:
              - 9.0.0
              - 9.0.0-SNAPSHOT
            policy:
              entitlement-module-name:
                - load_native_libraries
              entitlement-module-name-2:
                - set_https_connection_properties
            """;
        var base64EncodedPolicy = new String(
            Base64.getEncoder().encode(policyPatch.getBytes(StandardCharsets.UTF_8)),
            StandardCharsets.UTF_8
        );

        var policy = PolicyUtils.parseEncodedPolicyIfExists(
            base64EncodedPolicy,
            "9.1.0",
            true,
            "test-plugin",
            Set.of("entitlement-module-name", "entitlement-module-name-2")
        );

        assertThat(policy, nullValue());
    }

    public void testNoPatchWithValidationError() {

        // Nonexistent module names
        var policyPatch = """
            versions:
              - 9.0.0
              - 9.0.0-SNAPSHOT
            policy:
              entitlement-module-name:
                - load_native_libraries
              entitlement-module-name-2:
                - set_https_connection_properties
            """;
        var base64EncodedPolicy = new String(
            Base64.getEncoder().encode(policyPatch.getBytes(StandardCharsets.UTF_8)),
            StandardCharsets.UTF_8
        );

        assertThrows(
            IllegalStateException.class,
            () -> PolicyUtils.parseEncodedPolicyIfExists(base64EncodedPolicy, "9.0.0", true, "test-plugin", Set.of())
        );
    }

    public void testNoPatchWithParsingError() {

        // no <version> or <policy> field
        var policyPatch = """
            entitlement-module-name:
              - load_native_libraries
            entitlement-module-name-2:
              - set_https_connection_properties
            """;
        var base64EncodedPolicy = new String(
            Base64.getEncoder().encode(policyPatch.getBytes(StandardCharsets.UTF_8)),
            StandardCharsets.UTF_8
        );

        assertThrows(
            IllegalStateException.class,
            () -> PolicyUtils.parseEncodedPolicyIfExists(base64EncodedPolicy, "9.0.0", true, "test-plugin", Set.of())
        );
    }

    public void testMergeScopes() {
        var originalPolicy = List.of(
            new Scope("module1", List.of(new LoadNativeLibrariesEntitlement())),
            new Scope("module2", List.of(new ManageThreadsEntitlement())),
            new Scope("module3", List.of(new InboundNetworkEntitlement()))
        );

        var patchPolicy = List.of(
            new Scope("module2", List.of(new ManageThreadsEntitlement())),
            new Scope("module3", List.of(new OutboundNetworkEntitlement())),
            new Scope("module4", List.of(new WriteAllSystemPropertiesEntitlement()))
        );

        var resultPolicy = PolicyUtils.mergeScopes(originalPolicy, patchPolicy);
        assertThat(
            resultPolicy,
            containsInAnyOrder(
                equalTo(new Scope("module1", List.of(new LoadNativeLibrariesEntitlement()))),
                equalTo(new Scope("module2", List.of(new ManageThreadsEntitlement()))),
                both(transformedMatch(Scope::moduleName, equalTo("module3"))).and(
                    transformedMatch(
                        Scope::entitlements,
                        containsInAnyOrder(new InboundNetworkEntitlement(), new OutboundNetworkEntitlement())
                    )
                ),
                equalTo(new Scope("module4", List.of(new WriteAllSystemPropertiesEntitlement())))
            )
        );
    }

    public void testMergeSameFlagEntitlement() {
        var e1 = new InboundNetworkEntitlement();
        var e2 = new InboundNetworkEntitlement();

        assertThat(PolicyUtils.mergeEntitlement(e1, e2), equalTo(new InboundNetworkEntitlement()));
    }

    public void testMergeFilesEntitlement() {
        var e1 = new FilesEntitlement(
            List.of(
                FilesEntitlement.FileData.ofPath(Path.of("/a/b"), FilesEntitlement.Mode.READ),
                FilesEntitlement.FileData.ofPath(Path.of("/a/c"), FilesEntitlement.Mode.READ_WRITE),
                FilesEntitlement.FileData.ofRelativePath(Path.of("c/d"), PathLookup.BaseDir.CONFIG, FilesEntitlement.Mode.READ)
            )
        );
        var e2 = new FilesEntitlement(
            List.of(
                FilesEntitlement.FileData.ofPath(Path.of("/a/b"), FilesEntitlement.Mode.READ), // identical
                FilesEntitlement.FileData.ofPath(Path.of("/a/c"), FilesEntitlement.Mode.READ), // different mode
                FilesEntitlement.FileData.ofPath(Path.of("/c/d"), FilesEntitlement.Mode.READ)  // different type
            )
        );

        var merged = PolicyUtils.mergeEntitlement(e1, e2);
        assertThat(
            merged,
            transformedMatch(
                x -> ((FilesEntitlement) x).filesData(),
                containsInAnyOrder(
                    FilesEntitlement.FileData.ofPath(Path.of("/a/b"), FilesEntitlement.Mode.READ),
                    FilesEntitlement.FileData.ofPath(Path.of("/a/c"), FilesEntitlement.Mode.READ),
                    FilesEntitlement.FileData.ofPath(Path.of("/a/c"), FilesEntitlement.Mode.READ_WRITE),
                    FilesEntitlement.FileData.ofRelativePath(Path.of("c/d"), PathLookup.BaseDir.CONFIG, FilesEntitlement.Mode.READ),
                    FilesEntitlement.FileData.ofPath(Path.of("/c/d"), FilesEntitlement.Mode.READ)
                )
            )
        );
    }

    public void testMergeWritePropertyEntitlement() {
        var e1 = new WriteSystemPropertiesEntitlement(List.of("a", "b", "c"));
        var e2 = new WriteSystemPropertiesEntitlement(List.of("b", "c", "d"));

        var merged = PolicyUtils.mergeEntitlement(e1, e2);
        assertThat(
            merged,
            transformedMatch(x -> ((WriteSystemPropertiesEntitlement) x).properties(), containsInAnyOrder("a", "b", "c", "d"))
        );
    }

    public void testMergeEntitlements() {
        List<Entitlement> a = List.of(
            new InboundNetworkEntitlement(),
            new OutboundNetworkEntitlement(),
            new FilesEntitlement(
                List.of(
                    FilesEntitlement.FileData.ofPath(Path.of("/a/b"), FilesEntitlement.Mode.READ),
                    FilesEntitlement.FileData.ofPath(Path.of("/a/c"), FilesEntitlement.Mode.READ_WRITE)
                )
            )
        );
        List<Entitlement> b = List.of(
            new InboundNetworkEntitlement(),
            new LoadNativeLibrariesEntitlement(),
            new FilesEntitlement(List.of()),
            new WriteSystemPropertiesEntitlement(List.of("a"))
        );

        var merged = PolicyUtils.mergeEntitlements(a, b);
        assertThat(
            merged,
            containsInAnyOrder(
                new InboundNetworkEntitlement(),
                new OutboundNetworkEntitlement(),
                new LoadNativeLibrariesEntitlement(),
                new FilesEntitlement(
                    List.of(
                        FilesEntitlement.FileData.ofPath(Path.of("/a/b"), FilesEntitlement.Mode.READ),
                        FilesEntitlement.FileData.ofPath(Path.of("/a/c"), FilesEntitlement.Mode.READ_WRITE)
                    )
                ),
                new WriteSystemPropertiesEntitlement(List.of("a"))
            )
        );
    }

    /** Test that we can parse the set of entitlements correctly for a simple policy */
    public void testFormatSimplePolicy() {
        var pluginPolicy = new Policy(
            "test-plugin",
            List.of(new Scope("module1", List.of(new WriteSystemPropertiesEntitlement(List.of("property1", "property2")))))
        );

        Set<String> actual = PolicyUtils.getEntitlementsDescriptions(pluginPolicy);
        assertThat(actual, containsInAnyOrder("write_system_properties [property1]", "write_system_properties [property2]"));
    }

    /** Test that we can format the set of entitlements correctly for a complex policy */
    public void testFormatPolicyWithMultipleScopes() {
        var pluginPolicy = new Policy(
            "test-plugin",
            List.of(
                new Scope("module1", List.of(new CreateClassLoaderEntitlement())),
                new Scope("module2", List.of(new CreateClassLoaderEntitlement(), new OutboundNetworkEntitlement())),
                new Scope("module3", List.of(new InboundNetworkEntitlement(), new OutboundNetworkEntitlement()))
            )
        );

        Set<String> actual = PolicyUtils.getEntitlementsDescriptions(pluginPolicy);
        assertThat(actual, containsInAnyOrder("create_class_loader", "outbound_network", "inbound_network"));
    }

    /** Test that we can format some simple files entitlement properly */
    public void testFormatFilesEntitlement() {
        var pathAB = Path.of("/a/b");
        var pathCD = Path.of("c/d");
        var policy = new Policy(
            "test-plugin",
            List.of(
                new Scope(
                    "module1",
                    List.of(
                        new FilesEntitlement(
                            List.of(
                                FilesEntitlement.FileData.ofPath(pathAB, FilesEntitlement.Mode.READ_WRITE),
                                FilesEntitlement.FileData.ofRelativePath(pathCD, PathLookup.BaseDir.DATA, FilesEntitlement.Mode.READ)
                            )
                        )
                    )
                ),
                new Scope(
                    "module2",
                    List.of(
                        new FilesEntitlement(
                            List.of(
                                FilesEntitlement.FileData.ofPath(pathAB, FilesEntitlement.Mode.READ_WRITE),
                                FilesEntitlement.FileData.ofPathSetting("setting", PathLookup.BaseDir.DATA, FilesEntitlement.Mode.READ)
                            )
                        )
                    )
                )
            )
        );
        Set<String> actual = PolicyUtils.getEntitlementsDescriptions(policy);
        var pathABString = pathAB.toAbsolutePath().toString();
        var pathCDString = SEPARATOR + pathCD.toString();
        var pathSettingString = SEPARATOR + "<setting>";
        assertThat(
            actual,
            containsInAnyOrder(
                "files [READ_WRITE] " + pathABString,
                "files [READ] <DATA>" + pathCDString,
                "files [READ] <DATA>" + pathSettingString
            )
        );
    }

    /** Test that we can format some simple files entitlement properly */
    public void testFormatWriteSystemPropertiesEntitlement() {
        var policy = new Policy(
            "test-plugin",
            List.of(
                new Scope("module1", List.of(new WriteSystemPropertiesEntitlement(List.of("property1", "property2")))),
                new Scope("module2", List.of(new WriteSystemPropertiesEntitlement(List.of("property2", "property3"))))
            )
        );
        Set<String> actual = PolicyUtils.getEntitlementsDescriptions(policy);
        assertThat(
            actual,
            containsInAnyOrder(
                "write_system_properties [property1]",
                "write_system_properties [property2]",
                "write_system_properties [property3]"
            )
        );
    }
}
