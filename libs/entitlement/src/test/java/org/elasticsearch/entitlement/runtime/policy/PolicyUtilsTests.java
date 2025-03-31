/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.runtime.policy;

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

import static org.elasticsearch.entitlement.runtime.policy.PolicyUtils.mergeEntitlement;
import static org.elasticsearch.entitlement.runtime.policy.PolicyUtils.mergeEntitlements;
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

        var policy = PolicyUtils.parseEncodedPolicyIfExists(base64EncodedPolicy, "9.0.0", true, "test-plugin", Set.of());

        assertThat(policy, nullValue());
    }

    public void testNoPatchWithParsingError() {

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

        var policy = PolicyUtils.parseEncodedPolicyIfExists(base64EncodedPolicy, "9.0.0", true, "test-plugin", Set.of());

        assertThat(policy, nullValue());
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

        assertThat(mergeEntitlement(e1, e2), equalTo(new InboundNetworkEntitlement()));
    }

    public void testMergeFilesEntitlement() {
        var e1 = new FilesEntitlement(
            List.of(
                FilesEntitlement.FileData.ofPath(Path.of("/a/b"), FilesEntitlement.Mode.READ),
                FilesEntitlement.FileData.ofPath(Path.of("/a/c"), FilesEntitlement.Mode.READ_WRITE),
                FilesEntitlement.FileData.ofRelativePath(Path.of("c/d"), FilesEntitlement.BaseDir.CONFIG, FilesEntitlement.Mode.READ)
            )
        );
        var e2 = new FilesEntitlement(
            List.of(
                FilesEntitlement.FileData.ofPath(Path.of("/a/b"), FilesEntitlement.Mode.READ), // identical
                FilesEntitlement.FileData.ofPath(Path.of("/a/c"), FilesEntitlement.Mode.READ), // different mode
                FilesEntitlement.FileData.ofPath(Path.of("/c/d"), FilesEntitlement.Mode.READ)  // different type
            )
        );

        var merged = mergeEntitlement(e1, e2);
        assertThat(
            merged,
            transformedMatch(
                x -> ((FilesEntitlement) x).filesData(),
                containsInAnyOrder(
                    FilesEntitlement.FileData.ofPath(Path.of("/a/b"), FilesEntitlement.Mode.READ),
                    FilesEntitlement.FileData.ofPath(Path.of("/a/c"), FilesEntitlement.Mode.READ),
                    FilesEntitlement.FileData.ofPath(Path.of("/a/c"), FilesEntitlement.Mode.READ_WRITE),
                    FilesEntitlement.FileData.ofRelativePath(Path.of("c/d"), FilesEntitlement.BaseDir.CONFIG, FilesEntitlement.Mode.READ),
                    FilesEntitlement.FileData.ofPath(Path.of("/c/d"), FilesEntitlement.Mode.READ)
                )
            )
        );
    }

    public void testMergeWritePropertyEntitlement() {
        var e1 = new WriteSystemPropertiesEntitlement(List.of("a", "b", "c"));
        var e2 = new WriteSystemPropertiesEntitlement(List.of("b", "c", "d"));

        var merged = mergeEntitlement(e1, e2);
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

        var merged = mergeEntitlements(a, b);
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
}
