/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.runtime.policy;

import org.elasticsearch.entitlement.runtime.policy.entitlements.LoadNativeLibrariesEntitlement;
import org.elasticsearch.entitlement.runtime.policy.entitlements.SetHttpsConnectionPropertiesEntitlement;
import org.elasticsearch.test.ESTestCase;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.test.hamcrest.OptionalMatchers.isEmpty;
import static org.elasticsearch.test.hamcrest.OptionalMatchers.isPresentWith;

@ESTestCase.WithoutSecurityManager
public class PolicyParserUtilsTests extends ESTestCase {

    public void testCreatePluginPolicyWithOverride() {

        var policyForOverride = """
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
            Base64.getEncoder().encode(policyForOverride.getBytes(StandardCharsets.UTF_8)),
            StandardCharsets.UTF_8
        );
        var overrides = Map.of("test-plugin", base64EncodedPolicy);

        final Policy expectedPolicy = new Policy(
            "test-plugin",
            List.of(
                new Scope("entitlement-module-name", List.of(new LoadNativeLibrariesEntitlement())),
                new Scope("entitlement-module-name-2", List.of(new SetHttpsConnectionPropertiesEntitlement()))
            )
        );

        var policy = PolicyParserUtils.parsePolicyOverrideIfExists(
            overrides,
            "9.0.0",
            true,
            "test-plugin",
            Set.of("entitlement-module-name", "entitlement-module-name-2")
        );

        assertThat(policy, isPresentWith(expectedPolicy));
    }

    public void testCreatePluginPolicyWithOverrideAnyVersion() {

        var policyForOverride = """
            policy:
              entitlement-module-name:
                - load_native_libraries
              entitlement-module-name-2:
                - set_https_connection_properties
            """;
        var base64EncodedPolicy = new String(
            Base64.getEncoder().encode(policyForOverride.getBytes(StandardCharsets.UTF_8)),
            StandardCharsets.UTF_8
        );
        var overrides = Map.of("test-plugin", base64EncodedPolicy);

        final Policy expectedPolicy = new Policy(
            "test-plugin",
            List.of(
                new Scope("entitlement-module-name", List.of(new LoadNativeLibrariesEntitlement())),
                new Scope("entitlement-module-name-2", List.of(new SetHttpsConnectionPropertiesEntitlement()))
            )
        );

        var policy = PolicyParserUtils.parsePolicyOverrideIfExists(
            overrides,
            "abcdef",
            true,
            "test-plugin",
            Set.of("entitlement-module-name", "entitlement-module-name-2")
        );

        assertThat(policy, isPresentWith(expectedPolicy));
    }

    public void testNoOverriddenPolicyWithVersionMismatch() {

        var policyForOverride = """
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
            Base64.getEncoder().encode(policyForOverride.getBytes(StandardCharsets.UTF_8)),
            StandardCharsets.UTF_8
        );
        var overrides = Map.of("test-plugin", base64EncodedPolicy);

        var policy = PolicyParserUtils.parsePolicyOverrideIfExists(
            overrides,
            "9.1.0",
            true,
            "test-plugin",
            Set.of("entitlement-module-name", "entitlement-module-name-2")
        );

        assertThat(policy, isEmpty());
    }

    public void testNoOverriddenPolicyWithValidationError() {

        var policyForOverride = """
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
            Base64.getEncoder().encode(policyForOverride.getBytes(StandardCharsets.UTF_8)),
            StandardCharsets.UTF_8
        );
        var overrides = Map.of("test-plugin", base64EncodedPolicy);

        var policy = PolicyParserUtils.parsePolicyOverrideIfExists(overrides, "9.0.0", true, "test-plugin", Set.of());

        assertThat(policy, isEmpty());
    }

    public void testNoOverriddenPolicyWithParsingError() {

        var policyForOverride = """
            entitlement-module-name:
              - load_native_libraries
            entitlement-module-name-2:
              - set_https_connection_properties
            """;
        var base64EncodedPolicy = new String(
            Base64.getEncoder().encode(policyForOverride.getBytes(StandardCharsets.UTF_8)),
            StandardCharsets.UTF_8
        );
        var overrides = Map.of("test-plugin", base64EncodedPolicy);

        var policy = PolicyParserUtils.parsePolicyOverrideIfExists(overrides, "9.0.0", true, "test-plugin", Set.of());

        assertThat(policy, isEmpty());
    }
}
