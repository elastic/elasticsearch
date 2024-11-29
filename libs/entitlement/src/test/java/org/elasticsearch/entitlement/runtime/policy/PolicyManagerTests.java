/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.runtime.policy;

import org.elasticsearch.entitlement.runtime.api.NotEntitledException;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static java.util.Map.entry;
import static org.elasticsearch.entitlement.runtime.policy.PolicyManager.ALL_UNNAMED;
import static org.elasticsearch.test.LambdaMatchers.transformedMatch;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.sameInstance;

@ESTestCase.WithoutSecurityManager
public class PolicyManagerTests extends ESTestCase {

    public void testGetEntitlementsThrowsOnMissingPluginUnnamedModule() {
        var policyManager = new PolicyManager(
            createTestServerPolicy(),
            Map.of("plugin1", createPluginPolicy("plugin.module")),
            c -> "plugin1"
        );

        // Any class from the current module (unnamed) will do
        var callerClass = this.getClass();
        var requestingModule = callerClass.getModule();

        var ex = assertThrows(
            "No policy for the unnamed module",
            NotEntitledException.class,
            () -> policyManager.getEntitlementsOrThrow(callerClass, requestingModule)
        );

        assertEquals(
            "Caller [class org.elasticsearch.entitlement.runtime.policy.PolicyManagerTests] in module [null] does not have any policy",
            ex.getMessage()
        );
        assertThat(policyManager.moduleEntitlementsMap, hasEntry(requestingModule, PolicyManager.ModuleEntitlements.NONE));
    }

    public void testGetEntitlementsThrowsOnMissingPolicyForPlugin() {
        var policyManager = new PolicyManager(createTestServerPolicy(), Map.of(), c -> "plugin1");

        // Any class from the current module (unnamed) will do
        var callerClass = this.getClass();
        var requestingModule = callerClass.getModule();

        var ex = assertThrows(
            "No policy for this plugin",
            NotEntitledException.class,
            () -> policyManager.getEntitlementsOrThrow(callerClass, requestingModule)
        );

        assertEquals(
            "Caller [class org.elasticsearch.entitlement.runtime.policy.PolicyManagerTests] in module [null] does not have any policy",
            ex.getMessage()
        );
        assertThat(policyManager.moduleEntitlementsMap, hasEntry(requestingModule, PolicyManager.ModuleEntitlements.NONE));
    }

    public void testGetEntitlementsFailureIsCached() {
        var policyManager = new PolicyManager(createTestServerPolicy(), Map.of(), c -> "plugin1");

        // Any class from the current module (unnamed) will do
        var callerClass = this.getClass();
        var requestingModule = callerClass.getModule();

        assertThrows(NotEntitledException.class, () -> policyManager.getEntitlementsOrThrow(callerClass, requestingModule));
        assertThat(policyManager.moduleEntitlementsMap, hasEntry(requestingModule, PolicyManager.ModuleEntitlements.NONE));

        // A second time
        var ex = assertThrows(NotEntitledException.class, () -> policyManager.getEntitlementsOrThrow(callerClass, requestingModule));

        assertThat(ex.getMessage(), endsWith("[CACHED]"));
        // Nothing new in the map
        assertThat(policyManager.moduleEntitlementsMap, aMapWithSize(1));
    }

    public void testGetEntitlementsReturnsEntitlementsForUnnamedModulePolicy() {
        var policyManager = new PolicyManager(
            createTestServerPolicy(),
            Map.ofEntries(entry("plugin2", createPluginPolicy(ALL_UNNAMED))),
            c -> "plugin2"
        );

        // Any class from the current module (unnamed) will do
        var callerClass = this.getClass();
        var requestingModule = callerClass.getModule();

        var entitlements = policyManager.getEntitlementsOrThrow(callerClass, requestingModule);
        assertThat(
            entitlements.flagEntitlements,
            contains(transformedMatch(FlagEntitlement::type, equalTo(FlagEntitlementType.CREATE_CLASSLOADER)))
        );
    }

    public void testGetEntitlementsResultIsCached() {
        var policyManager = new PolicyManager(
            createTestServerPolicy(),
            Map.ofEntries(entry("plugin2", createPluginPolicy(ALL_UNNAMED))),
            c -> "plugin2"
        );

        // Any class from the current module (unnamed) will do
        var callerClass = this.getClass();
        var requestingModule = callerClass.getModule();

        var entitlements = policyManager.getEntitlementsOrThrow(callerClass, requestingModule);
        assertThat(
            entitlements.flagEntitlements,
            contains(transformedMatch(FlagEntitlement::type, equalTo(FlagEntitlementType.CREATE_CLASSLOADER)))
        );
        assertThat(policyManager.moduleEntitlementsMap, aMapWithSize(1));
        var cachedResult = policyManager.moduleEntitlementsMap.values().stream().findFirst().get();
        var entitlementsAgain = policyManager.getEntitlementsOrThrow(callerClass, requestingModule);

        // Nothing new in the map
        assertThat(policyManager.moduleEntitlementsMap, aMapWithSize(1));
        assertThat(entitlementsAgain, sameInstance(cachedResult));
    }

    private static Policy createTestServerPolicy() {
        return new Policy(
            "server",
            List.of(
                new Scope(
                    "org.elasticsearch.server",
                    List.of(
                        new FlagEntitlement(FlagEntitlementType.SYSTEM_EXIT),
                        new FlagEntitlement(FlagEntitlementType.CREATE_CLASSLOADER)
                    )
                )
            )
        );
    }

    private static Policy createPluginPolicy(String... pluginModules) {
        return new Policy(
            "plugin",
            Arrays.stream(pluginModules)
                .map(
                    name -> new Scope(
                        name,
                        List.of(
                            new FileEntitlement("/test/path", List.of(FileEntitlement.READ)),
                            new FlagEntitlement(FlagEntitlementType.CREATE_CLASSLOADER)
                        )
                    )
                )
                .toList()
        );
    }
}
