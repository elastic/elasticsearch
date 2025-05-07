/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.bootstrap;

import org.elasticsearch.entitlement.bridge.EntitlementChecker;
import org.elasticsearch.entitlement.initialization.DynamicInstrumentation;
import org.elasticsearch.entitlement.initialization.EntitlementCheckerUtils;
import org.elasticsearch.entitlement.initialization.FilesEntitlementsValidation;
import org.elasticsearch.entitlement.initialization.HardcodedEntitlements;
import org.elasticsearch.entitlement.runtime.api.ElasticsearchEntitlementChecker;
import org.elasticsearch.entitlement.runtime.policy.PathLookup;
import org.elasticsearch.entitlement.runtime.policy.Policy;
import org.elasticsearch.entitlement.runtime.policy.PolicyManager;

import java.lang.instrument.Instrumentation;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import java.util.Set;

/**
 * Test-only version of {@code EntitlementInitialization}
 */
public class TestEntitlementInitialization {

    private static final Module ENTITLEMENTS_MODULE = PolicyManager.class.getModule();

    private static ElasticsearchEntitlementChecker manager;

    // Note: referenced by bridge reflectively
    public static EntitlementChecker checker() {
        return manager;
    }

    public static void initialize(Instrumentation inst) throws Exception {
        manager = initChecker();
        DynamicInstrumentation.initialize(
            inst,
            EntitlementCheckerUtils.getVersionSpecificCheckerClass(EntitlementChecker.class, Runtime.version().feature()),
            false
        );
    }

    private static PolicyManager createPolicyManager() {

        // TODO: parse policies. Locate them using help from TestBuildInfo
        Map<String, Policy> pluginPolicies = Map.of();

        // TODO: create here the test pathLookup
        PathLookup pathLookup = null;

        FilesEntitlementsValidation.validate(pluginPolicies, pathLookup);

        return new PolicyManager(
            HardcodedEntitlements.serverPolicy(null, null),
            HardcodedEntitlements.agentEntitlements(),
            pluginPolicies,
            null, // TestScopeResolver.createScopeResolver
            Map.of(), // TODO: a map that always return nulls? Replace with functor
            ENTITLEMENTS_MODULE, // TODO: this will need to change -- encapsulate it when we extract isTriviallyAllowed
            pathLookup,
            Set.of()
        );
    }

    private static ElasticsearchEntitlementChecker initChecker() {
        final PolicyManager policyManager = createPolicyManager();

        final Class<?> clazz = EntitlementCheckerUtils.getVersionSpecificCheckerClass(
            ElasticsearchEntitlementChecker.class,
            Runtime.version().feature()
        );

        Constructor<?> constructor;
        try {
            constructor = clazz.getConstructor(PolicyManager.class);
        } catch (NoSuchMethodException e) {
            throw new AssertionError("entitlement impl is missing no arg constructor", e);
        }
        try {
            return (ElasticsearchEntitlementChecker) constructor.newInstance(policyManager);
        } catch (IllegalAccessException | InvocationTargetException | InstantiationException e) {
            throw new AssertionError(e);
        }
    }
}
