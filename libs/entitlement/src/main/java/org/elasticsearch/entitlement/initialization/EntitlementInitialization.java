/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.initialization;

import org.elasticsearch.core.Booleans;
import org.elasticsearch.entitlement.bootstrap.EntitlementBootstrap;
import org.elasticsearch.entitlement.bridge.EntitlementChecker;
import org.elasticsearch.entitlement.runtime.api.ElasticsearchEntitlementChecker;
import org.elasticsearch.entitlement.runtime.policy.PathLookup;
import org.elasticsearch.entitlement.runtime.policy.Policy;
import org.elasticsearch.entitlement.runtime.policy.PolicyChecker;
import org.elasticsearch.entitlement.runtime.policy.PolicyCheckerImpl;
import org.elasticsearch.entitlement.runtime.policy.PolicyManager;

import java.lang.instrument.Instrumentation;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import java.util.Set;

/**
 * Called by the agent during {@code agentmain} to configure the entitlement system,
 * instantiate and configure an {@link EntitlementChecker},
 * make it available to the bootstrap library via {@link #checker()},
 * and then install the {@link org.elasticsearch.entitlement.instrumentation.Instrumenter}
 * to begin injecting our instrumentation.
 */
public class EntitlementInitialization {

    private static final Module ENTITLEMENTS_MODULE = PolicyManager.class.getModule();

    private static ElasticsearchEntitlementChecker checker;

    // Note: referenced by bridge reflectively
    public static EntitlementChecker checker() {
        return checker;
    }

    /**
     * Initializes the Entitlement system:
     * <ol>
     * <li>
     * Initialize dynamic instrumentation via {@link DynamicInstrumentation#initialize}
     * </li>
     * <li>
     * Creates the {@link PolicyManager}
     * </li>
     * <li>
     * Creates the {@link ElasticsearchEntitlementChecker} instance referenced by the instrumented methods
     * </li>
     * </ol>
     * <p>
     * <strong>NOTE:</strong> this method is referenced by the agent reflectively
     * </p>
     *
     * @param inst the JVM instrumentation class instance
     */
    public static void initialize(Instrumentation inst) throws Exception {
        checker = initChecker(inst, createPolicyManager());
    }

    private static PolicyCheckerImpl createPolicyChecker(PolicyManager policyManager) {
        EntitlementBootstrap.BootstrapArgs bootstrapArgs = EntitlementBootstrap.bootstrapArgs();
        return new PolicyCheckerImpl(
            bootstrapArgs.suppressFailureLogPackages(),
            ENTITLEMENTS_MODULE,
            policyManager,
            bootstrapArgs.pathLookup()
        );
    }

    private static PolicyManager createPolicyManager() {
        EntitlementBootstrap.BootstrapArgs bootstrapArgs = EntitlementBootstrap.bootstrapArgs();
        Map<String, Policy> pluginPolicies = bootstrapArgs.pluginPolicies();
        PathLookup pathLookup = bootstrapArgs.pathLookup();

        FilesEntitlementsValidation.validate(pluginPolicies, pathLookup);

        return new PolicyManager(
            HardcodedEntitlements.serverPolicy(pathLookup.pidFile(), bootstrapArgs.serverPolicyPatch()),
            HardcodedEntitlements.agentEntitlements(),
            pluginPolicies,
            EntitlementBootstrap.bootstrapArgs().scopeResolver(),
            EntitlementBootstrap.bootstrapArgs().sourcePaths(),
            pathLookup
        );
    }

    /**
     * If bytecode verification is enabled, ensure these classes get loaded before transforming/retransforming them.
     * For these classes, the order in which we transform and verify them matters. Verification during class transformation is at least an
     * unforeseen (if not unsupported) scenario: we are loading a class, and while we are still loading it (during transformation) we try
     * to verify it. This in turn leads to more classes loading (for verification purposes), which could turn into those classes to be
     * transformed and undergo verification. In order to avoid circularity errors as much as possible, we force a partial order.
     */
    private static void ensureClassesSensitiveToVerificationAreInitialized() {
        var classesToInitialize = Set.of(
            "sun.net.www.protocol.http.HttpURLConnection",
            "sun.nio.ch.DatagramChannelImpl",
            "sun.nio.ch.ServerSocketChannelImpl"
        );
        for (String className : classesToInitialize) {
            try {
                Class.forName(className);
            } catch (ClassNotFoundException unexpected) {
                throw new AssertionError(unexpected);
            }
        }
    }

    static ElasticsearchEntitlementChecker initChecker(Instrumentation inst, PolicyManager policyManager) throws Exception {
        final PolicyChecker policyChecker = createPolicyChecker(policyManager);
        final Class<?> clazz = EntitlementCheckerUtils.getVersionSpecificCheckerClass(
            ElasticsearchEntitlementChecker.class,
            Runtime.version().feature()
        );

        Constructor<?> constructor;
        try {
            constructor = clazz.getConstructor(PolicyChecker.class);
        } catch (NoSuchMethodException e) {
            throw new AssertionError("entitlement impl is missing required constructor: [" + clazz.getName() + "]", e);
        }

        ElasticsearchEntitlementChecker checker;
        try {
            checker = (ElasticsearchEntitlementChecker) constructor.newInstance(policyChecker);
        } catch (IllegalAccessException | InvocationTargetException | InstantiationException e) {
            throw new AssertionError(e);
        }

        var verifyBytecode = Booleans.parseBoolean(System.getProperty("es.entitlements.verify_bytecode", "false"));
        if (verifyBytecode) {
            ensureClassesSensitiveToVerificationAreInitialized();
        }

        DynamicInstrumentation.initialize(
            inst,
            EntitlementCheckerUtils.getVersionSpecificCheckerClass(EntitlementChecker.class, Runtime.version().feature()),
            verifyBytecode
        );

        return checker;
    }
}
