/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.initialization;

import org.elasticsearch.core.internal.provider.ProviderLocator;
import org.elasticsearch.entitlement.bridge.EntitlementChecker;
import org.elasticsearch.entitlement.instrumentation.InstrumentationService;
import org.elasticsearch.entitlement.instrumentation.MethodKey;
import org.elasticsearch.entitlement.instrumentation.Transformer;
import org.elasticsearch.entitlement.runtime.api.ElasticsearchEntitlementChecker;
import org.elasticsearch.entitlement.runtime.policy.Policy;
import org.elasticsearch.entitlement.runtime.policy.PolicyManager;
import org.elasticsearch.entitlement.runtime.policy.PolicyParser;
import org.elasticsearch.entitlement.runtime.policy.Scope;

import java.lang.instrument.Instrumentation;
import java.lang.module.ModuleFinder;
import java.lang.module.ModuleReader;
import java.lang.module.ModuleReference;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Called by the agent during {@code agentmain} to configure the entitlement system,
 * instantiate and configure an {@link EntitlementChecker},
 * make it available to the bootstrap library via {@link #checker()},
 * and then install the {@link org.elasticsearch.entitlement.instrumentation.Instrumenter}
 * to begin injecting our instrumentation.
 */
public class EntitlementInitialization {

    private static final String POLICY_FILE_NAME = "entitlement-policy.yaml";
    private static Map<Path, Boolean> pluginData;

    private static ElasticsearchEntitlementChecker manager;

    // Note: referenced by bridge reflectively
    public static EntitlementChecker checker() {
        return manager;
    }

    public static void setPluginData(Map<Path, Boolean> pluginData) {
        if (EntitlementInitialization.pluginData != null) {
            throw new IllegalStateException("pluginData is already set");
        }
        EntitlementInitialization.pluginData = Collections.unmodifiableMap(Objects.requireNonNull(pluginData));
    }

    // Note: referenced by agent reflectively
    public static void initialize(Instrumentation inst) throws Exception {
        Map<String, Policy> pluginPolicies = new HashMap<>();

        for (Map.Entry<Path, Boolean> entry : EntitlementInitialization.pluginData.entrySet()) {
            Path pluginRoot = entry.getKey();

            String pluginName = pluginRoot.getFileName().toString();
            Path policyFile = pluginRoot.resolve(POLICY_FILE_NAME);
            Policy policy;

            if (Files.exists(policyFile)) {
                policy = new PolicyParser(Files.newInputStream(policyFile, StandardOpenOption.READ), pluginName).parsePolicy();
            } else {
                policy = new Policy(pluginName, List.of());
            }

            // TODO: what is our default module when isModular == false?
            boolean isModular = entry.getValue();

            ModuleFinder moduleFinder = ModuleFinder.of(pluginRoot);
            Set<ModuleReference> moduleReferences = moduleFinder.findAll();
            Set<String> moduleNames = new HashSet<>();
            for (ModuleReference moduleReference : moduleReferences) {
                moduleNames.add(moduleReference.descriptor().name());
            }

            // TODO: should this check actually be part of the parser?
            for (Scope scope : policy.scopes) {
                if (moduleNames.contains(scope.name) == false) {
                    throw new IllegalStateException("policy [" + policyFile + "] contains invalid module [" + scope.name + "]");
                }
            }

            pluginPolicies.put(pluginName, policy);
        }

        // TODO: what goes in the elasticsearch default policy? What should the name be?
        PolicyManager policyManager = new PolicyManager(new Policy("elasticsearch", List.of()), pluginPolicies);

        manager = new ElasticsearchEntitlementChecker(policyManager);

        // TODO: Configure actual entitlement grants instead of this hardcoded one
        Method targetMethod = System.class.getMethod("exit", int.class);
        Method instrumentationMethod = Class.forName("org.elasticsearch.entitlement.bridge.EntitlementChecker")
            .getMethod("checkSystemExit", Class.class, int.class);
        Map<MethodKey, Method> methodMap = Map.of(INSTRUMENTER_FACTORY.methodKeyForTarget(targetMethod), instrumentationMethod);

        inst.addTransformer(new Transformer(INSTRUMENTER_FACTORY.newInstrumenter("", methodMap), Set.of(internalName(System.class))), true);
        inst.retransformClasses(System.class);
    }

    private static String internalName(Class<?> c) {
        return c.getName().replace('.', '/');
    }

    private static final InstrumentationService INSTRUMENTER_FACTORY = new ProviderLocator<>(
        "entitlement",
        InstrumentationService.class,
        "org.elasticsearch.entitlement.instrumentation",
        Set.of()
    ).get();
}
