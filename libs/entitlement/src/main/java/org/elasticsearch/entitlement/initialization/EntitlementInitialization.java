/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.initialization;

import org.elasticsearch.core.Tuple;
import org.elasticsearch.core.internal.provider.ProviderLocator;
import org.elasticsearch.entitlement.bootstrap.EntitlementBootstrap;
import org.elasticsearch.entitlement.bridge.EntitlementChecker;
import org.elasticsearch.entitlement.instrumentation.CheckMethod;
import org.elasticsearch.entitlement.instrumentation.InstrumentationService;
import org.elasticsearch.entitlement.instrumentation.MethodKey;
import org.elasticsearch.entitlement.instrumentation.Transformer;
import org.elasticsearch.entitlement.runtime.api.ElasticsearchEntitlementChecker;
import org.elasticsearch.entitlement.runtime.policy.Policy;
import org.elasticsearch.entitlement.runtime.policy.PolicyManager;
import org.elasticsearch.entitlement.runtime.policy.PolicyParser;
import org.elasticsearch.entitlement.runtime.policy.Scope;

import java.io.IOException;
import java.lang.instrument.Instrumentation;
import java.lang.module.ModuleFinder;
import java.lang.module.ModuleReference;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.entitlement.runtime.policy.PolicyManager.ALL_UNNAMED;

/**
 * Called by the agent during {@code agentmain} to configure the entitlement system,
 * instantiate and configure an {@link EntitlementChecker},
 * make it available to the bootstrap library via {@link #checker()},
 * and then install the {@link org.elasticsearch.entitlement.instrumentation.Instrumenter}
 * to begin injecting our instrumentation.
 */
public class EntitlementInitialization {

    private static final String POLICY_FILE_NAME = "entitlement-policy.yaml";

    private static ElasticsearchEntitlementChecker manager;

    // Note: referenced by bridge reflectively
    public static EntitlementChecker checker() {
        return manager;
    }

    // Note: referenced by agent reflectively
    public static void initialize(Instrumentation inst) throws Exception {
        manager = initChecker();

        Map<MethodKey, CheckMethod> checkMethods = INSTRUMENTER_FACTORY.lookupMethodsToInstrument(
            "org.elasticsearch.entitlement.bridge.EntitlementChecker"
        );

        var classesToTransform = checkMethods.keySet().stream().map(MethodKey::className).collect(Collectors.toSet());

        inst.addTransformer(new Transformer(INSTRUMENTER_FACTORY.newInstrumenter(checkMethods), classesToTransform), true);
        // TODO: should we limit this array somehow?
        var classesToRetransform = classesToTransform.stream().map(EntitlementInitialization::internalNameToClass).toArray(Class[]::new);
        inst.retransformClasses(classesToRetransform);
    }

    private static Class<?> internalNameToClass(String internalName) {
        try {
            return Class.forName(internalName.replace('/', '.'), false, ClassLoader.getPlatformClassLoader());
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    private static PolicyManager createPolicyManager() throws IOException {
        Map<String, Policy> pluginPolicies = createPluginPolicies(EntitlementBootstrap.bootstrapArgs().pluginData());

        // TODO: What should the name be?
        // TODO(ES-10031): Decide what goes in the elasticsearch default policy and extend it
        var serverPolicy = new Policy("server", List.of());
        return new PolicyManager(serverPolicy, pluginPolicies, EntitlementBootstrap.bootstrapArgs().pluginResolver());
    }

    private static Map<String, Policy> createPluginPolicies(Collection<Tuple<Path, Boolean>> pluginData) throws IOException {
        Map<String, Policy> pluginPolicies = new HashMap<>(pluginData.size());
        for (Tuple<Path, Boolean> entry : pluginData) {
            Path pluginRoot = entry.v1();
            boolean isModular = entry.v2();

            String pluginName = pluginRoot.getFileName().toString();
            final Policy policy = loadPluginPolicy(pluginRoot, isModular, pluginName);

            pluginPolicies.put(pluginName, policy);
        }
        return pluginPolicies;
    }

    private static Policy loadPluginPolicy(Path pluginRoot, boolean isModular, String pluginName) throws IOException {
        Path policyFile = pluginRoot.resolve(POLICY_FILE_NAME);

        final Set<String> moduleNames = getModuleNames(pluginRoot, isModular);
        final Policy policy = parsePolicyIfExists(pluginName, policyFile);

        // TODO: should this check actually be part of the parser?
        for (Scope scope : policy.scopes) {
            if (moduleNames.contains(scope.name) == false) {
                throw new IllegalStateException("policy [" + policyFile + "] contains invalid module [" + scope.name + "]");
            }
        }
        return policy;
    }

    private static Policy parsePolicyIfExists(String pluginName, Path policyFile) throws IOException {
        if (Files.exists(policyFile)) {
            return new PolicyParser(Files.newInputStream(policyFile, StandardOpenOption.READ), pluginName).parsePolicy();
        }
        return new Policy(pluginName, List.of());
    }

    private static Set<String> getModuleNames(Path pluginRoot, boolean isModular) {
        if (isModular) {
            ModuleFinder moduleFinder = ModuleFinder.of(pluginRoot);
            Set<ModuleReference> moduleReferences = moduleFinder.findAll();

            return moduleReferences.stream().map(mr -> mr.descriptor().name()).collect(Collectors.toUnmodifiableSet());
        }
        // When isModular == false we use the same "ALL-UNNAMED" constant as the JDK to indicate (any) unnamed module for this plugin
        return Set.of(ALL_UNNAMED);
    }

    private static ElasticsearchEntitlementChecker initChecker() throws IOException {
        final PolicyManager policyManager = createPolicyManager();

        int javaVersion = Runtime.version().feature();
        final String classNamePrefix;
        if (javaVersion >= 23) {
            classNamePrefix = "Java23";
        } else {
            classNamePrefix = "";
        }
        final String className = "org.elasticsearch.entitlement.runtime.api." + classNamePrefix + "ElasticsearchEntitlementChecker";
        Class<?> clazz;
        try {
            clazz = Class.forName(className);
        } catch (ClassNotFoundException e) {
            throw new AssertionError("entitlement lib cannot find entitlement impl", e);
        }
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

    private static final InstrumentationService INSTRUMENTER_FACTORY = new ProviderLocator<>(
        "entitlement",
        InstrumentationService.class,
        "org.elasticsearch.entitlement.instrumentation",
        Set.of()
    ).get();
}
