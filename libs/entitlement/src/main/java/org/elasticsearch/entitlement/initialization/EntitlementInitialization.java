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
import org.elasticsearch.entitlement.bootstrap.EntitlementBootstrap;
import org.elasticsearch.entitlement.bridge.EntitlementChecker;
import org.elasticsearch.entitlement.instrumentation.CheckMethod;
import org.elasticsearch.entitlement.instrumentation.InstrumentationService;
import org.elasticsearch.entitlement.instrumentation.Instrumenter;
import org.elasticsearch.entitlement.instrumentation.MethodKey;
import org.elasticsearch.entitlement.instrumentation.Transformer;
import org.elasticsearch.entitlement.runtime.api.ElasticsearchEntitlementChecker;
import org.elasticsearch.entitlement.runtime.policy.CreateClassLoaderEntitlement;
import org.elasticsearch.entitlement.runtime.policy.Entitlement;
import org.elasticsearch.entitlement.runtime.policy.ExitVMEntitlement;
import org.elasticsearch.entitlement.runtime.policy.InboundNetworkEntitlement;
import org.elasticsearch.entitlement.runtime.policy.LoadNativeLibrariesEntitlement;
import org.elasticsearch.entitlement.runtime.policy.OutboundNetworkEntitlement;
import org.elasticsearch.entitlement.runtime.policy.Policy;
import org.elasticsearch.entitlement.runtime.policy.PolicyManager;
import org.elasticsearch.entitlement.runtime.policy.Scope;

import java.lang.instrument.Instrumentation;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.nio.file.FileSystems;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.spi.FileSystemProvider;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Called by the agent during {@code agentmain} to configure the entitlement system,
 * instantiate and configure an {@link EntitlementChecker},
 * make it available to the bootstrap library via {@link #checker()},
 * and then install the {@link org.elasticsearch.entitlement.instrumentation.Instrumenter}
 * to begin injecting our instrumentation.
 */
public class EntitlementInitialization {

    private static final Module ENTITLEMENTS_MODULE = PolicyManager.class.getModule();

    private static ElasticsearchEntitlementChecker manager;

    // Note: referenced by bridge reflectively
    public static EntitlementChecker checker() {
        return manager;
    }

    // Note: referenced by agent reflectively
    public static void initialize(Instrumentation inst) throws Exception {
        manager = initChecker();

        Map<MethodKey, CheckMethod> checkMethods = new HashMap<>(INSTRUMENTATION_SERVICE.lookupMethods(EntitlementChecker.class));

        var fileSystemProviderClass = FileSystems.getDefault().provider().getClass();
        addInstrumentationMethod(
            checkMethods,
            FileSystemProvider.class,
            "newInputStream",
            fileSystemProviderClass,
            EntitlementChecker.class,
            "checkNewInputStream",
            Path.class,
            OpenOption[].class
        );

        var classesToTransform = checkMethods.keySet().stream().map(MethodKey::className).collect(Collectors.toSet());

        Instrumenter instrumenter = INSTRUMENTATION_SERVICE.newInstrumenter(EntitlementChecker.class, checkMethods);
        inst.addTransformer(new Transformer(instrumenter, classesToTransform), true);
        inst.retransformClasses(findClassesToRetransform(inst.getAllLoadedClasses(), classesToTransform));
    }

    private static Class<?>[] findClassesToRetransform(Class<?>[] loadedClasses, Set<String> classesToTransform) {
        List<Class<?>> retransform = new ArrayList<>();
        for (Class<?> loadedClass : loadedClasses) {
            if (classesToTransform.contains(loadedClass.getName().replace(".", "/"))) {
                retransform.add(loadedClass);
            }
        }
        return retransform.toArray(new Class<?>[0]);
    }

    private static PolicyManager createPolicyManager() {
        Map<String, Policy> pluginPolicies = EntitlementBootstrap.bootstrapArgs().pluginPolicies();

        // TODO(ES-10031): Decide what goes in the elasticsearch default policy and extend it
        var serverPolicy = new Policy(
            "server",
            List.of(
                new Scope("org.elasticsearch.base", List.of(new CreateClassLoaderEntitlement())),
                new Scope("org.elasticsearch.xcontent", List.of(new CreateClassLoaderEntitlement())),
                new Scope(
                    "org.elasticsearch.server",
                    List.of(
                        new ExitVMEntitlement(),
                        new CreateClassLoaderEntitlement(),
                        new InboundNetworkEntitlement(),
                        new OutboundNetworkEntitlement(),
                        new LoadNativeLibrariesEntitlement()
                    )
                ),
                new Scope("org.apache.httpcomponents.httpclient", List.of(new OutboundNetworkEntitlement())),
                new Scope("io.netty.transport", List.of(new InboundNetworkEntitlement(), new OutboundNetworkEntitlement()))
            )
        );
        // agents run without a module, so this is a special hack for the apm agent
        // this should be removed once https://github.com/elastic/elasticsearch/issues/109335 is completed
        List<Entitlement> agentEntitlements = List.of(new CreateClassLoaderEntitlement());
        var resolver = EntitlementBootstrap.bootstrapArgs().pluginResolver();
        return new PolicyManager(serverPolicy, agentEntitlements, pluginPolicies, resolver, ENTITLEMENTS_MODULE);
    }

    static void addInstrumentationMethod(
        Map<MethodKey, CheckMethod> checkMethods,
        Class<?> clazz,
        String methodName,
        Class<?> implementationClass,
        Class<?> checkerClass,
        String checkMethodName,
        Class<?>... parameterTypes
    ) throws NoSuchMethodException {
        var instrumentationMethod = clazz.getMethod(methodName, parameterTypes);

        var checkerAdditionalArguments = Modifier.isStatic(instrumentationMethod.getModifiers())
            ? Stream.of(Class.class)
            : Stream.of(Class.class, clazz);

        var checkerArgumentTypes = Stream.concat(checkerAdditionalArguments, Arrays.stream(parameterTypes)).toArray(n -> new Class<?>[n]);
        var additionalMethod = INSTRUMENTATION_SERVICE.lookupImplementationMethod(
            implementationClass,
            instrumentationMethod,
            checkerClass.getMethod(checkMethodName, checkerArgumentTypes)
        );
        checkMethods.put(additionalMethod.methodToInstrument(), additionalMethod.checkMethod());
    }

    private static ElasticsearchEntitlementChecker initChecker() {
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

    private static final InstrumentationService INSTRUMENTATION_SERVICE = new ProviderLocator<>(
        "entitlement",
        InstrumentationService.class,
        "org.elasticsearch.entitlement.instrumentation",
        Set.of()
    ).get();
}
