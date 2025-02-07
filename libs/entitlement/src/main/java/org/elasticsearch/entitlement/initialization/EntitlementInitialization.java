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
import org.elasticsearch.entitlement.runtime.policy.Policy;
import org.elasticsearch.entitlement.runtime.policy.PolicyManager;
import org.elasticsearch.entitlement.runtime.policy.Scope;
import org.elasticsearch.entitlement.runtime.policy.entitlements.CreateClassLoaderEntitlement;
import org.elasticsearch.entitlement.runtime.policy.entitlements.Entitlement;
import org.elasticsearch.entitlement.runtime.policy.entitlements.ExitVMEntitlement;
import org.elasticsearch.entitlement.runtime.policy.entitlements.InboundNetworkEntitlement;
import org.elasticsearch.entitlement.runtime.policy.entitlements.LoadNativeLibrariesEntitlement;
import org.elasticsearch.entitlement.runtime.policy.entitlements.OutboundNetworkEntitlement;

import java.lang.instrument.Instrumentation;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.FileSystems;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.spi.FileSystemProvider;
import java.util.ArrayList;
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

    private static final String AGENTS_PACKAGE_NAME = "co.elastic.apm.agent";
    private static final Module ENTITLEMENTS_MODULE = PolicyManager.class.getModule();

    private static ElasticsearchEntitlementChecker manager;

    // Note: referenced by bridge reflectively
    public static EntitlementChecker checker() {
        return manager;
    }

    // Note: referenced by agent reflectively
    public static void initialize(Instrumentation inst) throws Exception {
        manager = initChecker();

        var latestCheckerInterface = getVersionSpecificCheckerClass(EntitlementChecker.class);

        Map<MethodKey, CheckMethod> checkMethods = new HashMap<>(INSTRUMENTATION_SERVICE.lookupMethods(latestCheckerInterface));
        var fileSystemProviderClass = FileSystems.getDefault().provider().getClass();
        Stream.of(
            INSTRUMENTATION_SERVICE.lookupImplementationMethod(
                FileSystemProvider.class,
                "newInputStream",
                fileSystemProviderClass,
                EntitlementChecker.class,
                "checkNewInputStream",
                Path.class,
                OpenOption[].class
            )
        ).forEach(instrumentation -> checkMethods.put(instrumentation.targetMethod(), instrumentation.checkMethod()));

        var classesToTransform = checkMethods.keySet().stream().map(MethodKey::className).collect(Collectors.toSet());

        Instrumenter instrumenter = INSTRUMENTATION_SERVICE.newInstrumenter(latestCheckerInterface, checkMethods);
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
                new Scope("io.netty.transport", List.of(new InboundNetworkEntitlement(), new OutboundNetworkEntitlement())),
                new Scope("org.apache.lucene.core", List.of(new LoadNativeLibrariesEntitlement())),
                new Scope("org.elasticsearch.nativeaccess", List.of(new LoadNativeLibrariesEntitlement()))
            )
        );
        // agents run without a module, so this is a special hack for the apm agent
        // this should be removed once https://github.com/elastic/elasticsearch/issues/109335 is completed
        List<Entitlement> agentEntitlements = List.of(new CreateClassLoaderEntitlement());
        var resolver = EntitlementBootstrap.bootstrapArgs().pluginResolver();
        return new PolicyManager(serverPolicy, agentEntitlements, pluginPolicies, resolver, AGENTS_PACKAGE_NAME, ENTITLEMENTS_MODULE);
    }

    /**
     * Returns the "most recent" checker class compatible with the current runtime Java version.
     * For checkers, we have (optionally) version specific classes, each with a prefix (e.g. Java23).
     * The mapping cannot be automatic, as it depends on the actual presence of these classes in the final Jar (see
     * the various mainXX source sets).
     */
    private static Class<?> getVersionSpecificCheckerClass(Class<?> baseClass) {
        String packageName = baseClass.getPackageName();
        String baseClassName = baseClass.getSimpleName();
        int javaVersion = Runtime.version().feature();

        final String classNamePrefix;
        if (javaVersion >= 23) {
            // All Java version from 23 onwards will be able to use che checks in the Java23EntitlementChecker interface and implementation
            classNamePrefix = "Java23";
        } else {
            // For any other Java version, the basic EntitlementChecker interface and implementation contains all the supported checks
            classNamePrefix = "";
        }
        final String className = packageName + "." + classNamePrefix + baseClassName;
        Class<?> clazz;
        try {
            clazz = Class.forName(className);
        } catch (ClassNotFoundException e) {
            throw new AssertionError("entitlement lib cannot find entitlement class " + className, e);
        }
        return clazz;
    }

    private static ElasticsearchEntitlementChecker initChecker() {
        final PolicyManager policyManager = createPolicyManager();

        final Class<?> clazz = getVersionSpecificCheckerClass(ElasticsearchEntitlementChecker.class);

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
