/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.agent;

import org.elasticsearch.bootstrap.EntitlementBootstrap;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.internal.provider.ProviderLocator;
import org.elasticsearch.entitlement.api.EntitlementChecks;
import org.elasticsearch.entitlement.api.EntitlementProvider;
import org.elasticsearch.entitlement.instrumentation.InstrumentationService;
import org.elasticsearch.entitlement.instrumentation.MethodKey;

import java.io.IOException;
import java.lang.instrument.Instrumentation;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.Set;
import java.util.jar.JarFile;

public class EntitlementAgent {

    public static void premain(String agentArgs, Instrumentation inst) throws Exception {
        agentmain(agentArgs, inst);
    }

    public static void agentmain(String agentArgs, Instrumentation inst) throws Exception {
        var parameters = EntitlementBootstrap.agentParameters();
        // Add the entitlement libraries to the classpath.
        // We can't actually reference the classes here for real before this, because they won't resolve.
        addJarToBootstrapClassLoader(inst, parameters.bridgeLibrary());
        addJarToSystemClassLoader(inst, parameters.runtimeLibrary());
        addJarToSystemClassLoader(inst, parameters.instrumentationLibrary());

        // Ensure the checks are available and ready before we start adding instrumentation that will try to use them
        EntitlementProvider.checks();

        System.out.println("*** PATDOYLE *** InstrumentationService module " + InstrumentationService.class.getModule() + "; layer " + InstrumentationService.class.getModule().getLayer());
        System.out.println("*** PATDOYLE *** EntitlementChecks module " + EntitlementChecks.class.getModule() + "; layer " + EntitlementChecks.class.getModule().getLayer());

        InstrumentationService instrumentationService = (new ProviderLocator<>(
            "entitlement-instrumentation",
            InstrumentationService.class,
            "org.elasticsearch.entitlement.instrumentation.impl",
            Set.of("org.objectweb.nonexistent.asm")
        )).get();

        Method targetMethod = System.class.getMethod("exit", int.class);
        Method instrumentationMethod = EntitlementChecks.class.getMethod("checkSystemExit", Class.class, int.class);
        Map<MethodKey, Method> methodMap = Map.of(instrumentationService.methodKeyForTarget(targetMethod), instrumentationMethod);

        inst.addTransformer(new Transformer(instrumentationService.newInstrumenter("", methodMap), Set.of(internalName(System.class))), true);
        inst.retransformClasses(System.class);
    }

    @SuppressForbidden(reason = "The appendToBootstrapClassLoaderSearch method takes a JarFile")
    private static void addJarToBootstrapClassLoader(Instrumentation inst, String jarString) throws IOException {
        inst.appendToBootstrapClassLoaderSearch(new JarFile(jarString));
    }

    @SuppressForbidden(reason = "The appendToSystemClassLoaderSearch method takes a JarFile")
    private static void addJarToSystemClassLoader(Instrumentation inst, String jarString) throws IOException {
        inst.appendToSystemClassLoaderSearch(new JarFile(jarString));
    }

    private static String internalName(Class<?> c) {
        return c.getName().replace('.', '/');
    }

    // private static final Logger LOGGER = LogManager.getLogger(EntitlementAgent.class);
}
