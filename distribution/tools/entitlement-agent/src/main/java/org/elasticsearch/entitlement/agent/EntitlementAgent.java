/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.agent;

import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.internal.provider.ProviderLocator;
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
        // Add the bridge library (the one with the entitlement checking interface) to the bootstrap classpath.
        // We can't actually reference the classes here for real before this point because they won't resolve.
        var bridgeJarName = System.getProperty("es.entitlements.bridgeJar");
        if (bridgeJarName == null) {
            throw new IllegalArgumentException("System property es.entitlements.bridgeJar is required");
        }
        addJarToBootstrapClassLoader(inst, bridgeJarName);

        Method targetMethod = System.class.getMethod("exit", int.class);
        Method instrumentationMethod = Class.forName("org.elasticsearch.entitlement.api.EntitlementChecks")
            .getMethod("checkSystemExit", Class.class, int.class);
        Map<MethodKey, Method> methodMap = Map.of(INSTRUMENTER_FACTORY.methodKeyForTarget(targetMethod), instrumentationMethod);

        inst.addTransformer(new Transformer(INSTRUMENTER_FACTORY.newInstrumenter("", methodMap), Set.of(internalName(System.class))), true);
        inst.retransformClasses(System.class);
    }

    @SuppressForbidden(reason = "The appendToBootstrapClassLoaderSearch method takes a JarFile")
    private static void addJarToBootstrapClassLoader(Instrumentation inst, String jarString) throws IOException {
        inst.appendToBootstrapClassLoaderSearch(new JarFile(jarString));
    }

    private static String internalName(Class<?> c) {
        return c.getName().replace('.', '/');
    }

    private static final InstrumentationService INSTRUMENTER_FACTORY = (new ProviderLocator<>(
        "entitlement-agent",
        InstrumentationService.class,
        "org.elasticsearch.entitlement.agent.impl",
        Set.of("org.objectweb.nonexistent.asm")
    )).get();

    // private static final Logger LOGGER = LogManager.getLogger(EntitlementAgent.class);
}
