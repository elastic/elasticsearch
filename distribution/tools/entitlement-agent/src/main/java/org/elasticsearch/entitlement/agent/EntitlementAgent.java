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
import org.elasticsearch.entitlement.api.EntitlementChecks;
import org.elasticsearch.entitlement.instrumentation.Instrumenter;
import org.elasticsearch.entitlement.instrumentation.MethodKey;

import java.io.File;
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
        var jarsString = System.getProperty("es.entitlements.bridgeJars");
        if (jarsString == null) {
            throw new IllegalArgumentException("System property es.entitlements.bridgeJars is required");
        }
        addJarsToBootstrapClassLoader(inst, jarsString);

        Method targetMethod = System.class.getDeclaredMethod("exit", int.class);
        Method instrumentationMethod = EntitlementChecks.class.getDeclaredMethod(
            "checkSystemExit", Class.class, int.class);
        Map<MethodKey, Method> methodMap = Map.of(MethodKey.forTargetMethod(targetMethod), instrumentationMethod);

        inst.addTransformer(
            new Transformer(
                new Instrumenter("", methodMap),
                Set.of(internalName(System.class))
            ),
            true
        );
        inst.retransformClasses(System.class);
    }

    @SuppressForbidden(reason = "The appendToBootstrapClassLoaderSearch method takes a JarFile")
    private static void addJarsToBootstrapClassLoader(Instrumentation inst, String jarsString) throws IOException {
        String[] jars = jarsString.split(File.pathSeparator);
        if (jars.length != 1) {
            throw new IllegalArgumentException(jarsString + " must point to a single jar file");
        }
        for (var jar : jars) {
            // System.out.println("Adding jar " + jar);
            inst.appendToBootstrapClassLoaderSearch(new JarFile(jar));
        }
    }

    private static String internalName(Class<?> c) {
        return c.getName().replace('.', '/');
    }

    // private static final Logger LOGGER = LogManager.getLogger(EntitlementAgent.class);
}
