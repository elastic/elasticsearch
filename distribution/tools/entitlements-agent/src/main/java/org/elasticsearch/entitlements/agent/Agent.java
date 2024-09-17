/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlements.agent;

import org.elasticsearch.entitlements.instrumentation.Instrumenter;
import org.elasticsearch.entitlements.instrumentation.MethodKey;
import org.elasticsearch.entitlements.runtime.api.EntitlementChecks;
import org.elasticsearch.entitlements.runtime.config.SystemMethods;

import java.lang.instrument.Instrumentation;
import java.lang.reflect.Method;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.jar.JarFile;

import static java.util.stream.Collectors.toSet;

public class Agent {

    public static void premain(String agentArgs, Instrumentation inst) throws Exception {
        // System.out.println("Starting premain");

        // Add the runtime library (the one with the entitlement checks) to the bootstrap classpath
        var jar = Paths.get(EntitlementChecks.class.getProtectionDomain().getCodeSource().getLocation().toURI()).toFile();
        inst.appendToBootstrapClassLoaderSearch(new JarFile(jar));

        // Hardcoded config for now
        Method targetMethod = System.class.getDeclaredMethod("exit", int.class);
        Method instrumentationMethod = SystemMethods.class.getDeclaredMethod("exit", Class.class, System.class, int.class);
        var methodMap = Map.of(MethodKey.forTargetMethod(targetMethod), instrumentationMethod);
        var classesToInstrument = List.of(System.class);

        inst.addTransformer(
            new Transformer(new Instrumenter("", methodMap), classesToInstrument.stream().map(Agent::internalName).collect(toSet())),
            true
        );
        // System.out.println("Starting retransformClasses");
        inst.retransformClasses(classesToInstrument.toArray(new Class<?>[0]));
        // System.out.println("Finished initialization");
    }

    private static String internalName(Class<?> c) {
        return c.getName().replace('.', '/');
    }

    // private static final Logger LOGGER = LogManager.getLogger(Agent.class);
}
