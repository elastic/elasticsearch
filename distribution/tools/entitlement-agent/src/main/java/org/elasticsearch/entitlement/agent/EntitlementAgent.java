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
import org.elasticsearch.entitlement.checks.EntitlementChecks;
import org.elasticsearch.entitlement.instrumentation.Instrumenter;

import java.io.File;
import java.io.IOException;
import java.lang.instrument.Instrumentation;
import java.util.List;
import java.util.jar.JarFile;

import static java.util.stream.Collectors.toSet;

public class EntitlementAgent {

    public static void premain(String agentArgs, Instrumentation inst) throws Exception {
        // Add the trampoline library (the one with the entitlement checking interface) to the bootstrap classpath.
        // We can't actually reference the classes here for real before this point because they won't resolve.
        var jarsString = System.getProperty("entitlements.trampolineJars");
        if (jarsString != null) {
            addJarsToBootstrapClassLoader(inst, jarsString);
        }

        ConfigurationScanner.ScanResults config = ConfigurationScanner.scan(List.of(EntitlementChecks.class));

        inst.addTransformer(
            new Transformer(
                new Instrumenter("", config.methodMap()),
                config.classesToInstrument().stream().map(EntitlementAgent::internalName).collect(toSet())
            ),
            true
        );
        // System.out.println("Starting retransformClasses");
        inst.retransformClasses(config.classesToInstrument().toArray(new Class<?>[0]));
        // System.out.println("Finished initialization");
    }

    @SuppressForbidden(reason = "The appendToBootstrapClassLoaderSearch method takes a JarFile")
    private static void addJarsToBootstrapClassLoader(Instrumentation inst, String jarsString) throws IOException {
        for (var jar : jarsString.split(File.pathSeparator)) {
            // System.out.println("Adding jar " + jar);
            inst.appendToBootstrapClassLoaderSearch(new JarFile(jar));
        }
    }

    private static String internalName(Class<?> c) {
        return c.getName().replace('.', '/');
    }

    // private static final Logger LOGGER = LogManager.getLogger(EntitlementAgent.class);
}
