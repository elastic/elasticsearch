/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.boot;

import java.lang.ModuleLayer.Controller;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.module.Configuration;
import java.lang.module.ModuleFinder;
import java.nio.file.Path;
import java.util.List;
import java.util.function.Predicate;

class BootESLauncher {

    private BootESLauncher() {}

    // The "real" module path is lib/mods
    private static final Path ES_MODULE_PATH = Path.of(System.getProperty("jdk.module.path")).resolve("mods").toAbsolutePath();

    private static final String SERVER_MODULE_NAME = "org.elasticsearch.server";
    private static final String SERVER_BOOT_PKG_NAME = "org.elasticsearch.bootstrap";
    private static final String SERVER_BOOT_CLS_NAME = SERVER_BOOT_PKG_NAME + ".Elasticsearch";

    // Server requires everything, so is enough for the root set
    private static final List<String> rootModules = List.of(SERVER_MODULE_NAME);

    public static void main(String[] args) throws Throwable {
        System.out.println("BootESLauncher: stdout main enter");
        System.err.println("BootESLauncher: stderr main enter");

        try {
            checkClasspathIsEmpty();
            ModuleLayer bootLayer = checkBootLayer();

            // Resolve and define the ES module layer
            ModuleFinder moduleFinder = ModuleFinder.of(ES_MODULE_PATH);
            Configuration esConfiguration = bootLayer.configuration().resolveAndBind(moduleFinder, ModuleFinder.of(), rootModules);
            Controller controller = ModuleLayer.defineModulesWithOneLoader(
                esConfiguration,
                List.of(bootLayer),
                ClassLoader.getSystemClassLoader()
            );
            ModuleLayer esLayer = controller.layer();

            // Tweak the graph to allow the e.s.boot module to start Elasticsearch
            Module bootModule = BootESLauncher.class.getModule();
            Module serverModule = esLayer.findModule(SERVER_MODULE_NAME).orElseThrow();
            controller.addOpens(serverModule, SERVER_BOOT_PKG_NAME, bootModule);
            bootModule.addReads(serverModule);

            // Start Elasticsearch
            ClassLoader cl = controller.layer().findLoader(SERVER_MODULE_NAME);
            Class<?> c = Class.forName(SERVER_BOOT_CLS_NAME, false, cl);
            MethodHandles.Lookup lookup = MethodHandles.privateLookupIn(c, MethodHandles.lookup());
            MethodHandle methodHandle = lookup.findStatic(c, "main", MethodType.methodType(void.class, String[].class));
            Thread.currentThread().setContextClassLoader(cl);
            methodHandle.invokeExact(args);
        } finally {
            System.out.println("BootESLauncher: stdout main exit");
            System.err.println("BootESLauncher: stderr main exit");
        }
    }

    /**
     * Checks constraints that apply to the boot layer.
     * Adjust as necessary, e.g. to allow one more module, Stable Plugin API.
     */
    static ModuleLayer checkBootLayer() {
        ModuleLayer booLayer = ModuleLayer.boot();
        final Predicate<String> JDKModule = name -> name.startsWith("jdk.") || name.startsWith("java.");
        List<String> modules = booLayer.modules().stream().map(Module::getName).filter(Predicate.not(JDKModule)).toList();
        if (modules.size() != 1) {
            throw new IllegalArgumentException("Expected only one module in the boot layer, but got: " + modules);
        }
        if (modules.get(0).equals(BootESLauncher.class.getModule().getName()) == false) {
            throw new IllegalArgumentException("Unexpected module: " + modules.get(0));
        }
        return booLayer;
    }

    /** Checks that the class path is not set - we don't need or allow it. */
    static void checkClasspathIsEmpty() {
        String cp = System.getProperty("java.class.path");
        if (cp.isEmpty() == false) {
            throw new IllegalArgumentException("Expected empty class path, got: " + cp);
        }
    }
}
