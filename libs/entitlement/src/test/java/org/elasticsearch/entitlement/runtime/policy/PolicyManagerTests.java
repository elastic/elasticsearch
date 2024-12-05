/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.runtime.policy;

import org.elasticsearch.entitlement.runtime.api.NotEntitledException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.compiler.InMemoryJavaCompiler;
import org.elasticsearch.test.jar.JarUtils;

import java.io.IOException;
import java.lang.module.Configuration;
import java.lang.module.ModuleFinder;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Map.entry;
import static org.elasticsearch.entitlement.runtime.policy.PolicyManager.ALL_UNNAMED;
import static org.elasticsearch.test.LambdaMatchers.transformedMatch;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

@ESTestCase.WithoutSecurityManager
public class PolicyManagerTests extends ESTestCase {

    public void testGetEntitlementsThrowsOnMissingPluginUnnamedModule() {
        var policyManager = new PolicyManager(
            createEmptyTestServerPolicy(),
            Map.of("plugin1", createPluginPolicy("plugin.module")),
            c -> "plugin1"
        );

        // Any class from the current module (unnamed) will do
        var callerClass = this.getClass();
        var requestingModule = callerClass.getModule();

        var ex = assertThrows(
            "No policy for the unnamed module",
            NotEntitledException.class,
            () -> policyManager.getEntitlementsOrThrow(callerClass, requestingModule)
        );

        assertEquals(
            "Missing entitlement policy: caller [class org.elasticsearch.entitlement.runtime.policy.PolicyManagerTests], module [null]",
            ex.getMessage()
        );
        assertThat(policyManager.moduleEntitlementsMap, hasEntry(requestingModule, PolicyManager.ModuleEntitlements.NONE));
    }

    public void testGetEntitlementsThrowsOnMissingPolicyForPlugin() {
        var policyManager = new PolicyManager(createEmptyTestServerPolicy(), Map.of(), c -> "plugin1");

        // Any class from the current module (unnamed) will do
        var callerClass = this.getClass();
        var requestingModule = callerClass.getModule();

        var ex = assertThrows(
            "No policy for this plugin",
            NotEntitledException.class,
            () -> policyManager.getEntitlementsOrThrow(callerClass, requestingModule)
        );

        assertEquals(
            "Missing entitlement policy: caller [class org.elasticsearch.entitlement.runtime.policy.PolicyManagerTests], module [null]",
            ex.getMessage()
        );
        assertThat(policyManager.moduleEntitlementsMap, hasEntry(requestingModule, PolicyManager.ModuleEntitlements.NONE));
    }

    public void testGetEntitlementsFailureIsCached() {
        var policyManager = new PolicyManager(createEmptyTestServerPolicy(), Map.of(), c -> "plugin1");

        // Any class from the current module (unnamed) will do
        var callerClass = this.getClass();
        var requestingModule = callerClass.getModule();

        assertThrows(NotEntitledException.class, () -> policyManager.getEntitlementsOrThrow(callerClass, requestingModule));
        assertThat(policyManager.moduleEntitlementsMap, hasEntry(requestingModule, PolicyManager.ModuleEntitlements.NONE));

        // A second time
        var ex = assertThrows(NotEntitledException.class, () -> policyManager.getEntitlementsOrThrow(callerClass, requestingModule));

        assertThat(ex.getMessage(), endsWith("[CACHED]"));
        // Nothing new in the map
        assertThat(policyManager.moduleEntitlementsMap, aMapWithSize(1));
    }

    public void testGetEntitlementsReturnsEntitlementsForPluginUnnamedModule() {
        var policyManager = new PolicyManager(
            createEmptyTestServerPolicy(),
            Map.ofEntries(entry("plugin2", createPluginPolicy(ALL_UNNAMED))),
            c -> "plugin2"
        );

        // Any class from the current module (unnamed) will do
        var callerClass = this.getClass();
        var requestingModule = callerClass.getModule();

        var entitlements = policyManager.getEntitlementsOrThrow(callerClass, requestingModule);
        assertThat(entitlements.hasEntitlement(CreateClassLoaderEntitlement.class), is(true));
    }

    public void testGetEntitlementsThrowsOnMissingPolicyForServer() throws ClassNotFoundException {
        var policyManager = new PolicyManager(createTestServerPolicy("example"), Map.of(), c -> null);

        // Tests do not run modular, so we cannot use a server class.
        // But we know that in production code the server module and its classes are in the boot layer.
        // So we use a random module in the boot layer, and a random class from that module (not java.base -- it is
        // loaded too early) to mimic a class that would be in the server module.
        var mockServerClass = ModuleLayer.boot().findLoader("jdk.httpserver").loadClass("com.sun.net.httpserver.HttpServer");
        var requestingModule = mockServerClass.getModule();

        var ex = assertThrows(
            "No policy for this module in server",
            NotEntitledException.class,
            () -> policyManager.getEntitlementsOrThrow(mockServerClass, requestingModule)
        );

        assertEquals(
            "Missing entitlement policy: caller [class com.sun.net.httpserver.HttpServer], module [jdk.httpserver]",
            ex.getMessage()
        );
        assertThat(policyManager.moduleEntitlementsMap, hasEntry(requestingModule, PolicyManager.ModuleEntitlements.NONE));
    }

    public void testGetEntitlementsReturnsEntitlementsForServerModule() throws ClassNotFoundException {
        var policyManager = new PolicyManager(createTestServerPolicy("jdk.httpserver"), Map.of(), c -> null);

        // Tests do not run modular, so we cannot use a server class.
        // But we know that in production code the server module and its classes are in the boot layer.
        // So we use a random module in the boot layer, and a random class from that module (not java.base -- it is
        // loaded too early) to mimic a class that would be in the server module.
        var mockServerClass = ModuleLayer.boot().findLoader("jdk.httpserver").loadClass("com.sun.net.httpserver.HttpServer");
        var requestingModule = mockServerClass.getModule();

        var entitlements = policyManager.getEntitlementsOrThrow(mockServerClass, requestingModule);
        assertThat(entitlements.hasEntitlement(CreateClassLoaderEntitlement.class), is(true));
        assertThat(entitlements.hasEntitlement(ExitVMEntitlement.class), is(true));
    }

    public void testGetEntitlementsReturnsEntitlementsForPluginModule() throws IOException, ClassNotFoundException {
        final Path home = createTempDir();

        Path jar = creteMockPluginJar(home);

        var policyManager = new PolicyManager(
            createEmptyTestServerPolicy(),
            Map.of("mock-plugin", createPluginPolicy("org.example.plugin")),
            c -> "mock-plugin"
        );

        var layer = createLayerForJar(jar, "org.example.plugin");
        var mockPluginClass = layer.findLoader("org.example.plugin").loadClass("q.B");
        var requestingModule = mockPluginClass.getModule();

        var entitlements = policyManager.getEntitlementsOrThrow(mockPluginClass, requestingModule);
        assertThat(entitlements.hasEntitlement(CreateClassLoaderEntitlement.class), is(true));
        assertThat(
            entitlements.getEntitlements(FileEntitlement.class).toList(),
            contains(transformedMatch(FileEntitlement::toString, containsString("/test/path")))
        );
    }

    public void testGetEntitlementsResultIsCached() {
        var policyManager = new PolicyManager(
            createEmptyTestServerPolicy(),
            Map.ofEntries(entry("plugin2", createPluginPolicy(ALL_UNNAMED))),
            c -> "plugin2"
        );

        // Any class from the current module (unnamed) will do
        var callerClass = this.getClass();
        var requestingModule = callerClass.getModule();

        var entitlements = policyManager.getEntitlementsOrThrow(callerClass, requestingModule);
        assertThat(entitlements.hasEntitlement(CreateClassLoaderEntitlement.class), is(true));
        assertThat(policyManager.moduleEntitlementsMap, aMapWithSize(1));
        var cachedResult = policyManager.moduleEntitlementsMap.values().stream().findFirst().get();
        var entitlementsAgain = policyManager.getEntitlementsOrThrow(callerClass, requestingModule);

        // Nothing new in the map
        assertThat(policyManager.moduleEntitlementsMap, aMapWithSize(1));
        assertThat(entitlementsAgain, sameInstance(cachedResult));
    }

    private static Policy createEmptyTestServerPolicy() {
        return new Policy("server", List.of());
    }

    private static Policy createTestServerPolicy(String scopeName) {
        return new Policy("server", List.of(new Scope(scopeName, List.of(new ExitVMEntitlement(), new CreateClassLoaderEntitlement()))));
    }

    private static Policy createPluginPolicy(String... pluginModules) {
        return new Policy(
            "plugin",
            Arrays.stream(pluginModules)
                .map(
                    name -> new Scope(
                        name,
                        List.of(new FileEntitlement("/test/path", List.of(FileEntitlement.READ)), new CreateClassLoaderEntitlement())
                    )
                )
                .toList()
        );
    }

    private static Path creteMockPluginJar(Path home) throws IOException {
        Path jar = home.resolve("mock-plugin.jar");

        Map<String, CharSequence> sources = Map.ofEntries(
            entry("module-info", "module org.example.plugin { exports q; }"),
            entry("q.B", "package q; public class B { }")
        );

        var classToBytes = InMemoryJavaCompiler.compile(sources);
        JarUtils.createJarWithEntries(
            jar,
            Map.ofEntries(entry("module-info.class", classToBytes.get("module-info")), entry("q/B.class", classToBytes.get("q.B")))
        );
        return jar;
    }

    private static ModuleLayer createLayerForJar(Path jar, String moduleName) {
        Configuration cf = ModuleLayer.boot().configuration().resolve(ModuleFinder.of(jar), ModuleFinder.of(), Set.of(moduleName));
        var moduleController = ModuleLayer.defineModulesWithOneLoader(
            cf,
            List.of(ModuleLayer.boot()),
            ClassLoader.getPlatformClassLoader()
        );
        return moduleController.layer();
    }
}
