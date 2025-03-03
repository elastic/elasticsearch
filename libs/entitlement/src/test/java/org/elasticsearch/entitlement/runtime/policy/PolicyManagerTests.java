/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.runtime.policy;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Strings;
import org.elasticsearch.entitlement.runtime.policy.PolicyManager.ModuleEntitlements;
import org.elasticsearch.entitlement.runtime.policy.agent.TestAgent;
import org.elasticsearch.entitlement.runtime.policy.agent.inner.TestInnerAgent;
import org.elasticsearch.entitlement.runtime.policy.entitlements.CreateClassLoaderEntitlement;
import org.elasticsearch.entitlement.runtime.policy.entitlements.ExitVMEntitlement;
import org.elasticsearch.entitlement.runtime.policy.entitlements.FilesEntitlement;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.compiler.InMemoryJavaCompiler;
import org.elasticsearch.test.jar.JarUtils;
import org.junit.BeforeClass;

import java.io.IOException;
import java.lang.module.Configuration;
import java.lang.module.ModuleFinder;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static java.util.Map.entry;
import static org.elasticsearch.entitlement.runtime.policy.PolicyManager.ALL_UNNAMED;
import static org.elasticsearch.entitlement.runtime.policy.PolicyManager.SERVER_COMPONENT_NAME;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

@ESTestCase.WithoutSecurityManager
public class PolicyManagerTests extends ESTestCase {

    /**
     * A test agent package name for use in tests.
     */
    private static final String TEST_AGENTS_PACKAGE_NAME = "org.elasticsearch.entitlement.runtime.policy.agent";

    /**
     * A module you can use for test cases that don't actually care about the
     * entitlement module.
     */
    private static Module NO_ENTITLEMENTS_MODULE;

    private static Path TEST_BASE_DIR;

    private static PathLookup TEST_PATH_LOOKUP;

    @BeforeClass
    public static void beforeClass() {
        try {
            // Any old module will do for tests using NO_ENTITLEMENTS_MODULE
            NO_ENTITLEMENTS_MODULE = makeClassInItsOwnModule().getModule();

            TEST_BASE_DIR = createTempDir().toAbsolutePath();
            TEST_PATH_LOOKUP = new PathLookup(
                TEST_BASE_DIR.resolve("/user/home"),
                TEST_BASE_DIR.resolve("/config"),
                new Path[] { TEST_BASE_DIR.resolve("/data1/"), TEST_BASE_DIR.resolve("/data2") },
                new Path[] { TEST_BASE_DIR.resolve("/shared1"), TEST_BASE_DIR.resolve("/shared2") },
                TEST_BASE_DIR.resolve("/temp"),
                Settings.EMPTY::getValues
            );
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    public void testGetEntitlementsThrowsOnMissingPluginUnnamedModule() {
        var policyManager = new PolicyManager(
            createEmptyTestServerPolicy(),
            List.of(),
            Map.of("plugin1", createPluginPolicy("plugin.module")),
            c -> "plugin1",
            TEST_AGENTS_PACKAGE_NAME,
            NO_ENTITLEMENTS_MODULE,
            TEST_PATH_LOOKUP,
            Set.of()
        );

        // Any class from the current module (unnamed) will do
        var callerClass = this.getClass();
        var requestingModule = callerClass.getModule();

        assertEquals(
            "No policy for the unnamed module",
            policyManager.defaultEntitlements("plugin1"),
            policyManager.getEntitlements(callerClass)
        );

        assertEquals(Map.of(requestingModule, policyManager.defaultEntitlements("plugin1")), policyManager.moduleEntitlementsMap);
    }

    public void testGetEntitlementsThrowsOnMissingPolicyForPlugin() {
        var policyManager = new PolicyManager(
            createEmptyTestServerPolicy(),
            List.of(),
            Map.of(),
            c -> "plugin1",
            TEST_AGENTS_PACKAGE_NAME,
            NO_ENTITLEMENTS_MODULE,
            TEST_PATH_LOOKUP,
            Set.of()
        );

        // Any class from the current module (unnamed) will do
        var callerClass = this.getClass();
        var requestingModule = callerClass.getModule();

        assertEquals("No policy for this plugin", policyManager.defaultEntitlements("plugin1"), policyManager.getEntitlements(callerClass));

        assertEquals(Map.of(requestingModule, policyManager.defaultEntitlements("plugin1")), policyManager.moduleEntitlementsMap);
    }

    public void testGetEntitlementsFailureIsCached() {
        var policyManager = new PolicyManager(
            createEmptyTestServerPolicy(),
            List.of(),
            Map.of(),
            c -> "plugin1",
            TEST_AGENTS_PACKAGE_NAME,
            NO_ENTITLEMENTS_MODULE,
            TEST_PATH_LOOKUP,
            Set.of()
        );

        // Any class from the current module (unnamed) will do
        var callerClass = this.getClass();
        var requestingModule = callerClass.getModule();

        assertEquals(policyManager.defaultEntitlements("plugin1"), policyManager.getEntitlements(callerClass));
        assertEquals(Map.of(requestingModule, policyManager.defaultEntitlements("plugin1")), policyManager.moduleEntitlementsMap);

        // A second time
        assertEquals(policyManager.defaultEntitlements("plugin1"), policyManager.getEntitlements(callerClass));

        // Nothing new in the map
        assertEquals(Map.of(requestingModule, policyManager.defaultEntitlements("plugin1")), policyManager.moduleEntitlementsMap);
    }

    public void testGetEntitlementsReturnsEntitlementsForPluginUnnamedModule() {
        var policyManager = new PolicyManager(
            createEmptyTestServerPolicy(),
            List.of(),
            Map.ofEntries(entry("plugin2", createPluginPolicy(ALL_UNNAMED))),
            c -> "plugin2",
            TEST_AGENTS_PACKAGE_NAME,
            NO_ENTITLEMENTS_MODULE,
            TEST_PATH_LOOKUP,
            Set.of()
        );

        // Any class from the current module (unnamed) will do
        var callerClass = this.getClass();

        var entitlements = policyManager.getEntitlements(callerClass);
        assertThat(entitlements.hasEntitlement(CreateClassLoaderEntitlement.class), is(true));
    }

    public void testGetEntitlementsThrowsOnMissingPolicyForServer() throws ClassNotFoundException {
        var policyManager = new PolicyManager(
            createTestServerPolicy("example"),
            List.of(),
            Map.of(),
            c -> null,
            TEST_AGENTS_PACKAGE_NAME,
            NO_ENTITLEMENTS_MODULE,
            TEST_PATH_LOOKUP,
            Set.of()
        );

        // Tests do not run modular, so we cannot use a server class.
        // But we know that in production code the server module and its classes are in the boot layer.
        // So we use a random module in the boot layer, and a random class from that module (not java.base -- it is
        // loaded too early) to mimic a class that would be in the server module.
        var mockServerClass = ModuleLayer.boot().findLoader("jdk.httpserver").loadClass("com.sun.net.httpserver.HttpServer");
        var requestingModule = mockServerClass.getModule();

        assertEquals(
            "No policy for this module in server",
            policyManager.defaultEntitlements(SERVER_COMPONENT_NAME),
            policyManager.getEntitlements(mockServerClass)
        );

        assertEquals(
            Map.of(requestingModule, policyManager.defaultEntitlements(SERVER_COMPONENT_NAME)),
            policyManager.moduleEntitlementsMap
        );
    }

    public void testGetEntitlementsReturnsEntitlementsForServerModule() throws ClassNotFoundException {
        var policyManager = new PolicyManager(
            createTestServerPolicy("jdk.httpserver"),
            List.of(),
            Map.of(),
            c -> null,
            TEST_AGENTS_PACKAGE_NAME,
            NO_ENTITLEMENTS_MODULE,
            TEST_PATH_LOOKUP,
            Set.of()
        );

        // Tests do not run modular, so we cannot use a server class.
        // But we know that in production code the server module and its classes are in the boot layer.
        // So we use a random module in the boot layer, and a random class from that module (not java.base -- it is
        // loaded too early) to mimic a class that would be in the server module.
        var mockServerClass = ModuleLayer.boot().findLoader("jdk.httpserver").loadClass("com.sun.net.httpserver.HttpServer");

        var entitlements = policyManager.getEntitlements(mockServerClass);
        assertThat(entitlements.hasEntitlement(CreateClassLoaderEntitlement.class), is(true));
        assertThat(entitlements.hasEntitlement(ExitVMEntitlement.class), is(true));
    }

    public void testGetEntitlementsReturnsEntitlementsForPluginModule() throws IOException, ClassNotFoundException {
        final Path home = createTempDir();

        Path jar = createMockPluginJar(home);

        var policyManager = new PolicyManager(
            createEmptyTestServerPolicy(),
            List.of(),
            Map.of("mock-plugin", createPluginPolicy("org.example.plugin")),
            c -> "mock-plugin",
            TEST_AGENTS_PACKAGE_NAME,
            NO_ENTITLEMENTS_MODULE,
            TEST_PATH_LOOKUP,
            Set.of()
        );

        var layer = createLayerForJar(jar, "org.example.plugin");
        var mockPluginClass = layer.findLoader("org.example.plugin").loadClass("q.B");

        var entitlements = policyManager.getEntitlements(mockPluginClass);
        assertThat(entitlements.hasEntitlement(CreateClassLoaderEntitlement.class), is(true));
        assertThat(entitlements.fileAccess().canRead(TEST_BASE_DIR), is(true));
    }

    public void testGetEntitlementsResultIsCached() {
        var policyManager = new PolicyManager(
            createEmptyTestServerPolicy(),
            List.of(),
            Map.ofEntries(entry("plugin2", createPluginPolicy(ALL_UNNAMED))),
            c -> "plugin2",
            TEST_AGENTS_PACKAGE_NAME,
            NO_ENTITLEMENTS_MODULE,
            TEST_PATH_LOOKUP,
            Set.of()
        );

        // Any class from the current module (unnamed) will do
        var callerClass = this.getClass();

        var entitlements = policyManager.getEntitlements(callerClass);
        assertThat(entitlements.hasEntitlement(CreateClassLoaderEntitlement.class), is(true));
        assertThat(policyManager.moduleEntitlementsMap, aMapWithSize(1));
        var cachedResult = policyManager.moduleEntitlementsMap.values().stream().findFirst().orElseThrow();
        var entitlementsAgain = policyManager.getEntitlements(callerClass);

        // Nothing new in the map
        assertThat(policyManager.moduleEntitlementsMap, aMapWithSize(1));
        assertThat(entitlementsAgain, sameInstance(cachedResult));
    }

    public void testRequestingClassFastPath() throws IOException, ClassNotFoundException {
        var callerClass = makeClassInItsOwnModule();
        assertEquals(callerClass, policyManager(TEST_AGENTS_PACKAGE_NAME, NO_ENTITLEMENTS_MODULE).requestingClass(callerClass));
    }

    public void testRequestingModuleWithStackWalk() throws IOException, ClassNotFoundException {
        var entitlementsClass = makeClassInItsOwnModule();    // A class in the entitlements library itself
        var requestingClass = makeClassInItsOwnModule();      // This guy is always the right answer
        var instrumentedClass = makeClassInItsOwnModule();    // The class that called the check method
        var ignorableClass = makeClassInItsOwnModule();

        var policyManager = policyManager(TEST_AGENTS_PACKAGE_NAME, entitlementsClass.getModule());

        assertEquals(
            "Skip entitlement library and the instrumented method",
            requestingClass,
            policyManager.findRequestingClass(Stream.of(entitlementsClass, instrumentedClass, requestingClass, ignorableClass)).orElse(null)
        );
        assertEquals(
            "Skip multiple library frames",
            requestingClass,
            policyManager.findRequestingClass(Stream.of(entitlementsClass, entitlementsClass, instrumentedClass, requestingClass))
                .orElse(null)
        );
        assertThrows(
            "Non-modular caller frames are not supported",
            NullPointerException.class,
            () -> policyManager.findRequestingClass(Stream.of(entitlementsClass, null))
        );
    }

    public void testAgentsEntitlements() throws IOException, ClassNotFoundException {
        Path home = createTempDir();
        Path unnamedJar = createMockPluginJarForUnnamedModule(home);
        var notAgentClass = makeClassInItsOwnModule();
        var policyManager = new PolicyManager(
            createEmptyTestServerPolicy(),
            List.of(new CreateClassLoaderEntitlement()),
            Map.of(),
            c -> c.getPackageName().startsWith(TEST_AGENTS_PACKAGE_NAME) ? null : "test",
            TEST_AGENTS_PACKAGE_NAME,
            NO_ENTITLEMENTS_MODULE,
            TEST_PATH_LOOKUP,
            Set.of()
        );
        ModuleEntitlements agentsEntitlements = policyManager.getEntitlements(TestAgent.class);
        assertThat(agentsEntitlements.hasEntitlement(CreateClassLoaderEntitlement.class), is(true));
        agentsEntitlements = policyManager.getEntitlements(TestInnerAgent.class);
        assertThat(agentsEntitlements.hasEntitlement(CreateClassLoaderEntitlement.class), is(true));
        ModuleEntitlements notAgentsEntitlements = policyManager.getEntitlements(notAgentClass);
        assertThat(notAgentsEntitlements.hasEntitlement(CreateClassLoaderEntitlement.class), is(false));
        try (URLClassLoader classLoader = new URLClassLoader(new URL[] { unnamedJar.toUri().toURL() }, getClass().getClassLoader())) {
            var unnamedNotAgentClass = classLoader.loadClass("q.B");
            notAgentsEntitlements = policyManager.getEntitlements(unnamedNotAgentClass);
            assertThat(notAgentsEntitlements.hasEntitlement(CreateClassLoaderEntitlement.class), is(false));
        }
    }

    public void testDuplicateEntitlements() {
        IllegalArgumentException iae = expectThrows(
            IllegalArgumentException.class,
            () -> new PolicyManager(
                new Policy(
                    "server",
                    List.of(new Scope("test", List.of(new CreateClassLoaderEntitlement(), new CreateClassLoaderEntitlement())))
                ),
                List.of(),
                Map.of(),
                c -> "test",
                TEST_AGENTS_PACKAGE_NAME,
                NO_ENTITLEMENTS_MODULE,
                TEST_PATH_LOOKUP,
                Set.of()
            )
        );
        assertEquals(
            "[(server)] using module [test] found duplicate entitlement " + "[" + CreateClassLoaderEntitlement.class.getName() + "]",
            iae.getMessage()
        );

        iae = expectThrows(
            IllegalArgumentException.class,
            () -> new PolicyManager(
                createEmptyTestServerPolicy(),
                List.of(new CreateClassLoaderEntitlement(), new CreateClassLoaderEntitlement()),
                Map.of(),
                c -> "test",
                TEST_AGENTS_PACKAGE_NAME,
                NO_ENTITLEMENTS_MODULE,
                TEST_PATH_LOOKUP,
                Set.of()
            )
        );
        assertEquals(
            "[(APM agent)] using module [ALL-UNNAMED] found duplicate entitlement "
                + "["
                + CreateClassLoaderEntitlement.class.getName()
                + "]",
            iae.getMessage()
        );

        iae = expectThrows(
            IllegalArgumentException.class,
            () -> new PolicyManager(
                createEmptyTestServerPolicy(),
                List.of(),
                Map.of(
                    "plugin1",
                    new Policy(
                        "test",
                        List.of(
                            new Scope(
                                "test",
                                List.of(
                                    FilesEntitlement.EMPTY,
                                    new CreateClassLoaderEntitlement(),
                                    new FilesEntitlement(
                                        List.of(FilesEntitlement.FileData.ofPath(Path.of("/tmp/test"), FilesEntitlement.Mode.READ))
                                    )
                                )
                            )
                        )
                    )
                ),
                c -> "plugin1",
                TEST_AGENTS_PACKAGE_NAME,
                NO_ENTITLEMENTS_MODULE,
                TEST_PATH_LOOKUP,
                Set.of()
            )
        );
        assertEquals(
            "[plugin1] using module [test] found duplicate entitlement " + "[" + FilesEntitlement.class.getName() + "]",
            iae.getMessage()
        );
    }

    public void testFilesEntitlementsWithExclusive() {
        var baseTestPath = Path.of("/tmp").toAbsolutePath();
        var testPath1 = Path.of("/tmp/test").toAbsolutePath();
        var testPath2 = Path.of("/tmp/test/foo").toAbsolutePath();
        var iae = expectThrows(
            IllegalArgumentException.class,
            () -> new PolicyManager(
                createEmptyTestServerPolicy(),
                List.of(),
                Map.of(
                    "plugin1",
                    new Policy(
                        "test",
                        List.of(
                            new Scope(
                                "test",
                                List.of(
                                    new FilesEntitlement(
                                        List.of(FilesEntitlement.FileData.ofPath(testPath1, FilesEntitlement.Mode.READ).withExclusive(true))
                                    )
                                )
                            )
                        )
                    ),
                    "plugin2",
                    new Policy(
                        "test",
                        List.of(
                            new Scope(
                                "test",
                                List.of(
                                    new FilesEntitlement(
                                        List.of(FilesEntitlement.FileData.ofPath(testPath1, FilesEntitlement.Mode.READ).withExclusive(true))
                                    )
                                )
                            )
                        )
                    )
                ),
                c -> "",
                TEST_AGENTS_PACKAGE_NAME,
                NO_ENTITLEMENTS_MODULE,
                TEST_PATH_LOOKUP,
                Set.of()
            )
        );
        assertTrue(iae.getMessage().contains("duplicate/overlapping exclusive paths found in files entitlements:"));
        assertTrue(iae.getMessage().contains(Strings.format("[test] [%s]]", testPath1.toString())));

        iae = expectThrows(
            IllegalArgumentException.class,
            () -> new PolicyManager(
                new Policy(
                    "test",
                    List.of(
                        new Scope(
                            "test",
                            List.of(
                                new FilesEntitlement(
                                    List.of(
                                        FilesEntitlement.FileData.ofPath(testPath2, FilesEntitlement.Mode.READ).withExclusive(true),
                                        FilesEntitlement.FileData.ofPath(baseTestPath, FilesEntitlement.Mode.READ)
                                    )
                                )
                            )
                        )
                    )
                ),
                List.of(),
                Map.of(
                    "plugin1",
                    new Policy(
                        "test",
                        List.of(
                            new Scope(
                                "test",
                                List.of(
                                    new FilesEntitlement(
                                        List.of(FilesEntitlement.FileData.ofPath(testPath1, FilesEntitlement.Mode.READ).withExclusive(true))
                                    )
                                )
                            )
                        )
                    )
                ),
                c -> "",
                TEST_AGENTS_PACKAGE_NAME,
                NO_ENTITLEMENTS_MODULE,
                TEST_PATH_LOOKUP,
                Set.of()
            )
        );
        assertEquals(
            Strings.format(
                "duplicate/overlapping exclusive paths found in files entitlements: "
                    + "[[plugin1] [test] [%s]] and [[(server)] [test] [%s]]",
                testPath1,
                testPath2
            ),
            iae.getMessage()
        );
    }

    /**
     * If the plugin resolver tells us a class is in a plugin, don't conclude that it's in an agent.
     */
    public void testPluginResolverOverridesAgents() {
        var policyManager = new PolicyManager(
            createEmptyTestServerPolicy(),
            List.of(new CreateClassLoaderEntitlement()),
            Map.of(),
            c -> "test", // Insist that the class is in a plugin
            TEST_AGENTS_PACKAGE_NAME,
            NO_ENTITLEMENTS_MODULE,
            TEST_PATH_LOOKUP,
            Set.of()
        );
        ModuleEntitlements notAgentsEntitlements = policyManager.getEntitlements(TestAgent.class);
        assertThat(notAgentsEntitlements.hasEntitlement(CreateClassLoaderEntitlement.class), is(false));
    }

    private static Class<?> makeClassInItsOwnModule() throws IOException, ClassNotFoundException {
        final Path home = createTempDir();
        Path jar = createMockPluginJar(home);
        var layer = createLayerForJar(jar, "org.example.plugin");
        return layer.findLoader("org.example.plugin").loadClass("q.B");
    }

    private static PolicyManager policyManager(String agentsPackageName, Module entitlementsModule) {
        return new PolicyManager(
            createEmptyTestServerPolicy(),
            List.of(),
            Map.of(),
            c -> "test",
            agentsPackageName,
            entitlementsModule,
            TEST_PATH_LOOKUP,
            Set.of()
        );
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
                        List.of(
                            new FilesEntitlement(List.of(FilesEntitlement.FileData.ofPath(TEST_BASE_DIR, FilesEntitlement.Mode.READ))),
                            new CreateClassLoaderEntitlement()
                        )
                    )
                )
                .toList()
        );
    }

    private static Path createMockPluginJarForUnnamedModule(Path home) throws IOException {
        Path jar = home.resolve("unnamed-mock-plugin.jar");

        Map<String, CharSequence> sources = Map.ofEntries(entry("q.B", "package q; public class B { }"));

        var classToBytes = InMemoryJavaCompiler.compile(sources);
        JarUtils.createJarWithEntries(jar, Map.ofEntries(entry("q/B.class", classToBytes.get("q.B"))));
        return jar;
    }

    private static Path createMockPluginJar(Path home) throws IOException {
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
