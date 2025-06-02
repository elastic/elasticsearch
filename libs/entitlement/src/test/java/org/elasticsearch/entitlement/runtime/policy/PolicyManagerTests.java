/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.runtime.policy;

import org.elasticsearch.bootstrap.ScopeResolver;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Strings;
import org.elasticsearch.entitlement.runtime.policy.PolicyManager.ModuleEntitlements;
import org.elasticsearch.entitlement.runtime.policy.PolicyManager.PolicyScope;
import org.elasticsearch.entitlement.runtime.policy.agent.TestAgent;
import org.elasticsearch.entitlement.runtime.policy.agent.inner.TestInnerAgent;
import org.elasticsearch.entitlement.runtime.policy.entitlements.CreateClassLoaderEntitlement;
import org.elasticsearch.entitlement.runtime.policy.entitlements.ExitVMEntitlement;
import org.elasticsearch.entitlement.runtime.policy.entitlements.FilesEntitlement;
import org.elasticsearch.entitlement.runtime.policy.entitlements.OutboundNetworkEntitlement;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.compiler.InMemoryJavaCompiler;
import org.elasticsearch.test.jar.JarUtils;
import org.junit.BeforeClass;

import java.io.IOException;
import java.lang.StackWalker.StackFrame;
import java.lang.module.Configuration;
import java.lang.module.ModuleFinder;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Map.entry;
import static org.elasticsearch.entitlement.runtime.policy.PolicyManager.ComponentKind.SERVER;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class PolicyManagerTests extends ESTestCase {

    /**
     * A test agent package name for use in tests.
     */
    private static final String TEST_AGENTS_PACKAGE_NAME = "org.elasticsearch.entitlement.runtime.policy.agent";

    /**
     * A module you can use for test cases that don't actually care about the
     * entitlement module.
     */
    static Module NO_ENTITLEMENTS_MODULE;

    static PathLookup TEST_PATH_LOOKUP;

    @BeforeClass
    public static void beforeClass() {
        try {
            // Any old module will do for tests using NO_ENTITLEMENTS_MODULE
            NO_ENTITLEMENTS_MODULE = makeClassInItsOwnModule().getModule();

            Path baseDir = createTempDir().toAbsolutePath();
            TEST_PATH_LOOKUP = new PathLookupImpl(
                baseDir.resolve("/user/home"),
                baseDir.resolve("/config"),
                new Path[] { baseDir.resolve("/data1/"), baseDir.resolve("/data2") },
                new Path[] { baseDir.resolve("/shared1"), baseDir.resolve("/shared2") },
                baseDir.resolve("/lib"),
                baseDir.resolve("/modules"),
                baseDir.resolve("/plugins"),
                baseDir.resolve("/logs"),
                baseDir.resolve("/tmp"),
                null,
                Settings.EMPTY::getValues
            );
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    public void testGetEntitlements() {
        // A mutable policyScope we can use to program specific replies
        AtomicReference<PolicyScope> policyScope = new AtomicReference<>();

        // A common policy with a variety of entitlements to test
        Path thisSourcePath = PolicyManager.getComponentPathFromClass(getClass());
        var plugin1SourcePath = Path.of("modules", "plugin1");
        var policyManager = new PolicyManager(
            new Policy("server", List.of(new Scope("org.example.httpclient", List.of(new OutboundNetworkEntitlement())))),
            List.of(),
            Map.of("plugin1", new Policy("plugin1", List.of(new Scope("plugin.module1", List.of(new ExitVMEntitlement()))))),
            c -> policyScope.get(),
            Map.of("plugin1", plugin1SourcePath),
            TEST_PATH_LOOKUP
        );

        // "Unspecified" below means that the module is not named in the policy

        policyScope.set(PolicyScope.server("org.example.httpclient"));
        resetAndCheckEntitlements(
            "Specified entitlements for server",
            getClass(),
            policyManager.policyEntitlements(
                SERVER.componentName,
                thisSourcePath,
                "org.example.httpclient",
                List.of(new OutboundNetworkEntitlement())
            ),
            policyManager
        );

        policyScope.set(PolicyScope.server("plugin.unspecifiedModule"));
        resetAndCheckEntitlements(
            "Default entitlements for unspecified module",
            getClass(),
            policyManager.defaultEntitlements(SERVER.componentName, thisSourcePath, "plugin.unspecifiedModule"),
            policyManager
        );

        policyScope.set(PolicyScope.plugin("plugin1", "plugin.module1"));
        resetAndCheckEntitlements(
            "Specified entitlements for plugin",
            getClass(),
            policyManager.policyEntitlements("plugin1", plugin1SourcePath, "plugin.module1", List.of(new ExitVMEntitlement())),
            policyManager
        );

        policyScope.set(PolicyScope.plugin("plugin1", "plugin.unspecifiedModule"));
        resetAndCheckEntitlements(
            "Default entitlements for plugin",
            getClass(),
            policyManager.defaultEntitlements("plugin1", plugin1SourcePath, "plugin.unspecifiedModule"),
            policyManager
        );
    }

    private void resetAndCheckEntitlements(
        String message,
        Class<?> requestingClass,
        ModuleEntitlements expectedEntitlements,
        PolicyManager policyManager
    ) {
        policyManager.moduleEntitlementsMap.clear();
        assertEquals(message, expectedEntitlements, policyManager.getEntitlements(requestingClass));
        assertEquals(
            "Map has precisely the one expected entry",
            Map.of(requestingClass.getModule(), expectedEntitlements),
            policyManager.moduleEntitlementsMap
        );

        // Fetch a second time and verify the map is unchanged
        policyManager.getEntitlements(requestingClass);
        assertEquals("Map is unchanged", Map.of(requestingClass.getModule(), expectedEntitlements), policyManager.moduleEntitlementsMap);
    }

    public void testAgentsEntitlements() throws IOException, ClassNotFoundException {
        Path home = createTempDir();
        Path unnamedJar = createMockPluginJarForUnnamedModule(home);
        var notAgentClass = makeClassInItsOwnModule();
        var policyManager = new PolicyManager(
            createEmptyTestServerPolicy(),
            List.of(new CreateClassLoaderEntitlement()),
            Map.of(),
            c -> c.getPackageName().startsWith(TEST_AGENTS_PACKAGE_NAME)
                ? PolicyScope.apmAgent("test.agent.module")
                : PolicyScope.plugin("test", "test.plugin.module"),
            Map.of(),
            TEST_PATH_LOOKUP
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
                c -> PolicyScope.plugin("test", moduleName(c)),
                Map.of(),
                TEST_PATH_LOOKUP
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
                c -> PolicyScope.plugin("test", moduleName(c)),
                Map.of(),
                TEST_PATH_LOOKUP
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
                c -> PolicyScope.plugin("plugin1", moduleName(c)),
                Map.of("plugin1", Path.of("modules", "plugin1")),
                TEST_PATH_LOOKUP
            )
        );
        assertEquals(
            "[plugin1] using module [test] found duplicate entitlement " + "[" + FilesEntitlement.class.getName() + "]",
            iae.getMessage()
        );
    }

    public void testFilesEntitlementsWithExclusive() {
        var baseTestPath = Path.of("/base").toAbsolutePath();
        var testPath1 = Path.of("/base/test").toAbsolutePath();
        var testPath2 = Path.of("/base/test/foo").toAbsolutePath();
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
                                "test.module1",
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
                                "test.module2",
                                List.of(
                                    new FilesEntitlement(
                                        List.of(FilesEntitlement.FileData.ofPath(testPath1, FilesEntitlement.Mode.READ).withExclusive(true))
                                    )
                                )
                            )
                        )
                    )
                ),
                c -> PolicyScope.plugin("", moduleName(c)),
                Map.of("plugin1", Path.of("modules", "plugin1"), "plugin2", Path.of("modules", "plugin2")),
                TEST_PATH_LOOKUP
            )
        );
        assertThat(
            iae.getMessage(),
            allOf(
                containsString("Path [" + testPath1 + "] is already exclusive"),
                containsString("[plugin1][test.module1]"),
                containsString("[plugin2][test.module2]"),
                containsString("cannot add exclusive access")
            )
        );

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
                c -> PolicyScope.plugin("", moduleName(c)),
                Map.of(),
                TEST_PATH_LOOKUP
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

    static Class<?> makeClassInItsOwnModule() throws IOException, ClassNotFoundException {
        final Path home = createTempDir();
        Path jar = createMockPluginJar(home);
        var layer = createLayerForJar(jar, "org.example.plugin");
        return layer.findLoader("org.example.plugin").loadClass("q.B");
    }

    static Policy createEmptyTestServerPolicy() {
        return new Policy("server", List.of());
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

    record MockFrame(Class<?> declaringClass) implements StackFrame {
        @Override
        public String getClassName() {
            return getDeclaringClass().getName();
        }

        @Override
        public String getMethodName() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Class<?> getDeclaringClass() {
            return declaringClass;
        }

        @Override
        public int getByteCodeIndex() {
            throw new UnsupportedOperationException();
        }

        @Override
        public String getFileName() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getLineNumber() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isNativeMethod() {
            throw new UnsupportedOperationException();
        }

        @Override
        public StackTraceElement toStackTraceElement() {
            throw new UnsupportedOperationException();
        }
    }

    static String moduleName(Class<?> c) {
        return ScopeResolver.getScopeName(c.getModule());
    }

}
