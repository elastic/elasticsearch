/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.bootstrap;

import org.elasticsearch.plugins.PluginBundle;
import org.elasticsearch.plugins.PluginDescriptor;
import org.elasticsearch.plugins.PluginsLoader;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.compiler.InMemoryJavaCompiler;
import org.elasticsearch.test.jar.JarUtils;

import java.io.IOException;
import java.lang.module.Configuration;
import java.lang.module.ModuleFinder;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static java.util.Map.entry;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ESTestCase.WithoutSecurityManager
public class PluginsResolverTests extends ESTestCase {

    private record TestPluginLayer(PluginBundle pluginBundle, ClassLoader pluginClassLoader, ModuleLayer pluginModuleLayer)
        implements
            PluginsLoader.PluginLayer {}

    public void testResolveModularPlugin() throws IOException, ClassNotFoundException {
        String moduleName = "modular.plugin";
        String pluginName = "modular-plugin";

        final Path home = createTempDir();

        Path jar = createModularPluginJar(home, pluginName, moduleName, "p", "A");

        var layer = createModuleLayer(moduleName, jar);
        var loader = layer.findLoader(moduleName);

        PluginBundle bundle = createMockBundle(pluginName, moduleName, "p.A");
        PluginsLoader mockPluginsLoader = mock(PluginsLoader.class);

        when(mockPluginsLoader.pluginLayers()).thenReturn(Stream.of(new TestPluginLayer(bundle, loader, layer)));
        PluginsResolver pluginsResolver = PluginsResolver.create(mockPluginsLoader);

        var testClass = loader.loadClass("p.A");
        var resolvedPluginName = pluginsResolver.resolveClassToPluginName(testClass);
        var unresolvedPluginName1 = pluginsResolver.resolveClassToPluginName(PluginsResolver.class);
        var unresolvedPluginName2 = pluginsResolver.resolveClassToPluginName(String.class);

        assertEquals(pluginName, resolvedPluginName);
        assertNull(unresolvedPluginName1);
        assertNull(unresolvedPluginName2);
    }

    public void testResolveMultipleModularPlugins() throws IOException, ClassNotFoundException {
        final Path home = createTempDir();

        Path jar1 = createModularPluginJar(home, "plugin1", "module.one", "p", "A");
        Path jar2 = createModularPluginJar(home, "plugin2", "module.two", "q", "B");

        var layer1 = createModuleLayer("module.one", jar1);
        var loader1 = layer1.findLoader("module.one");
        var layer2 = createModuleLayer("module.two", jar2);
        var loader2 = layer2.findLoader("module.two");

        PluginBundle bundle1 = createMockBundle("plugin1", "module.one", "p.A");
        PluginBundle bundle2 = createMockBundle("plugin2", "module.two", "q.B");
        PluginsLoader mockPluginsLoader = mock(PluginsLoader.class);

        when(mockPluginsLoader.pluginLayers()).thenReturn(
            Stream.of(new TestPluginLayer(bundle1, loader1, layer1), new TestPluginLayer(bundle2, loader2, layer2))
        );
        PluginsResolver pluginsResolver = PluginsResolver.create(mockPluginsLoader);

        var testClass1 = loader1.loadClass("p.A");
        var testClass2 = loader2.loadClass("q.B");
        var resolvedPluginName1 = pluginsResolver.resolveClassToPluginName(testClass1);
        var resolvedPluginName2 = pluginsResolver.resolveClassToPluginName(testClass2);

        assertEquals("plugin1", resolvedPluginName1);
        assertEquals("plugin2", resolvedPluginName2);
    }

    public void testResolveReferencedModulesInModularPlugins() throws IOException, ClassNotFoundException {
        final Path home = createTempDir();

        Path dependencyJar = createModularPluginJar(home, "plugin1", "module.one", "p", "A");
        Path pluginJar = home.resolve("plugin2.jar");

        Map<String, CharSequence> sources = Map.ofEntries(
            entry("module-info", "module module.two { exports q; requires module.one; }"),
            entry("q.B", "package q; public class B { public p.A a = null; }")
        );

        var classToBytes = InMemoryJavaCompiler.compile(sources, "--add-modules", "module.one", "-p", home.toString());
        JarUtils.createJarWithEntries(
            pluginJar,
            Map.ofEntries(entry("module-info.class", classToBytes.get("module-info")), entry("q/B.class", classToBytes.get("q.B")))
        );

        var layer = createModuleLayer("module.two", pluginJar, dependencyJar);
        var loader = layer.findLoader("module.two");

        PluginBundle bundle = createMockBundle("plugin2", "module.two", "q.B");
        PluginsLoader mockPluginsLoader = mock(PluginsLoader.class);

        when(mockPluginsLoader.pluginLayers()).thenReturn(Stream.of(new TestPluginLayer(bundle, loader, layer)));
        PluginsResolver pluginsResolver = PluginsResolver.create(mockPluginsLoader);

        var testClass1 = loader.loadClass("p.A");
        var testClass2 = loader.loadClass("q.B");
        var resolvedPluginName1 = pluginsResolver.resolveClassToPluginName(testClass1);
        var resolvedPluginName2 = pluginsResolver.resolveClassToPluginName(testClass2);

        assertEquals("plugin2", resolvedPluginName1);
        assertEquals("plugin2", resolvedPluginName2);
    }

    public void testResolveMultipleNonModularPlugins() throws IOException, ClassNotFoundException {
        final Path home = createTempDir();

        Path jar1 = createNonModularPluginJar(home, "plugin1", "p", "A");
        Path jar2 = createNonModularPluginJar(home, "plugin2", "q", "B");

        try (var loader1 = createClassLoader(jar1); var loader2 = createClassLoader(jar2)) {

            PluginBundle bundle1 = createMockBundle("plugin1", null, "p.A");
            PluginBundle bundle2 = createMockBundle("plugin2", null, "q.B");
            PluginsLoader mockPluginsLoader = mock(PluginsLoader.class);

            when(mockPluginsLoader.pluginLayers()).thenReturn(
                Stream.of(
                    new TestPluginLayer(bundle1, loader1, ModuleLayer.boot()),
                    new TestPluginLayer(bundle2, loader2, ModuleLayer.boot())
                )
            );
            PluginsResolver pluginsResolver = PluginsResolver.create(mockPluginsLoader);

            var testClass1 = loader1.loadClass("p.A");
            var testClass2 = loader2.loadClass("q.B");
            var resolvedPluginName1 = pluginsResolver.resolveClassToPluginName(testClass1);
            var resolvedPluginName2 = pluginsResolver.resolveClassToPluginName(testClass2);

            assertEquals("plugin1", resolvedPluginName1);
            assertEquals("plugin2", resolvedPluginName2);
        }
    }

    public void testResolveNonModularPlugin() throws IOException, ClassNotFoundException {
        String pluginName = "non-modular-plugin";

        final Path home = createTempDir();

        Path jar = createNonModularPluginJar(home, pluginName, "p", "A");

        try (var loader = createClassLoader(jar)) {
            PluginBundle bundle = createMockBundle(pluginName, null, "p.A");
            PluginsLoader mockPluginsLoader = mock(PluginsLoader.class);

            when(mockPluginsLoader.pluginLayers()).thenReturn(Stream.of(new TestPluginLayer(bundle, loader, ModuleLayer.boot())));
            PluginsResolver pluginsResolver = PluginsResolver.create(mockPluginsLoader);

            var testClass = loader.loadClass("p.A");
            var resolvedPluginName = pluginsResolver.resolveClassToPluginName(testClass);
            var unresolvedPluginName1 = pluginsResolver.resolveClassToPluginName(PluginsResolver.class);
            var unresolvedPluginName2 = pluginsResolver.resolveClassToPluginName(String.class);

            assertEquals(pluginName, resolvedPluginName);
            assertNull(unresolvedPluginName1);
            assertNull(unresolvedPluginName2);
        }
    }

    private static URLClassLoader createClassLoader(Path jar) throws MalformedURLException {
        return new URLClassLoader(new URL[] { jar.toUri().toURL() });
    }

    private static ModuleLayer createModuleLayer(String moduleName, Path... jars) {
        var finder = ModuleFinder.of(jars);
        Configuration cf = ModuleLayer.boot().configuration().resolve(finder, ModuleFinder.of(), Set.of(moduleName));
        var moduleController = ModuleLayer.defineModulesWithOneLoader(
            cf,
            List.of(ModuleLayer.boot()),
            ClassLoader.getPlatformClassLoader()
        );
        return moduleController.layer();
    }

    private static PluginBundle createMockBundle(String pluginName, String moduleName, String fqClassName) {
        PluginDescriptor pd = new PluginDescriptor(
            pluginName,
            null,
            null,
            null,
            null,
            fqClassName,
            moduleName,
            List.of(),
            false,
            false,
            true,
            false
        );

        PluginBundle bundle = mock(PluginBundle.class);
        when(bundle.pluginDescriptor()).thenReturn(pd);
        return bundle;
    }

    private static Path createModularPluginJar(Path home, String pluginName, String moduleName, String packageName, String className)
        throws IOException {
        Path jar = home.resolve(pluginName + ".jar");
        String fqClassName = packageName + "." + className;

        Map<String, CharSequence> sources = Map.ofEntries(
            entry("module-info", "module " + moduleName + " { exports " + packageName + "; }"),
            entry(fqClassName, "package " + packageName + "; public class " + className + " {}")
        );

        var classToBytes = InMemoryJavaCompiler.compile(sources);
        JarUtils.createJarWithEntries(
            jar,
            Map.ofEntries(
                entry("module-info.class", classToBytes.get("module-info")),
                entry(packageName + "/" + className + ".class", classToBytes.get(fqClassName))
            )
        );
        return jar;
    }

    private static Path createNonModularPluginJar(Path home, String pluginName, String packageName, String className) throws IOException {
        Path jar = home.resolve(pluginName + ".jar");
        String fqClassName = packageName + "." + className;

        Map<String, CharSequence> sources = Map.ofEntries(
            entry(fqClassName, "package " + packageName + "; public class " + className + " {}")
        );

        var classToBytes = InMemoryJavaCompiler.compile(sources);
        JarUtils.createJarWithEntries(jar, Map.ofEntries(entry(packageName + "/" + className + ".class", classToBytes.get(fqClassName))));
        return jar;
    }
}
