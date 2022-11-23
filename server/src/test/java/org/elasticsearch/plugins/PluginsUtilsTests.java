/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins;

import org.apache.logging.log4j.Level;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.Version;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.jdk.JarHell;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;

@LuceneTestCase.SuppressFileSystems(value = "ExtrasFS")
public class PluginsUtilsTests extends ESTestCase {

    PluginDescriptor newTestDescriptor(String name, List<String> deps) {
        String javaVersion = Runtime.version().toString();
        return new PluginDescriptor(name, "desc", "1.0", Version.CURRENT, javaVersion, "MyPlugin", null, deps, false, false, false, false);
    }

    public void testExistingPluginMissingDescriptor() throws Exception {
        Path pluginsDir = createTempDir();
        Files.createDirectory(pluginsDir.resolve("plugin-missing-descriptor"));
        IllegalStateException e = expectThrows(IllegalStateException.class, () -> PluginsUtils.getPluginBundles(pluginsDir));
        assertThat(e.getMessage(), containsString("Plugin [plugin-missing-descriptor] is missing a descriptor properties file"));
    }

    public void testSortBundlesCycleSelfReference() throws Exception {
        Path pluginDir = createTempDir();
        PluginDescriptor info = newTestDescriptor("foo", List.of("foo"));
        PluginBundle bundle = new PluginBundle(info, pluginDir);
        IllegalStateException e = expectThrows(IllegalStateException.class, () -> PluginsUtils.sortBundles(Collections.singleton(bundle)));
        assertEquals("Cycle found in plugin dependencies: foo -> foo", e.getMessage());
    }

    public void testSortBundlesCycle() throws Exception {
        Path pluginDir = createTempDir();
        Set<PluginBundle> bundles = new LinkedHashSet<>(); // control iteration order, so we get know the beginning of the cycle
        PluginDescriptor info = newTestDescriptor("foo", List.of("bar", "other"));
        bundles.add(new PluginBundle(info, pluginDir));
        PluginDescriptor info2 = newTestDescriptor("bar", List.of("baz"));
        bundles.add(new PluginBundle(info2, pluginDir));
        PluginDescriptor info3 = newTestDescriptor("baz", List.of("foo"));
        bundles.add(new PluginBundle(info3, pluginDir));
        PluginDescriptor info4 = newTestDescriptor("other", List.of());
        bundles.add(new PluginBundle(info4, pluginDir));

        IllegalStateException e = expectThrows(IllegalStateException.class, () -> PluginsUtils.sortBundles(bundles));
        assertEquals("Cycle found in plugin dependencies: foo -> bar -> baz -> foo", e.getMessage());
    }

    public void testSortBundlesSingle() throws Exception {
        Path pluginDir = createTempDir();
        PluginDescriptor info = newTestDescriptor("foo", List.of());
        PluginBundle bundle = new PluginBundle(info, pluginDir);
        List<PluginBundle> sortedBundles = PluginsUtils.sortBundles(Collections.singleton(bundle));
        assertThat(sortedBundles, Matchers.contains(bundle));
    }

    public void testSortBundlesNoDeps() throws Exception {
        Path pluginDir = createTempDir();
        Set<PluginBundle> bundles = new LinkedHashSet<>(); // control iteration order
        PluginDescriptor info1 = newTestDescriptor("foo", List.of());
        PluginBundle bundle1 = new PluginBundle(info1, pluginDir);
        bundles.add(bundle1);
        PluginDescriptor info2 = newTestDescriptor("bar", List.of());
        PluginBundle bundle2 = new PluginBundle(info2, pluginDir);
        bundles.add(bundle2);
        PluginDescriptor info3 = newTestDescriptor("baz", List.of());
        PluginBundle bundle3 = new PluginBundle(info3, pluginDir);
        bundles.add(bundle3);
        List<PluginBundle> sortedBundles = PluginsUtils.sortBundles(bundles);
        assertThat(sortedBundles, Matchers.contains(bundle1, bundle2, bundle3));
    }

    public void testSortBundlesMissingDep() throws Exception {
        Path pluginDir = createTempDir();
        PluginDescriptor info = newTestDescriptor("foo", List.of("dne"));
        PluginBundle bundle = new PluginBundle(info, pluginDir);
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> PluginsUtils.sortBundles(Collections.singleton(bundle))
        );
        assertEquals("Missing plugin [dne], dependency of [foo]", e.getMessage());
    }

    public void testSortBundlesCommonDep() throws Exception {
        Path pluginDir = createTempDir();
        Set<PluginBundle> bundles = new LinkedHashSet<>(); // control iteration order
        PluginDescriptor info1 = newTestDescriptor("grandparent", List.of());
        PluginBundle bundle1 = new PluginBundle(info1, pluginDir);
        bundles.add(bundle1);
        PluginDescriptor info2 = newTestDescriptor("foo", List.of("common"));
        PluginBundle bundle2 = new PluginBundle(info2, pluginDir);
        bundles.add(bundle2);
        PluginDescriptor info3 = newTestDescriptor("bar", List.of("common"));
        PluginBundle bundle3 = new PluginBundle(info3, pluginDir);
        bundles.add(bundle3);
        PluginDescriptor info4 = newTestDescriptor("common", List.of("grandparent"));
        PluginBundle bundle4 = new PluginBundle(info4, pluginDir);
        bundles.add(bundle4);
        List<PluginBundle> sortedBundles = PluginsUtils.sortBundles(bundles);
        assertThat(sortedBundles, Matchers.contains(bundle1, bundle4, bundle2, bundle3));
    }

    public void testSortBundlesAlreadyOrdered() throws Exception {
        Path pluginDir = createTempDir();
        Set<PluginBundle> bundles = new LinkedHashSet<>(); // control iteration order
        PluginDescriptor info1 = newTestDescriptor("dep", List.of());
        PluginBundle bundle1 = new PluginBundle(info1, pluginDir);
        bundles.add(bundle1);
        PluginDescriptor info2 = newTestDescriptor("myplugin", List.of("dep"));
        PluginBundle bundle2 = new PluginBundle(info2, pluginDir);
        bundles.add(bundle2);
        List<PluginBundle> sortedBundles = PluginsUtils.sortBundles(bundles);
        assertThat(sortedBundles, Matchers.contains(bundle1, bundle2));
    }

    public static class DummyClass1 {}

    public static class DummyClass2 {}

    public static class DummyClass3 {}

    void makeJar(Path jarFile, Class<?>... classes) throws Exception {
        try (ZipOutputStream out = new ZipOutputStream(Files.newOutputStream(jarFile))) {
            for (Class<?> clazz : classes) {
                String relativePath = clazz.getCanonicalName().replaceAll("\\.", "/") + ".class";
                if (relativePath.contains(PluginsUtilsTests.class.getSimpleName())) {
                    // static inner class of this test
                    relativePath = relativePath.replace("/" + clazz.getSimpleName(), "$" + clazz.getSimpleName());
                }

                Path codebase = PathUtils.get(clazz.getProtectionDomain().getCodeSource().getLocation().toURI());
                if (codebase.toString().endsWith(".jar")) {
                    // copy from jar, exactly as is
                    out.putNextEntry(new ZipEntry(relativePath));
                    try (ZipInputStream in = new ZipInputStream(Files.newInputStream(codebase))) {
                        ZipEntry entry = in.getNextEntry();
                        while (entry != null) {
                            if (entry.getName().equals(relativePath)) {
                                byte[] buffer = new byte[10 * 1024];
                                int read = in.read(buffer);
                                while (read != -1) {
                                    out.write(buffer, 0, read);
                                    read = in.read(buffer);
                                }
                                break;
                            }
                            in.closeEntry();
                            entry = in.getNextEntry();
                        }
                    }
                } else {
                    // copy from dir, and use a different canonical path to not conflict with test classpath
                    out.putNextEntry(new ZipEntry("test/" + clazz.getSimpleName() + ".class"));
                    Files.copy(codebase.resolve(relativePath), out);
                }
                out.closeEntry();
            }
        }
    }

    public void testJarHellDuplicateCodebaseWithDep() throws Exception {
        Path pluginDir = createTempDir();
        Path dupJar = pluginDir.resolve("dup.jar");
        makeJar(dupJar);
        Map<String, Set<URL>> transitiveDeps = new HashMap<>();
        transitiveDeps.put("dep", Collections.singleton(dupJar.toUri().toURL()));
        PluginDescriptor info1 = newTestDescriptor("myplugin", List.of("dep"));
        PluginBundle bundle = new PluginBundle(info1, pluginDir);
        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> PluginsUtils.checkBundleJarHell(JarHell.parseModulesAndClassPath(), bundle, transitiveDeps)
        );
        assertEquals("failed to load plugin myplugin due to jar hell", e.getMessage());
        assertThat(e.getCause().getMessage(), containsString("jar hell! duplicate codebases with extended plugin"));
    }

    public void testJarHellDuplicateCodebaseAcrossDeps() throws Exception {
        Path pluginDir = createTempDir();
        Path pluginJar = pluginDir.resolve("plugin.jar");
        makeJar(pluginJar, DummyClass1.class);
        Path otherDir = createTempDir();
        Path dupJar = otherDir.resolve("dup.jar");
        makeJar(dupJar, DummyClass2.class);
        Map<String, Set<URL>> transitiveDeps = new HashMap<>();
        transitiveDeps.put("dep1", Collections.singleton(dupJar.toUri().toURL()));
        transitiveDeps.put("dep2", Collections.singleton(dupJar.toUri().toURL()));
        PluginDescriptor info1 = newTestDescriptor("myplugin", List.of("dep1", "dep2"));
        PluginBundle bundle = new PluginBundle(info1, pluginDir);
        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> PluginsUtils.checkBundleJarHell(JarHell.parseModulesAndClassPath(), bundle, transitiveDeps)
        );
        assertEquals("failed to load plugin myplugin due to jar hell", e.getMessage());
        assertThat(e.getCause().getMessage(), containsString("jar hell!"));
        assertThat(e.getCause().getMessage(), containsString("duplicate codebases"));
    }

    // Note: testing dup codebase with core is difficult because it requires a symlink, but we have mock filesystems and security manager
    public void testJarHellDuplicateClassWithCore() throws Exception {
        // need a jar file of core dep, use log4j here
        Path pluginDir = createTempDir();
        Path pluginJar = pluginDir.resolve("plugin.jar");
        makeJar(pluginJar, Level.class);
        PluginDescriptor info1 = newTestDescriptor("myplugin", List.of());
        PluginBundle bundle = new PluginBundle(info1, pluginDir);
        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> PluginsUtils.checkBundleJarHell(JarHell.parseModulesAndClassPath(), bundle, new HashMap<>())
        );
        assertEquals("failed to load plugin myplugin due to jar hell", e.getMessage());
        assertThat(e.getCause().getMessage(), containsString("jar hell!"));
        assertThat(e.getCause().getMessage(), containsString("Level"));
    }

    public void testJarHellWhenExtendedPluginJarNotFound() throws Exception {
        Path pluginDir = createTempDir();
        Path pluginJar = pluginDir.resolve("dummy.jar");

        Path otherDir = createTempDir();
        Path extendedPlugin = otherDir.resolve("extendedDep-not-present.jar");

        PluginDescriptor info = newTestDescriptor("dummy", List.of("extendedPlugin"));
        PluginBundle bundle = new PluginBundle(info, pluginDir);
        Map<String, Set<URL>> transitiveUrls = new HashMap<>();
        transitiveUrls.put("extendedPlugin", Collections.singleton(extendedPlugin.toUri().toURL()));

        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> PluginsUtils.checkBundleJarHell(JarHell.parseModulesAndClassPath(), bundle, transitiveUrls)
        );

        assertEquals("failed to load plugin dummy while checking for jar hell", e.getMessage());
    }

    public void testJarHellDuplicateClassWithDep() throws Exception {
        Path pluginDir = createTempDir();
        Path pluginJar = pluginDir.resolve("plugin.jar");
        makeJar(pluginJar, DummyClass1.class);
        Path depDir = createTempDir();
        Path depJar = depDir.resolve("dep.jar");
        makeJar(depJar, DummyClass1.class);
        Map<String, Set<URL>> transitiveDeps = new HashMap<>();
        transitiveDeps.put("dep", Collections.singleton(depJar.toUri().toURL()));
        PluginDescriptor info1 = newTestDescriptor("myplugin", List.of("dep"));
        PluginBundle bundle = new PluginBundle(info1, pluginDir);
        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> PluginsUtils.checkBundleJarHell(JarHell.parseModulesAndClassPath(), bundle, transitiveDeps)
        );
        assertEquals("failed to load plugin myplugin due to jar hell", e.getMessage());
        assertThat(e.getCause().getMessage(), containsString("jar hell!"));
        assertThat(e.getCause().getMessage(), containsString("DummyClass1"));
    }

    public void testJarHellDuplicateClassAcrossDeps() throws Exception {
        Path pluginDir = createTempDir();
        Path pluginJar = pluginDir.resolve("plugin.jar");
        makeJar(pluginJar, DummyClass1.class);
        Path dep1Dir = createTempDir();
        Path dep1Jar = dep1Dir.resolve("dep1.jar");
        makeJar(dep1Jar, DummyClass2.class);
        Path dep2Dir = createTempDir();
        Path dep2Jar = dep2Dir.resolve("dep2.jar");
        makeJar(dep2Jar, DummyClass2.class);
        Map<String, Set<URL>> transitiveDeps = new HashMap<>();
        transitiveDeps.put("dep1", Collections.singleton(dep1Jar.toUri().toURL()));
        transitiveDeps.put("dep2", Collections.singleton(dep2Jar.toUri().toURL()));
        PluginDescriptor info1 = newTestDescriptor("myplugin", List.of("dep1", "dep2"));
        PluginBundle bundle = new PluginBundle(info1, pluginDir);
        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> PluginsUtils.checkBundleJarHell(JarHell.parseModulesAndClassPath(), bundle, transitiveDeps)
        );
        assertEquals("failed to load plugin myplugin due to jar hell", e.getMessage());
        assertThat(e.getCause().getMessage(), containsString("jar hell!"));
        assertThat(e.getCause().getMessage(), containsString("DummyClass2"));
    }

    public void testJarHellTransitiveMap() throws Exception {
        Path pluginDir = createTempDir();
        Path pluginJar = pluginDir.resolve("plugin.jar");
        makeJar(pluginJar, DummyClass1.class);
        Path dep1Dir = createTempDir();
        Path dep1Jar = dep1Dir.resolve("dep1.jar");
        makeJar(dep1Jar, DummyClass2.class);
        Path dep2Dir = createTempDir();
        Path dep2Jar = dep2Dir.resolve("dep2.jar");
        makeJar(dep2Jar, DummyClass3.class);
        Map<String, Set<URL>> transitiveDeps = new HashMap<>();
        transitiveDeps.put("dep1", Collections.singleton(dep1Jar.toUri().toURL()));
        transitiveDeps.put("dep2", Collections.singleton(dep2Jar.toUri().toURL()));
        PluginDescriptor info1 = newTestDescriptor("myplugin", List.of("dep1", "dep2"));
        PluginBundle bundle = new PluginBundle(info1, pluginDir);
        PluginsUtils.checkBundleJarHell(JarHell.parseModulesAndClassPath(), bundle, transitiveDeps);
        Set<URL> deps = transitiveDeps.get("myplugin");
        assertNotNull(deps);
        assertThat(deps, containsInAnyOrder(pluginJar.toUri().toURL(), dep1Jar.toUri().toURL(), dep2Jar.toUri().toURL()));
    }

    public void testJarHellSpiAddedToTransitiveDeps() throws Exception {
        Path pluginDir = createTempDir();
        Path pluginJar = pluginDir.resolve("plugin.jar");
        makeJar(pluginJar, DummyClass2.class);
        Path spiDir = pluginDir.resolve("spi");
        Files.createDirectories(spiDir);
        Path spiJar = spiDir.resolve("spi.jar");
        makeJar(spiJar, DummyClass3.class);
        Path depDir = createTempDir();
        Path depJar = depDir.resolve("dep.jar");
        makeJar(depJar, DummyClass1.class);
        Map<String, Set<URL>> transitiveDeps = new HashMap<>();
        transitiveDeps.put("dep", Collections.singleton(depJar.toUri().toURL()));
        PluginDescriptor info1 = newTestDescriptor("myplugin", List.of("dep"));
        PluginBundle bundle = new PluginBundle(info1, pluginDir);
        PluginsUtils.checkBundleJarHell(JarHell.parseModulesAndClassPath(), bundle, transitiveDeps);
        Set<URL> transitive = transitiveDeps.get("myplugin");
        assertThat(transitive, containsInAnyOrder(spiJar.toUri().toURL(), depJar.toUri().toURL()));
    }

    public void testJarHellSpiConflict() throws Exception {
        Path pluginDir = createTempDir();
        Path pluginJar = pluginDir.resolve("plugin.jar");
        makeJar(pluginJar, DummyClass2.class);
        Path spiDir = pluginDir.resolve("spi");
        Files.createDirectories(spiDir);
        Path spiJar = spiDir.resolve("spi.jar");
        makeJar(spiJar, DummyClass1.class);
        Path depDir = createTempDir();
        Path depJar = depDir.resolve("dep.jar");
        makeJar(depJar, DummyClass1.class);
        Map<String, Set<URL>> transitiveDeps = new HashMap<>();
        transitiveDeps.put("dep", Collections.singleton(depJar.toUri().toURL()));
        PluginDescriptor info1 = newTestDescriptor("myplugin", List.of("dep"));
        PluginBundle bundle = new PluginBundle(info1, pluginDir);
        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> PluginsUtils.checkBundleJarHell(JarHell.parseModulesAndClassPath(), bundle, transitiveDeps)
        );
        assertEquals("failed to load plugin myplugin due to jar hell", e.getMessage());
        assertThat(e.getCause().getMessage(), containsString("jar hell!"));
        assertThat(e.getCause().getMessage(), containsString("DummyClass1"));
    }

    public void testIncompatibleElasticsearchVersion() throws Exception {
        PluginDescriptor info = new PluginDescriptor(
            "my_plugin",
            "desc",
            "1.0",
            Version.fromId(6000099),
            "1.8",
            "FakePlugin",
            null,
            Collections.emptyList(),
            false,
            false,
            false,
            false
        );
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> PluginsUtils.verifyCompatibility(info));
        assertThat(e.getMessage(), containsString("was built for Elasticsearch version 6.0.0"));
    }

    public void testIncompatibleJavaVersion() throws Exception {
        PluginDescriptor info = new PluginDescriptor(
            "my_plugin",
            "desc",
            "1.0",
            Version.CURRENT,
            "1000",
            "FakePlugin",
            null,
            Collections.emptyList(),
            false,
            false,
            false,
            false
        );
        IllegalStateException e = expectThrows(IllegalStateException.class, () -> PluginsUtils.verifyCompatibility(info));
        assertThat(e.getMessage(), containsString("my_plugin requires Java"));
    }

    public void testFindPluginDirs() throws IOException {
        final Path plugins = createTempDir();

        final Path fake = plugins.resolve("fake");

        PluginTestUtil.writePluginProperties(
            fake,
            "description",
            "description",
            "name",
            "fake",
            "version",
            "1.0.0",
            "elasticsearch.version",
            Version.CURRENT.toString(),
            "java.version",
            System.getProperty("java.specification.version"),
            "classname",
            "test.DummyPlugin"
        );

        try (InputStream jar = PluginsUtilsTests.class.getResourceAsStream("dummy-plugin.jar")) {
            Files.copy(jar, fake.resolve("plugin.jar"));
        }

        assertThat(PluginsUtils.findPluginDirs(plugins), containsInAnyOrder(fake));
    }

}
