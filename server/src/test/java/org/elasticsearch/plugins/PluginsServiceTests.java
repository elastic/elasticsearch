/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.Constants;
import org.elasticsearch.Version;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Strings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.plugins.spi.BarPlugin;
import org.elasticsearch.plugins.spi.BarTestService;
import org.elasticsearch.plugins.spi.TestService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.PrivilegedOperations;
import org.elasticsearch.test.compiler.InMemoryJavaCompiler;
import org.elasticsearch.test.jar.JarUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.sameInstance;

@LuceneTestCase.SuppressFileSystems(value = "ExtrasFS")
public class PluginsServiceTests extends ESTestCase {
    public static class FilterablePlugin extends Plugin implements ScriptPlugin {}

    static PluginsService newPluginsService(Settings settings) {
        return new PluginsService(settings, null, null, TestEnvironment.newEnvironment(settings).pluginsFile());
    }

    static PluginsService newMockPluginsService(List<Class<? extends Plugin>> classpathPlugins) {
        Settings settings = Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir())
            .put("my.setting", "test")
            .put(IndexModule.INDEX_STORE_TYPE_SETTING.getKey(), IndexModule.Type.NIOFS.getSettingsKey())
            .build();
        return new MockPluginsService(settings, TestEnvironment.newEnvironment(settings), classpathPlugins);
    }

    // This test uses a mock in order to use plugins from the classpath
    public void testFilterPlugins() {
        PluginsService service = newMockPluginsService(List.of(FakePlugin.class, FilterablePlugin.class));
        List<ScriptPlugin> scriptPlugins = service.filterPlugins(ScriptPlugin.class);
        assertEquals(1, scriptPlugins.size());
        assertEquals(FilterablePlugin.class, scriptPlugins.get(0).getClass());
    }

    // This test uses a mock in order to use plugins from the classpath
    public void testMapPlugins() {
        PluginsService service = newMockPluginsService(List.of(FakePlugin.class, FilterablePlugin.class));
        List<String> mapResult = service.map(p -> p.getClass().getSimpleName()).toList();
        assertThat(mapResult, containsInAnyOrder("FakePlugin", "FilterablePlugin"));

        List<String> flatmapResult = service.flatMap(p -> List.of(p.getClass().getSimpleName())).toList();
        assertThat(flatmapResult, containsInAnyOrder("FakePlugin", "FilterablePlugin"));

        List<String> forEachConsumer = new ArrayList<>();
        service.forEach(p -> forEachConsumer.add(p.getClass().getSimpleName()));
        assertThat(forEachConsumer, containsInAnyOrder("FakePlugin", "FilterablePlugin"));
    }

    public void testHiddenFiles() throws IOException {
        final Path home = createTempDir();
        final Settings settings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), home).build();
        final Path hidden = home.resolve("plugins").resolve(".hidden");
        Files.createDirectories(hidden);
        final IllegalStateException e = expectThrows(IllegalStateException.class, () -> newPluginsService(settings));

        final String expected = "Plugin [.hidden] is missing a descriptor properties file";
        assertThat(e, hasToString(containsString(expected)));
    }

    public void testDesktopServicesStoreFiles() throws IOException {
        final Path home = createTempDir();
        final Settings settings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), home).build();
        final Path plugins = home.resolve("plugins");
        Files.createDirectories(plugins);
        final Path desktopServicesStore = plugins.resolve(".DS_Store");
        Files.createFile(desktopServicesStore);
        if (Constants.MAC_OS_X) {
            final PluginsService pluginsService = newPluginsService(settings);
            assertNotNull(pluginsService);
        } else {
            final IllegalStateException e = expectThrows(IllegalStateException.class, () -> newPluginsService(settings));
            assertThat(e.getMessage(), containsString("Plugin [.DS_Store] is missing a descriptor properties file"));
        }
    }

    public void testStartupWithRemovingMarker() throws IOException {
        final Path home = createTempDir();
        final Settings settings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), home).build();
        final Path fake = home.resolve("plugins").resolve("fake");
        Files.createDirectories(fake);
        Files.createFile(fake.resolve("plugin.jar"));
        final Path removing = home.resolve("plugins").resolve(".removing-fake");
        Files.createFile(removing);
        PluginTestUtil.writePluginProperties(
            fake,
            "description",
            "fake",
            "name",
            "fake",
            "version",
            "1.0.0",
            "elasticsearch.version",
            Version.CURRENT.toString(),
            "java.version",
            System.getProperty("java.specification.version"),
            "classname",
            "Fake",
            "has.native.controller",
            "false"
        );
        final IllegalStateException e = expectThrows(IllegalStateException.class, () -> newPluginsService(settings));
        final String expected = String.format(
            Locale.ROOT,
            "found file [%s] from a failed attempt to remove the plugin [fake]; execute [elasticsearch-plugin remove fake]",
            removing
        );
        assertThat(e, hasToString(containsString(expected)));
    }

    public void testLoadPluginWithNoPublicConstructor() {
        class NoPublicConstructorPlugin extends Plugin {

            private NoPublicConstructorPlugin() {

            }

        }

        final Path home = createTempDir();
        final Settings settings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), home).build();
        final IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> PluginsService.loadPlugin(NoPublicConstructorPlugin.class, settings, home)
        );
        assertThat(e, hasToString(containsString("no public constructor")));
    }

    public void testLoadPluginWithMultiplePublicConstructors() {
        class MultiplePublicConstructorsPlugin extends Plugin {

            @SuppressWarnings("unused")
            public MultiplePublicConstructorsPlugin() {

            }

            @SuppressWarnings("unused")
            public MultiplePublicConstructorsPlugin(final Settings settings) {

            }

        }

        final Path home = createTempDir();
        final Settings settings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), home).build();
        final IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> PluginsService.loadPlugin(MultiplePublicConstructorsPlugin.class, settings, home)
        );
        assertThat(e, hasToString(containsString("no unique public constructor")));
    }

    public void testLoadPluginWithNoPublicConstructorOfCorrectSignature() {
        class TooManyParametersPlugin extends Plugin {

            @SuppressWarnings("unused")
            public TooManyParametersPlugin(Settings settings, Path configPath, Object object) {

            }

        }

        class TwoParametersFirstIncorrectType extends Plugin {

            @SuppressWarnings("unused")
            public TwoParametersFirstIncorrectType(Object object, Path configPath) {

            }
        }

        class TwoParametersSecondIncorrectType extends Plugin {

            @SuppressWarnings("unused")
            public TwoParametersSecondIncorrectType(Settings settings, Object object) {

            }

        }

        class OneParameterIncorrectType extends Plugin {

            @SuppressWarnings("unused")
            public OneParameterIncorrectType(Object object) {

            }
        }

        final Collection<Class<? extends Plugin>> classes = Arrays.asList(
            TooManyParametersPlugin.class,
            TwoParametersFirstIncorrectType.class,
            TwoParametersSecondIncorrectType.class,
            OneParameterIncorrectType.class
        );
        for (Class<? extends Plugin> pluginClass : classes) {
            final Path home = createTempDir();
            final Settings settings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), home).build();
            final IllegalStateException e = expectThrows(
                IllegalStateException.class,
                () -> PluginsService.loadPlugin(pluginClass, settings, home)
            );
            assertThat(e, hasToString(containsString("no public constructor of correct signature")));
        }
    }

    public void testNonExtensibleDep() throws Exception {
        // This test opens a child classloader, reading a jar under the test temp
        // dir (a dummy plugin). Classloaders are closed by GC, so when test teardown
        // occurs the jar is deleted while the classloader is still open. However, on
        // windows, files cannot be deleted when they are still open by a process.
        assumeFalse("windows deletion behavior is asinine", Constants.WINDOWS);

        Path homeDir = createTempDir();
        Settings settings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), homeDir).build();
        Path pluginsDir = homeDir.resolve("plugins");
        Path mypluginDir = pluginsDir.resolve("myplugin");
        PluginTestUtil.writePluginProperties(
            mypluginDir,
            "description",
            "whatever",
            "name",
            "myplugin",
            "version",
            "1.0.0",
            "elasticsearch.version",
            Version.CURRENT.toString(),
            "java.version",
            System.getProperty("java.specification.version"),
            "extended.plugins",
            "nonextensible",
            "classname",
            "test.DummyPlugin"
        );
        try (InputStream jar = PluginsServiceTests.class.getResourceAsStream("dummy-plugin.jar")) {
            Files.copy(jar, mypluginDir.resolve("plugin.jar"));
        }
        Path nonextensibleDir = pluginsDir.resolve("nonextensible");
        PluginTestUtil.writePluginProperties(
            nonextensibleDir,
            "description",
            "whatever",
            "name",
            "nonextensible",
            "version",
            "1.0.0",
            "elasticsearch.version",
            Version.CURRENT.toString(),
            "java.version",
            System.getProperty("java.specification.version"),
            "classname",
            "test.NonExtensiblePlugin"
        );
        try (InputStream jar = PluginsServiceTests.class.getResourceAsStream("non-extensible-plugin.jar")) {
            Files.copy(jar, nonextensibleDir.resolve("plugin.jar"));
        }
        IllegalStateException e = expectThrows(IllegalStateException.class, () -> newPluginsService(settings));
        assertEquals("Plugin [myplugin] cannot extend non-extensible plugin [nonextensible]", e.getMessage());
    }

    public void testPassingMandatoryPluginCheck() {
        PluginsService.checkMandatoryPlugins(
            Set.of("org.elasticsearch.plugins.PluginsServiceTests$FakePlugin"),
            Set.of("org.elasticsearch.plugins.PluginsServiceTests$FakePlugin")
        );
    }

    public void testFailingMandatoryPluginCheck() {
        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> PluginsService.checkMandatoryPlugins(Set.of(), Set.of("org.elasticsearch.plugins.PluginsServiceTests$FakePlugin"))
        );
        assertEquals(
            "missing mandatory plugins [org.elasticsearch.plugins.PluginsServiceTests$FakePlugin], found plugins []",
            e.getMessage()
        );
    }

    public static class FakePlugin extends Plugin {

        public FakePlugin() {

        }

    }

    public void testPluginNameClash() throws IOException {
        // This test opens a child classloader, reading a jar under the test temp
        // dir (a dummy plugin). Classloaders are closed by GC, so when test teardown
        // occurs the jar is deleted while the classloader is still open. However, on
        // windows, files cannot be deleted when they are still open by a process.
        assumeFalse("windows deletion behavior is asinine", Constants.WINDOWS);
        final Path pathHome = createTempDir();
        final Path plugins = pathHome.resolve("plugins");
        final Path fake1 = plugins.resolve("fake1");
        final Path fake2 = plugins.resolve("fake2");

        PluginTestUtil.writePluginProperties(
            fake1,
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
        try (InputStream jar = PluginsServiceTests.class.getResourceAsStream("dummy-plugin.jar")) {
            Files.copy(jar, fake1.resolve("plugin.jar"));
        }

        PluginTestUtil.writePluginProperties(
            fake2,
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
            "test.NonExtensiblePlugin"
        );
        try (InputStream jar = PluginsServiceTests.class.getResourceAsStream("non-extensible-plugin.jar")) {
            Files.copy(jar, fake2.resolve("plugin.jar"));
        }

        final Settings settings = Settings.builder().put("path.home", pathHome).build();
        IllegalStateException e = expectThrows(IllegalStateException.class, () -> newPluginsService(settings));
        assertThat(e.getMessage(), containsString("duplicate plugin: "));
        assertThat(e.getMessage(), containsString("Name: fake"));
    }

    public void testExistingMandatoryInstalledPlugin() throws IOException {
        final Path pathHome = createTempDir(getTestName());
        final Path plugins = pathHome.resolve("plugins");
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
        try (InputStream jar = PluginsServiceTests.class.getResourceAsStream("dummy-plugin.jar")) {
            Files.copy(jar, fake.resolve("plugin.jar"));
        }

        final Settings settings = Settings.builder().put("path.home", pathHome).put("plugin.mandatory", "fake").build();
        var pluginsService = newPluginsService(settings);
        closePluginLoaders(pluginsService);
    }

    public void testPluginFromParentClassLoader() throws IOException {
        final Path pathHome = createTempDir();
        final Path plugins = pathHome.resolve("plugins");
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
            TestPlugin.class.getName()
        ); // set a class defined outside the bundle (in parent class-loader of plugin)

        final Settings settings = Settings.builder().put("path.home", pathHome).put("plugin.mandatory", "fake").build();
        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> newPluginsService(settings));
        assertThat(
            exception,
            hasToString(
                containsString(
                    "Plugin [fake] must reference a class loader local Plugin class ["
                        + TestPlugin.class.getName()
                        + "] (class loader ["
                        + PluginsServiceTests.class.getClassLoader()
                        + "])"
                )
            )
        );
    }

    public void testExtensiblePlugin() {
        TestExtensiblePlugin extensiblePlugin = new TestExtensiblePlugin();
        PluginsService.loadExtensions(
            List.of(
                new PluginsService.LoadedPlugin(
                    new PluginDescriptor("extensible", null, null, null, null, null, null, List.of(), false, false, false, false),
                    extensiblePlugin
                )
            )
        );

        assertThat(extensiblePlugin.extensions, notNullValue());
        assertThat(extensiblePlugin.extensions, hasSize(0));

        extensiblePlugin = new TestExtensiblePlugin();
        TestPlugin testPlugin = new TestPlugin();
        PluginsService.loadExtensions(
            List.of(
                new PluginsService.LoadedPlugin(
                    new PluginDescriptor("extensible", null, null, null, null, null, null, List.of(), false, false, false, false),
                    extensiblePlugin
                ),
                new PluginsService.LoadedPlugin(
                    new PluginDescriptor("test", null, null, null, null, null, null, List.of("extensible"), false, false, false, false),
                    testPlugin
                )
            )
        );

        assertThat(extensiblePlugin.extensions, notNullValue());
        assertThat(extensiblePlugin.extensions, hasSize(2));
        assertThat(extensiblePlugin.extensions.get(0), instanceOf(TestExtension1.class));
        assertThat(extensiblePlugin.extensions.get(1), instanceOf(TestExtension2.class));
        assertThat(((TestExtension2) extensiblePlugin.extensions.get(1)).plugin, sameInstance(testPlugin));
    }

    public void testNoExtensionConstructors() {
        TestPlugin plugin = new TestPlugin();
        class TestExtension implements TestExtensionPoint {
            private TestExtension() {}
        }
        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> { PluginsService.createExtension(TestExtension.class, TestExtensionPoint.class, plugin); }
        );

        assertThat(
            e,
            hasToString(
                containsString(
                    "no public constructor for extension ["
                        + TestExtension.class.getName()
                        + "] of type ["
                        + TestExtensionPoint.class.getName()
                        + "]"
                )
            )
        );
    }

    public void testMultipleExtensionConstructors() {
        TestPlugin plugin = new TestPlugin();
        class TestExtension implements TestExtensionPoint {
            public TestExtension() {}

            public TestExtension(TestPlugin plugin) {

            }

            public TestExtension(TestPlugin plugin, String anotherArg) {

            }
        }
        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> { PluginsService.createExtension(TestExtension.class, TestExtensionPoint.class, plugin); }
        );

        assertThat(
            e,
            hasToString(
                containsString(
                    "no unique public constructor for extension ["
                        + TestExtension.class.getName()
                        + "] of type ["
                        + TestExtensionPoint.class.getName()
                        + "]"
                )
            )
        );
    }

    public void testBadSingleParameterConstructor() {
        TestPlugin plugin = new TestPlugin();
        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> { PluginsService.createExtension(BadSingleParameterConstructorExtension.class, TestExtensionPoint.class, plugin); }
        );

        assertThat(
            e,
            hasToString(
                containsString(
                    "signature of constructor for extension ["
                        + BadSingleParameterConstructorExtension.class.getName()
                        + "] of type ["
                        + TestExtensionPoint.class.getName()
                        + "] must be either () or ("
                        + TestPlugin.class.getName()
                        + "), not ("
                        + String.class.getName()
                        + ")"
                )
            )
        );
    }

    public void testTooManyParametersExtensionConstructors() {
        TestPlugin plugin = new TestPlugin();
        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> { PluginsService.createExtension(TooManyParametersConstructorExtension.class, TestExtensionPoint.class, plugin); }
        );

        assertThat(
            e,
            hasToString(
                containsString(
                    "signature of constructor for extension ["
                        + TooManyParametersConstructorExtension.class.getName()
                        + "] of type ["
                        + TestExtensionPoint.class.getName()
                        + "] must be either () or ("
                        + TestPlugin.class.getName()
                        + ")"
                )
            )
        );
    }

    public void testThrowingConstructor() {
        TestPlugin plugin = new TestPlugin();
        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> { PluginsService.createExtension(ThrowingConstructorExtension.class, TestExtensionPoint.class, plugin); }
        );

        assertThat(
            e,
            hasToString(
                containsString(
                    "failed to create extension ["
                        + ThrowingConstructorExtension.class.getName()
                        + "] of type ["
                        + TestExtensionPoint.class.getName()
                        + "]"
                )
            )
        );
        assertThat(e.getCause(), instanceOf(InvocationTargetException.class));
        assertThat(e.getCause().getCause(), instanceOf(IllegalArgumentException.class));
        assertThat(e.getCause().getCause(), hasToString(containsString("test constructor failure")));
    }

    private URLClassLoader buildTestProviderPlugin(String name) throws Exception {
        Map<String, CharSequence> sources = Map.of("r.FooPlugin", """
            package r;
            import org.elasticsearch.plugins.ActionPlugin;
            import org.elasticsearch.plugins.Plugin;
            public final class FooPlugin extends Plugin implements ActionPlugin { }
            """, "r.FooTestService", Strings.format("""
            package r;
            import org.elasticsearch.plugins.spi.TestService;
            public final class FooTestService implements TestService {
                @Override
                public String name() {
                    return "%s";
                }
            }
            """, name));

        var classToBytes = InMemoryJavaCompiler.compile(sources);

        Map<String, byte[]> jarEntries = new HashMap<>();
        jarEntries.put("r/FooPlugin.class", classToBytes.get("r.FooPlugin"));
        jarEntries.put("r/FooTestService.class", classToBytes.get("r.FooTestService"));
        jarEntries.put("META-INF/services/org.elasticsearch.plugins.spi.TestService", "r.FooTestService".getBytes(StandardCharsets.UTF_8));

        Path topLevelDir = createTempDir(getTestName());
        Path jar = topLevelDir.resolve(Strings.format("custom_plugin_%s.jar", name));
        JarUtils.createJarWithEntries(jar, jarEntries);
        URL[] urls = new URL[] { jar.toUri().toURL() };

        URLClassLoader loader = URLClassLoader.newInstance(urls, this.getClass().getClassLoader());
        return loader;
    }

    public void testLoadServiceProviders() throws Exception {
        URLClassLoader fakeClassLoader = buildTestProviderPlugin("integer");
        URLClassLoader fakeClassLoader1 = buildTestProviderPlugin("string");
        try {
            @SuppressWarnings("unchecked")
            Class<? extends Plugin> fakePluginClass = (Class<? extends Plugin>) fakeClassLoader.loadClass("r.FooPlugin");
            @SuppressWarnings("unchecked")
            Class<? extends Plugin> fakePluginClass1 = (Class<? extends Plugin>) fakeClassLoader1.loadClass("r.FooPlugin");

            assertFalse(fakePluginClass.getClassLoader().equals(fakePluginClass1.getClassLoader()));

            getClass().getModule().addUses(TestService.class);

            PluginsService service = newMockPluginsService(List.of(fakePluginClass, fakePluginClass1));

            List<? extends TestService> providers = service.loadServiceProviders(TestService.class);
            assertEquals(2, providers.size());
            assertThat(providers.stream().map(p -> p.name()).toList(), containsInAnyOrder("string", "integer"));

            service = newMockPluginsService(List.of(fakePluginClass));
            providers = service.loadServiceProviders(TestService.class);

            assertEquals(1, providers.size());
            assertThat(providers.stream().map(p -> p.name()).toList(), containsInAnyOrder("integer"));

            service = newMockPluginsService(new ArrayList<>());
            providers = service.loadServiceProviders(TestService.class);

            assertEquals(0, providers.size());
        } finally {
            PrivilegedOperations.closeURLClassLoader(fakeClassLoader);
            PrivilegedOperations.closeURLClassLoader(fakeClassLoader1);
        }
    }

    // The mock node loads plugins in the same class loader, make sure we can find the appropriate
    // plugin to use in the constructor in that case too
    public void testLoadServiceProvidersInSameClassLoader() {
        PluginsService service = newMockPluginsService(List.of(BarPlugin.class, PluginOther.class));

        // There's only one TestService implementation, FooTestService which uses FooPlugin in the constructor.
        // We should find only one instance of this service when we load with two plugins in the same class loader.
        @SuppressWarnings("unchecked")
        List<TestService> testServices = (List<TestService>) service.loadServiceProviders(TestService.class);
        assertEquals(1, testServices.size());

        var fooPlugin = (BarPlugin) service.plugins().stream().filter(p -> p.instance() instanceof BarPlugin).findAny().get().instance();
        var othPlugin = (PluginOther) service.plugins()
            .stream()
            .filter(p -> p.instance() instanceof PluginOther)
            .findAny()
            .get()
            .instance();

        // We shouldn't find the FooTestService implementation with PluginOther
        assertThat(MockPluginsService.createExtensions(TestService.class, othPlugin), empty());

        // We should find the FooTestService implementation when we use FooPlugin, because it matches the constructor arg.
        var providers = MockPluginsService.createExtensions(TestService.class, fooPlugin);

        assertThat(providers, allOf(hasSize(1), everyItem(instanceOf(BarTestService.class))));
    }

    public void testDeprecatedPluginInterface() throws Exception {
        final Path home = createTempDir();
        final Settings settings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), home).build();
        final Path plugins = home.resolve("plugins");
        final Path plugin = plugins.resolve("deprecated-plugin");
        Files.createDirectories(plugin);
        PluginTestUtil.writeSimplePluginDescriptor(plugin, "deprecated-plugin", "p.DeprecatedPlugin");
        Path jar = plugin.resolve("impl.jar");
        JarUtils.createJarWithEntries(jar, Map.of("p/DeprecatedPlugin.class", InMemoryJavaCompiler.compile("p.DeprecatedPlugin", """
            package p;
            import org.elasticsearch.plugins.*;
            public class DeprecatedPlugin extends Plugin implements NetworkPlugin {}
            """)));

        var pluginService = newPluginsService(settings);
        try {
            assertWarnings(
                "Plugin class p.DeprecatedPlugin from plugin deprecated-plugin implements "
                    + "deprecated plugin interface NetworkPlugin. "
                    + "This plugin interface will be removed in a future release."
            );
        } finally {
            closePluginLoaders(pluginService);
        }
    }

    public void testDeprecatedPluginMethod() throws Exception {
        final Path home = createTempDir();
        final Settings settings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), home).build();
        final Path plugins = home.resolve("plugins");
        final Path plugin = plugins.resolve("deprecated-plugin");
        Files.createDirectories(plugin);
        PluginTestUtil.writeSimplePluginDescriptor(plugin, "deprecated-plugin", "p.DeprecatedPlugin");
        Path jar = plugin.resolve("impl.jar");
        JarUtils.createJarWithEntries(jar, Map.of("p/DeprecatedPlugin.class", InMemoryJavaCompiler.compile("p.DeprecatedPlugin", """
            package p;
            import java.util.Map;
            import org.elasticsearch.plugins.*;
            import org.elasticsearch.cluster.coordination.ElectionStrategy;
            public class DeprecatedPlugin extends Plugin implements DiscoveryPlugin {
                @Override
                public Map<String, ElectionStrategy> getElectionStrategies() {
                    return Map.of();
                }
            }
            """)));

        var pluginService = newPluginsService(settings);
        try {
            assertWarnings(
                "Plugin class p.DeprecatedPlugin from plugin deprecated-plugin implements deprecated method "
                    + "getElectionStrategies from plugin interface DiscoveryPlugin. This method will be removed in a future release."
            );
        } finally {
            closePluginLoaders(pluginService);
        }
    }

    // Closes the URLClassLoaders of plugins loaded by the given plugin service.
    static void closePluginLoaders(PluginsService pluginService) {
        for (var lp : pluginService.plugins()) {
            if (lp.loader()instanceof URLClassLoader urlClassLoader) {
                try {
                    PrivilegedOperations.closeURLClassLoader(urlClassLoader);
                } catch (IOException unexpected) {
                    throw new UncheckedIOException(unexpected);
                }
            }
        }
    }

    private static class TestExtensiblePlugin extends Plugin implements ExtensiblePlugin {
        private List<TestExtensionPoint> extensions;

        @Override
        public void loadExtensions(ExtensionLoader loader) {
            assert extensions == null;
            extensions = loader.loadExtensions(TestExtensionPoint.class);
            // verify unmodifiable.
            expectThrows(UnsupportedOperationException.class, () -> extensions.add(new TestExtension1()));
        }
    }

    public static class TestPlugin extends Plugin {}

    public interface TestExtensionPoint {}

    public static class TestExtension1 implements TestExtensionPoint {}

    public static class TestExtension2 implements TestExtensionPoint {
        public Plugin plugin;

        public TestExtension2(TestPlugin plugin) {
            this.plugin = plugin;
        }
    }

    public static class BadSingleParameterConstructorExtension implements TestExtensionPoint {
        public BadSingleParameterConstructorExtension(String bad) {}
    }

    public static class TooManyParametersConstructorExtension implements TestExtensionPoint {
        public TooManyParametersConstructorExtension(String bad) {}
    }

    public static class ThrowingConstructorExtension implements TestExtensionPoint {
        public ThrowingConstructorExtension() {
            throw new IllegalArgumentException("test constructor failure");
        }
    }

    public static class PluginOther extends Plugin {
        public PluginOther() {}
    }
}
