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
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.FileSystemException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Locale;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.sameInstance;

@LuceneTestCase.SuppressFileSystems(value = "ExtrasFS")
public class PluginsServiceTests extends ESTestCase {
    public static class AdditionalSettingsPlugin1 extends Plugin {
        @Override
        public Settings additionalSettings() {
            return Settings.builder()
                .put("foo.bar", "1")
                .put(IndexModule.INDEX_STORE_TYPE_SETTING.getKey(), IndexModule.Type.MMAPFS.getSettingsKey())
                .build();
        }
    }

    public static class AdditionalSettingsPlugin2 extends Plugin {
        @Override
        public Settings additionalSettings() {
            return Settings.builder().put("foo.bar", "2").build();
        }
    }

    public static class FilterablePlugin extends Plugin implements ScriptPlugin {}

    static PluginsService newPluginsService(Settings settings) {
        return new PluginsService(settings, null, null, TestEnvironment.newEnvironment(settings).pluginsFile(), List.of());
    }

    static PluginsService newPluginsService(Settings settings, Class<? extends Plugin> classpathPlugin) {
        return new PluginsService(settings, null, null, TestEnvironment.newEnvironment(settings).pluginsFile(), List.of(classpathPlugin));
    }

    static PluginsService newPluginsService(Settings settings, List<Class<? extends Plugin>> classpathPlugins) {
        return new PluginsService(settings, null, null, TestEnvironment.newEnvironment(settings).pluginsFile(), classpathPlugins);
    }

    static PluginsService newPluginsService(
        Settings settings,
        Class<? extends Plugin> classpathPlugin1,
        Class<? extends Plugin> classpathPlugin2
    ) {
        return new PluginsService(
            settings,
            null,
            null,
            TestEnvironment.newEnvironment(settings).pluginsFile(),
            List.of(classpathPlugin1, classpathPlugin2)
        );
    }

    public void testAdditionalSettings() {
        Settings settings = Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir())
            .put("my.setting", "test")
            .put(IndexModule.INDEX_STORE_TYPE_SETTING.getKey(), IndexModule.Type.NIOFS.getSettingsKey())
            .build();
        PluginsService service = newPluginsService(settings, AdditionalSettingsPlugin1.class);
        Settings newSettings = service.updatedSettings();
        assertEquals("test", newSettings.get("my.setting")); // previous settings still exist
        assertEquals("1", newSettings.get("foo.bar")); // added setting exists
        // does not override pre existing settings
        assertEquals(IndexModule.Type.NIOFS.getSettingsKey(), newSettings.get(IndexModule.INDEX_STORE_TYPE_SETTING.getKey()));
    }

    public void testAdditionalSettingsClash() {
        Settings settings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir()).build();
        PluginsService service = newPluginsService(settings, AdditionalSettingsPlugin1.class, AdditionalSettingsPlugin2.class);
        try {
            service.updatedSettings();
            fail("Expected exception when building updated settings");
        } catch (IllegalArgumentException e) {
            String msg = e.getMessage();
            assertTrue(msg, msg.contains("Cannot have additional setting [foo.bar]"));
            assertTrue(msg, msg.contains("plugin [" + AdditionalSettingsPlugin1.class.getName()));
            assertTrue(msg, msg.contains("plugin [" + AdditionalSettingsPlugin2.class.getName()));
        }
    }

    public void testFilterPlugins() {
        Settings settings = Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir())
            .put("my.setting", "test")
            .put(IndexModule.INDEX_STORE_TYPE_SETTING.getKey(), IndexModule.Type.NIOFS.getSettingsKey())
            .build();
        PluginsService service = newPluginsService(settings, AdditionalSettingsPlugin1.class, FilterablePlugin.class);
        List<ScriptPlugin> scriptPlugins = service.filterPlugins(ScriptPlugin.class);
        assertEquals(1, scriptPlugins.size());
        assertEquals(FilterablePlugin.class, scriptPlugins.get(0).getClass());
    }

    public void testHiddenFiles() throws IOException {
        final Path home = createTempDir();
        final Settings settings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), home).build();
        final Path hidden = home.resolve("plugins").resolve(".hidden");
        Files.createDirectories(hidden);
        final IllegalStateException e = expectThrows(IllegalStateException.class, () -> newPluginsService(settings));

        final String expected = "Could not load plugin descriptor for plugin directory [.hidden]";
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
            assertThat(e.getMessage(), containsString("Could not load plugin descriptor for plugin directory [.DS_Store]"));
            assertNotNull(e.getCause());
            assertThat(e.getCause(), instanceOf(FileSystemException.class));
            if (Constants.WINDOWS) {
                assertThat(e.getCause(), instanceOf(NoSuchFileException.class));
            } else {
                // force a "Not a directory" exception to be thrown so that we can extract the locale-dependent message
                final String expected;
                try (InputStream ignored = Files.newInputStream(desktopServicesStore.resolve("not-a-directory"))) {
                    throw new AssertionError();
                } catch (final FileSystemException inner) {
                    // locale-dependent translation of "Not a directory"
                    expected = inner.getReason();
                }
                assertThat(e.getCause(), hasToString(containsString(expected)));
            }
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
            () -> newPluginsService(settings, NoPublicConstructorPlugin.class)
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
            () -> newPluginsService(settings, MultiplePublicConstructorsPlugin.class)
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
            final IllegalStateException e = expectThrows(IllegalStateException.class, () -> newPluginsService(settings, pluginClass));
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

    public void testExistingMandatoryClasspathPlugin() {
        final Settings settings = Settings.builder()
            .put("path.home", createTempDir())
            .put("plugin.mandatory", "org.elasticsearch.plugins.PluginsServiceTests$FakePlugin")
            .build();
        newPluginsService(settings, FakePlugin.class);
    }

    public static class FakePlugin extends Plugin {

        public FakePlugin() {

        }

    }

    public void testExistingMandatoryInstalledPlugin() throws IOException {
        // This test opens a child classloader, reading a jar under the test temp
        // dir (a dummy plugin). Classloaders are closed by GC, so when test teardown
        // occurs the jar is deleted while the classloader is still open. However, on
        // windows, files cannot be deleted when they are still open by a process.
        assumeFalse("windows deletion behavior is asinine", Constants.WINDOWS);
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
            "test.DummyPlugin"
        );
        try (InputStream jar = PluginsServiceTests.class.getResourceAsStream("dummy-plugin.jar")) {
            Files.copy(jar, fake.resolve("plugin.jar"));
        }

        final Settings settings = Settings.builder().put("path.home", pathHome).put("plugin.mandatory", "fake").build();
        newPluginsService(settings);
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
                    new PluginInfo("extensible", null, null, null, null, null, null, List.of(), false, PluginType.ISOLATED, "", false),
                    extensiblePlugin,
                    null
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
                    new PluginInfo("extensible", null, null, null, null, null, null, List.of(), false, PluginType.ISOLATED, "", false),
                    extensiblePlugin,
                    null
                ),
                new PluginsService.LoadedPlugin(
                    new PluginInfo(
                        "test",
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        List.of("extensible"),
                        false,
                        PluginType.ISOLATED,
                        "",
                        false
                    ),
                    testPlugin,
                    null
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
}
