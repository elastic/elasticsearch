/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.plugins;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.Version;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.plugin.analysis.CharFilterFactory;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.PrivilegedOperations;
import org.elasticsearch.test.compiler.InMemoryJavaCompiler;
import org.elasticsearch.test.jar.JarUtils;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;

import static java.util.Map.entry;
import static org.elasticsearch.test.LambdaMatchers.transformedMatch;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

@ESTestCase.WithoutSecurityManager
@LuceneTestCase.SuppressFileSystems(value = "ExtrasFS")
public class PluginsLoaderTests extends ESTestCase {

    private static final Logger logger = LogManager.getLogger(PluginsLoaderTests.class);

    static PluginsLoader newPluginsLoader(Settings settings) {
        return PluginsLoader.createPluginsLoader(
            Set.of(),
            PluginsLoader.loadPluginsBundles(TestEnvironment.newEnvironment(settings).pluginsFile()),
            false
        );
    }

    public void testToModuleName() {
        assertThat(PluginsLoader.toModuleName("module.name"), equalTo("module.name"));
        assertThat(PluginsLoader.toModuleName("module-name"), equalTo("module.name"));
        assertThat(PluginsLoader.toModuleName("module-name1"), equalTo("module.name1"));
        assertThat(PluginsLoader.toModuleName("1module-name"), equalTo("module.name"));
        assertThat(PluginsLoader.toModuleName("module-name!"), equalTo("module.name"));
        assertThat(PluginsLoader.toModuleName("module!@#name!"), equalTo("module.name"));
        assertThat(PluginsLoader.toModuleName("!module-name!"), equalTo("module.name"));
        assertThat(PluginsLoader.toModuleName("module_name"), equalTo("module_name"));
        assertThat(PluginsLoader.toModuleName("-module-name-"), equalTo("module.name"));
        assertThat(PluginsLoader.toModuleName("_module_name"), equalTo("_module_name"));
        assertThat(PluginsLoader.toModuleName("_"), equalTo("_"));
    }

    public void testStablePluginLoading() throws Exception {
        final Path home = createTempDir();
        final Settings settings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), home).build();
        final Path plugins = home.resolve("plugins");
        final Path plugin = plugins.resolve("stable-plugin");
        Files.createDirectories(plugin);
        PluginTestUtil.writeStablePluginProperties(
            plugin,
            "description",
            "description",
            "name",
            "stable-plugin",
            "version",
            "1.0.0",
            "elasticsearch.version",
            Version.CURRENT.toString(),
            "java.version",
            System.getProperty("java.specification.version")
        );

        Path jar = plugin.resolve("impl.jar");
        JarUtils.createJarWithEntries(jar, Map.of("p/A.class", InMemoryJavaCompiler.compile("p.A", """
            package p;
            import java.util.Map;
            import org.elasticsearch.plugin.analysis.CharFilterFactory;
            import org.elasticsearch.plugin.NamedComponent;
            import java.io.Reader;
            @NamedComponent( "a_name")
            public class A  implements CharFilterFactory {
                 @Override
                public Reader create(Reader reader) {
                    return reader;
                }
            }
            """)));
        Path namedComponentFile = plugin.resolve("named_components.json");
        Files.writeString(namedComponentFile, """
            {
              "org.elasticsearch.plugin.analysis.CharFilterFactory": {
                "a_name": "p.A"
              }
            }
            """);

        var pluginsLoader = newPluginsLoader(settings);
        try {
            var loadedLayers = pluginsLoader.pluginLayers().toList();

            assertThat(loadedLayers, hasSize(1));
            assertThat(loadedLayers.get(0).pluginBundle().pluginDescriptor().getName(), equalTo("stable-plugin"));
            assertThat(loadedLayers.get(0).pluginBundle().pluginDescriptor().isStable(), is(true));

            assertThat(pluginsLoader.pluginDescriptors(), hasSize(1));
            assertThat(pluginsLoader.pluginDescriptors().get(0).getName(), equalTo("stable-plugin"));
            assertThat(pluginsLoader.pluginDescriptors().get(0).isStable(), is(true));

            var pluginClassLoader = loadedLayers.get(0).pluginClassLoader();
            var pluginModuleLayer = loadedLayers.get(0).pluginModuleLayer();
            assertThat(pluginClassLoader, instanceOf(UberModuleClassLoader.class));
            assertThat(pluginModuleLayer, is(not(ModuleLayer.boot())));
            assertThat(pluginModuleLayer.modules(), contains(transformedMatch(Module::getName, equalTo("synthetic.stable.plugin"))));

            if (CharFilterFactory.class.getModule().isNamed() == false) {
                // test frameworks run with stable api classes on classpath, so we
                // have no choice but to let our class read the unnamed module that
                // owns the stable api classes
                ((UberModuleClassLoader) pluginClassLoader).addReadsSystemClassLoaderUnnamedModule();
            }

            Class<?> stableClass = pluginClassLoader.loadClass("p.A");
            assertThat(stableClass.getModule().getName(), equalTo("synthetic.stable.plugin"));
        } finally {
            closePluginLoaders(pluginsLoader);
        }
    }

    public void testModularPluginLoading() throws Exception {
        final Path home = createTempDir();
        final Settings settings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), home).build();
        final Path plugins = home.resolve("plugins");
        final Path plugin = plugins.resolve("modular-plugin");
        Files.createDirectories(plugin);
        PluginTestUtil.writePluginProperties(
            plugin,
            "description",
            "description",
            "name",
            "modular-plugin",
            "classname",
            "p.A",
            "modulename",
            "modular.plugin",
            "version",
            "1.0.0",
            "elasticsearch.version",
            Version.CURRENT.toString(),
            "java.version",
            System.getProperty("java.specification.version")
        );

        Path jar = plugin.resolve("impl.jar");
        Map<String, CharSequence> sources = Map.ofEntries(entry("module-info", "module modular.plugin { exports p; }"), entry("p.A", """
            package p;
            import org.elasticsearch.plugins.Plugin;

            public class A extends Plugin {
            }
            """));

        // Usually org.elasticsearch.plugins.Plugin would be in the org.elasticsearch.server module.
        // Unfortunately, as tests run non-modular, it will be in the unnamed module, so we need to add a read for it.
        var classToBytes = InMemoryJavaCompiler.compile(sources, "--add-reads", "modular.plugin=ALL-UNNAMED");

        JarUtils.createJarWithEntries(
            jar,
            Map.ofEntries(entry("module-info.class", classToBytes.get("module-info")), entry("p/A.class", classToBytes.get("p.A")))
        );

        var pluginsLoader = newPluginsLoader(settings);
        try {
            var loadedLayers = pluginsLoader.pluginLayers().toList();

            assertThat(loadedLayers, hasSize(1));
            assertThat(loadedLayers.get(0).pluginBundle().pluginDescriptor().getName(), equalTo("modular-plugin"));
            assertThat(loadedLayers.get(0).pluginBundle().pluginDescriptor().isStable(), is(false));
            assertThat(loadedLayers.get(0).pluginBundle().pluginDescriptor().isModular(), is(true));

            assertThat(pluginsLoader.pluginDescriptors(), hasSize(1));
            assertThat(pluginsLoader.pluginDescriptors().get(0).getName(), equalTo("modular-plugin"));
            assertThat(pluginsLoader.pluginDescriptors().get(0).isModular(), is(true));

            var pluginModuleLayer = loadedLayers.get(0).pluginModuleLayer();
            assertThat(pluginModuleLayer, is(not(ModuleLayer.boot())));
            assertThat(pluginModuleLayer.modules(), contains(transformedMatch(Module::getName, equalTo("modular.plugin"))));
        } finally {
            closePluginLoaders(pluginsLoader);
        }
    }

    public void testNonModularPluginLoading() throws Exception {
        final Path home = createTempDir();
        final Settings settings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), home).build();
        final Path plugins = home.resolve("plugins");
        final Path plugin = plugins.resolve("non-modular-plugin");
        Files.createDirectories(plugin);
        PluginTestUtil.writePluginProperties(
            plugin,
            "description",
            "description",
            "name",
            "non-modular-plugin",
            "classname",
            "p.A",
            "version",
            "1.0.0",
            "elasticsearch.version",
            Version.CURRENT.toString(),
            "java.version",
            System.getProperty("java.specification.version")
        );

        Path jar = plugin.resolve("impl.jar");
        Map<String, CharSequence> sources = Map.ofEntries(entry("p.A", """
            package p;
            import org.elasticsearch.plugins.Plugin;

            public class A extends Plugin {
            }
            """));

        var classToBytes = InMemoryJavaCompiler.compile(sources);

        JarUtils.createJarWithEntries(jar, Map.ofEntries(entry("p/A.class", classToBytes.get("p.A"))));

        var pluginsLoader = newPluginsLoader(settings);
        try {
            var loadedLayers = pluginsLoader.pluginLayers().toList();

            assertThat(loadedLayers, hasSize(1));
            assertThat(loadedLayers.get(0).pluginBundle().pluginDescriptor().getName(), equalTo("non-modular-plugin"));
            assertThat(loadedLayers.get(0).pluginBundle().pluginDescriptor().isStable(), is(false));
            assertThat(loadedLayers.get(0).pluginBundle().pluginDescriptor().isModular(), is(false));

            assertThat(pluginsLoader.pluginDescriptors(), hasSize(1));
            assertThat(pluginsLoader.pluginDescriptors().get(0).getName(), equalTo("non-modular-plugin"));
            assertThat(pluginsLoader.pluginDescriptors().get(0).isModular(), is(false));

            var pluginModuleLayer = loadedLayers.get(0).pluginModuleLayer();
            assertThat(pluginModuleLayer, is(ModuleLayer.boot()));
        } finally {
            closePluginLoaders(pluginsLoader);
        }
    }

    // Closes the URLClassLoaders and UberModuleClassloaders created by the given plugin loader.
    // We can use the direct ClassLoader from the plugin because tests do not use any parent SPI ClassLoaders.
    static void closePluginLoaders(PluginsLoader pluginsLoader) {
        pluginsLoader.pluginLayers().forEach(lp -> {
            if (lp.pluginClassLoader() instanceof URLClassLoader urlClassLoader) {
                try {
                    PrivilegedOperations.closeURLClassLoader(urlClassLoader);
                } catch (IOException unexpected) {
                    throw new UncheckedIOException(unexpected);
                }
            } else if (lp.pluginClassLoader() instanceof UberModuleClassLoader loader) {
                try {
                    PrivilegedOperations.closeURLClassLoader(loader.getInternalLoader());
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            } else {
                logger.info("Cannot close unexpected classloader " + lp.pluginClassLoader());
            }
        });
    }
}
