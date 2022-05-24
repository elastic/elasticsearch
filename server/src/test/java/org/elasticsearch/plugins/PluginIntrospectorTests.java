/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.health.HealthIndicatorService;
import org.elasticsearch.index.analysis.TokenFilterFactory;
import org.elasticsearch.indices.analysis.AnalysisModule;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.PrivilegedOperations;
import org.elasticsearch.test.compiler.InMemoryJavaCompiler;
import org.elasticsearch.test.jar.JarUtils;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xcontent.NamedXContentRegistry;

import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Path;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;

public class PluginIntrospectorTests extends ESTestCase {

    final PluginIntrospector pluginIntrospector = PluginIntrospector.getInstance();

    public void testEmpty() {
        class FooPlugin extends Plugin {}
        assertThat(pluginIntrospector.interfaces(FooPlugin.class), empty());
    }

    public void testInterfacesBasic() {
        class FooPlugin extends Plugin implements ActionPlugin {}
        assertThat(pluginIntrospector.interfaces(FooPlugin.class), contains("ActionPlugin"));
    }

    public void testInterfaceExtends() {
        interface BarActionPlugin extends ActionPlugin {}
        class FooPlugin extends Plugin implements BarActionPlugin {}
        assertThat(pluginIntrospector.interfaces(FooPlugin.class), contains("ActionPlugin"));
    }

    public void testInterfaceExtends2() {
        interface BazAnalysisPlugin extends AnalysisPlugin {}
        interface GusAnalysisPlugin extends BazAnalysisPlugin {}
        class FooPlugin extends Plugin implements GusAnalysisPlugin {}
        assertThat(pluginIntrospector.interfaces(FooPlugin.class), contains("AnalysisPlugin"));
    }

    public void testPluginExtends() {
        abstract class AbstractPlugin extends Plugin implements AnalysisPlugin {}
        abstract class FooPlugin extends AbstractPlugin {}
        abstract class BarPlugin extends FooPlugin {}
        class BazPlugin extends BarPlugin {}
        assertThat(pluginIntrospector.interfaces(BazPlugin.class), contains("AnalysisPlugin"));
    }

    public void testPluginExtends2() {
        abstract class AbstractPlugin extends Plugin implements NetworkPlugin {}
        abstract class FooPlugin extends AbstractPlugin implements ClusterPlugin {}
        abstract class BarPlugin extends FooPlugin implements DiscoveryPlugin {}
        final class BazPlugin extends BarPlugin implements IngestPlugin {}

        assertThat(
            pluginIntrospector.interfaces(BazPlugin.class),
            contains("ClusterPlugin", "DiscoveryPlugin", "IngestPlugin", "NetworkPlugin")
        );
    }

    // Ensures that BiFunction is filtered out of interface list
    public void testNameFilterBasic() {
        class FooPlugin<T, U, R> extends Plugin implements BiFunction<T, U, R>, IndexStorePlugin {
            @Override
            public Map<String, DirectoryFactory> getDirectoryFactories() {
                return null;
            }

            @Override
            public R apply(T o, U o2) {
                return null;
            }
        }
        assertThat(pluginIntrospector.interfaces(FooPlugin.class), contains("IndexStorePlugin"));
    }

    public void testNameFilter() throws Exception {
        Map<String, CharSequence> sources = new HashMap<>();
        sources.put("p.MyPlugin", """
            package p;
            public interface MyPlugin extends org.elasticsearch.plugins.ActionPlugin {}
            """);
        sources.put("q.AbstractFooPlugin", """
            package q;
            public abstract class AbstractFooPlugin extends org.elasticsearch.plugins.Plugin implements p.MyPlugin { }
            """);
        sources.put("r.FooPlugin", """
            package r;
            public final class FooPlugin extends q.AbstractFooPlugin { }
            """);
        var classToBytes = InMemoryJavaCompiler.compile(sources);

        Map<String, byte[]> jarEntries = new HashMap<>();
        jarEntries.put("p/MyPlugin.class", classToBytes.get("p.MyPlugin"));
        jarEntries.put("q/AbstractFooPlugin.class", classToBytes.get("q.AbstractFooPlugin"));
        jarEntries.put("r/FooPlugin.class", classToBytes.get("r.FooPlugin"));

        Path topLevelDir = createTempDir(getTestName());
        Path jar = topLevelDir.resolve("custom_plugin.jar");
        JarUtils.createJarWithEntries(jar, jarEntries);
        URL[] urls = new URL[] { jar.toUri().toURL() };

        URLClassLoader loader = URLClassLoader.newInstance(urls, PluginIntrospectorTests.class.getClassLoader());
        try {
            assertThat(pluginIntrospector.interfaces(loader.loadClass("r.FooPlugin")), contains("ActionPlugin"));
        } finally {
            PrivilegedOperations.closeURLClassLoader(loader);
        }
    }

    // overriddenMethods

    public void testOverriddenMethodsBasic() {
        class FooPlugin extends Plugin {
            @Override
            public Collection<Object> createComponents(
                Client client,
                ClusterService clusterService,
                ThreadPool threadPool,
                ResourceWatcherService resourceWatcherService,
                ScriptService scriptService,
                NamedXContentRegistry xContentRegistry,
                Environment environment,
                NodeEnvironment nodeEnvironment,
                NamedWriteableRegistry namedWriteableRegistry,
                IndexNameExpressionResolver indexNameExpressionResolver,
                Supplier<RepositoriesService> repositoriesServiceSupplier
            ) {
                return null;
            }
        }

        assertThat(pluginIntrospector.overriddenMethods(FooPlugin.class), contains("createComponents"));
    }

    public void testOverriddenMethodsBasic2() {
        class BarZPlugin extends Plugin implements AnalysisPlugin, HealthPlugin {

            @Override
            public Settings additionalSettings() {  // from Plugin
                return null;
            }

            @Override
            public Map<String, AnalysisModule.AnalysisProvider<TokenFilterFactory>> getTokenFilters() { // from analysis
                return null;
            }

            @Override
            public Collection<HealthIndicatorService> getHealthIndicatorServices() { // from Health
                return null;
            }
        }

        assertThat(
            pluginIntrospector.overriddenMethods(BarZPlugin.class),
            contains("additionalSettings", "getHealthIndicatorServices", "getTokenFilters")
        );
    }

    public void testOverrideVirtualWithDefaultMethod() {
        interface BartSystemIndexPlugin extends SystemIndexPlugin {
            @Override
            default String getFeatureName() {
                return "";
            }

        }
        class BartPlugin extends Plugin implements BartSystemIndexPlugin {
            @Override
            public String getFeatureDescription() {
                return "";
            }
        }

        assertThat(pluginIntrospector.overriddenMethods(BartPlugin.class), contains("getFeatureDescription", "getFeatureName"));
    }

    // TODO: add more test coverage, and ensure all combinations covered
}
