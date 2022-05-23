/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.PrivilegedOperations;
import org.elasticsearch.test.compiler.InMemoryJavaCompiler;
import org.elasticsearch.test.jar.JarUtils;

import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;

public class PluginIntrospectorTests extends ESTestCase {

    public void testEmpty() {
        class FooPlugin extends Plugin {}
        assertThat(PluginIntrospector.interfaces(FooPlugin.class), empty());
    }

    public void testInterfacesBasic() {
        class FooPlugin extends Plugin implements ActionPlugin {}
        assertThat(PluginIntrospector.interfaces(FooPlugin.class), contains("org.elasticsearch.plugins.ActionPlugin"));
    }

    public void testInterfaceExtends() {
        interface BarActionPlugin extends ActionPlugin {}
        class FooPlugin extends Plugin implements BarActionPlugin {}
        assertThat(
            PluginIntrospector.interfaces(FooPlugin.class),
            containsInAnyOrder(
                "org.elasticsearch.plugins.ActionPlugin",
                "org.elasticsearch.plugins.PluginIntrospectorTests$1BarActionPlugin"
            )
        );
    }

    public void testInterfaceExtends2() {
        interface BazAnalysisPlugin extends AnalysisPlugin {}
        interface GusAnalysisPlugin extends BazAnalysisPlugin {}
        class FooPlugin extends Plugin implements GusAnalysisPlugin {}
        assertThat(
            PluginIntrospector.interfaces(FooPlugin.class),
            containsInAnyOrder(
                "org.elasticsearch.plugins.AnalysisPlugin",
                "org.elasticsearch.plugins.PluginIntrospectorTests$1BazAnalysisPlugin",
                "org.elasticsearch.plugins.PluginIntrospectorTests$1GusAnalysisPlugin"
            )
        );
    }

    public void testPluginExtends() {
        abstract class AbstractPlugin extends Plugin implements AnalysisPlugin {}
        abstract class FooPlugin extends AbstractPlugin {}
        abstract class BarPlugin extends FooPlugin {}
        class BazPlugin extends BarPlugin {}
        assertThat(PluginIntrospector.interfaces(BazPlugin.class), containsInAnyOrder("org.elasticsearch.plugins.AnalysisPlugin"));
    }

    public void testPluginExtends2() {
        abstract class AbstractPlugin extends Plugin implements NetworkPlugin {}
        abstract class FooPlugin extends AbstractPlugin implements ClusterPlugin {}
        abstract class BarPlugin extends FooPlugin implements DiscoveryPlugin {}
        final class BazPlugin extends BarPlugin implements IngestPlugin {}

        assertThat(
            PluginIntrospector.interfaces(BazPlugin.class),
            containsInAnyOrder(
                "org.elasticsearch.plugins.NetworkPlugin",
                "org.elasticsearch.plugins.ClusterPlugin",
                "org.elasticsearch.plugins.DiscoveryPlugin",
                "org.elasticsearch.plugins.IngestPlugin"
            )
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
        assertThat(PluginIntrospector.interfaces(FooPlugin.class), contains("org.elasticsearch.plugins.IndexStorePlugin"));
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
            assertThat(PluginIntrospector.interfaces(loader.loadClass("r.FooPlugin")), contains("org.elasticsearch.plugins.ActionPlugin"));
        } finally {
            PrivilegedOperations.closeURLClassLoader(loader);
        }
    }
}
