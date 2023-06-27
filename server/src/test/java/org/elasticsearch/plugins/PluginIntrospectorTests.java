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
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.allocator.ShardsAllocator;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.health.HealthIndicatorService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.TokenFilterFactory;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.indices.analysis.AnalysisModule;
import org.elasticsearch.indices.breaker.BreakerSettings;
import org.elasticsearch.indices.recovery.plan.RecoveryPlannerService;
import org.elasticsearch.indices.recovery.plan.ShardSnapshotsService;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.PrivilegedOperations;
import org.elasticsearch.test.compiler.InMemoryJavaCompiler;
import org.elasticsearch.test.jar.JarUtils;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.tracing.Tracer;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xcontent.NamedXContentRegistry;

import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Path;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasEntry;

public class PluginIntrospectorTests extends ESTestCase {

    final PluginIntrospector pluginIntrospector = PluginIntrospector.getInstance();

    public void testInterfacesEmpty() {
        class FooPlugin extends Plugin {}
        assertThat(pluginIntrospector.interfaces(FooPlugin.class), empty());
    }

    public void testInterfacesBasic() {
        class FooPlugin extends Plugin implements ActionPlugin {}
        assertThat(pluginIntrospector.interfaces(FooPlugin.class), contains("ActionPlugin"));
    }

    public void testInterfacesInterfaceExtends() {
        interface BarActionPlugin extends ActionPlugin {}
        class FooPlugin extends Plugin implements BarActionPlugin {}
        assertThat(pluginIntrospector.interfaces(FooPlugin.class), contains("ActionPlugin"));
    }

    public void testInterfacesInterfaceExtends2() {
        interface BazRepositoryPlugin extends RepositoryPlugin {}
        interface GusRepositoryPlugin extends BazRepositoryPlugin {}
        interface RobRepositoryPlugin extends GusRepositoryPlugin {}
        class FooRepositoryPlugin extends Plugin implements RobRepositoryPlugin {}
        assertThat(pluginIntrospector.interfaces(FooRepositoryPlugin.class), contains("RepositoryPlugin"));
    }

    public void testInterfacesPluginExtends() {
        abstract class AbstractPersistentTaskPlugin extends Plugin implements PersistentTaskPlugin {}
        abstract class FooPersistentTaskPlugin extends AbstractPersistentTaskPlugin {}
        abstract class BarPersistentTaskPlugin extends FooPersistentTaskPlugin {}
        class BazPersistentTaskPlugin extends BarPersistentTaskPlugin {}
        assertThat(pluginIntrospector.interfaces(BazPersistentTaskPlugin.class), contains("PersistentTaskPlugin"));
    }

    public void testInterfacesPluginExtends2() {
        abstract class AbstractPlugin extends Plugin implements NetworkPlugin {}
        abstract class FooPlugin extends AbstractPlugin implements ClusterPlugin {}
        abstract class BarPlugin extends FooPlugin implements DiscoveryPlugin {}
        final class BazPlugin extends BarPlugin implements IngestPlugin {}

        assertThat(
            pluginIntrospector.interfaces(BazPlugin.class),
            contains("ClusterPlugin", "DiscoveryPlugin", "IngestPlugin", "NetworkPlugin")
        );
    }

    public void testInterfacesAllPlugin() {
        class AllPlugin extends Plugin
            implements
                ActionPlugin,
                AnalysisPlugin,
                CircuitBreakerPlugin,
                ClusterPlugin,
                DiscoveryPlugin,
                EnginePlugin,
                ExtensiblePlugin,
                HealthPlugin,
                IndexStorePlugin,
                IngestPlugin,
                MapperPlugin,
                NetworkPlugin,
                PersistentTaskPlugin,
                RecoveryPlannerPlugin,
                ReloadablePlugin,
                RepositoryPlugin,
                ScriptPlugin,
                SearchPlugin,
                ShutdownAwarePlugin,
                SystemIndexPlugin {
            @Override
            public BreakerSettings getCircuitBreaker(Settings settings) {
                return null;
            }

            @Override
            public void setCircuitBreaker(CircuitBreaker circuitBreaker) {}

            @Override
            public Optional<EngineFactory> getEngineFactory(IndexSettings indexSettings) {
                return null;
            }

            @Override
            public Collection<HealthIndicatorService> getHealthIndicatorServices() {
                return null;
            }

            @Override
            public Map<String, DirectoryFactory> getDirectoryFactories() {
                return null;
            }

            @Override
            public Optional<RecoveryPlannerService> createRecoveryPlannerService(ShardSnapshotsService shardSnapshotsService) {
                return Optional.empty();
            }

            @Override
            public void reload(Settings settings) {}

            @Override
            public boolean safeToShutdown(String nodeId, SingleNodeShutdownMetadata.Type shutdownType) {
                return false;
            }

            @Override
            public void signalShutdown(Collection<String> shutdownNodeIds) {}

            @Override
            public String getFeatureName() {
                return null;
            }

            @Override
            public String getFeatureDescription() {
                return null;
            }
        }

        assertThat(
            pluginIntrospector.interfaces(AllPlugin.class),
            contains(
                "ActionPlugin",
                "AnalysisPlugin",
                "CircuitBreakerPlugin",
                "ClusterPlugin",
                "DiscoveryPlugin",
                "EnginePlugin",
                "ExtensiblePlugin",
                "HealthPlugin",
                "IndexStorePlugin",
                "IngestPlugin",
                "MapperPlugin",
                "NetworkPlugin",
                "PersistentTaskPlugin",
                "RecoveryPlannerPlugin",
                "ReloadablePlugin",
                "RepositoryPlugin",
                "ScriptPlugin",
                "SearchPlugin",
                "ShutdownAwarePlugin",
                "SystemIndexPlugin"
            )
        );
    }

    // Ensures that BiFunction is filtered out of interface list
    public void testInterfacesNonESPluginInfFilteredOut() {
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

    public void testInterfacesNonESPluginInfFilteredOut2() throws Exception {
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
                Supplier<RepositoriesService> repositoriesServiceSupplier,
                Tracer tracer,
                AllocationService allocationService
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
        assertThat(pluginIntrospector.interfaces(BarZPlugin.class), contains("AnalysisPlugin", "HealthPlugin"));
    }

    public void testOverriddenMethodsDefaultMethod() {
        interface BartSystemIndexPlugin extends SystemIndexPlugin {
            @Override
            default String getFeatureName() {
                return "";
            }

        }
        class BartSystemIndexPluginImpl extends Plugin implements BartSystemIndexPlugin {
            @Override
            public String getFeatureDescription() {
                return "";
            }
        }

        assertThat(
            pluginIntrospector.overriddenMethods(BartSystemIndexPluginImpl.class),
            contains("getFeatureDescription", "getFeatureName")
        );
        assert ActionPlugin.class.isAssignableFrom(SystemIndexPlugin.class);
        assertThat(pluginIntrospector.interfaces(BartSystemIndexPluginImpl.class), contains("ActionPlugin", "SystemIndexPlugin"));
    }

    public void testOverriddenMethodsNoDuplicateEntries() {
        class BazIngestPlugin extends Plugin implements IngestPlugin {
            @Override
            public Map<String, Processor.Factory> getProcessors(Processor.Parameters parameters) {
                return null;
            }
        }
        class SubBazIngestPlugin extends BazIngestPlugin {}

        assertThat(pluginIntrospector.overriddenMethods(BazIngestPlugin.class), contains("getProcessors"));
        assertThat(pluginIntrospector.overriddenMethods(SubBazIngestPlugin.class), contains("getProcessors"));
        assertThat(pluginIntrospector.interfaces(BazIngestPlugin.class), contains("IngestPlugin"));
        assertThat(pluginIntrospector.interfaces(SubBazIngestPlugin.class), contains("IngestPlugin"));
    }

    public void testOverriddenMethodsCaseSensitivity() {
        abstract class AbstractShutdownAwarePlugin extends Plugin implements ShutdownAwarePlugin {

            // does not override - initial char uppercase
            public boolean SafeToShutdown(String nodeId, SingleNodeShutdownMetadata.Type shutdownType) {
                return false;
            }

            // does not override - DOWN uppercased
            public void signalShutDOWN(Collection<String> shutdownNodeIds) {}
        }

        assertThat(pluginIntrospector.overriddenMethods(AbstractShutdownAwarePlugin.class), empty());
        assertThat(pluginIntrospector.interfaces(AbstractShutdownAwarePlugin.class), contains("ShutdownAwarePlugin"));
    }

    public void testOverriddenMethodsParamTypes() {
        abstract class AbstractShutdownAwarePlugin extends Plugin implements ShutdownAwarePlugin {

            // does not override - initial param type int (rather than String)
            public boolean safeToShutdown(int nodeId, SingleNodeShutdownMetadata.Type shutdownType) {
                return false;
            }

            // does not override - initial param type List (rather than Collection)
            public void signalShutdown(List<String> shutdownNodeIds) {}
        }

        assertThat(pluginIntrospector.overriddenMethods(AbstractShutdownAwarePlugin.class), empty());
        assertThat(pluginIntrospector.interfaces(AbstractShutdownAwarePlugin.class), contains("ShutdownAwarePlugin"));
    }

    public void testDeprecatedInterface() {
        class DeprecatedPlugin extends Plugin implements NetworkPlugin {}
        assertThat(pluginIntrospector.deprecatedInterfaces(DeprecatedPlugin.class), contains("NetworkPlugin"));
    }

    public void testDeprecatedMethod() {
        class TestClusterPlugin extends Plugin implements ClusterPlugin {
            @SuppressWarnings("removal")
            @Override
            public Map<String, Supplier<ShardsAllocator>> getShardsAllocators(Settings settings, ClusterSettings clusterSettings) {
                return Map.of();
            }
        }
        assertThat(pluginIntrospector.deprecatedMethods(TestClusterPlugin.class), hasEntry("getShardsAllocators", "ClusterPlugin"));
    }
}
