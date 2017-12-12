/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.plugins;

import org.elasticsearch.action.ActionModule;
import org.elasticsearch.bootstrap.BootstrapCheck;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.discovery.DiscoveryModule;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.indices.analysis.AnalysisModule;
import org.elasticsearch.repositories.RepositoriesModule;
import org.elasticsearch.script.ScriptModule;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.threadpool.ExecutorBuilder;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;

/**
 * An extension point allowing to plug in custom functionality. This class has a number of extension points that are available to all
 * plugins, in addition you can implement any of the following interfaces to further customize Elasticsearch:
 * <ul>
 * <li>{@link ActionPlugin}
 * <li>{@link AnalysisPlugin}
 * <li>{@link ClusterPlugin}
 * <li>{@link DiscoveryPlugin}
 * <li>{@link IngestPlugin}
 * <li>{@link MapperPlugin}
 * <li>{@link NetworkPlugin}
 * <li>{@link RepositoryPlugin}
 * <li>{@link ScriptPlugin}
 * <li>{@link SearchPlugin}
 * </ul>
 * <p>In addition to extension points this class also declares some {@code @Deprecated} {@code public final void onModule} methods. These
 * methods should cause any extensions of {@linkplain Plugin} that used the pre-5.x style extension syntax to fail to build and point the
 * plugin author at the new extension syntax. We hope that these make the process of upgrading a plugin from 2.x to 5.x only mildly painful.
 */
public abstract class Plugin implements Closeable {

    /**
     * Node level guice modules.
     */
    public Collection<Module> createGuiceModules() {
        return Collections.emptyList();
    }

    /**
     * Node level services that will be automatically started/stopped/closed. This classes must be constructed
     * by injection with guice.
     */
    public Collection<Class<? extends LifecycleComponent>> getGuiceServiceClasses() {
        return Collections.emptyList();
    }

    /**
     * Returns components added by this plugin.
     *
     * Any components returned that implement {@link LifecycleComponent} will have their lifecycle managed.
     * Note: To aid in the migration away from guice, all objects returned as components will be bound in guice
     * to themselves.
     *
     * @param client A client to make requests to the system
     * @param clusterService A service to allow watching and updating cluster state
     * @param threadPool A service to allow retrieving an executor to run an async action
     * @param resourceWatcherService A service to watch for changes to node local files
     * @param scriptService A service to allow running scripts on the local node
     * @param xContentRegistry the registry for extensible xContent parsing
     * @param environment the environment for path and setting configurations
     * @param nodeEnvironment the node environment used coordinate access to the data paths
     * @param namedWriteableRegistry the registry for {@link NamedWriteable} object parsing
     */
    public Collection<Object> createComponents(Client client, ClusterService clusterService, ThreadPool threadPool,
                                               ResourceWatcherService resourceWatcherService, ScriptService scriptService,
                                               NamedXContentRegistry xContentRegistry, Environment environment,
                                               NodeEnvironment nodeEnvironment, NamedWriteableRegistry namedWriteableRegistry) {
        return Collections.emptyList();
    }

    /**
     * Additional node settings loaded by the plugin. Note that settings that are explicit in the nodes settings can't be
     * overwritten with the additional settings. These settings added if they don't exist.
     */
    public Settings additionalSettings() {
        return Settings.Builder.EMPTY_SETTINGS;
    }

    /**
     * Returns parsers for {@link NamedWriteable} this plugin will use over the transport protocol.
     * @see NamedWriteableRegistry
     */
    public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        return Collections.emptyList();
    }

    /**
     * Returns parsers for named objects this plugin will parse from {@link XContentParser#namedObject(Class, String, Object)}.
     * @see NamedWriteableRegistry
     */
    public List<NamedXContentRegistry.Entry> getNamedXContent() {
        return Collections.emptyList();
    }

    /**
     * Called before a new index is created on a node. The given module can be used to register index-level
     * extensions.
     */
    public void onIndexModule(IndexModule indexModule) {}

    /**
     * Returns a list of additional {@link Setting} definitions for this plugin.
     */
    public List<Setting<?>> getSettings() { return Collections.emptyList(); }

    /**
     * Returns a list of additional settings filter for this plugin
     */
    public List<String> getSettingsFilter() { return Collections.emptyList(); }

    /**
     * Provides a function to modify global custom meta data on startup.
     * <p>
     * Plugins should return the input custom map via {@link UnaryOperator#identity()} if no upgrade is required.
     * <p>
     * The order of custom meta data upgraders calls is undefined and can change between runs so, it is expected that
     * plugins will modify only data owned by them to avoid conflicts.
     * <p>
     * @return Never {@code null}. The same or upgraded {@code MetaData.Custom} map.
     * @throws IllegalStateException if the node should not start because at least one {@code MetaData.Custom}
     *                               is unsupported
     */
    public UnaryOperator<Map<String, MetaData.Custom>> getCustomMetaDataUpgrader() {
        return UnaryOperator.identity();
    }

    /**
     * Provides a function to modify index template meta data on startup.
     * <p>
     * Plugins should return the input template map via {@link UnaryOperator#identity()} if no upgrade is required.
     * <p>
     * The order of the template upgrader calls is undefined and can change between runs so, it is expected that
     * plugins will modify only templates owned by them to avoid conflicts.
     * <p>
     * @return Never {@code null}. The same or upgraded {@code IndexTemplateMetaData} map.
     * @throws IllegalStateException if the node should not start because at least one {@code IndexTemplateMetaData}
     *                               cannot be upgraded
     */
    public UnaryOperator<Map<String, IndexTemplateMetaData>> getIndexTemplateMetaDataUpgrader() {
        return UnaryOperator.identity();
    }

    /**
     * Provides a function to modify index meta data when an index is introduced into the cluster state for the first time.
     * <p>
     * Plugins should return the input index metadata via {@link UnaryOperator#identity()} if no upgrade is required.
     * <p>
     * The order of the index upgrader calls for the same index is undefined and can change between runs so, it is expected that
     * plugins will modify only indices owned by them to avoid conflicts.
     * <p>
     * @return Never {@code null}. The same or upgraded {@code IndexMetaData}.
     * @throws IllegalStateException if the node should not start because the index is unsupported
     */
    public UnaryOperator<IndexMetaData> getIndexMetaDataUpgrader() {
        return UnaryOperator.identity();
    }

    /**
     * Provides the list of this plugin's custom thread pools, empty if
     * none.
     *
     * @param settings the current settings
     * @return executors builders for this plugin's custom thread pools
     */
    public List<ExecutorBuilder<?>> getExecutorBuilders(Settings settings) {
        return Collections.emptyList();
    }

    /**
     * Returns a list of checks that are enforced when a node starts up once a node has the transport protocol bound to a non-loopback
     * interface. In this case we assume the node is running in production and all bootstrap checks must pass. This allows plugins
     * to provide a better out of the box experience by pre-configuring otherwise (in production) mandatory settings or to enforce certain
     * configurations like OS settings or 3rd party resources.
     */
    public List<BootstrapCheck> getBootstrapChecks() { return Collections.emptyList(); }

    /**
     * Close the resources opened by this plugin.
     *
     * @throws IOException if the plugin failed to close its resources
     */
    @Override
    public void close() throws IOException {

    }

    /**
     * Old-style guice index level extension point. {@code @Deprecated} and {@code final} to act as a signpost for plugin authors upgrading
     * from 2.x.
     *
     * @deprecated use #onIndexModule instead
     */
    @Deprecated
    public final void onModule(IndexModule indexModule) {}


    /**
     * Old-style guice settings extension point. {@code @Deprecated} and {@code final} to act as a signpost for plugin authors upgrading
     * from 2.x.
     *
     * @deprecated use #getSettings and #getSettingsFilter instead
     */
    @Deprecated
    public final void onModule(SettingsModule settingsModule) {}

    /**
     * Old-style guice scripting extension point. {@code @Deprecated} and {@code final} to act as a signpost for plugin authors upgrading
     * from 2.x.
     *
     * @deprecated implement {@link ScriptPlugin} instead
     */
    @Deprecated
    public final void onModule(ScriptModule module) {}

    /**
     * Old-style analysis extension point. {@code @Deprecated} and {@code final} to act as a signpost for plugin authors upgrading
     * from 2.x.
     *
     * @deprecated implement {@link AnalysisPlugin} instead
     */
    @Deprecated
    public final void onModule(AnalysisModule module) {}

    /**
     * Old-style action extension point. {@code @Deprecated} and {@code final} to act as a signpost for plugin authors upgrading
     * from 2.x.
     *
     * @deprecated implement {@link ActionPlugin} instead
     */
    @Deprecated
    public final void onModule(ActionModule module) {}

    /**
     * Old-style search extension point. {@code @Deprecated} and {@code final} to act as a signpost for plugin authors upgrading
     * from 2.x.
     *
     * @deprecated implement {@link SearchPlugin} instead
     */
    @Deprecated
    public final void onModule(SearchModule module) {}

    /**
     * Old-style network extension point. {@code @Deprecated} and {@code final} to act as a signpost for plugin authors upgrading
     * from 2.x.
     *
     * @deprecated implement {@link NetworkPlugin} instead
     */
    @Deprecated
    public final void onModule(NetworkModule module) {}

    /**
     * Old-style snapshot/restore extension point. {@code @Deprecated} and {@code final} to act as a signpost for plugin authors upgrading
     * from 2.x.
     *
     * @deprecated implement {@link RepositoryPlugin} instead
     */
    @Deprecated
    public final void onModule(RepositoriesModule module) {}

    /**
     * Old-style cluster extension point. {@code @Deprecated} and {@code final} to act as a signpost for plugin authors upgrading
     * from 2.x.
     *
     * @deprecated implement {@link ClusterPlugin} instead
     */
    @Deprecated
    public final void onModule(ClusterModule module) {}

    /**
     * Old-style discovery extension point. {@code @Deprecated} and {@code final} to act as a signpost for plugin authors upgrading
     * from 2.x.
     *
     * @deprecated implement {@link DiscoveryPlugin} instead
     */
    @Deprecated
    public final void onModule(DiscoveryModule module) {}
}
