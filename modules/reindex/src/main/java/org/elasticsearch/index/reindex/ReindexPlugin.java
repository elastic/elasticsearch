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

package org.elasticsearch.index.reindex;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.persistent.PersistentTaskParams;
import org.elasticsearch.persistent.PersistentTaskState;
import org.elasticsearch.persistent.PersistentTasksExecutor;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.PersistentTaskPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

public class ReindexPlugin extends Plugin implements ActionPlugin, PersistentTaskPlugin {

    public static final String NAME = "reindex";
    private final SetOnce<NamedXContentRegistry> namedXContentRegistry = new SetOnce<>();

    private final SetOnce<ScriptService> scriptService = new SetOnce<>();
    private final SetOnce<ReindexSslConfig> reindexSslConfig = new SetOnce<>();
    private final SetOnce<ClusterService> clusterService = new SetOnce<>();

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return Arrays.asList(new ActionHandler<>(ReindexAction.INSTANCE, TransportReindexAction.class),
                new ActionHandler<>(UpdateByQueryAction.INSTANCE, TransportUpdateByQueryAction.class),
                new ActionHandler<>(DeleteByQueryAction.INSTANCE, TransportDeleteByQueryAction.class),
                new ActionHandler<>(RethrottleAction.INSTANCE, TransportRethrottleAction.class),
                new ActionHandler<>(StartReindexTaskAction.INSTANCE, TransportStartReindexTaskAction.class),
                new ActionHandler<>(RethrottlePersistentReindexAction.INSTANCE, TransportRethrottlePersistentReindexAction.class)
            );
    }

    @Override
    public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        return Arrays.asList(
            new NamedWriteableRegistry.Entry(Task.Status.class, BulkByScrollTask.Status.NAME, BulkByScrollTask.Status::new),
            new NamedWriteableRegistry.Entry(PersistentTaskParams.class, ReindexTaskParams.NAME, ReindexTaskParams::new),
            new NamedWriteableRegistry.Entry(Task.Status.class, ReindexPersistentTaskState.NAME, ReindexPersistentTaskState::new),
            new NamedWriteableRegistry.Entry(PersistentTaskState.class, ReindexPersistentTaskState.NAME, ReindexPersistentTaskState::new));
    }

    @Override
    public List<NamedXContentRegistry.Entry> getNamedXContent() {
        return Arrays.asList(
            new NamedXContentRegistry.Entry(PersistentTaskParams.class, new ParseField(ReindexTaskParams.NAME),
                ReindexTaskParams::fromXContent),
            new NamedXContentRegistry.Entry(Task.Status.class, new ParseField(ReindexPersistentTaskState.NAME),
                ReindexPersistentTaskState::fromXContent),
            new NamedXContentRegistry.Entry(PersistentTaskState.class, new ParseField(ReindexPersistentTaskState.NAME),
                ReindexPersistentTaskState::fromXContent));
    }

    @Override
    public List<RestHandler> getRestHandlers(Settings settings, RestController restController, ClusterSettings clusterSettings,
            IndexScopedSettings indexScopedSettings, SettingsFilter settingsFilter, IndexNameExpressionResolver indexNameExpressionResolver,
            Supplier<DiscoveryNodes> nodesInCluster) {
        return Arrays.asList(
                new RestReindexAction(clusterService.get()),
                new RestUpdateByQueryAction(),
                new RestDeleteByQueryAction(),
                new RestRethrottleAction(nodesInCluster));
    }

    @Override
    public Collection<Object> createComponents(Client client, ClusterService clusterService, ThreadPool threadPool,
                                               ResourceWatcherService resourceWatcherService, ScriptService scriptService,
                                               NamedXContentRegistry xContentRegistry, Environment environment,
                                               NodeEnvironment nodeEnvironment, NamedWriteableRegistry namedWriteableRegistry,
                                               IndexNameExpressionResolver expressionResolver) {
        this.scriptService.set(scriptService);
        this.reindexSslConfig.set(new ReindexSslConfig(environment.settings(), environment, resourceWatcherService));
        namedXContentRegistry.set(xContentRegistry);
        this.clusterService.set(clusterService);
        return Collections.singletonList(reindexSslConfig.get());
    }

    @Override
    public List<Setting<?>> getSettings() {
        final List<Setting<?>> settings = new ArrayList<>();
        settings.add(TransportReindexAction.REMOTE_CLUSTER_WHITELIST);
        settings.add(TransportStartReindexTaskAction.MAX_CONCURRENT_REINDEX_TASKS);
        settings.addAll(ReindexSslConfig.getSettings());
        return settings;
    }

    @Override
    public List<PersistentTasksExecutor<?>> getPersistentTasksExecutor(ClusterService clusterService, ThreadPool threadPool, Client client,
                                                                       SettingsModule settingsModule,
                                                                       IndexNameExpressionResolver expressionResolver) {
        ScriptService scriptService = this.scriptService.get();
        assert scriptService != null;
        ReindexSslConfig reindexSslConfig = this.reindexSslConfig.get();
        assert reindexSslConfig != null;
        return Collections.singletonList(new ReindexTask.ReindexPersistentTasksExecutor(clusterService, client, namedXContentRegistry.get(),
            threadPool, scriptService, reindexSslConfig));
    }
}
