/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.indexlifecycle;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.threadpool.ExecutorBuilder;
import org.elasticsearch.threadpool.FixedExecutorBuilder;
import org.elasticsearch.xpack.XPackPlugin;
import org.elasticsearch.xpack.XPackSettings;
import org.elasticsearch.xpack.security.InternalClient;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

public class IndexLifecycle implements ActionPlugin {
    private static final Logger logger = Loggers.getLogger(XPackPlugin.class);
    public static final String NAME = "index_lifecycle";
    public static final String BASE_PATH = "/_xpack/index_lifecycle/";
    public static final String THREAD_POOL_NAME = NAME;
    private final SetOnce<IndexLifecycleInitialisationService> indexLifecycleInitialisationService = new SetOnce<>();
    private Settings settings;
    private boolean enabled;
    private boolean transportClientMode;
    private boolean tribeNode;
    private boolean tribeNodeClient;

    public static final Setting LIFECYCLE_TIMESERIES_SETTING = Setting.groupSetting("index.lifecycle.timeseries.", (settings) -> {
        ESLoggerFactory.getLogger("INDEX-LIFECYCLE-PLUGIN").error("validating setting internally: " + settings);
        if (settings.size() == 0) {
            return;
        }
    }, Setting.Property.Dynamic, Setting.Property.IndexScope);

    public IndexLifecycle(Settings settings) {
        this.settings = settings;
        this.enabled = XPackSettings.INDEX_LIFECYCLE_ENABLED.get(settings);
        this.transportClientMode = XPackPlugin.transportClientMode(settings);
        this.tribeNode = XPackPlugin.isTribeNode(settings);
        this.tribeNodeClient = XPackPlugin.isTribeClientNode(settings);
    }

    public Collection<Module> nodeModules() {
        List<Module> modules = new ArrayList<>();

        if (transportClientMode) {
            return modules;
        }

        modules.add(b -> XPackPlugin.bindFeatureSet(b, IndexLifecycleFeatureSet.class));

        return modules;
    }

    public void onIndexModule(IndexModule indexModule) {
        Index index = indexModule.getIndex();
        ESLoggerFactory.getLogger("INDEX-LIFECYCLE-PLUGIN").error("onIndexModule: " + index.getName());
        long creationDate = settings.getAsLong("index.creation_date", -1L);
        indexModule.addSettingsUpdateConsumer(LIFECYCLE_TIMESERIES_SETTING,
            (Settings s) -> indexLifecycleInitialisationService.get().setLifecycleSettings(index, creationDate, s));
        indexModule.addIndexEventListener(indexLifecycleInitialisationService.get());
    }

    public Collection<Object> createComponents(InternalClient internalClient, ClusterService clusterService, Clock clock) {
        indexLifecycleInitialisationService.set(new IndexLifecycleInitialisationService(settings, internalClient, clusterService, clock));
        return Collections.singletonList(indexLifecycleInitialisationService.get());
    }

    public List<Setting<?>> getSettings() {
        return Arrays.asList(LIFECYCLE_TIMESERIES_SETTING);
    }

    @Override
    public List<RestHandler> getRestHandlers(Settings settings, RestController restController, ClusterSettings clusterSettings,
                                             IndexScopedSettings indexScopedSettings, SettingsFilter settingsFilter,
                                             IndexNameExpressionResolver indexNameExpressionResolver,
                                             Supplier<DiscoveryNodes> nodesInCluster) {
        if (!enabled || tribeNodeClient) {
            return Collections.emptyList();
        }

        return Arrays.asList(
//                new RestLifecycleStatusAction(settings, restController)
        );

    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        if (!enabled) {
            return Collections.emptyList();
        }
        return Arrays.asList(
//                new ActionHandler<>(LifecycleStatusAction.INSTANCE, LifecycleStatusAction.TransportAction.class)
        );
    }

    public List<ExecutorBuilder<?>> getExecutorBuilders(Settings settings) {
        if (false == enabled || tribeNode || tribeNodeClient || transportClientMode) {
            return Collections.emptyList();
        }

        FixedExecutorBuilder indexing = new FixedExecutorBuilder(settings, IndexLifecycle.THREAD_POOL_NAME, 4, 4,
                "xpack.index_lifecycle.thread_pool");

        return Collections.singletonList(indexing);
    }

//    public Collection<PersistentTasksExecutor<?>> getPersistentTasksExecutors(InternalClient client,
//                                                                              ClusterService clusterService,
//                                                                              SchedulerEngine schedulerEngine) {
//        return Collections.singletonList(
//                new IndexLifecycleTask.IndexLifecycleJobPersistentTasksExecutor(settings, client, clusterService, schedulerEngine));
//    }

    public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        return Arrays.asList(
//                // Metadata
//                new NamedWriteableRegistry.Entry(MetaData.Custom.class, "rollup", RollupMetadata::new),
//                new NamedWriteableRegistry.Entry(NamedDiff.class, "rollup", RollupMetadata.RollupMetadataDiff::new),
//
//                // Persistent action requests
//                new NamedWriteableRegistry.Entry(PersistentTaskParams.class, RollupJobTask.TASK_NAME,
//                        RollupJob::new),
//
//                // Task statuses
//                new NamedWriteableRegistry.Entry(Task.Status.class, RollupJobStatus.NAME, RollupJobStatus::new)
        );
    }

    public List<NamedXContentRegistry.Entry> getNamedXContent() {
        return Arrays.asList(
//                // Custom metadata
//                new NamedXContentRegistry.Entry(MetaData.Custom.class, new ParseField("rollup"),
//                        parser -> RollupMetadata.METADATA_PARSER.parse(parser, null).build()),
//
//                // Persistent action requests
//                new NamedXContentRegistry.Entry(PersistentTaskParams.class, new ParseField(RollupJobTask.TASK_NAME),
//                        parser -> RollupJob.Builder.fromXContent(parser).build())
//
//                // Task statuses
//                //new NamedXContentRegistry.Entry(Task.Status.class, new ParseField(RollupJobStatus.NAME), RollupJobStatus::fromXContent)
        );
    }
}
