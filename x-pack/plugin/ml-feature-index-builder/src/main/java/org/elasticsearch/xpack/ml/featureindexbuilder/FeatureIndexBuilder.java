/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.featureindexbuilder;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.persistent.PersistentTaskParams;
import org.elasticsearch.persistent.PersistentTaskState;
import org.elasticsearch.persistent.PersistentTasksExecutor;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.PersistentTaskPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ExecutorBuilder;
import org.elasticsearch.threadpool.FixedExecutorBuilder;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.scheduler.SchedulerEngine;
import org.elasticsearch.xpack.ml.featureindexbuilder.action.DeleteFeatureIndexBuilderJobAction;
import org.elasticsearch.xpack.ml.featureindexbuilder.action.PutFeatureIndexBuilderJobAction;
import org.elasticsearch.xpack.ml.featureindexbuilder.action.StartFeatureIndexBuilderJobAction;
import org.elasticsearch.xpack.ml.featureindexbuilder.action.StopFeatureIndexBuilderJobAction;
import org.elasticsearch.xpack.ml.featureindexbuilder.action.TransportDeleteFeatureIndexBuilderJobAction;
import org.elasticsearch.xpack.ml.featureindexbuilder.action.TransportPutFeatureIndexBuilderJobAction;
import org.elasticsearch.xpack.ml.featureindexbuilder.action.TransportStartFeatureIndexBuilderJobAction;
import org.elasticsearch.xpack.ml.featureindexbuilder.action.TransportStopFeatureIndexBuilderJobAction;
import org.elasticsearch.xpack.ml.featureindexbuilder.job.FeatureIndexBuilderJob;
import org.elasticsearch.xpack.ml.featureindexbuilder.job.FeatureIndexBuilderJobPersistentTasksExecutor;
import org.elasticsearch.xpack.ml.featureindexbuilder.job.FeatureIndexBuilderJobState;
import org.elasticsearch.xpack.ml.featureindexbuilder.rest.action.RestDeleteFeatureIndexBuilderJobAction;
import org.elasticsearch.xpack.ml.featureindexbuilder.rest.action.RestPutFeatureIndexBuilderJobAction;
import org.elasticsearch.xpack.ml.featureindexbuilder.rest.action.RestStartFeatureIndexBuilderJobAction;
import org.elasticsearch.xpack.ml.featureindexbuilder.rest.action.RestStopFeatureIndexBuilderJobAction;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

import static java.util.Collections.emptyList;

public class FeatureIndexBuilder extends Plugin implements ActionPlugin, PersistentTaskPlugin {

    public static final String NAME = "feature_index_builder";
    public static final String BASE_PATH = "/_xpack/feature_index_builder/";
    public static final String TASK_THREAD_POOL_NAME = "ml_feature_index_builder_indexing";

    // list of headers that will be stored when a job is created
    public static final Set<String> HEADER_FILTERS = new HashSet<>(
            Arrays.asList("es-security-runas-user", "_xpack_security_authentication"));

    private final boolean enabled;
    private final Settings settings;
    private final boolean transportClientMode;

    public FeatureIndexBuilder(Settings settings) {
        this.settings = settings;

        // todo: XPackSettings.FEATURE_INDEX_BUILDER_ENABLED.get(settings);
        this.enabled = true;
        this.transportClientMode = XPackPlugin.transportClientMode(settings);
    }

    @Override
    public Collection<Module> createGuiceModules() {
        List<Module> modules = new ArrayList<>();

        if (transportClientMode) {
            return modules;
        }

        modules.add(b -> XPackPlugin.bindFeatureSet(b, FeatureIndexBuilderFeatureSet.class));
        return modules;
    }

    protected XPackLicenseState getLicenseState() { return XPackPlugin.getSharedLicenseState(); }

    @Override
    public List<RestHandler> getRestHandlers(final Settings settings, final RestController restController,
            final ClusterSettings clusterSettings, final IndexScopedSettings indexScopedSettings, final SettingsFilter settingsFilter,
            final IndexNameExpressionResolver indexNameExpressionResolver, final Supplier<DiscoveryNodes> nodesInCluster) {

        if (!enabled) {
            return emptyList();
        }

        return Arrays.asList(
                new RestPutFeatureIndexBuilderJobAction(settings, restController),
                new RestStartFeatureIndexBuilderJobAction(settings, restController),
                new RestStopFeatureIndexBuilderJobAction(settings, restController),
                new RestDeleteFeatureIndexBuilderJobAction(settings, restController)
        );
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        if (!enabled) {
            return emptyList();
        }

        return Arrays.asList(
                new ActionHandler<>(PutFeatureIndexBuilderJobAction.INSTANCE, TransportPutFeatureIndexBuilderJobAction.class),
                new ActionHandler<>(StartFeatureIndexBuilderJobAction.INSTANCE, TransportStartFeatureIndexBuilderJobAction.class),
                new ActionHandler<>(StopFeatureIndexBuilderJobAction.INSTANCE, TransportStopFeatureIndexBuilderJobAction.class),
                new ActionHandler<>(DeleteFeatureIndexBuilderJobAction.INSTANCE, TransportDeleteFeatureIndexBuilderJobAction.class)
                );
    }

    @Override
    public List<ExecutorBuilder<?>> getExecutorBuilders(Settings settings) {
        if (false == enabled || transportClientMode) {
            return emptyList();
        }

        FixedExecutorBuilder indexing = new FixedExecutorBuilder(settings, TASK_THREAD_POOL_NAME, 4, 4,
                "xpack.feature_index_builder.task_thread_pool");

        return Collections.singletonList(indexing);
    }

    @Override
    public List<PersistentTasksExecutor<?>> getPersistentTasksExecutor(ClusterService clusterService, ThreadPool threadPool,
            Client client) {
        if (enabled == false || transportClientMode) {
            return emptyList();
        }

        SchedulerEngine schedulerEngine = new SchedulerEngine(settings, Clock.systemUTC());
        return Collections.singletonList(new FeatureIndexBuilderJobPersistentTasksExecutor(settings, client,
                schedulerEngine, threadPool));
    }

    @Override
    public List<NamedXContentRegistry.Entry> getNamedXContent() {
        if (enabled == false) {
            return emptyList();
        }
        return  Arrays.asList(
                new NamedXContentRegistry.Entry(PersistentTaskParams.class, new ParseField("xpack/feature_index_builder/job"),
                        FeatureIndexBuilderJob::fromXContent),
                new NamedXContentRegistry.Entry(Task.Status.class, new ParseField(FeatureIndexBuilderJobState.NAME),
                        FeatureIndexBuilderJobState::fromXContent),
                new NamedXContentRegistry.Entry(PersistentTaskState.class, new ParseField(FeatureIndexBuilderJobState.NAME),
                        FeatureIndexBuilderJobState::fromXContent)
                );
    }
}
