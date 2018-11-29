/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.dataframe;

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
import org.elasticsearch.common.settings.SettingsModule;
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
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.scheduler.SchedulerEngine;
import org.elasticsearch.xpack.dataframe.action.DeleteDataFrameJobAction;
import org.elasticsearch.xpack.dataframe.action.GetDataFrameJobsAction;
import org.elasticsearch.xpack.dataframe.action.GetDataFrameJobsStatsAction;
import org.elasticsearch.xpack.dataframe.action.PutDataFrameJobAction;
import org.elasticsearch.xpack.dataframe.action.StartDataFrameJobAction;
import org.elasticsearch.xpack.dataframe.action.StopDataFrameJobAction;
import org.elasticsearch.xpack.dataframe.action.TransportDeleteDataFrameJobAction;
import org.elasticsearch.xpack.dataframe.action.TransportGetDataFrameJobsAction;
import org.elasticsearch.xpack.dataframe.action.TransportGetDataFrameJobsStatsAction;
import org.elasticsearch.xpack.dataframe.action.TransportPutDataFrameJobAction;
import org.elasticsearch.xpack.dataframe.action.TransportStartDataFrameJobAction;
import org.elasticsearch.xpack.dataframe.action.TransportStopDataFrameJobAction;
import org.elasticsearch.xpack.dataframe.job.DataFrameJob;
import org.elasticsearch.xpack.dataframe.job.DataFrameJobPersistentTasksExecutor;
import org.elasticsearch.xpack.dataframe.job.DataFrameJobState;
import org.elasticsearch.xpack.dataframe.rest.action.RestDeleteFeatureIndexBuilderJobAction;
import org.elasticsearch.xpack.dataframe.rest.action.RestGetDataFrameJobsAction;
import org.elasticsearch.xpack.dataframe.rest.action.RestGetDataFrameJobsStatsAction;
import org.elasticsearch.xpack.dataframe.rest.action.RestPutDataFrameJobAction;
import org.elasticsearch.xpack.dataframe.rest.action.RestStartDataFrameJobAction;
import org.elasticsearch.xpack.dataframe.rest.action.RestStopDataFrameJobAction;

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

public class DataFrame extends Plugin implements ActionPlugin, PersistentTaskPlugin {

    public static final String NAME = "data_frame";
    public static final String TASK_NAME = "data_frame/jobs";
    public static final String BASE_PATH = "/_data_frame/";
    public static final String BASE_PATH_JOBS_BY_ID = BASE_PATH + "jobs/{id}/";
    public static final String TASK_THREAD_POOL_NAME = "data_frame_indexing";

    // list of headers that will be stored when a job is created
    public static final Set<String> HEADER_FILTERS = new HashSet<>(
            Arrays.asList("es-security-runas-user", "_xpack_security_authentication"));

    private final boolean enabled;
    private final Settings settings;
    private final boolean transportClientMode;

    public DataFrame(Settings settings) {
        this.settings = settings;

        this.enabled = XPackSettings.DATA_FRAME_ENABLED.get(settings);
        this.transportClientMode = XPackPlugin.transportClientMode(settings);
    }

    @Override
    public Collection<Module> createGuiceModules() {
        List<Module> modules = new ArrayList<>();

        if (transportClientMode) {
            return modules;
        }

        modules.add(b -> XPackPlugin.bindFeatureSet(b, DataFrameFeatureSet.class));
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
                new RestPutDataFrameJobAction(settings, restController),
                new RestStartDataFrameJobAction(settings, restController),
                new RestStopDataFrameJobAction(settings, restController),
                new RestDeleteFeatureIndexBuilderJobAction(settings, restController),
                new RestGetDataFrameJobsAction(settings, restController),
                new RestGetDataFrameJobsStatsAction(settings, restController)
        );
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        if (!enabled) {
            return emptyList();
        }

        return Arrays.asList(
                new ActionHandler<>(PutDataFrameJobAction.INSTANCE, TransportPutDataFrameJobAction.class),
                new ActionHandler<>(StartDataFrameJobAction.INSTANCE, TransportStartDataFrameJobAction.class),
                new ActionHandler<>(StopDataFrameJobAction.INSTANCE, TransportStopDataFrameJobAction.class),
                new ActionHandler<>(DeleteDataFrameJobAction.INSTANCE, TransportDeleteDataFrameJobAction.class),
                new ActionHandler<>(GetDataFrameJobsAction.INSTANCE, TransportGetDataFrameJobsAction.class),
                new ActionHandler<>(GetDataFrameJobsStatsAction.INSTANCE, TransportGetDataFrameJobsStatsAction.class)
                );
    }

    @Override
    public List<ExecutorBuilder<?>> getExecutorBuilders(Settings settings) {
        if (false == enabled || transportClientMode) {
            return emptyList();
        }

        FixedExecutorBuilder indexing = new FixedExecutorBuilder(settings, TASK_THREAD_POOL_NAME, 4, 4,
                "data_frame.task_thread_pool");

        return Collections.singletonList(indexing);
    }

    @Override
    public List<PersistentTasksExecutor<?>> getPersistentTasksExecutor(ClusterService clusterService, ThreadPool threadPool,
            Client client, SettingsModule settingsModule) {
        if (enabled == false || transportClientMode) {
            return emptyList();
        }

        SchedulerEngine schedulerEngine = new SchedulerEngine(settings, Clock.systemUTC());
        return Collections.singletonList(new DataFrameJobPersistentTasksExecutor(client,
                schedulerEngine, threadPool));
    }

    @Override
    public List<NamedXContentRegistry.Entry> getNamedXContent() {
        if (enabled == false) {
            return emptyList();
        }
        return  Arrays.asList(
                new NamedXContentRegistry.Entry(PersistentTaskParams.class, new ParseField(TASK_NAME),
                        DataFrameJob::fromXContent),
                new NamedXContentRegistry.Entry(Task.Status.class, new ParseField(DataFrameJobState.NAME),
                        DataFrameJobState::fromXContent),
                new NamedXContentRegistry.Entry(PersistentTaskState.class, new ParseField(DataFrameJobState.NAME),
                        DataFrameJobState::fromXContent)
                );
    }
}
