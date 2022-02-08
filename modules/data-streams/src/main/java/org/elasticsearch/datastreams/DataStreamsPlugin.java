/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.datastreams;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.datastreams.CreateDataStreamAction;
import org.elasticsearch.action.datastreams.DataStreamsStatsAction;
import org.elasticsearch.action.datastreams.DeleteDataStreamAction;
import org.elasticsearch.action.datastreams.GetDataStreamAction;
import org.elasticsearch.action.datastreams.MigrateToDataStreamAction;
import org.elasticsearch.action.datastreams.ModifyDataStreamsAction;
import org.elasticsearch.action.datastreams.PromoteDataStreamAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.datastreams.action.CreateDataStreamTransportAction;
import org.elasticsearch.datastreams.action.DataStreamsStatsTransportAction;
import org.elasticsearch.datastreams.action.DeleteDataStreamTransportAction;
import org.elasticsearch.datastreams.action.GetDataStreamsTransportAction;
import org.elasticsearch.datastreams.action.MigrateToDataStreamTransportAction;
import org.elasticsearch.datastreams.action.ModifyDataStreamsTransportAction;
import org.elasticsearch.datastreams.action.PromoteDataStreamTransportAction;
import org.elasticsearch.datastreams.rest.RestCreateDataStreamAction;
import org.elasticsearch.datastreams.rest.RestDataStreamsStatsAction;
import org.elasticsearch.datastreams.rest.RestDeleteDataStreamAction;
import org.elasticsearch.datastreams.rest.RestGetDataStreamsAction;
import org.elasticsearch.datastreams.rest.RestMigrateToDataStreamAction;
import org.elasticsearch.datastreams.rest.RestModifyDataStreamsAction;
import org.elasticsearch.datastreams.rest.RestPromoteDataStreamAction;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.IndexSettingProvider;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xcontent.NamedXContentRegistry;

import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;

public class DataStreamsPlugin extends Plugin implements ActionPlugin {

    public static final Setting<TimeValue> TIME_SERIES_POLL_INTERVAL = Setting.timeSetting(
        "time_series.poll_interval",
        TimeValue.timeValueMinutes(5),
        TimeValue.timeValueMinutes(1),
        TimeValue.timeValueMinutes(10),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    @Override
    public List<Setting<?>> getSettings() {
        if (IndexSettings.isTimeSeriesModeEnabled() == false) {
            return List.of();
        }

        return List.of(TIME_SERIES_POLL_INTERVAL);
    }

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
        if (IndexSettings.isTimeSeriesModeEnabled() == false) {
            return List.of();
        }

        var service = new UpdateTimeSeriesRangeService(environment.settings(), threadPool, clusterService);
        return List.of(service);
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        var createDsAction = new ActionHandler<>(CreateDataStreamAction.INSTANCE, CreateDataStreamTransportAction.class);
        var deleteDsInfoAction = new ActionHandler<>(DeleteDataStreamAction.INSTANCE, DeleteDataStreamTransportAction.class);
        var getDsAction = new ActionHandler<>(GetDataStreamAction.INSTANCE, GetDataStreamsTransportAction.class);
        var dsStatsAction = new ActionHandler<>(DataStreamsStatsAction.INSTANCE, DataStreamsStatsTransportAction.class);
        var migrateAction = new ActionHandler<>(MigrateToDataStreamAction.INSTANCE, MigrateToDataStreamTransportAction.class);
        var promoteAction = new ActionHandler<>(PromoteDataStreamAction.INSTANCE, PromoteDataStreamTransportAction.class);
        var modifyAction = new ActionHandler<>(ModifyDataStreamsAction.INSTANCE, ModifyDataStreamsTransportAction.class);
        return List.of(createDsAction, deleteDsInfoAction, getDsAction, dsStatsAction, migrateAction, promoteAction, modifyAction);
    }

    @Override
    public List<RestHandler> getRestHandlers(
        Settings settings,
        RestController restController,
        ClusterSettings clusterSettings,
        IndexScopedSettings indexScopedSettings,
        SettingsFilter settingsFilter,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<DiscoveryNodes> nodesInCluster
    ) {
        var createDsAction = new RestCreateDataStreamAction();
        var deleteDsAction = new RestDeleteDataStreamAction();
        var getDsAction = new RestGetDataStreamsAction();
        var dsStatsAction = new RestDataStreamsStatsAction();
        var migrateAction = new RestMigrateToDataStreamAction();
        var promoteAction = new RestPromoteDataStreamAction();
        var modifyAction = new RestModifyDataStreamsAction();
        return List.of(createDsAction, deleteDsAction, getDsAction, dsStatsAction, migrateAction, promoteAction, modifyAction);
    }

    @Override
    public Collection<IndexSettingProvider> getAdditionalIndexSettingProviders() {
        return List.of(new DataStreamIndexSettingsProvider());
    }
}
