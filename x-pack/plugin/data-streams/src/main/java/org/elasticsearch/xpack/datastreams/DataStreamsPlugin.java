/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.datastreams;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.index.mapper.DataStreamTimestampFieldMapper;
import org.elasticsearch.index.mapper.MetadataFieldMapper;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.xpack.core.MigrateToDataStreamAction;
import org.elasticsearch.xpack.core.action.CreateDataStreamAction;
import org.elasticsearch.xpack.core.action.DataStreamsStatsAction;
import org.elasticsearch.xpack.core.action.DeleteDataStreamAction;
import org.elasticsearch.xpack.core.action.GetDataStreamAction;
import org.elasticsearch.xpack.core.action.PromoteDataStreamAction;
import org.elasticsearch.xpack.core.action.XPackInfoFeatureAction;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureAction;
import org.elasticsearch.xpack.datastreams.action.CreateDataStreamTransportAction;
import org.elasticsearch.xpack.datastreams.action.DataStreamInfoTransportAction;
import org.elasticsearch.xpack.datastreams.action.DataStreamUsageTransportAction;
import org.elasticsearch.xpack.datastreams.action.DataStreamsStatsTransportAction;
import org.elasticsearch.xpack.datastreams.action.DeleteDataStreamTransportAction;
import org.elasticsearch.xpack.datastreams.action.GetDataStreamsTransportAction;
import org.elasticsearch.xpack.datastreams.action.MigrateToDataStreamTransportAction;
import org.elasticsearch.xpack.datastreams.action.PromoteDataStreamTransportAction;
import org.elasticsearch.xpack.datastreams.rest.RestCreateDataStreamAction;
import org.elasticsearch.xpack.datastreams.rest.RestDataStreamsStatsAction;
import org.elasticsearch.xpack.datastreams.rest.RestDeleteDataStreamAction;
import org.elasticsearch.xpack.datastreams.rest.RestGetDataStreamsAction;
import org.elasticsearch.xpack.datastreams.rest.RestMigrateToDataStreamAction;
import org.elasticsearch.xpack.datastreams.rest.RestPromoteDataStreamAction;

import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

public class DataStreamsPlugin extends Plugin implements ActionPlugin, MapperPlugin {

    @Override
    public Map<String, MetadataFieldMapper.TypeParser> getMetadataMappers() {
        return Map.of(DataStreamTimestampFieldMapper.NAME, DataStreamTimestampFieldMapper.PARSER);
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        var createDsAction = new ActionHandler<>(CreateDataStreamAction.INSTANCE, CreateDataStreamTransportAction.class);
        var deleteDsInfoAction = new ActionHandler<>(DeleteDataStreamAction.INSTANCE, DeleteDataStreamTransportAction.class);
        var getDsAction = new ActionHandler<>(GetDataStreamAction.INSTANCE, GetDataStreamsTransportAction.class);
        var dsStatsAction = new ActionHandler<>(DataStreamsStatsAction.INSTANCE, DataStreamsStatsTransportAction.class);
        var dsUsageAction = new ActionHandler<>(XPackUsageFeatureAction.DATA_STREAMS, DataStreamUsageTransportAction.class);
        var dsInfoAction = new ActionHandler<>(XPackInfoFeatureAction.DATA_STREAMS, DataStreamInfoTransportAction.class);
        var migrateAction = new ActionHandler<>(MigrateToDataStreamAction.INSTANCE, MigrateToDataStreamTransportAction.class);
        var promoteAction = new ActionHandler<>(PromoteDataStreamAction.INSTANCE, PromoteDataStreamTransportAction.class);
        return List.of(
            createDsAction,
            deleteDsInfoAction,
            getDsAction,
            dsStatsAction,
            dsUsageAction,
            dsInfoAction,
            migrateAction,
            promoteAction
        );
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
        return List.of(createDsAction, deleteDsAction, getDsAction, dsStatsAction, migrateAction, promoteAction);
    }
}
