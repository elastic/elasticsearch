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
import org.elasticsearch.common.inject.Module;
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
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.action.CreateDataStreamAction;
import org.elasticsearch.xpack.core.action.DataStreamsStatsAction;
import org.elasticsearch.xpack.core.action.DeleteDataStreamAction;
import org.elasticsearch.xpack.core.action.GetDataStreamAction;
import org.elasticsearch.xpack.core.action.PromoteDataStreamAction;
import org.elasticsearch.xpack.datastreams.action.CreateDataStreamTransportAction;
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

public class DataStreamsPlugin extends Plugin implements ActionPlugin, MapperPlugin {

    private final boolean transportClientMode;

    public DataStreamsPlugin(Settings settings) {
        this.transportClientMode = XPackPlugin.transportClientMode(settings);
    }

    @Override
    public Map<String, MetadataFieldMapper.TypeParser> getMetadataMappers() {
        return Collections.singletonMap(DataStreamTimestampFieldMapper.NAME, DataStreamTimestampFieldMapper.PARSER);
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return Arrays.asList(
            new ActionHandler<>(CreateDataStreamAction.INSTANCE, CreateDataStreamTransportAction.class),
            new ActionHandler<>(DeleteDataStreamAction.INSTANCE, DeleteDataStreamTransportAction.class),
            new ActionHandler<>(GetDataStreamAction.INSTANCE, GetDataStreamsTransportAction.class),
            new ActionHandler<>(DataStreamsStatsAction.INSTANCE, DataStreamsStatsTransportAction.class),
            new ActionHandler<>(MigrateToDataStreamAction.INSTANCE, MigrateToDataStreamTransportAction.class),
            new ActionHandler<>(PromoteDataStreamAction.INSTANCE, PromoteDataStreamTransportAction.class)
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
        RestHandler createDsAction = new RestCreateDataStreamAction();
        RestHandler deleteDsAction = new RestDeleteDataStreamAction();
        RestHandler getDsAction = new RestGetDataStreamsAction();
        RestHandler dsStatsAction = new RestDataStreamsStatsAction();
        RestHandler migrateAction = new RestMigrateToDataStreamAction();
        RestHandler promoteAction = new RestPromoteDataStreamAction();
        return Arrays.asList(createDsAction, deleteDsAction, getDsAction, dsStatsAction, migrateAction, promoteAction);
    }

    public Collection<Module> createGuiceModules() {
        List<Module> modules = new ArrayList<>();

        if (transportClientMode) {
            return modules;
        }

        modules.add(b -> XPackPlugin.bindFeatureSet(b, DataStreamFeatureSet.class));

        return modules;
    }
}
