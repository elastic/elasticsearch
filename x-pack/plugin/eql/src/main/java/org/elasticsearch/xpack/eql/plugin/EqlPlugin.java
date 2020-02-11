/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.eql.plugin;

import org.elasticsearch.Build;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xpack.core.action.XPackInfoFeatureAction;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureAction;
import org.elasticsearch.xpack.eql.EqlInfoTransportAction;
import org.elasticsearch.xpack.eql.EqlUsageTransportAction;
import org.elasticsearch.xpack.eql.action.EqlSearchAction;
import org.elasticsearch.xpack.eql.execution.PlanExecutor;
import org.elasticsearch.xpack.ql.index.IndexResolver;
import org.elasticsearch.xpack.ql.type.DefaultDataTypeRegistry;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

public class EqlPlugin extends Plugin implements ActionPlugin {

    public static final Setting<Boolean> EQL_ENABLED_SETTING = Setting.boolSetting(
        "xpack.eql.enabled",
        false,
        Setting.Property.NodeScope
    );

    @Override
    public Collection<Object> createComponents(Client client, ClusterService clusterService, ThreadPool threadPool,
            ResourceWatcherService resourceWatcherService, ScriptService scriptService, NamedXContentRegistry xContentRegistry,
            Environment environment, NodeEnvironment nodeEnvironment, NamedWriteableRegistry namedWriteableRegistry) {

        return createComponents(client, clusterService.getClusterName().value(), namedWriteableRegistry);
    }

    private Collection<Object> createComponents(Client client, String clusterName, NamedWriteableRegistry namedWriteableRegistry) {
        IndexResolver indexResolver = new IndexResolver(client, clusterName, DefaultDataTypeRegistry.INSTANCE);
        PlanExecutor planExecutor = new PlanExecutor(client, indexResolver, namedWriteableRegistry);
        return Arrays.asList(planExecutor);
    }


    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return Arrays.asList(
            new ActionHandler<>(EqlSearchAction.INSTANCE, TransportEqlSearchAction.class),
            new ActionHandler<>(EqlStatsAction.INSTANCE, TransportEqlStatsAction.class),
            new ActionHandler<>(XPackUsageFeatureAction.EQL, EqlUsageTransportAction.class),
            new ActionHandler<>(XPackInfoFeatureAction.EQL, EqlInfoTransportAction.class)
        );
    }

    /**
     * The settings defined by EQL plugin.
     *
     * @return the settings
     */
    @Override
    public List<Setting<?>> getSettings() {
        if (isSnapshot()) {
            return List.of(EQL_ENABLED_SETTING);
        } else {
            return List.of();
        }
    }

    boolean isSnapshot() {
        return Build.CURRENT.isSnapshot();
    }

    // TODO: this needs to be used by all plugin methods - including getActions and createComponents
    public static boolean isEnabled(Settings settings) {
        return EQL_ENABLED_SETTING.get(settings);
    }

    @Override
    public List<RestHandler> getRestHandlers(Settings settings,
                                             RestController restController,
                                             ClusterSettings clusterSettings,
                                             IndexScopedSettings indexScopedSettings,
                                             SettingsFilter settingsFilter,
                                             IndexNameExpressionResolver indexNameExpressionResolver,
                                             Supplier<DiscoveryNodes> nodesInCluster) {

        if (isEnabled(settings) == false) {
            return Collections.emptyList();
        }
        return Arrays.asList(new RestEqlSearchAction(), new RestEqlStatsAction());
    }
}
