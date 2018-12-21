/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.upgrade;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
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
import org.elasticsearch.xpack.core.upgrade.actions.IndexUpgradeAction;
import org.elasticsearch.xpack.core.upgrade.actions.IndexUpgradeInfoAction;
import org.elasticsearch.xpack.upgrade.actions.TransportIndexUpgradeAction;
import org.elasticsearch.xpack.upgrade.actions.TransportIndexUpgradeInfoAction;
import org.elasticsearch.xpack.upgrade.rest.RestIndexUpgradeAction;
import org.elasticsearch.xpack.upgrade.rest.RestIndexUpgradeInfoAction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Supplier;

public class Upgrade extends Plugin implements ActionPlugin {

    public static final Version UPGRADE_INTRODUCED = Version.CURRENT.minimumCompatibilityVersion();

    private final List<BiFunction<Client, ClusterService, IndexUpgradeCheck>> upgradeCheckFactories;

    public Upgrade() {
        this.upgradeCheckFactories = new ArrayList<>();
    }

    @Override
    public Collection<Object> createComponents(Client client, ClusterService clusterService, ThreadPool threadPool,
                                               ResourceWatcherService resourceWatcherService, ScriptService scriptService,
                                               NamedXContentRegistry xContentRegistry, Environment environment,
                                               NodeEnvironment nodeEnvironment, NamedWriteableRegistry namedWriteableRegistry) {
        List<IndexUpgradeCheck> upgradeChecks = new ArrayList<>(upgradeCheckFactories.size());
        for (BiFunction<Client, ClusterService, IndexUpgradeCheck> checkFactory : upgradeCheckFactories) {
            upgradeChecks.add(checkFactory.apply(client, clusterService));
        }
        return Collections.singletonList(new IndexUpgradeService(Collections.unmodifiableList(upgradeChecks)));
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return Arrays.asList(
                new ActionHandler<>(IndexUpgradeInfoAction.INSTANCE, TransportIndexUpgradeInfoAction.class),
                new ActionHandler<>(IndexUpgradeAction.INSTANCE, TransportIndexUpgradeAction.class)
        );
    }

    @Override
    public List<RestHandler> getRestHandlers(Settings settings, RestController restController, ClusterSettings clusterSettings,
                                             IndexScopedSettings indexScopedSettings, SettingsFilter settingsFilter,
                                             IndexNameExpressionResolver indexNameExpressionResolver,
                                             Supplier<DiscoveryNodes> nodesInCluster) {
        return Arrays.asList(
                new RestIndexUpgradeInfoAction(settings, restController),
                new RestIndexUpgradeAction(settings, restController)
        );
    }

}
