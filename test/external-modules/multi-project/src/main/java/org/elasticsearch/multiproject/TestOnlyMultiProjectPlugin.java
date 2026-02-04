/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.multiproject;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.multiproject.action.DeleteProjectAction;
import org.elasticsearch.multiproject.action.PutProjectAction;
import org.elasticsearch.multiproject.action.RestDeleteProjectAction;
import org.elasticsearch.multiproject.action.RestPutProjectAction;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.rest.RestHeaderDefinition;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import java.util.function.Supplier;

public class TestOnlyMultiProjectPlugin extends Plugin implements ActionPlugin {

    public static final Setting<Boolean> MULTI_PROJECT_ENABLED = Setting.boolSetting(
        "test.multi_project.enabled",
        false,
        Setting.Property.NodeScope
    );

    private static final Logger logger = LogManager.getLogger(TestOnlyMultiProjectPlugin.class);

    public final SetOnce<ThreadPool> threadPool = new SetOnce<>();

    private final boolean multiProjectEnabled;

    public TestOnlyMultiProjectPlugin(Settings settings) {
        multiProjectEnabled = MULTI_PROJECT_ENABLED.get(settings);
        logger.info("multi-project is [{}]", multiProjectEnabled ? "enabled" : "disabled");
    }

    @Override
    public Collection<RestHandler> getRestHandlers(
        Settings settings,
        NamedWriteableRegistry namedWriteableRegistry,
        RestController restController,
        ClusterSettings clusterSettings,
        IndexScopedSettings indexScopedSettings,
        SettingsFilter settingsFilter,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<DiscoveryNodes> nodesInCluster,
        Predicate<NodeFeature> clusterSupportsFeature
    ) {
        if (multiProjectEnabled) {
            return List.of(new RestPutProjectAction(), new RestDeleteProjectAction());
        } else {
            return List.of();
        }
    }

    @Override
    public Collection<ActionHandler> getActions() {
        if (multiProjectEnabled) {
            return List.of(
                new ActionHandler(PutProjectAction.INSTANCE, PutProjectAction.TransportPutProjectAction.class),
                new ActionHandler(DeleteProjectAction.INSTANCE, DeleteProjectAction.TransportDeleteProjectAction.class)
            );
        } else {
            return List.of();
        }
    }

    @Override
    public Collection<RestHeaderDefinition> getRestHeaders() {
        if (multiProjectEnabled) {
            return Set.of(new RestHeaderDefinition(Task.X_ELASTIC_PROJECT_ID_HTTP_HEADER, false));
        } else {
            return Set.of();
        }
    }

    @Override
    public Collection<?> createComponents(PluginServices services) {
        threadPool.set(services.threadPool());
        return List.of();
    }

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(MULTI_PROJECT_ENABLED);
    }

    public ThreadPool getThreadPool() {
        return threadPool.get();
    }

    public boolean multiProjectEnabled() {
        return multiProjectEnabled;
    }
}
