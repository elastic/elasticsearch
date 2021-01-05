/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.textstructure;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.threadpool.ExecutorBuilder;
import org.elasticsearch.threadpool.ScalingExecutorBuilder;
import org.elasticsearch.xpack.core.textstructure.action.FindFileStructureAction;
import org.elasticsearch.xpack.textstructure.rest.RestFindFileStructureAction;
import org.elasticsearch.xpack.textstructure.transport.TransportFindFileStructureAction;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

/**
 * This plugin provides APIs for text structure analysis.
 *
 */
public class TextStructurePlugin extends Plugin implements ActionPlugin {

    public static final String BASE_PATH = "/_text_structure/";
    public static final String UTILITY_THREAD_POOL_NAME = "text_structure_utility";

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
        return Arrays.asList(new RestFindFileStructureAction());
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return Arrays.asList(new ActionHandler<>(FindFileStructureAction.INSTANCE, TransportFindFileStructureAction.class));
    }

    @Override
    public List<ExecutorBuilder<?>> getExecutorBuilders(Settings settings) {
        // This threadpool is used for text structure utility work that should not block other threads.
        ScalingExecutorBuilder utility = new ScalingExecutorBuilder(
            UTILITY_THREAD_POOL_NAME,
            1,
            256,
            TimeValue.timeValueMinutes(10),
            "xpack.text_structure.utility_thread_pool"
        );

        return Collections.singletonList(utility);
    }

}
