/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.xpack.enterprisesearch;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.xpack.enterprisesearch.action.EntSearchAction;
import org.elasticsearch.xpack.enterprisesearch.action.RestEntSearchAction;
import org.elasticsearch.xpack.enterprisesearch.action.TransportEntSearchAction;
import org.elasticsearch.xpack.enterprisesearch.action.XSearchAction;

import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;

import static java.util.Collections.singletonList;

public class EnterpriseSearchPlugin extends Plugin implements ActionPlugin {

    @Override
    public List<RestHandler> getRestHandlers(final Settings settings,
                                             final RestController restController,
                                             final ClusterSettings clusterSettings,
                                             final IndexScopedSettings indexScopedSettings,
                                             final SettingsFilter settingsFilter,
                                             final IndexNameExpressionResolver indexNameExpressionResolver,
                                             final Supplier<DiscoveryNodes> nodesInCluster) {

        return Arrays.asList(
            new XSearchAction(),
            new RestEntSearchAction()
        );
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return singletonList(
            new ActionHandler<>(EntSearchAction.INSTANCE, TransportEntSearchAction.class)
        );
    }
}
