/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.root;

import org.elasticsearch.Build;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.node.Node;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

public class TransportMainAction extends TransportAction<MainRequest, MainResponse> {

    private final String nodeName;
    private final ClusterService clusterService;

    @Inject
    public TransportMainAction(
        Settings settings,
        TransportService transportService,
        ActionFilters actionFilters,
        ClusterService clusterService
    ) {
        super(MainRestPlugin.MAIN_ACTION.name(), actionFilters, transportService.getTaskManager(), EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.nodeName = Node.NODE_NAME_SETTING.get(settings);
        this.clusterService = clusterService;
    }

    @Override
    protected void doExecute(Task task, MainRequest request, ActionListener<MainResponse> listener) {
        ClusterState clusterState = clusterService.state();
        listener.onResponse(
            new MainResponse(
                nodeName,
                IndexVersion.current().luceneVersion().toString(),
                clusterState.getClusterName(),
                clusterState.metadata().clusterUUID(),
                Build.current()
            )
        );
    }
}
