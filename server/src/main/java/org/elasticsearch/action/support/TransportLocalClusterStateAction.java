/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.support;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.UpdateForV10;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.xcontent.ToXContent;

import java.util.concurrent.Executor;

/**
 * Analogue of {@link org.elasticsearch.action.support.master.TransportMasterNodeReadAction} except that it runs on the local node rather
 * than delegating to the master.
 */
public abstract class TransportLocalClusterStateAction<Request extends ActionRequest, Response extends ActionResponse> extends
    TransportAction<Request, Response> {

    protected final ClusterService clusterService;
    protected final Executor executor;

    protected TransportLocalClusterStateAction(
        String actionName,
        ActionFilters actionFilters,
        TaskManager taskManager,
        ClusterService clusterService,
        Executor executor
    ) {
        // TODO replace DIRECT_EXECUTOR_SERVICE when removing workaround for https://github.com/elastic/elasticsearch/issues/97916
        super(actionName, actionFilters, taskManager, EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.clusterService = clusterService;
        this.executor = executor;
    }

    protected abstract ClusterBlockException checkBlock(Request request, ClusterState state);

    @Override
    protected final void doExecute(Task task, Request request, ActionListener<Response> listener) {
        final var state = clusterService.state();
        final var clusterBlockException = checkBlock(request, state);
        if (clusterBlockException != null) {
            throw clusterBlockException;
        }

        // Workaround for https://github.com/elastic/elasticsearch/issues/97916 - TODO remove this when we can
        executor.execute(ActionRunnable.wrap(listener, l -> localClusterStateOperation(task, request, state, l)));
    }

    protected abstract void localClusterStateOperation(Task task, Request request, ClusterState state, ActionListener<Response> listener)
        throws Exception;

    // Remove the BWC support for the deprecated ?master_timeout parameter.
    // NOTE: ensure each usage of this method has been deprecated for long enough to remove it.
    @UpdateForV10(owner = UpdateForV10.Owner.DISTRIBUTED_COORDINATION)
    public static void consumeDeprecatedMasterTimeoutParameter(ToXContent.Params params) {
        if (params.paramAsBoolean("master_timeout", false)) {
            DeprecationLogger.getLogger(TransportLocalClusterStateAction.class)
                .critical(
                    DeprecationCategory.API,
                    "TransportLocalClusterStateAction-master-timeout-parameter",
                    "the [?master_timeout] query parameter to this API has no effect, is now deprecated, "
                        + "and will be removed in a future version"
                );
        }
    }

    // Remove the BWC support for the deprecated ?local parameter.
    // NOTE: ensure each usage of this method has been deprecated for long enough to remove it.
    @UpdateForV10(owner = UpdateForV10.Owner.DISTRIBUTED_COORDINATION)
    public static void consumeDeprecatedLocalParameter(ToXContent.Params params) {
        if (params.paramAsBoolean("local", false)) {
            DeprecationLogger.getLogger(TransportLocalClusterStateAction.class)
                .critical(
                    DeprecationCategory.API,
                    "TransportLocalClusterStateAction-local-parameter",
                    "the [?local] query parameter to this API has no effect, is now deprecated, and will be removed in a future version"
                );
        }
    }
}
