/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.action.admin.cluster.node.tasks.list.TransportListTasksAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.esql.action.EsqlListQueriesAction;
import org.elasticsearch.xpack.esql.action.EsqlListQueriesRequest;
import org.elasticsearch.xpack.esql.action.EsqlQueryAction;
import org.elasticsearch.xpack.esql.core.async.AsyncTaskManagementService;

import java.util.List;

import static org.elasticsearch.xpack.core.ClientHelper.ESQL_ORIGIN;

public class TransportEsqlListQueriesAction extends HandledTransportAction<EsqlListQueriesRequest, EsqlListQueriesResponse> {
    private final NodeClient nodeClient;

    @Inject
    public TransportEsqlListQueriesAction(TransportService transportService, NodeClient nodeClient, ActionFilters actionFilters) {
        super(
            EsqlListQueriesAction.NAME,
            transportService,
            actionFilters,
            EsqlListQueriesRequest::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.nodeClient = nodeClient;
    }

    @Override
    protected void doExecute(Task task, EsqlListQueriesRequest request, ActionListener<EsqlListQueriesResponse> listener) {
        ClientHelper.executeAsyncWithOrigin(
            nodeClient,
            ESQL_ORIGIN,
            TransportListTasksAction.TYPE,
            new ListTasksRequest().setActions(EsqlQueryAction.NAME, EsqlQueryAction.NAME + AsyncTaskManagementService.ASYNC_ACTION_SUFFIX)
                .setDetailed(true),
            new ActionListener<>() {
                @Override
                public void onResponse(ListTasksResponse response) {
                    List<EsqlListQueriesResponse.Query> queries = response.getTasks()
                        .stream()
                        .map(TransportEsqlListQueriesAction::toQuery)
                        .toList();
                    listener.onResponse(new EsqlListQueriesResponse(queries));
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }
            }
        );
    }

    private static EsqlListQueriesResponse.Query toQuery(TaskInfo taskInfo) {
        return new EsqlListQueriesResponse.Query(
            taskInfo.taskId(),
            taskInfo.startTime(),
            taskInfo.runningTimeNanos(),
            taskInfo.description()
        );
    }
}
