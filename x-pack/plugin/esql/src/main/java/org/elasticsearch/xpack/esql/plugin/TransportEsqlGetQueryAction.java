/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.tasks.get.GetTaskRequestBuilder;
import org.elasticsearch.action.admin.cluster.node.tasks.get.GetTaskResponse;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksRequestBuilder;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.compute.operator.DriverTaskRunner;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.action.EsqlGetQueryAction;
import org.elasticsearch.xpack.esql.action.EsqlGetQueryRequest;

public class TransportEsqlGetQueryAction extends HandledTransportAction<EsqlGetQueryRequest, EsqlGetQueryResponse> {
    private final NodeClient nodeClient;

    @Inject
    public TransportEsqlGetQueryAction(TransportService transportService, NodeClient nodeClient, ActionFilters actionFilters) {
        super(EsqlGetQueryAction.NAME, transportService, actionFilters, EsqlGetQueryRequest::new, EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.nodeClient = nodeClient;
    }

    @Override
    protected void doExecute(Task task, EsqlGetQueryRequest request, ActionListener<EsqlGetQueryResponse> listener) {
        new GetTaskRequestBuilder(nodeClient).setTaskId(request.id()).execute(new ActionListener<>() {
            @Override
            public void onResponse(GetTaskResponse response) {
                TaskInfo task = response.getTask().getTask();
                new ListTasksRequestBuilder(nodeClient).setDetailed(true)
                    .setActions(DriverTaskRunner.ACTION_NAME)
                    .setTargetParentTaskId(request.id())
                    .execute(new ActionListener<>() {
                        @Override
                        public void onResponse(ListTasksResponse response) {
                            listener.onResponse(new EsqlGetQueryResponse(toDetailedQuery(task, response)));
                        }

                        @Override
                        public void onFailure(Exception e) {
                            listener.onFailure(e);
                        }
                    });
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }
        });
    }

    private static EsqlGetQueryResponse.DetailedQuery toDetailedQuery(TaskInfo task, ListTasksResponse response) {
        return new EsqlGetQueryResponse.DetailedQuery(
            task.taskId(),
            task.startTime(),
            task.runningTimeNanos(),
            task.description(), // Query
            task.node(), // Coordinating node
            response.getTasks().stream().map(TaskInfo::node).distinct().toList() // Data nodes
        );
    }
}
