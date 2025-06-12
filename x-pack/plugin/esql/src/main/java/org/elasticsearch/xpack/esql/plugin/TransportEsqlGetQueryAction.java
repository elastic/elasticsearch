/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.tasks.get.GetTaskRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.get.GetTaskResponse;
import org.elasticsearch.action.admin.cluster.node.tasks.get.TransportGetTaskAction;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.action.admin.cluster.node.tasks.list.TransportListTasksAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.compute.operator.DriverStatus;
import org.elasticsearch.compute.operator.DriverTaskRunner;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.esql.action.EsqlGetQueryAction;
import org.elasticsearch.xpack.esql.action.EsqlGetQueryRequest;
import org.elasticsearch.xpack.esql.action.EsqlQueryAction;

import java.util.Set;
import java.util.TreeSet;

import static org.elasticsearch.xpack.core.ClientHelper.ESQL_ORIGIN;

public class TransportEsqlGetQueryAction extends HandledTransportAction<EsqlGetQueryRequest, EsqlGetQueryResponse> {
    private final NodeClient nodeClient;

    @Inject
    public TransportEsqlGetQueryAction(TransportService transportService, NodeClient nodeClient, ActionFilters actionFilters) {
        super(EsqlGetQueryAction.NAME, transportService, actionFilters, EsqlGetQueryRequest::new, EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.nodeClient = nodeClient;
    }

    @Override
    protected void doExecute(Task task, EsqlGetQueryRequest request, ActionListener<EsqlGetQueryResponse> listener) {
        ClientHelper.executeAsyncWithOrigin(
            nodeClient,
            ESQL_ORIGIN,
            TransportGetTaskAction.TYPE,
            new GetTaskRequest().setTaskId(request.id()),
            new ActionListener<>() {
                @Override
                public void onResponse(GetTaskResponse response) {
                    TaskInfo task = response.getTask().getTask();
                    if (task.action().startsWith(EsqlQueryAction.NAME) == false) {
                        listener.onFailure(new IllegalArgumentException("Task [" + request.id() + "] is not an ESQL query task"));
                        return;
                    }
                    ClientHelper.executeAsyncWithOrigin(
                        nodeClient,
                        ESQL_ORIGIN,
                        TransportListTasksAction.TYPE,
                        new ListTasksRequest().setDetailed(true)
                            .setActions(DriverTaskRunner.ACTION_NAME)
                            .setTargetParentTaskId(request.id()),
                        new ActionListener<>() {
                            @Override
                            public void onResponse(ListTasksResponse response) {
                                listener.onResponse(new EsqlGetQueryResponse(toDetailedQuery(task, response)));
                            }

                            @Override
                            public void onFailure(Exception e) {
                                listener.onFailure(e);
                            }
                        }
                    );
                }

                @Override
                public void onFailure(Exception e) {
                    // The underlying root cause is meaningless to the user, but that is what will be shown, so we remove it.
                    var withoutCause = new Exception(e.getMessage());
                    listener.onFailure(withoutCause);
                }
            }
        );
    }

    private static EsqlGetQueryResponse.DetailedQuery toDetailedQuery(TaskInfo main, ListTasksResponse sub) {
        String query = main.description();
        String coordinatingNode = main.node();

        // TODO include completed drivers in documentsFound and valuesLoaded
        long documentsFound = 0;
        long valuesLoaded = 0;
        Set<String> dataNodes = new TreeSet<>();
        for (TaskInfo info : sub.getTasks()) {
            DriverStatus status = (DriverStatus) info.status();
            documentsFound += status.documentsFound();
            valuesLoaded += status.valuesLoaded();
            dataNodes.add(info.node());
        }

        return new EsqlGetQueryResponse.DetailedQuery(
            main.taskId(),
            main.startTime(),
            main.runningTimeNanos(),
            documentsFound,
            valuesLoaded,
            query,
            coordinatingNode,
            sub.getTasks().stream().map(TaskInfo::node).distinct().toList() // Data nodes
        );
    }
}
