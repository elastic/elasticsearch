/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.protocol.xpack.XPackUsageRequest;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackFeatureSet;

import java.util.ArrayList;
import java.util.List;

public class TransportXPackUsageAction extends TransportMasterNodeAction<XPackUsageRequest, XPackUsageResponse> {

    private final NodeClient client;
    private final List<ActionType<XPackUsageFeatureResponse>> usageActions;

    @SuppressWarnings("this-escape")
    @Inject
    public TransportXPackUsageAction(
        ThreadPool threadPool,
        TransportService transportService,
        ClusterService clusterService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        NodeClient client
    ) {
        super(
            XPackUsageAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            XPackUsageRequest::new,
            indexNameExpressionResolver,
            XPackUsageResponse::new,
            threadPool.executor(ThreadPool.Names.MANAGEMENT)
        );
        this.client = client;
        this.usageActions = usageActions();
    }

    // overrideable for tests
    protected List<ActionType<XPackUsageFeatureResponse>> usageActions() {
        return XPackUsageFeatureAction.ALL;
    }

    @Override
    protected void masterOperation(Task task, XPackUsageRequest request, ClusterState state, ActionListener<XPackUsageResponse> listener) {
        new ActionRunnable<>(listener) {
            final List<XPackFeatureSet.Usage> responses = new ArrayList<>(usageActions.size());

            @Override
            protected void doRun() {
                if (responses.size() < usageActions().size()) {
                    final var childRequest = new XPackUsageRequest(request.masterNodeTimeout());
                    childRequest.setParentTask(request.getParentTask());
                    client.executeLocally(
                        usageActions.get(responses.size()),
                        childRequest,
                        listener.delegateFailure((delegate, response) -> {
                            responses.add(response.getUsage());
                            run(); // XPackUsageFeatureTransportAction always forks to MANAGEMENT so no risk of stack overflow here
                        })
                    );
                } else {
                    assert responses.size() == usageActions.size() : responses.size() + " vs " + usageActions.size();
                    listener.onResponse(new XPackUsageResponse(responses));
                }
            }
        }.run();
    }

    @Override
    protected ClusterBlockException checkBlock(XPackUsageRequest request, ClusterState state) {
        return null;
    }
}
