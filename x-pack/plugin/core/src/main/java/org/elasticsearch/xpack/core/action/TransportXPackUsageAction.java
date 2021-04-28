/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.protocol.xpack.XPackUsageRequest;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackFeatureSet;
import org.elasticsearch.xpack.core.XPackFeatureSet.Usage;
import org.elasticsearch.xpack.core.common.IteratingActionListener;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.BiConsumer;

public class TransportXPackUsageAction extends TransportMasterNodeAction<XPackUsageRequest, XPackUsageResponse> {

    private final NodeClient client;
    private final List<XPackUsageFeatureAction> usageActions;

    @Inject
    public TransportXPackUsageAction(ThreadPool threadPool, TransportService transportService,
                                     ClusterService clusterService, ActionFilters actionFilters,
                                     IndexNameExpressionResolver indexNameExpressionResolver, NodeClient client) {
        super(XPackUsageAction.NAME, transportService, clusterService, threadPool, actionFilters, XPackUsageRequest::new,
            indexNameExpressionResolver, XPackUsageResponse::new, ThreadPool.Names.MANAGEMENT);
        this.client = client;
        this.usageActions = usageActions();
    }

    // overrideable for tests
    protected List<XPackUsageFeatureAction> usageActions() {
        return XPackUsageFeatureAction.ALL;
    }

    @Override
    protected void masterOperation(Task task, XPackUsageRequest request, ClusterState state, ActionListener<XPackUsageResponse> listener) {
        final ActionListener<List<XPackFeatureSet.Usage>> usageActionListener =
                listener.delegateFailure((l, usages) -> l.onResponse(new XPackUsageResponse(usages)));
        final AtomicReferenceArray<Usage> featureSetUsages = new AtomicReferenceArray<>(usageActions.size());
        final AtomicInteger position = new AtomicInteger(0);
        final BiConsumer<XPackUsageFeatureAction, ActionListener<List<Usage>>> consumer = (featureUsageAction, iteratingListener) -> {
            // Since we're executing the actions locally we should create a new request
            // to avoid mutating the original request and setting the wrong parent task,
            // since it is possible that the parent task gets cancelled and new child tasks are banned.
            final XPackUsageRequest childRequest = new XPackUsageRequest();
            childRequest.setParentTask(request.getParentTask());
            client.executeLocally(featureUsageAction, childRequest, iteratingListener.delegateFailure((l, usageResponse) -> {
                featureSetUsages.set(position.getAndIncrement(), usageResponse.getUsage());
                // the value sent back doesn't matter since our predicate keeps iterating
                l.onResponse(Collections.emptyList());
            }));
        };
        IteratingActionListener<List<XPackFeatureSet.Usage>, XPackUsageFeatureAction> iteratingActionListener =
                new IteratingActionListener<>(usageActionListener, consumer, usageActions,
                        threadPool.getThreadContext(), (ignore) -> {
                    final List<Usage> usageList = new ArrayList<>(featureSetUsages.length());
                    for (int i = 0; i < featureSetUsages.length(); i++) {
                        usageList.add(featureSetUsages.get(i));
                    }
                    return usageList;
                }, (ignore) -> true);
        iteratingActionListener.run();
    }

    @Override
    protected ClusterBlockException checkBlock(XPackUsageRequest request, ClusterState state) {
        return null;
    }
}
