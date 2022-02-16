/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.protocol.xpack.XPackUsageRequest;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.transport.Transports;
import org.elasticsearch.xpack.core.XPackFeatureSet;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CancellationException;

public class TransportXPackUsageAction extends TransportMasterNodeAction<XPackUsageRequest, XPackUsageResponse> {

    private final List<XPackFeatureSet> featureSets;

    @Inject
    public TransportXPackUsageAction(
        ThreadPool threadPool,
        TransportService transportService,
        ClusterService clusterService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Set<XPackFeatureSet> featureSets
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
            ThreadPool.Names.MANAGEMENT
        );
        this.featureSets = Collections.unmodifiableList(new ArrayList<>(featureSets));
    }

    @Override
    protected void masterOperation(
        final XPackUsageRequest request,
        final ClusterState state,
        final ActionListener<XPackUsageResponse> listener
    ) {
        throw new UnsupportedOperationException("The task parameter is required");
    }

    @Override
    protected void masterOperation(Task task, XPackUsageRequest request, ClusterState state, ActionListener<XPackUsageResponse> listener) {
        new ActionRunnable<XPackUsageResponse>(listener) {
            final List<XPackFeatureSet.Usage> responses = new ArrayList<>(featureSets.size());

            @Override
            protected void doRun() throws Exception {
                if (responses.size() < featureSets.size()) {
                    assert Transports.assertNotTransportThread(
                        "calculating usage can be more expensive than we allow on transport threads"
                    );
                    if (task instanceof CancellableTask && ((CancellableTask) task).isCancelled()) {
                        throw new CancellationException("Task cancelled");
                    }

                    featureSets.get(responses.size()).usage(listener.delegateFailure((l, usage) -> {
                        responses.add(usage);
                        threadPool.executor(ThreadPool.Names.MANAGEMENT).execute(this);
                    }));
                } else {
                    assert responses.size() == featureSets.size() : responses.size() + " vs " + featureSets.size();
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
