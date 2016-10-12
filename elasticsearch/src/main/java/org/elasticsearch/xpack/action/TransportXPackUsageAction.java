/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.XPackFeatureSet;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class TransportXPackUsageAction extends TransportMasterNodeAction<XPackUsageRequest, XPackUsageResponse> {

    private final Set<XPackFeatureSet> featureSets;

    @Inject
    public TransportXPackUsageAction(Settings settings, ThreadPool threadPool, TransportService transportService,
                                     ClusterService clusterService, ActionFilters actionFilters,
                                     IndexNameExpressionResolver indexNameExpressionResolver, Set<XPackFeatureSet> featureSets) {
        super(settings, XPackUsageAction.NAME, transportService, clusterService, threadPool, actionFilters, indexNameExpressionResolver,
                XPackUsageRequest::new);
        this.featureSets = featureSets;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.MANAGEMENT;
    }

    @Override
    protected XPackUsageResponse newResponse() {
        return new XPackUsageResponse();
    }

    @Override
    protected void masterOperation(XPackUsageRequest request, ClusterState state, ActionListener<XPackUsageResponse> listener)
            throws Exception {
        List<XPackFeatureSet.Usage> usages = featureSets.stream().map(XPackFeatureSet::usage).collect(Collectors.toList());
        listener.onResponse(new XPackUsageResponse(usages));
    }

    @Override
    protected ClusterBlockException checkBlock(XPackUsageRequest request, ClusterState state) {
        return null;
    }
}
