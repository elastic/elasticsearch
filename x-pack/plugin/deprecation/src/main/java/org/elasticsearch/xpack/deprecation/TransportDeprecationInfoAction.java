/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.deprecation;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsRequest;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeReadAction;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.deprecation.DeprecationInfoAction;

public class TransportDeprecationInfoAction extends TransportMasterNodeReadAction<DeprecationInfoAction.Request,
        DeprecationInfoAction.Response> {

    private final XPackLicenseState licenseState;
    private final NodeClient client;
    private final IndexNameExpressionResolver indexNameExpressionResolver;

    @Inject
    public TransportDeprecationInfoAction(TransportService transportService, ClusterService clusterService,
                                          ThreadPool threadPool, ActionFilters actionFilters,
                                          IndexNameExpressionResolver indexNameExpressionResolver,
                                          XPackLicenseState licenseState, NodeClient client) {
        super(DeprecationInfoAction.NAME, transportService, clusterService, threadPool, actionFilters,
            DeprecationInfoAction.Request::new, indexNameExpressionResolver);
        this.licenseState = licenseState;
        this.client = client;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.GENERIC;
    }

    @Override
    protected DeprecationInfoAction.Response newResponse() {
        return new DeprecationInfoAction.Response();
    }

    @Override
    protected ClusterBlockException checkBlock(DeprecationInfoAction.Request request, ClusterState state) {
        // Cluster is not affected but we look up repositories in metadata
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }

    @Override
    protected final void masterOperation(final DeprecationInfoAction.Request request, ClusterState state,
                                         final ActionListener<DeprecationInfoAction.Response> listener) {
        if (licenseState.isDeprecationAllowed()) {
            NodesInfoRequest nodesInfoRequest = new NodesInfoRequest("_local").settings(true).plugins(true);
            NodesStatsRequest nodesStatsRequest = new NodesStatsRequest("_local").fs(true);

            final ThreadContext threadContext = client.threadPool().getThreadContext();
            ClientHelper.executeAsyncWithOrigin(threadContext, ClientHelper.DEPRECATION_ORIGIN, nodesInfoRequest,
                    ActionListener.<NodesInfoResponse>wrap(
                    nodesInfoResponse -> {
                        if (nodesInfoResponse.hasFailures()) {
                            throw nodesInfoResponse.failures().get(0);
                        }
                        ClientHelper.executeAsyncWithOrigin(threadContext, ClientHelper.DEPRECATION_ORIGIN, nodesStatsRequest,
                                ActionListener.<NodesStatsResponse>wrap(
                                    nodesStatsResponse -> {
                                        if (nodesStatsResponse.hasFailures()) {
                                            throw nodesStatsResponse.failures().get(0);
                                        }
                                        listener.onResponse(DeprecationInfoAction.Response.from(nodesInfoResponse.getNodes(),
                                                nodesStatsResponse.getNodes(), state, indexNameExpressionResolver,
                                                request.indices(), request.indicesOptions(),
                                                DeprecationChecks.CLUSTER_SETTINGS_CHECKS, DeprecationChecks.NODE_SETTINGS_CHECKS,
                                                DeprecationChecks.INDEX_SETTINGS_CHECKS));
                                    }, listener::onFailure),
                                client.admin().cluster()::nodesStats);
                    }, listener::onFailure), client.admin().cluster()::nodesInfo);
        } else {
            listener.onFailure(LicenseUtils.newComplianceException(XPackField.DEPRECATION));
        }
    }
}
