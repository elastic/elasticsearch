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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.deprecation.DeprecationInfoAction;
import org.elasticsearch.xpack.core.ml.action.GetDatafeedsAction;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;

import java.util.Collections;
import java.util.List;

import static org.elasticsearch.xpack.core.ClientHelper.DEPRECATION_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;
import static org.elasticsearch.xpack.deprecation.DeprecationChecks.CLUSTER_SETTINGS_CHECKS;
import static org.elasticsearch.xpack.deprecation.DeprecationChecks.INDEX_SETTINGS_CHECKS;
import static org.elasticsearch.xpack.deprecation.DeprecationChecks.NODE_SETTINGS_CHECKS;
import static org.elasticsearch.xpack.deprecation.DeprecationChecks.ML_SETTINGS_CHECKS;

public class TransportDeprecationInfoAction extends TransportMasterNodeReadAction<DeprecationInfoAction.Request,
        DeprecationInfoAction.Response> {

    private final XPackLicenseState licenseState;
    private final NodeClient client;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final Settings settings;

    @Inject
    public TransportDeprecationInfoAction(Settings settings, TransportService transportService,
                                          ClusterService clusterService, ThreadPool threadPool,
                                          ActionFilters actionFilters,
                                          IndexNameExpressionResolver indexNameExpressionResolver,
                                          XPackLicenseState licenseState, NodeClient client) {
        super(settings, DeprecationInfoAction.NAME, transportService, clusterService, threadPool, actionFilters,
                indexNameExpressionResolver, DeprecationInfoAction.Request::new);
        this.licenseState = licenseState;
        this.client = client;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.settings = settings;
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
            executeAsyncWithOrigin(threadContext, DEPRECATION_ORIGIN, nodesInfoRequest,
                    ActionListener.<NodesInfoResponse>wrap(
                    nodesInfoResponse -> {
                        if (nodesInfoResponse.hasFailures()) {
                            throw nodesInfoResponse.failures().get(0);
                        }
                        executeAsyncWithOrigin(threadContext, DEPRECATION_ORIGIN, nodesStatsRequest,
                                ActionListener.<NodesStatsResponse>wrap(
                                        nodesStatsResponse -> {
                                            if (nodesStatsResponse.hasFailures()) {
                                                throw nodesStatsResponse.failures().get(0);
                                            }
                                            getDatafeedConfigs(ActionListener.wrap(
                                                    datafeeds -> {
                                                        listener.onResponse(
                                                                DeprecationInfoAction.Response.from(nodesInfoResponse.getNodes(),
                                                                        nodesStatsResponse.getNodes(), state, indexNameExpressionResolver,
                                                                        request.indices(), request.indicesOptions(), datafeeds,
                                                                        CLUSTER_SETTINGS_CHECKS, NODE_SETTINGS_CHECKS,
                                                                        INDEX_SETTINGS_CHECKS, ML_SETTINGS_CHECKS));
                                                    },
                                                    listener::onFailure
                                            ));
                                        }, listener::onFailure),
                                client.admin().cluster()::nodesStats);
                    }, listener::onFailure), client.admin().cluster()::nodesInfo);
        } else {
            listener.onFailure(LicenseUtils.newComplianceException(XPackField.DEPRECATION));
        }
    }

    private void getDatafeedConfigs(ActionListener<List<DatafeedConfig>> listener) {
        if (XPackSettings.MACHINE_LEARNING_ENABLED.get(settings) == false) {
            listener.onResponse(Collections.emptyList());
        } else {
            executeAsyncWithOrigin(client, DEPRECATION_ORIGIN, GetDatafeedsAction.INSTANCE,
                    new GetDatafeedsAction.Request(GetDatafeedsAction.ALL), ActionListener.wrap(
                            datafeedsResponse -> listener.onResponse(datafeedsResponse.getResponse().results()),
                            listener::onFailure
                    ));
        }
    }
}
