/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.deprecation;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeReadAction;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.deprecation.DeprecationInfoAction;
import org.elasticsearch.xpack.core.deprecation.NodesDeprecationCheckAction;
import org.elasticsearch.xpack.core.deprecation.NodesDeprecationCheckRequest;
import org.elasticsearch.xpack.core.ml.action.GetDatafeedsAction;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.deprecation.DeprecationChecks.CLUSTER_SETTINGS_CHECKS;
import static org.elasticsearch.xpack.deprecation.DeprecationChecks.INDEX_SETTINGS_CHECKS;
import static org.elasticsearch.xpack.deprecation.DeprecationChecks.ML_SETTINGS_CHECKS;

public class TransportDeprecationInfoAction extends TransportMasterNodeReadAction<DeprecationInfoAction.Request,
        DeprecationInfoAction.Response> {
    private static final Logger logger = LogManager.getLogger(TransportDeprecationInfoAction.class);

    private final XPackLicenseState licenseState;
    private final NodeClient client;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final Settings settings;
    private final NamedXContentRegistry xContentRegistry;

    @Inject
    public TransportDeprecationInfoAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                          ThreadPool threadPool, ActionFilters actionFilters,
                                          IndexNameExpressionResolver indexNameExpressionResolver,
                                          XPackLicenseState licenseState, NodeClient client, NamedXContentRegistry xContentRegistry) {
        super(DeprecationInfoAction.NAME, transportService, clusterService, threadPool, actionFilters,
            DeprecationInfoAction.Request::new, indexNameExpressionResolver);
        this.licenseState = licenseState;
        this.client = client;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.settings = settings;
        this.xContentRegistry = xContentRegistry;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.GENERIC;
    }

    @Override
    protected DeprecationInfoAction.Response read(StreamInput in) throws IOException {
        return new DeprecationInfoAction.Response(in);
    }

    @Override
    protected ClusterBlockException checkBlock(DeprecationInfoAction.Request request, ClusterState state) {
        // Cluster is not affected but we look up repositories in metadata
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }

    @Override
    protected final void masterOperation(Task task, final DeprecationInfoAction.Request request, ClusterState state,
                                         final ActionListener<DeprecationInfoAction.Response> listener) {
        if (licenseState.isAllowed(XPackLicenseState.Feature.DEPRECATION)) {

            NodesDeprecationCheckRequest nodeDepReq = new NodesDeprecationCheckRequest("_all");
            ClientHelper.executeAsyncWithOrigin(client, ClientHelper.DEPRECATION_ORIGIN,
                NodesDeprecationCheckAction.INSTANCE, nodeDepReq,
                ActionListener.wrap(response -> {
                if (response.hasFailures()) {
                    List<String> failedNodeIds = response.failures().stream()
                        .map(failure -> failure.nodeId() + ": " + failure.getMessage())
                        .collect(Collectors.toList());
                    logger.warn("nodes failed to run deprecation checks: {}", failedNodeIds);
                    for (FailedNodeException failure : response.failures()) {
                        logger.debug("node {} failed to run deprecation checks: {}", failure.nodeId(), failure);
                    }
                }
                getDatafeedConfigs(ActionListener.wrap(
                    datafeeds -> {
                        listener.onResponse(
                            DeprecationInfoAction.Response.from(state, xContentRegistry, indexNameExpressionResolver,
                                request.indices(), request.indicesOptions(), datafeeds,
                                response, INDEX_SETTINGS_CHECKS, CLUSTER_SETTINGS_CHECKS,
                                ML_SETTINGS_CHECKS));
                    },
                    listener::onFailure
                ));

            }, listener::onFailure));
        } else {
            listener.onFailure(LicenseUtils.newComplianceException(XPackField.DEPRECATION));
        }
    }

    private void getDatafeedConfigs(ActionListener<List<DatafeedConfig>> listener) {
        if (XPackSettings.MACHINE_LEARNING_ENABLED.get(settings) == false) {
            listener.onResponse(Collections.emptyList());
        } else {
            ClientHelper.executeAsyncWithOrigin(client, ClientHelper.DEPRECATION_ORIGIN, GetDatafeedsAction.INSTANCE,
                    new GetDatafeedsAction.Request(GetDatafeedsAction.ALL), ActionListener.wrap(
                            datafeedsResponse -> listener.onResponse(datafeedsResponse.getResponse().results()),
                            listener::onFailure
                    ));
        }
    }
}
