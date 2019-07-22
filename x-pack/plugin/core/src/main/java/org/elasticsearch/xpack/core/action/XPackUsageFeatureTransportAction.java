/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.action;

import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.protocol.xpack.XPackUsageRequest;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;

public abstract class XPackUsageFeatureTransportAction extends TransportMasterNodeAction<XPackUsageRequest, XPackUsageFeatureResponse> {

    public XPackUsageFeatureTransportAction(String name, TransportService transportService, ClusterService clusterService,
                                            ThreadPool threadPool, ActionFilters actionFilters,
                                            IndexNameExpressionResolver indexNameExpressionResolver) {
        super(name, transportService, clusterService, threadPool,
              actionFilters, XPackUsageRequest::new, indexNameExpressionResolver);
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.MANAGEMENT;
    }

    @Override
    protected XPackUsageFeatureResponse read(StreamInput in) throws IOException {
        return new XPackUsageFeatureResponse(in);
    }

    @Override
    protected ClusterBlockException checkBlock(XPackUsageRequest request, ClusterState state) {
        return null;
    }
}
