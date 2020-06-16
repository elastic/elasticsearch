/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ilm.action;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ilm.IndexLifecycleMetadata;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicyMetadata;
import org.elasticsearch.xpack.core.ilm.action.GetLifecycleAction;
import org.elasticsearch.xpack.core.ilm.action.GetLifecycleAction.LifecyclePolicyResponseItem;
import org.elasticsearch.xpack.core.ilm.action.GetLifecycleAction.Request;
import org.elasticsearch.xpack.core.ilm.action.GetLifecycleAction.Response;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class TransportGetLifecycleAction extends TransportMasterNodeAction<Request, Response> {

    @Inject
    public TransportGetLifecycleAction(TransportService transportService, ClusterService clusterService, ThreadPool threadPool,
                                       ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver) {
        super(GetLifecycleAction.NAME, transportService, clusterService, threadPool, actionFilters, Request::new,
            indexNameExpressionResolver);
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected Response read(StreamInput in) throws IOException {
        return new Response(in);
    }

    @Override
    protected void masterOperation(Task task, Request request, ClusterState state, ActionListener<Response> listener) {
        IndexLifecycleMetadata metadata = clusterService.state().metadata().custom(IndexLifecycleMetadata.TYPE);
        if (metadata == null) {
            if (request.getPolicyNames().length == 0) {
                listener.onResponse(new Response(Collections.emptyList()));
            } else {
                listener.onFailure(new ResourceNotFoundException("Lifecycle policy not found: {}",
                    Arrays.toString(request.getPolicyNames())));
            }
        } else {
            List<LifecyclePolicyResponseItem> requestedPolicies;
            // if no policies explicitly provided, behave as if `*` was specified
            if (request.getPolicyNames().length == 0) {
                requestedPolicies = new ArrayList<>(metadata.getPolicyMetadatas().size());
                for (LifecyclePolicyMetadata policyMetadata : metadata.getPolicyMetadatas().values()) {
                    requestedPolicies.add(new LifecyclePolicyResponseItem(policyMetadata.getPolicy(),
                        policyMetadata.getVersion(), policyMetadata.getModifiedDateString()));
                }
            } else {
                requestedPolicies = new ArrayList<>(request.getPolicyNames().length);
                for (String name : request.getPolicyNames()) {
                    LifecyclePolicyMetadata policyMetadata = metadata.getPolicyMetadatas().get(name);
                    if (policyMetadata == null) {
                        listener.onFailure(new ResourceNotFoundException("Lifecycle policy not found: {}", name));
                        return;
                    }
                    requestedPolicies.add(new LifecyclePolicyResponseItem(policyMetadata.getPolicy(),
                        policyMetadata.getVersion(), policyMetadata.getModifiedDateString()));
                }
            }
            listener.onResponse(new Response(requestedPolicies));
        }
    }

    @Override
    protected ClusterBlockException checkBlock(Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
