/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
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
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ilm.IndexLifecycleMetadata;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicyMetadata;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicyUtils;
import org.elasticsearch.xpack.core.ilm.action.GetLifecycleAction;
import org.elasticsearch.xpack.core.ilm.action.GetLifecycleAction.LifecyclePolicyResponseItem;
import org.elasticsearch.xpack.core.ilm.action.GetLifecycleAction.Request;
import org.elasticsearch.xpack.core.ilm.action.GetLifecycleAction.Response;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class TransportGetLifecycleAction extends TransportMasterNodeAction<Request, Response> {

    @Inject
    public TransportGetLifecycleAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            GetLifecycleAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            Request::new,
            indexNameExpressionResolver,
            Response::new,
            ThreadPool.Names.SAME
        );
    }

    @Override
    protected void masterOperation(Task task, Request request, ClusterState state, ActionListener<Response> listener) {
        IndexLifecycleMetadata metadata = clusterService.state().metadata().custom(IndexLifecycleMetadata.TYPE);
        if (metadata == null) {
            if (request.getPolicyNames().length == 0) {
                listener.onResponse(new Response(Collections.emptyList()));
            } else {
                listener.onFailure(
                    new ResourceNotFoundException("Lifecycle policy not found: {}", Arrays.toString(request.getPolicyNames()))
                );
            }
        } else {
            List<String> names;
            if (request.getPolicyNames().length == 0) {
                names = List.of("*");
            } else {
                names = Arrays.asList(request.getPolicyNames());
            }

            if (names.size() > 1 && names.stream().filter(Regex::isSimpleMatchPattern).count() > 0) {
                throw new IllegalArgumentException(
                    "wildcard only supports a single value, please use comma-separated values or a single wildcard value"
                );
            }

            Map<String, LifecyclePolicyResponseItem> policyResponseItemMap = new LinkedHashMap<>();
            for (String name : names) {
                if (Regex.isSimpleMatchPattern(name)) {
                    for (Map.Entry<String, LifecyclePolicyMetadata> entry : metadata.getPolicyMetadatas().entrySet()) {
                        LifecyclePolicyMetadata policyMetadata = entry.getValue();
                        if (Regex.simpleMatch(name, entry.getKey())) {
                            policyResponseItemMap.put(
                                entry.getKey(),
                                new LifecyclePolicyResponseItem(
                                    policyMetadata.getPolicy(),
                                    policyMetadata.getVersion(),
                                    policyMetadata.getModifiedDateString(),
                                    LifecyclePolicyUtils.calculateUsage(indexNameExpressionResolver, state, policyMetadata.getName())
                                )
                            );
                        }
                    }
                } else {
                    LifecyclePolicyMetadata policyMetadata = metadata.getPolicyMetadatas().get(name);
                    if (policyMetadata == null) {
                        listener.onFailure(new ResourceNotFoundException("Lifecycle policy not found: {}", name));
                        return;
                    }
                    policyResponseItemMap.put(
                        name,
                        new LifecyclePolicyResponseItem(
                            policyMetadata.getPolicy(),
                            policyMetadata.getVersion(),
                            policyMetadata.getModifiedDateString(),
                            LifecyclePolicyUtils.calculateUsage(indexNameExpressionResolver, state, policyMetadata.getName())
                        )
                    );
                }
            }
            List<LifecyclePolicyResponseItem> requestedPolicies = new ArrayList<>(policyResponseItemMap.values());
            listener.onResponse(new Response(requestedPolicies));
        }
    }

    @Override
    protected ClusterBlockException checkBlock(Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
