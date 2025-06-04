/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ilm.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ChannelActionListener;
import org.elasticsearch.action.support.local.TransportLocalProjectMetadataAction;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.core.UpdateForV10;
import org.elasticsearch.exception.ResourceNotFoundException;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ilm.IndexLifecycleMetadata;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicyMetadata;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicyUsageCalculator;
import org.elasticsearch.xpack.core.ilm.action.GetLifecycleAction;
import org.elasticsearch.xpack.core.ilm.action.GetLifecycleAction.LifecyclePolicyResponseItem;
import org.elasticsearch.xpack.core.ilm.action.GetLifecycleAction.Request;
import org.elasticsearch.xpack.core.ilm.action.GetLifecycleAction.Response;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class TransportGetLifecycleAction extends TransportLocalProjectMetadataAction<Request, Response> {

    private final IndexNameExpressionResolver indexNameExpressionResolver;

    /**
     * NB prior to 9.1 this was a TransportMasterNodeAction so for BwC it must be registered with the TransportService until
     * we no longer need to support calling this action remotely.
     */
    @UpdateForV10(owner = UpdateForV10.Owner.DATA_MANAGEMENT)
    @SuppressWarnings("this-escape")
    @Inject
    public TransportGetLifecycleAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        ProjectResolver projectResolver,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            GetLifecycleAction.NAME,
            actionFilters,
            transportService.getTaskManager(),
            clusterService,
            threadPool.executor(ThreadPool.Names.MANAGEMENT),
            projectResolver
        );
        this.indexNameExpressionResolver = indexNameExpressionResolver;

        transportService.registerRequestHandler(
            actionName,
            executor,
            false,
            true,
            GetLifecycleAction.Request::new,
            (request, channel, task) -> executeDirect(task, request, new ChannelActionListener<>(channel))
        );
    }

    @Override
    protected void localClusterStateOperation(Task task, Request request, ProjectState state, ActionListener<Response> listener) {
        assert task instanceof CancellableTask : "get lifecycle requests should be cancellable";
        final CancellableTask cancellableTask = (CancellableTask) task;
        if (cancellableTask.notifyIfCancelled(listener)) {
            return;
        }
        var project = state.metadata();

        IndexLifecycleMetadata metadata = project.custom(IndexLifecycleMetadata.TYPE);
        if (metadata == null) {
            if (request.getPolicyNames().length == 0) {
                listener.onResponse(new Response(List.of()));
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

            if (names.size() > 1 && names.stream().anyMatch(Regex::isSimpleMatchPattern)) {
                throw new IllegalArgumentException(
                    "wildcard only supports a single value, please use comma-separated values or a single wildcard value"
                );
            }

            var lifecyclePolicyUsageCalculator = new LifecyclePolicyUsageCalculator(indexNameExpressionResolver, project, names);
            Map<String, LifecyclePolicyResponseItem> policyResponseItemMap = new LinkedHashMap<>();
            for (String name : names) {
                if (Regex.isSimpleMatchPattern(name)) {
                    for (Map.Entry<String, LifecyclePolicyMetadata> entry : metadata.getPolicyMetadatas().entrySet()) {
                        if (cancellableTask.notifyIfCancelled(listener)) {
                            return;
                        }
                        LifecyclePolicyMetadata policyMetadata = entry.getValue();
                        if (Regex.simpleMatch(name, entry.getKey())) {
                            policyResponseItemMap.put(
                                entry.getKey(),
                                new LifecyclePolicyResponseItem(
                                    policyMetadata.getPolicy(),
                                    policyMetadata.getVersion(),
                                    policyMetadata.getModifiedDateString(),
                                    lifecyclePolicyUsageCalculator.retrieveCalculatedUsage(policyMetadata.getName())
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
                            lifecyclePolicyUsageCalculator.retrieveCalculatedUsage(policyMetadata.getName())
                        )
                    );
                }
            }
            List<LifecyclePolicyResponseItem> requestedPolicies = new ArrayList<>(policyResponseItemMap.values());
            listener.onResponse(new Response(requestedPolicies));
        }
    }

    @Override
    protected ClusterBlockException checkBlock(Request request, ProjectState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
