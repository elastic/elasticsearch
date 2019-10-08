/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ilm.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.ilm.IndexLifecycleMetadata;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicy;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicyMetadata;
import org.elasticsearch.xpack.core.ilm.action.PutLifecycleAction;
import org.elasticsearch.xpack.core.ilm.action.PutLifecycleAction.Request;
import org.elasticsearch.xpack.core.ilm.action.PutLifecycleAction.Response;

import java.io.IOException;
import java.time.Instant;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * This class is responsible for bootstrapping {@link IndexLifecycleMetadata} into the cluster-state, as well
 * as adding the desired new policy to be inserted.
 */
public class TransportPutLifecycleAction extends TransportMasterNodeAction<Request, Response> {

    private static final Logger logger = LogManager.getLogger(TransportPutLifecycleAction.class);

    @Inject
    public TransportPutLifecycleAction(TransportService transportService, ClusterService clusterService, ThreadPool threadPool,
                                       ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver) {
        super(PutLifecycleAction.NAME, transportService, clusterService, threadPool, actionFilters, Request::new,
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
        // headers from the thread context stored by the AuthenticationService to be shared between the
        // REST layer and the Transport layer here must be accessed within this thread and not in the
        // cluster state thread in the ClusterStateUpdateTask below since that thread does not share the
        // same context, and therefore does not have access to the appropriate security headers.
        Map<String, String> filteredHeaders = threadPool.getThreadContext().getHeaders().entrySet().stream()
            .filter(e -> ClientHelper.SECURITY_HEADER_FILTERS.contains(e.getKey()))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        LifecyclePolicy.validatePolicyName(request.getPolicy().getName());
        clusterService.submitStateUpdateTask("put-lifecycle-" + request.getPolicy().getName(),
                new AckedClusterStateUpdateTask<Response>(request, listener) {
                    @Override
                    protected Response newResponse(boolean acknowledged) {
                        return new Response(acknowledged);
                    }

                    @Override
                    public ClusterState execute(ClusterState currentState) throws Exception {
                        ClusterState.Builder newState = ClusterState.builder(currentState);
                        IndexLifecycleMetadata currentMetadata = currentState.metaData().custom(IndexLifecycleMetadata.TYPE);
                        if (currentMetadata == null) { // first time using index-lifecycle feature, bootstrap metadata
                            currentMetadata = IndexLifecycleMetadata.EMPTY;
                        }
                        LifecyclePolicyMetadata existingPolicyMetadata = currentMetadata.getPolicyMetadatas()
                            .get(request.getPolicy().getName());
                        long nextVersion = (existingPolicyMetadata == null) ? 1L : existingPolicyMetadata.getVersion() + 1L;
                        SortedMap<String, LifecyclePolicyMetadata> newPolicies = new TreeMap<>(currentMetadata.getPolicyMetadatas());
                        LifecyclePolicyMetadata lifecyclePolicyMetadata = new LifecyclePolicyMetadata(request.getPolicy(), filteredHeaders,
                            nextVersion, Instant.now().toEpochMilli());
                        LifecyclePolicyMetadata oldPolicy = newPolicies.put(lifecyclePolicyMetadata.getName(), lifecyclePolicyMetadata);
                        if (oldPolicy == null) {
                            logger.info("adding index lifecycle policy [{}]", request.getPolicy().getName());
                        } else {
                            logger.info("updating index lifecycle policy [{}]", request.getPolicy().getName());
                        }
                        IndexLifecycleMetadata newMetadata = new IndexLifecycleMetadata(newPolicies, currentMetadata.getOperationMode());
                        newState.metaData(MetaData.builder(currentState.getMetaData())
                                .putCustom(IndexLifecycleMetadata.TYPE, newMetadata).build());
                        return newState.build();
                    }
                });
    }

    @Override
    protected ClusterBlockException checkBlock(Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
