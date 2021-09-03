/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ilm.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.ilm.IndexLifecycleMetadata;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicy;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicyMetadata;
import org.elasticsearch.xpack.core.ilm.Phase;
import org.elasticsearch.xpack.core.ilm.SearchableSnapshotAction;
import org.elasticsearch.xpack.core.ilm.action.PutLifecycleAction;
import org.elasticsearch.xpack.core.ilm.action.PutLifecycleAction.Request;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.core.ilm.PhaseCacheManagement.updateIndicesForPolicy;

/**
 * This class is responsible for bootstrapping {@link IndexLifecycleMetadata} into the cluster-state, as well
 * as adding the desired new policy to be inserted.
 */
public class TransportPutLifecycleAction extends TransportMasterNodeAction<Request, AcknowledgedResponse> {

    private static final Logger logger = LogManager.getLogger(TransportPutLifecycleAction.class);
    private final NamedXContentRegistry xContentRegistry;
    private final Client client;
    private final XPackLicenseState licenseState;

    @Inject
    public TransportPutLifecycleAction(TransportService transportService, ClusterService clusterService, ThreadPool threadPool,
                                       ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                                       NamedXContentRegistry namedXContentRegistry, XPackLicenseState licenseState, Client client) {
        super(PutLifecycleAction.NAME, transportService, clusterService, threadPool, actionFilters, Request::new,
            indexNameExpressionResolver, AcknowledgedResponse::readFrom, ThreadPool.Names.SAME);
        this.xContentRegistry = namedXContentRegistry;
        this.licenseState = licenseState;
        this.client = client;
    }

    @Override
    protected void masterOperation(Task task, Request request, ClusterState state, ActionListener<AcknowledgedResponse> listener) {
        // headers from the thread context stored by the AuthenticationService to be shared between the
        // REST layer and the Transport layer here must be accessed within this thread and not in the
        // cluster state thread in the ClusterStateUpdateTask below since that thread does not share the
        // same context, and therefore does not have access to the appropriate security headers.
        Map<String, String> filteredHeaders = ClientHelper.filterSecurityHeaders(threadPool.getThreadContext().getHeaders());
        LifecyclePolicy.validatePolicyName(request.getPolicy().getName());
        List<Phase> phasesDefiningSearchableSnapshot = request.getPolicy().getPhases().values().stream()
            .filter(phase -> phase.getActions().containsKey(SearchableSnapshotAction.NAME))
            .collect(Collectors.toList());
        if (phasesDefiningSearchableSnapshot.isEmpty() == false) {
            if (licenseState.isAllowed(XPackLicenseState.Feature.SEARCHABLE_SNAPSHOTS) == false) {
                throw new IllegalArgumentException("policy [" + request.getPolicy().getName() + "] defines the [" +
                    SearchableSnapshotAction.NAME + "] action but the current license is non-compliant for [searchable-snapshots]");
            }
        }
        clusterService.submitStateUpdateTask("put-lifecycle-" + request.getPolicy().getName(),
                new AckedClusterStateUpdateTask(request, listener) {
                    @Override
                    public ClusterState execute(ClusterState currentState) throws Exception {
                        ClusterState.Builder stateBuilder = ClusterState.builder(currentState);
                        IndexLifecycleMetadata currentMetadata = currentState.metadata().custom(IndexLifecycleMetadata.TYPE);
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
                        stateBuilder.metadata(Metadata.builder(currentState.getMetadata())
                                .putCustom(IndexLifecycleMetadata.TYPE, newMetadata).build());
                        ClusterState nonRefreshedState = stateBuilder.build();
                        if (oldPolicy == null) {
                            return nonRefreshedState;
                        } else {
                            try {
                                return updateIndicesForPolicy(nonRefreshedState, xContentRegistry, client,
                                    oldPolicy.getPolicy(), lifecyclePolicyMetadata, licenseState);
                            } catch (Exception e) {
                                logger.warn(new ParameterizedMessage("unable to refresh indices phase JSON for updated policy [{}]",
                                    oldPolicy.getName()), e);
                                // Revert to the non-refreshed state
                                return nonRefreshedState;
                            }
                        }
                    }
                });
    }

    @Override
    protected ClusterBlockException checkBlock(Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
