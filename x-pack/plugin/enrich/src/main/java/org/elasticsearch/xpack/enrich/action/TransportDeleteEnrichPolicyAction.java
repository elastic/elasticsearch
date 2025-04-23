/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.enrich.action;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.AcknowledgedTransportMasterNodeProjectAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.ingest.IngestService;
import org.elasticsearch.ingest.PipelineConfiguration;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.enrich.EnrichPolicy;
import org.elasticsearch.xpack.core.enrich.action.DeleteEnrichPolicyAction;
import org.elasticsearch.xpack.enrich.AbstractEnrichProcessor;
import org.elasticsearch.xpack.enrich.EnrichPolicyLocks;
import org.elasticsearch.xpack.enrich.EnrichPolicyLocks.EnrichPolicyLock;
import org.elasticsearch.xpack.enrich.EnrichStore;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static org.elasticsearch.xpack.core.ClientHelper.ENRICH_ORIGIN;

public class TransportDeleteEnrichPolicyAction extends AcknowledgedTransportMasterNodeProjectAction<DeleteEnrichPolicyAction.Request> {

    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final EnrichPolicyLocks enrichPolicyLocks;
    private final IngestService ingestService;
    private final Client client;
    // the most lenient we can get in order to not bomb out if no indices are found, which is a valid case
    // where a user creates and deletes a policy before running execute
    private static final IndicesOptions LENIENT_OPTIONS = IndicesOptions.fromOptions(true, true, true, true);

    @Inject
    public TransportDeleteEnrichPolicyAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        ProjectResolver projectResolver,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Client client,
        EnrichPolicyLocks enrichPolicyLocks,
        IngestService ingestService
    ) {
        super(
            DeleteEnrichPolicyAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            DeleteEnrichPolicyAction.Request::new,
            projectResolver,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.client = client;
        this.enrichPolicyLocks = enrichPolicyLocks;
        this.ingestService = ingestService;
    }

    @Override
    protected void masterOperation(
        Task task,
        DeleteEnrichPolicyAction.Request request,
        ProjectState state,
        ActionListener<AcknowledgedResponse> listener
    ) {
        final String policyName = request.getName();
        final EnrichPolicy policy = EnrichStore.getPolicy(policyName, state.metadata()); // ensure the policy exists first
        if (policy == null) {
            throw new ResourceNotFoundException("policy [{}] not found", policyName);
        }

        EnrichPolicyLock policyLock = enrichPolicyLocks.lockPolicy(policyName);
        try {
            final List<PipelineConfiguration> pipelines = IngestService.getPipelines(state.metadata());
            final List<String> pipelinesWithProcessors = new ArrayList<>();

            for (PipelineConfiguration pipelineConfiguration : pipelines) {
                List<AbstractEnrichProcessor> enrichProcessors = ingestService.getProcessorsInPipeline(
                    state.projectId(),
                    pipelineConfiguration.getId(),
                    AbstractEnrichProcessor.class
                );
                for (AbstractEnrichProcessor processor : enrichProcessors) {
                    if (processor.getPolicyName().equals(policyName)) {
                        pipelinesWithProcessors.add(pipelineConfiguration.getId());
                    }
                }
            }

            if (pipelinesWithProcessors.isEmpty() == false) {
                throw new ElasticsearchStatusException(
                    "Could not delete policy [{}] because a pipeline is referencing it {}",
                    RestStatus.CONFLICT,
                    policyName,
                    pipelinesWithProcessors
                );
            }
        } catch (Exception e) {
            policyLock.close();
            listener.onFailure(e);
            return;
        }

        try {
            final GetIndexRequest indices = new GetIndexRequest(request.masterNodeTimeout()).indices(
                EnrichPolicy.getBaseName(policyName) + "-*"
            ).indicesOptions(IndicesOptions.lenientExpand());

            String[] concreteIndices = indexNameExpressionResolver.concreteIndexNamesWithSystemIndexAccess(state.metadata(), indices);

            // the wildcard expansion could be too wide (e.g. in the case of a policy named policy-1 and another named policy-10),
            // so we need to filter down to just the concrete indices that are actually indices for this policy
            concreteIndices = Stream.of(concreteIndices).filter(i -> EnrichPolicy.isPolicyForIndex(policyName, i)).toArray(String[]::new);

            deleteIndicesAndPolicy(state.projectId(), concreteIndices, policyName, ActionListener.runBefore(listener, policyLock::close));
        } catch (Exception e) {
            policyLock.close();
            listener.onFailure(e);
        }
    }

    private void deleteIndicesAndPolicy(ProjectId projectId, String[] indices, String name, ActionListener<AcknowledgedResponse> listener) {
        if (indices.length == 0) {
            deletePolicy(projectId, name, listener);
            return;
        }

        // delete all enrich indices for this policy, we delete concrete indices here but not a wildcard index expression
        // as the setting 'action.destructive_requires_name' may be set to true
        DeleteIndexRequest deleteRequest = new DeleteIndexRequest().indices(indices).indicesOptions(LENIENT_OPTIONS);

        new OriginSettingClient(client, ENRICH_ORIGIN).admin()
            .indices()
            .delete(deleteRequest, listener.delegateFailureAndWrap((delegate, response) -> {
                if (response.isAcknowledged() == false) {
                    delegate.onFailure(
                        new ElasticsearchStatusException(
                            "Could not fetch indices to delete during policy delete of [{}]",
                            RestStatus.INTERNAL_SERVER_ERROR,
                            name
                        )
                    );
                } else {
                    deletePolicy(projectId, name, delegate);
                }
            }));
    }

    private void deletePolicy(ProjectId projectId, String name, ActionListener<AcknowledgedResponse> listener) {
        EnrichStore.deletePolicy(projectId, name, clusterService, e -> {
            if (e == null) {
                listener.onResponse(AcknowledgedResponse.TRUE);
            } else {
                listener.onFailure(e);
            }
        });
    }

    @Override
    protected ClusterBlockException checkBlock(DeleteEnrichPolicyAction.Request request, ProjectState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
