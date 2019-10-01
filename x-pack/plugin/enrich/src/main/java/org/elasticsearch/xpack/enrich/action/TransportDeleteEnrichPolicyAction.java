/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.enrich.action;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.ingest.IngestService;
import org.elasticsearch.ingest.PipelineConfiguration;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.enrich.EnrichPolicy;
import org.elasticsearch.xpack.core.enrich.action.DeleteEnrichPolicyAction;
import org.elasticsearch.xpack.enrich.AbstractEnrichProcessor;
import org.elasticsearch.xpack.enrich.EnrichPolicyLocks;
import org.elasticsearch.xpack.enrich.EnrichStore;

import java.util.ArrayList;
import java.util.List;

public class TransportDeleteEnrichPolicyAction extends HandledTransportAction<DeleteEnrichPolicyAction.Request, AcknowledgedResponse> {

    private final EnrichPolicyLocks enrichPolicyLocks;
    private final IngestService ingestService;
    private final Client client;
    // the most lenient we can get in order to not bomb out if no indices are found, which is a valid case
    // where a user creates and deletes a policy before running execute
    private static final IndicesOptions LENIENT_OPTIONS = IndicesOptions.fromOptions(true, true, true, true);
    private final ClusterService clusterService;


    @Inject
    public TransportDeleteEnrichPolicyAction(TransportService transportService,
                                             ActionFilters actionFilters,
                                             ClusterService clusterService,
                                             Client client,
                                             EnrichPolicyLocks enrichPolicyLocks,
                                             IngestService ingestService) {
        super(DeleteEnrichPolicyAction.NAME, transportService, actionFilters, DeleteEnrichPolicyAction.Request::new);
        this.client = client;
        this.enrichPolicyLocks = enrichPolicyLocks;
        this.ingestService = ingestService;
        this.clusterService = clusterService;
    }

    @Override
    protected void doExecute(Task task, DeleteEnrichPolicyAction.Request request, ActionListener<AcknowledgedResponse> listener) {

        EnrichStore.getPolicy(request.getName(), clusterService.state(), client, ActionListener.wrap(
            policy -> {
                if (policy == null) {
                    throw new ResourceNotFoundException("policy [{}] not found", request.getName());
                }

                enrichPolicyLocks.lockPolicy(request.getName());

                List<String> pipelines = getPipelinesForPolicy(request.getName(), clusterService.state());
                if (pipelines.isEmpty() == false) {
                    enrichPolicyLocks.releasePolicy(request.getName());
                    throw new ElasticsearchStatusException("Could not delete policy [{}] because a pipeline is referencing it {}",
                            RestStatus.CONFLICT, request.getName(), pipelines);
                }

                deleteIndicesAndPolicy(request.getName(), ActionListener.wrap(
                    (response) -> {
                        enrichPolicyLocks.releasePolicy(request.getName());
                        listener.onResponse(response);
                    },
                    (exc) -> {
                        enrichPolicyLocks.releasePolicy(request.getName());
                        listener.onFailure(exc);
                    }
                ));
            },
            listener::onFailure));
    }

    private List<String> getPipelinesForPolicy(String policyName, ClusterState state) {
        List<PipelineConfiguration> pipelines = IngestService.getPipelines(state);
        List<String> pipelinesWithProcessors = new ArrayList<>();

        for (PipelineConfiguration pipelineConfiguration : pipelines) {
            List<AbstractEnrichProcessor> enrichProcessors =
                ingestService.getProcessorsInPipeline(pipelineConfiguration.getId(), AbstractEnrichProcessor.class);
            for (AbstractEnrichProcessor processor : enrichProcessors) {
                if (processor.getPolicyName().equals(policyName)) {
                    pipelinesWithProcessors.add(pipelineConfiguration.getId());
                }
            }
        }
        return pipelinesWithProcessors;
    }


    private void deleteIndicesAndPolicy(String name, ActionListener<AcknowledgedResponse> listener) {
        // delete all enrich indices for this policy
        DeleteIndexRequest deleteRequest = new DeleteIndexRequest()
            .indices(EnrichPolicy.getBaseName(name) + "-*")
            .indicesOptions(LENIENT_OPTIONS);

        client.admin().indices().delete(deleteRequest, ActionListener.wrap(
            (response) -> {
                if (response.isAcknowledged() == false) {
                    listener.onFailure(new ElasticsearchStatusException("Could not fetch indices to delete during policy delete of [{}]",
                        RestStatus.INTERNAL_SERVER_ERROR, name));
                } else {
                    deletePolicy(name, listener);
                }
            },
            (error) -> listener.onFailure(error)
        ));
    }

    private void deletePolicy(String name, ActionListener<AcknowledgedResponse> listener) {
        EnrichStore.deletePolicy(name, clusterService, client, e -> {
            if (e == null) {
                listener.onResponse(new AcknowledgedResponse(true));
            } else {
                listener.onFailure(e);
            }
        });
    }
}
