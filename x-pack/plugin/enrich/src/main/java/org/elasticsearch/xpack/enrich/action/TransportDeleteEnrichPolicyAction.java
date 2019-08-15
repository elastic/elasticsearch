/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.enrich.action;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.ingest.IngestService;
import org.elasticsearch.ingest.PipelineConfiguration;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.enrich.EnrichPolicy;
import org.elasticsearch.xpack.core.enrich.action.DeleteEnrichPolicyAction;
import org.elasticsearch.xpack.enrich.AbstractEnrichProcessor;
import org.elasticsearch.xpack.enrich.EnrichPolicyLocks;
import org.elasticsearch.xpack.enrich.EnrichStore;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TransportDeleteEnrichPolicyAction extends TransportMasterNodeAction<DeleteEnrichPolicyAction.Request, AcknowledgedResponse> {

    private final EnrichPolicyLocks enrichPolicyLocks;
    private final IngestService ingestService;

    @Inject
    public TransportDeleteEnrichPolicyAction(TransportService transportService,
                                             ClusterService clusterService,
                                             ThreadPool threadPool,
                                             ActionFilters actionFilters,
                                             IndexNameExpressionResolver indexNameExpressionResolver,
                                             EnrichPolicyLocks enrichPolicyLocks,
                                             IngestService ingestService) {
        super(DeleteEnrichPolicyAction.NAME, transportService, clusterService, threadPool, actionFilters,
            DeleteEnrichPolicyAction.Request::new, indexNameExpressionResolver);
        this.enrichPolicyLocks = enrichPolicyLocks;
        this.ingestService = ingestService;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    protected AcknowledgedResponse newResponse() {
        throw new UnsupportedOperationException("usage of Streamable is to be replaced by Writeable");
    }

    @Override
    protected AcknowledgedResponse read(StreamInput in) throws IOException {
        return new AcknowledgedResponse(in);
    }

    @Override
    protected void masterOperation(DeleteEnrichPolicyAction.Request request, ClusterState state,
                                   ActionListener<AcknowledgedResponse> listener) throws Exception {
        enrichPolicyLocks.lockPolicy(request.getName());
        List<PipelineConfiguration> pipelines = IngestService.getPipelines(state);
        EnrichPolicy policy = EnrichStore.getPolicy(request.getName(), state);
        List<String> pipelinesWithProcessors = new ArrayList<>();

        for (PipelineConfiguration pipelineConfiguration : pipelines) {
            List<AbstractEnrichProcessor> enrichProcessors =
                ingestService.getProcessorsInPipeline(pipelineConfiguration.getId(), AbstractEnrichProcessor.class);
            for (AbstractEnrichProcessor processor: enrichProcessors) {
                if (processor.getPolicyName().equals(request.getName())) {
                    pipelinesWithProcessors.add(pipelineConfiguration.getId());
                }
            }
        }

        if (pipelinesWithProcessors.isEmpty() == false) {
            enrichPolicyLocks.releasePolicy(request.getName());
            listener.onFailure(
                new ElasticsearchStatusException("Could not delete policy [{}] because a pipeline is referencing it {}",
                    RestStatus.CONFLICT, request.getName(), pipelinesWithProcessors));
            return;
        }

        EnrichStore.deletePolicy(request.getName(), clusterService, e -> {
            enrichPolicyLocks.releasePolicy(request.getName());
           if (e == null) {
               listener.onResponse(new AcknowledgedResponse(true));
           } else {
               listener.onFailure(e);
           }
        });
    }

    @Override
    protected ClusterBlockException checkBlock(DeleteEnrichPolicyAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
