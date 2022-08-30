/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.action.ingest;

import org.elasticsearch.ElasticsearchGenerationException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.ingest.IngestMetadata;
import org.elasticsearch.ingest.IngestService;
import org.elasticsearch.reservedstate.ReservedClusterStateHandler;
import org.elasticsearch.reservedstate.TransformState;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * This Action is the reserved state save version of RestPutPipelineAction/RestDeletePipelineAction
 * <p>
 * It is used by the ReservedClusterStateService to add/update or remove ingest pipelines. Typical usage
 * for this action is in the context of file based state.
 */
public class ReservedPipelineAction implements ReservedClusterStateHandler<List<PutPipelineRequest>> {
    public static final String NAME = "ingest_pipelines";

    private final IngestService ingestService;
    private final NodeClient nodeClient;

    /**
     * Creates a ReservedPipelineAction
     *
     * @param ingestService requires {@link IngestService} for storing/deleting the pipelines
     */
    public ReservedPipelineAction(IngestService ingestService, NodeClient nodeClient) {
        this.ingestService = ingestService;
        this.nodeClient = nodeClient;
    }

    @Override
    public String name() {
        return NAME;
    }

    @SuppressWarnings("unchecked")
    public Collection<PutPipelineRequest> prepare(NodesInfoResponse nodeInfos, Object input) {
        List<PutPipelineRequest> pipelines = (List<PutPipelineRequest>) input;

        var exceptions = new ArrayList<String>();
        for (var pipeline : pipelines) {
            try {
                ingestService.validatePipelineRequest(pipeline, nodeInfos);
            } catch (Exception e) {
                exceptions.add(e.getMessage());
            }
        }

        if (exceptions.isEmpty() == false) {
            throw new IllegalStateException(String.join(", ", exceptions));
        }

        return pipelines;
    }

    // package private for testing
    NodesInfoResponse getNodeInfos() {
        NodesInfoRequest nodesInfoRequest = new NodesInfoRequest();
        nodesInfoRequest.clear();
        nodesInfoRequest.addMetric(NodesInfoRequest.Metric.INGEST.metricName());
        return nodeClient.admin().cluster().nodesInfo(nodesInfoRequest).actionGet();
    }

    private ClusterState wrapIngestTaskExecute(IngestService.PipelineClusterStateUpdateTask task, ClusterState state) {
        final var allIndexMetadata = state.metadata().indices().values();
        final IngestMetadata currentIndexMetadata = state.metadata().custom(IngestMetadata.TYPE);

        var updatedIngestMetadata = task.execute(currentIndexMetadata, allIndexMetadata);
        return state.copyAndUpdateMetadata(b -> b.putCustom(IngestMetadata.TYPE, updatedIngestMetadata));
    }

    @Override
    public TransformState transform(Object source, TransformState prevState) throws Exception {
        NodesInfoRequest nodesInfoRequest = new NodesInfoRequest();
        nodesInfoRequest.clear();
        nodesInfoRequest.addMetric(NodesInfoRequest.Metric.INGEST.metricName());
        var nodeInfos = getNodeInfos();

        var requests = prepare(nodeInfos, source);

        ClusterState state = prevState.state();

        for (var request : requests) {
            var nopUpdate = ingestService.isNoOpPipelineUpdate(state, request, new ActionListener<>() {
                @Override
                public void onResponse(AcknowledgedResponse acknowledgedResponse) {}

                @Override
                public void onFailure(Exception e) {
                    throw new IllegalStateException(e.getMessage(), e);
                }
            });

            if (nopUpdate) {
                continue;
            }

            var task = new IngestService.PutPipelineClusterStateUpdateTask(request);
            state = wrapIngestTaskExecute(task, state);
        }

        Set<String> entities = requests.stream().map(r -> r.getId()).collect(Collectors.toSet());

        Set<String> toDelete = new HashSet<>(prevState.keys());
        toDelete.removeAll(entities);

        for (var pipelineToDelete : toDelete) {
            var task = new IngestService.DeletePipelineClusterStateUpdateTask(pipelineToDelete);
            state = wrapIngestTaskExecute(task, state);
        }

        return new TransformState(state, entities);
    }

    @Override
    public List<PutPipelineRequest> fromXContent(XContentParser parser) throws IOException {
        List<PutPipelineRequest> result = new ArrayList<>();

        Map<String, ?> source = parser.map();

        for (String id : source.keySet()) {
            @SuppressWarnings("unchecked")
            Map<String, ?> content = (Map<String, ?>) source.get(id);
            try (XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON)) {
                builder.map(content);
                result.add(new PutPipelineRequest(id, BytesReference.bytes(builder), XContentType.JSON));
            } catch (Exception e) {
                throw new ElasticsearchGenerationException("Failed to generate [" + source + "]", e);
            }
        }

        return result;
    }
}
