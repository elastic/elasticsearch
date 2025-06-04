/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.ingest;

import org.elasticsearch.exception.ElasticsearchGenerationException;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.ingest.IngestMetadata;
import org.elasticsearch.ingest.IngestService;
import org.elasticsearch.reservedstate.ReservedProjectStateHandler;
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
public class ReservedPipelineAction implements ReservedProjectStateHandler<List<PutPipelineRequest>> {
    public static final String NAME = "ingest_pipelines";

    /**
     * Creates a ReservedPipelineAction
     *
     */
    public ReservedPipelineAction() {}

    @Override
    public String name() {
        return NAME;
    }

    private Collection<PutPipelineRequest> prepare(List<PutPipelineRequest> requests) {
        var exceptions = new ArrayList<Exception>();
        for (var pipeline : requests) {
            try {
                validate(pipeline);
            } catch (Exception e) {
                exceptions.add(e);
            }
        }

        if (exceptions.isEmpty() == false) {
            var illegalArgumentException = new IllegalArgumentException("Error processing ingest pipelines");
            exceptions.forEach(illegalArgumentException::addSuppressed);
            throw illegalArgumentException;
        }

        return requests;
    }

    private static ProjectMetadata wrapIngestTaskExecute(IngestService.PipelineClusterStateUpdateTask task, ProjectMetadata metadata) {
        final var allIndexMetadata = metadata.indices().values();
        final IngestMetadata currentIndexMetadata = metadata.custom(IngestMetadata.TYPE);

        var updatedIngestMetadata = task.execute(currentIndexMetadata, allIndexMetadata);
        return ProjectMetadata.builder(metadata).putCustom(IngestMetadata.TYPE, updatedIngestMetadata).build();
    }

    @Override
    public TransformState transform(ProjectId projectId, List<PutPipelineRequest> source, TransformState prevState) throws Exception {
        var requests = prepare(source);

        ClusterState clusterState = prevState.state();
        ProjectMetadata projectMetadata = clusterState.metadata().getProject(projectId);

        for (var request : requests) {
            var nopUpdate = IngestService.isNoOpPipelineUpdate(projectMetadata, request);

            if (nopUpdate) {
                continue;
            }

            var task = new IngestService.PutPipelineClusterStateUpdateTask(projectMetadata.id(), request);
            projectMetadata = wrapIngestTaskExecute(task, projectMetadata);
        }

        Set<String> entities = requests.stream().map(PutPipelineRequest::getId).collect(Collectors.toSet());

        Set<String> toDelete = new HashSet<>(prevState.keys());
        toDelete.removeAll(entities);

        for (var pipelineToDelete : toDelete) {
            var task = new IngestService.DeletePipelineClusterStateUpdateTask(
                projectMetadata.id(),
                null,
                new DeletePipelineRequest(
                    RESERVED_CLUSTER_STATE_HANDLER_IGNORED_TIMEOUT,
                    RESERVED_CLUSTER_STATE_HANDLER_IGNORED_TIMEOUT,
                    pipelineToDelete
                )
            );
            projectMetadata = wrapIngestTaskExecute(task, projectMetadata);
        }

        return new TransformState(ClusterState.builder(clusterState).putProjectMetadata(projectMetadata).build(), entities);
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
                result.add(
                    new PutPipelineRequest(
                        RESERVED_CLUSTER_STATE_HANDLER_IGNORED_TIMEOUT,
                        RESERVED_CLUSTER_STATE_HANDLER_IGNORED_TIMEOUT,
                        id,
                        BytesReference.bytes(builder),
                        XContentType.JSON
                    )
                );
            } catch (Exception e) {
                throw new ElasticsearchGenerationException("Failed to generate [" + source + "]", e);
            }
        }

        return result;
    }

}
