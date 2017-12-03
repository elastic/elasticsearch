/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.ingest;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ingest.DeletePipelineRequest;
import org.elasticsearch.action.ingest.PutPipelineRequest;
import org.elasticsearch.action.ingest.WritePipelineResponse;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateApplier;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class PipelineStore extends AbstractComponent implements ClusterStateApplier {

    private final Pipeline.Factory factory = new Pipeline.Factory();
    private final Map<String, Processor.Factory> processorFactories;

    // Ideally this should be in IngestMetadata class, but we don't have the processor factories around there.
    // We know of all the processor factories when a node with all its plugin have been initialized. Also some
    // processor factories rely on other node services. Custom metadata is statically registered when classes
    // are loaded, so in the cluster state we just save the pipeline config and here we keep the actual pipelines around.
    volatile Map<String, Pipeline> pipelines = new HashMap<>();

    public PipelineStore(Settings settings, Map<String, Processor.Factory> processorFactories) {
        super(settings);
        this.processorFactories = processorFactories;
    }

    @Override
    public void applyClusterState(ClusterChangedEvent event) {
        innerUpdatePipelines(event.previousState(), event.state());
    }

    void innerUpdatePipelines(ClusterState previousState, ClusterState state) {
        IngestMetadata ingestMetadata = state.getMetaData().custom(IngestMetadata.TYPE);
        IngestMetadata previousIngestMetadata = previousState.getMetaData().custom(IngestMetadata.TYPE);
        if (Objects.equals(ingestMetadata, previousIngestMetadata)) {
            return;
        }

        Map<String, Pipeline> pipelines = new HashMap<>();
        for (PipelineConfiguration pipeline : ingestMetadata.getPipelines().values()) {
            try {
                pipelines.put(pipeline.getId(), factory.create(pipeline.getId(), pipeline.getConfigAsMap(), processorFactories));
            } catch (ElasticsearchParseException e) {
                throw e;
            } catch (Exception e) {
                throw new ElasticsearchParseException("Error updating pipeline with id [" + pipeline.getId() + "]", e);
            }
        }
        this.pipelines = Collections.unmodifiableMap(pipelines);
    }

    /**
     * Deletes the pipeline specified by id in the request.
     */
    public void delete(ClusterService clusterService, DeletePipelineRequest request, ActionListener<WritePipelineResponse> listener) {
        clusterService.submitStateUpdateTask("delete-pipeline-" + request.getId(),
                new AckedClusterStateUpdateTask<WritePipelineResponse>(request, listener) {

            @Override
            protected WritePipelineResponse newResponse(boolean acknowledged) {
                return new WritePipelineResponse(acknowledged);
            }

            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                return innerDelete(request, currentState);
            }
        });
    }

    ClusterState innerDelete(DeletePipelineRequest request, ClusterState currentState) {
        IngestMetadata currentIngestMetadata = currentState.metaData().custom(IngestMetadata.TYPE);
        if (currentIngestMetadata == null) {
            return currentState;
        }
        Map<String, PipelineConfiguration> pipelines = currentIngestMetadata.getPipelines();
        Set<String> toRemove = new HashSet<>();
        for (String pipelineKey : pipelines.keySet()) {
            if (Regex.simpleMatch(request.getId(), pipelineKey)) {
                toRemove.add(pipelineKey);
            }
        }
        if (toRemove.isEmpty() && Regex.isMatchAllPattern(request.getId()) == false) {
            throw new ResourceNotFoundException("pipeline [{}] is missing", request.getId());
        } else if (toRemove.isEmpty()) {
            return currentState;
        }
        final Map<String, PipelineConfiguration> pipelinesCopy = new HashMap<>(pipelines);
        for (String key : toRemove) {
            pipelinesCopy.remove(key);
        }
        ClusterState.Builder newState = ClusterState.builder(currentState);
        newState.metaData(MetaData.builder(currentState.getMetaData())
                .putCustom(IngestMetadata.TYPE, new IngestMetadata(pipelinesCopy))
                .build());
        return newState.build();
    }

    /**
     * Stores the specified pipeline definition in the request.
     */
    public void put(ClusterService clusterService, Map<DiscoveryNode, IngestInfo> ingestInfos, PutPipelineRequest request,
                    ActionListener<WritePipelineResponse> listener) throws Exception {
        // validates the pipeline and processor configuration before submitting a cluster update task:
        validatePipeline(ingestInfos, request);
        clusterService.submitStateUpdateTask("put-pipeline-" + request.getId(),
                new AckedClusterStateUpdateTask<WritePipelineResponse>(request, listener) {

            @Override
            protected WritePipelineResponse newResponse(boolean acknowledged) {
                return new WritePipelineResponse(acknowledged);
            }

            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                return innerPut(request, currentState);
            }
        });
    }

    void validatePipeline(Map<DiscoveryNode, IngestInfo> ingestInfos, PutPipelineRequest request) throws Exception {
        if (ingestInfos.isEmpty()) {
            throw new IllegalStateException("Ingest info is empty");
        }

        Map<String, Object> pipelineConfig = XContentHelper.convertToMap(request.getSource(), false, request.getXContentType()).v2();
        Pipeline pipeline = factory.create(request.getId(), pipelineConfig, processorFactories);
        List<Exception> exceptions = new ArrayList<>();
        for (Processor processor : pipeline.flattenAllProcessors()) {
            for (Map.Entry<DiscoveryNode, IngestInfo> entry : ingestInfos.entrySet()) {
                if (entry.getValue().containsProcessor(processor.getType()) == false) {
                    String message = "Processor type [" + processor.getType() + "] is not installed on node [" + entry.getKey() + "]";
                    exceptions.add(ConfigurationUtils.newConfigurationException(processor.getType(), processor.getTag(), null, message));
                }
            }
        }
        ExceptionsHelper.rethrowAndSuppress(exceptions);
    }

    ClusterState innerPut(PutPipelineRequest request, ClusterState currentState) {
        IngestMetadata currentIngestMetadata = currentState.metaData().custom(IngestMetadata.TYPE);
        Map<String, PipelineConfiguration> pipelines;
        if (currentIngestMetadata != null) {
            pipelines = new HashMap<>(currentIngestMetadata.getPipelines());
        } else {
            pipelines = new HashMap<>();
        }

        pipelines.put(request.getId(), new PipelineConfiguration(request.getId(), request.getSource(), request.getXContentType()));
        ClusterState.Builder newState = ClusterState.builder(currentState);
        newState.metaData(MetaData.builder(currentState.getMetaData())
            .putCustom(IngestMetadata.TYPE, new IngestMetadata(pipelines))
            .build());
        return newState.build();
    }

    /**
     * Returns the pipeline by the specified id
     */
    public Pipeline get(String id) {
        return pipelines.get(id);
    }

    public Map<String, Processor.Factory> getProcessorFactories() {
        return processorFactories;
    }

    /**
     * @return pipeline configuration specified by id. If multiple ids or wildcards are specified multiple pipelines
     * may be returned
     */
    // Returning PipelineConfiguration instead of Pipeline, because Pipeline and Processor interface don't
    // know how to serialize themselves.
    public List<PipelineConfiguration> getPipelines(ClusterState clusterState, String... ids) {
        IngestMetadata ingestMetadata = clusterState.getMetaData().custom(IngestMetadata.TYPE);
        return innerGetPipelines(ingestMetadata, ids);
    }

    List<PipelineConfiguration> innerGetPipelines(IngestMetadata ingestMetadata, String... ids) {
        if (ingestMetadata == null) {
            return Collections.emptyList();
        }

        // if we didn't ask for _any_ ID, then we get them all (this is the same as if they ask for '*')
        if (ids.length == 0) {
            return new ArrayList<>(ingestMetadata.getPipelines().values());
        }

        List<PipelineConfiguration> result = new ArrayList<>(ids.length);
        for (String id : ids) {
            if (Regex.isSimpleMatchPattern(id)) {
                for (Map.Entry<String, PipelineConfiguration> entry : ingestMetadata.getPipelines().entrySet()) {
                    if (Regex.simpleMatch(id, entry.getKey())) {
                        result.add(entry.getValue());
                    }
                }
            } else {
                PipelineConfiguration pipeline = ingestMetadata.getPipelines().get(id);
                if (pipeline != null) {
                    result.add(pipeline);
                }
            }
        }
        return result;
    }
}
