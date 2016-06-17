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
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.ingest.core.IngestInfo;
import org.elasticsearch.ingest.core.Pipeline;
import org.elasticsearch.ingest.core.Processor;
import org.elasticsearch.ingest.core.TemplateService;
import org.elasticsearch.script.ScriptService;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PipelineStore extends AbstractComponent implements Closeable, ClusterStateListener {

    private final Pipeline.Factory factory = new Pipeline.Factory();
    private ProcessorsRegistry processorRegistry;

    // Ideally this should be in IngestMetadata class, but we don't have the processor factories around there.
    // We know of all the processor factories when a node with all its plugin have been initialized. Also some
    // processor factories rely on other node services. Custom metadata is statically registered when classes
    // are loaded, so in the cluster state we just save the pipeline config and here we keep the actual pipelines around.
    volatile Map<String, Pipeline> pipelines = new HashMap<>();

    public PipelineStore(Settings settings) {
        super(settings);
    }

    public void buildProcessorFactoryRegistry(ProcessorsRegistry.Builder processorsRegistryBuilder, ScriptService scriptService, ClusterService clusterService) {
        this.processorRegistry = processorsRegistryBuilder.build(scriptService, clusterService);
    }

    @Override
    public void close() throws IOException {
        // TODO: When org.elasticsearch.node.Node can close Closable instances we should try to remove this code,
        // since any wired closable should be able to close itself
        processorRegistry.close();
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        innerUpdatePipelines(event.state());
    }

    void innerUpdatePipelines(ClusterState state) {
        IngestMetadata ingestMetadata = state.getMetaData().custom(IngestMetadata.TYPE);
        if (ingestMetadata == null) {
            return;
        }

        Map<String, Pipeline> pipelines = new HashMap<>();
        for (PipelineConfiguration pipeline : ingestMetadata.getPipelines().values()) {
            try {
                pipelines.put(pipeline.getId(), factory.create(pipeline.getId(), pipeline.getConfigAsMap(), processorRegistry));
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
        clusterService.submitStateUpdateTask("delete-pipeline-" + request.getId(), new AckedClusterStateUpdateTask<WritePipelineResponse>(request, listener) {

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
        if (pipelines.containsKey(request.getId()) == false) {
            throw new ResourceNotFoundException("pipeline [{}] is missing", request.getId());
        } else {
            pipelines = new HashMap<>(pipelines);
            pipelines.remove(request.getId());
            ClusterState.Builder newState = ClusterState.builder(currentState);
            newState.metaData(MetaData.builder(currentState.getMetaData())
                    .putCustom(IngestMetadata.TYPE, new IngestMetadata(pipelines))
                    .build());
            return newState.build();
        }
    }

    /**
     * Stores the specified pipeline definition in the request.
     */
    public void put(ClusterService clusterService, Map<DiscoveryNode, IngestInfo> ingestInfos, PutPipelineRequest request, ActionListener<WritePipelineResponse> listener) throws Exception {
        // validates the pipeline and processor configuration before submitting a cluster update task:
        validatePipeline(ingestInfos, request);
        clusterService.submitStateUpdateTask("put-pipeline-" + request.getId(), new AckedClusterStateUpdateTask<WritePipelineResponse>(request, listener) {

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

        Map<String, Object> pipelineConfig = XContentHelper.convertToMap(request.getSource(), false).v2();
        Pipeline pipeline = factory.create(request.getId(), pipelineConfig, processorRegistry);
        List<IllegalArgumentException> exceptions = new ArrayList<>();
        for (Processor processor : pipeline.flattenAllProcessors()) {
            for (Map.Entry<DiscoveryNode, IngestInfo> entry : ingestInfos.entrySet()) {
                if (entry.getValue().containsProcessor(processor.getType()) == false) {
                    String message = "Processor type [" + processor.getType() + "] is not installed on node [" + entry.getKey() + "]";
                    exceptions.add(new IllegalArgumentException(message));
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

        pipelines.put(request.getId(), new PipelineConfiguration(request.getId(), request.getSource()));
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

    public ProcessorsRegistry getProcessorRegistry() {
        return processorRegistry;
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
