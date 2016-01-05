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

package org.elasticsearch.plugin.ingest;

import org.apache.lucene.util.IOUtils;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.SearchScrollIterator;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.ingest.Pipeline;
import org.elasticsearch.ingest.ProcessorFactoryProvider;
import org.elasticsearch.ingest.TemplateService;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.plugin.ingest.transport.delete.DeletePipelineRequest;
import org.elasticsearch.plugin.ingest.transport.put.PutPipelineRequest;
import org.elasticsearch.plugin.ingest.transport.reload.ReloadPipelinesAction;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.transport.TransportService;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PipelineStore extends AbstractComponent implements Closeable {

    public final static String INDEX = ".ingest";
    public final static String TYPE = "pipeline";

    private Client client;
    private final TimeValue scrollTimeout;
    private final ReloadPipelinesAction reloadPipelinesAction;
    private final Pipeline.Factory factory = new Pipeline.Factory();
    private Map<String, Processor.Factory> processorFactoryRegistry;

    private volatile boolean started = false;
    private volatile Map<String, PipelineDefinition> pipelines = new HashMap<>();

    public PipelineStore(Settings settings, ClusterService clusterService, TransportService transportService) {
        super(settings);
        this.scrollTimeout = settings.getAsTime("ingest.pipeline.store.scroll.timeout", TimeValue.timeValueSeconds(30));
        this.reloadPipelinesAction = new ReloadPipelinesAction(settings, this, clusterService, transportService);
    }

    public void setClient(Client client) {
        this.client = client;
    }

    public void buildProcessorFactoryRegistry(Map<String, ProcessorFactoryProvider> processorFactoryProviders, Environment environment, ScriptService scriptService) {
        Map<String, Processor.Factory> processorFactories = new HashMap<>();
        TemplateService templateService = new InternalTemplateService(scriptService);
        for (Map.Entry<String, ProcessorFactoryProvider> entry : processorFactoryProviders.entrySet()) {
            Processor.Factory processorFactory = entry.getValue().get(environment, templateService);
            processorFactories.put(entry.getKey(), processorFactory);
        }
        this.processorFactoryRegistry = Collections.unmodifiableMap(processorFactories);
    }

    @Override
    public void close() throws IOException {
        stop("closing");
        // TODO: When org.elasticsearch.node.Node can close Closable instances we should try to remove this code,
        // since any wired closable should be able to close itself
        List<Closeable> closeables = new ArrayList<>();
        for (Processor.Factory factory : processorFactoryRegistry.values()) {
            if (factory instanceof Closeable) {
                closeables.add((Closeable) factory);
            }
        }
        IOUtils.close(closeables);
    }

    /**
     * Deletes the pipeline specified by id in the request.
     */
    public void delete(DeletePipelineRequest request, ActionListener<DeleteResponse> listener) {
        ensureReady();

        DeleteRequest deleteRequest = new DeleteRequest(request);
        deleteRequest.index(PipelineStore.INDEX);
        deleteRequest.type(PipelineStore.TYPE);
        deleteRequest.id(request.id());
        deleteRequest.refresh(true);
        client.delete(deleteRequest, handleWriteResponseAndReloadPipelines(listener));
    }

    /**
     * Stores the specified pipeline definition in the request.
     *
     * @throws IllegalArgumentException If the pipeline holds incorrect configuration
     */
    public void put(PutPipelineRequest request, ActionListener<IndexResponse> listener) throws IllegalArgumentException {
        ensureReady();

        try {
            // validates the pipeline and processor configuration:
            Map<String, Object> pipelineConfig = XContentHelper.convertToMap(request.source(), false).v2();
            constructPipeline(request.id(), pipelineConfig);
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid pipeline configuration", e);
        }

        IndexRequest indexRequest = new IndexRequest(request);
        indexRequest.index(PipelineStore.INDEX);
        indexRequest.type(PipelineStore.TYPE);
        indexRequest.id(request.id());
        indexRequest.source(request.source());
        indexRequest.refresh(true);
        client.index(indexRequest, handleWriteResponseAndReloadPipelines(listener));
    }

    /**
     * Returns the pipeline by the specified id
     */
    public Pipeline get(String id) {
        ensureReady();

        PipelineDefinition ref = pipelines.get(id);
        if (ref != null) {
            return ref.getPipeline();
        } else {
            return null;
        }
    }

    public Map<String, Processor.Factory> getProcessorFactoryRegistry() {
        return processorFactoryRegistry;
    }

    public List<PipelineDefinition> getReference(String... ids) {
        ensureReady();

        List<PipelineDefinition> result = new ArrayList<>(ids.length);
        for (String id : ids) {
            if (Regex.isSimpleMatchPattern(id)) {
                for (Map.Entry<String, PipelineDefinition> entry : pipelines.entrySet()) {
                    if (Regex.simpleMatch(id, entry.getKey())) {
                        result.add(entry.getValue());
                    }
                }
            } else {
                PipelineDefinition reference = pipelines.get(id);
                if (reference != null) {
                    result.add(reference);
                }
            }
        }
        return result;
    }

    public synchronized void updatePipelines() throws Exception {
        // note: this process isn't fast or smart, but the idea is that there will not be many pipelines,
        // so for that reason the goal is to keep the update logic simple.

        int changed = 0;
        Map<String, PipelineDefinition> newPipelines = new HashMap<>(pipelines);
        for (SearchHit hit : readAllPipelines()) {
            String pipelineId = hit.getId();
            BytesReference pipelineSource = hit.getSourceRef();
            PipelineDefinition current = newPipelines.get(pipelineId);
            if (current != null) {
                // If we first read from a primary shard copy and then from a replica copy,
                // and a write did not yet make it into the replica shard
                // then the source is not equal but we don't update because the current pipeline is the latest:
                if (current.getVersion() > hit.getVersion()) {
                    continue;
                }
                if (current.getSource().equals(pipelineSource)) {
                    continue;
                }
            }

            changed++;
            Pipeline pipeline = constructPipeline(hit.getId(), hit.sourceAsMap());
            newPipelines.put(pipelineId, new PipelineDefinition(pipeline, hit.getVersion(), pipelineSource));
        }

        int removed = 0;
        for (String existingPipelineId : pipelines.keySet()) {
            if (pipelineExists(existingPipelineId) == false) {
                newPipelines.remove(existingPipelineId);
                removed++;
            }
        }

        if (changed != 0 || removed != 0) {
            logger.debug("adding or updating [{}] pipelines and [{}] pipelines removed", changed, removed);
            pipelines = newPipelines;
        } else {
            logger.debug("no pipelines changes detected");
        }
    }

    private Pipeline constructPipeline(String id, Map<String, Object> config) throws Exception {
        return factory.create(id, config, processorFactoryRegistry);
    }

    boolean pipelineExists(String pipelineId) {
        GetRequest request = new GetRequest(PipelineStore.INDEX, PipelineStore.TYPE, pipelineId);
        try {
            GetResponse response = client.get(request).actionGet();
            return response.isExists();
        } catch (IndexNotFoundException e) {
            // the ingest index doesn't exist, so the pipeline doesn't either:
            return false;
        }
    }

    synchronized void start() throws Exception {
        if (started) {
            logger.debug("Pipeline already started");
        } else {
            updatePipelines();
            started = true;
            logger.debug("Pipeline store started with [{}] pipelines", pipelines.size());
        }
    }

    synchronized void stop(String reason) {
        if (started) {
            started = false;
            pipelines = new HashMap<>();
            logger.debug("Pipeline store stopped, reason [{}]", reason);
        } else {
            logger.debug("Pipeline alreadt stopped");
        }
    }

    public boolean isStarted() {
        return started;
    }

    private Iterable<SearchHit> readAllPipelines() {
        // TODO: the search should be replaced with an ingest API when it is available
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.version(true);
        sourceBuilder.sort("_doc", SortOrder.ASC);
        SearchRequest searchRequest = new SearchRequest(PipelineStore.INDEX);
        searchRequest.source(sourceBuilder);
        searchRequest.indicesOptions(IndicesOptions.lenientExpandOpen());
        return SearchScrollIterator.createIterator(client, scrollTimeout, searchRequest);
    }

    private void ensureReady() {
        if (started == false) {
            throw new IllegalStateException("pipeline store isn't ready yet");
        }
    }

    @SuppressWarnings("unchecked")
    private <T> ActionListener<T> handleWriteResponseAndReloadPipelines(ActionListener<T> listener) {
        return new ActionListener<T>() {
            @Override
            public void onResponse(T result) {
                try {
                    reloadPipelinesAction.reloadPipelinesOnAllNodes(reloadResult -> listener.onResponse(result));
                } catch (Throwable e) {
                    listener.onFailure(e);
                }
            }

            @Override
            public void onFailure(Throwable e) {
                listener.onFailure(e);
            }
        };
    }

}
