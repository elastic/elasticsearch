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
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.common.SearchScrollIterator;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.component.LifecycleListener;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Provider;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.env.Environment;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.ingest.Pipeline;
import org.elasticsearch.ingest.processor.Processor;
import org.elasticsearch.plugin.ingest.transport.delete.DeletePipelineRequest;
import org.elasticsearch.plugin.ingest.transport.put.PutPipelineRequest;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.*;

public class PipelineStore extends AbstractComponent {

    public final static String INDEX = ".ingest";
    public final static String TYPE = "pipeline";

    private final ThreadPool threadPool;
    private final TimeValue scrollTimeout;
    private final ClusterService clusterService;
    private final Provider<Client> clientProvider;
    private final TimeValue pipelineUpdateInterval;
    private final Pipeline.Factory factory = new Pipeline.Factory();
    private final Map<String, Processor.Factory> processorFactoryRegistry;

    private volatile Client client;
    private volatile Map<String, PipelineDefinition> pipelines = new HashMap<>();

    @Inject
    public PipelineStore(Settings settings, Provider<Client> clientProvider, ThreadPool threadPool, Environment environment, ClusterService clusterService, Map<String, Processor.Factory> processors) {
        super(settings);
        this.threadPool = threadPool;
        this.clusterService = clusterService;
        this.clientProvider = clientProvider;
        this.scrollTimeout = settings.getAsTime("ingest.pipeline.store.scroll.timeout", TimeValue.timeValueSeconds(30));
        this.pipelineUpdateInterval = settings.getAsTime("ingest.pipeline.store.update.interval", TimeValue.timeValueSeconds(1));
        for (Processor.Factory factory : processors.values()) {
            factory.setConfigDirectory(environment.configFile());
        }
        this.processorFactoryRegistry = Collections.unmodifiableMap(processors);
        clusterService.add(new PipelineStoreListener());
        clusterService.addLifecycleListener(new LifecycleListener() {
            @Override
            public void beforeClose() {
                // Ideally we would implement Closeable, but when a node is stopped this doesn't get invoked:
                try {
                    IOUtils.close(processorFactoryRegistry.values());
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        });
    }

    /**
     * Deletes the pipeline specified by id in the request.
     */
    public void delete(DeletePipelineRequest request, ActionListener<DeleteResponse> listener) {
        DeleteRequest deleteRequest = new DeleteRequest(request);
        deleteRequest.index(PipelineStore.INDEX);
        deleteRequest.type(PipelineStore.TYPE);
        deleteRequest.id(request.id());
        deleteRequest.refresh(true);
        client().delete(deleteRequest, listener);
    }

    /**
     * Stores the specified pipeline definition in the request.
     *
     * @throws IllegalArgumentException If the pipeline holds incorrect configuration
     */
    public void put(PutPipelineRequest request, ActionListener<IndexResponse> listener) throws IllegalArgumentException {
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
        client().index(indexRequest, listener);
    }

    /**
     * Returns the pipeline by the specified id
     */
    public Pipeline get(String id) {
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

    Pipeline constructPipeline(String id, Map<String, Object> config) throws Exception {
        return factory.create(id, config, processorFactoryRegistry);
    }

    synchronized void updatePipelines() throws Exception {
        // note: this process isn't fast or smart, but the idea is that there will not be many pipelines,
        // so for that reason the goal is to keep the update logic simple.

        int changed = 0;
        Map<String, PipelineDefinition> newPipelines = new HashMap<>(pipelines);
        for (SearchHit hit : readAllPipelines()) {
            String pipelineId = hit.getId();
            BytesReference pipelineSource = hit.getSourceRef();
            PipelineDefinition previous = newPipelines.get(pipelineId);
            if (previous != null) {
                if (previous.getSource().equals(pipelineSource)) {
                    continue;
                }
            }

            changed++;
            Pipeline pipeline = constructPipeline(hit.getId(), hit.sourceAsMap());
            newPipelines.put(pipelineId, new PipelineDefinition(pipeline, hit.getVersion(), pipelineSource));
        }

        int removed = 0;
        for (String existingPipelineId : pipelines.keySet()) {
            if (!existPipeline(existingPipelineId)) {
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

    void startUpdateWorker() {
        threadPool.schedule(pipelineUpdateInterval, ThreadPool.Names.GENERIC, new Updater());
    }

    boolean existPipeline(String pipelineId) {
        GetRequest request = new GetRequest(PipelineStore.INDEX, PipelineStore.TYPE, pipelineId);
        GetResponse response = client().get(request).actionGet();
        return response.isExists();
    }

    Iterable<SearchHit> readAllPipelines() {
        // TODO: the search should be replaced with an ingest API when it is available
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.version(true);
        sourceBuilder.sort("_doc", SortOrder.ASC);
        SearchRequest searchRequest = new SearchRequest(PipelineStore.INDEX);
        searchRequest.source(sourceBuilder);
        searchRequest.indicesOptions(IndicesOptions.lenientExpandOpen());
        return SearchScrollIterator.createIterator(client(), scrollTimeout, searchRequest);
    }


    private Client client() {
        if (client == null) {
            client = clientProvider.get();
        }
        return client;
    }

    class Updater implements Runnable {

        @Override
        public void run() {
            try {
                updatePipelines();
            } catch (Exception e) {
                logger.error("pipeline store update failure", e);
            } finally {
                startUpdateWorker();
            }
        }

    }

    class PipelineStoreListener implements ClusterStateListener {

        @Override
        public void clusterChanged(ClusterChangedEvent event) {
            if (event.state().blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK) == false) {
                startUpdateWorker();
                clusterService.remove(this);
            }
        }
    }

}
