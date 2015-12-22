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

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Pipeline;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Map;
import java.util.function.Consumer;

public class PipelineExecutionService {

    static final String THREAD_POOL_NAME = IngestPlugin.NAME;

    private final PipelineStore store;
    private final ThreadPool threadPool;

    public PipelineExecutionService(PipelineStore store, ThreadPool threadPool) {
        this.store = store;
        this.threadPool = threadPool;
    }

    public void execute(IndexRequest request, String pipelineId, Consumer<Throwable> failureHandler, Consumer<Boolean> completionHandler) {
        Pipeline pipeline = getPipeline(pipelineId);
        threadPool.executor(THREAD_POOL_NAME).execute(() -> {
            try {
                innerExecute(request, pipeline);
                completionHandler.accept(true);
            } catch (Exception e) {
                failureHandler.accept(e);
            }
        });
    }

    public void execute(Iterable<ActionRequest> actionRequests, String pipelineId,
                        Consumer<Throwable> itemFailureHandler, Consumer<Boolean> completionHandler) {
        Pipeline pipeline = getPipeline(pipelineId);
        threadPool.executor(THREAD_POOL_NAME).execute(() -> {
            for (ActionRequest actionRequest : actionRequests) {
                if ((actionRequest instanceof IndexRequest) == false) {
                    continue;
                }

                IndexRequest indexRequest = (IndexRequest) actionRequest;
                try {
                    innerExecute(indexRequest, pipeline);
                } catch (Throwable e) {
                    if (itemFailureHandler != null) {
                        itemFailureHandler.accept(e);
                    }
                }
            }
            completionHandler.accept(true);
        });
    }

    private void innerExecute(IndexRequest indexRequest, Pipeline pipeline) throws Exception {
        String index = indexRequest.index();
        String type = indexRequest.type();
        String id = indexRequest.id();
        String routing = indexRequest.routing();
        String parent = indexRequest.parent();
        String timestamp = indexRequest.timestamp();
        String ttl = indexRequest.ttl() == null ? null : indexRequest.ttl().toString();
        Map<String, Object> sourceAsMap = indexRequest.sourceAsMap();
        IngestDocument ingestDocument = new IngestDocument(index, type, id, routing, parent, timestamp, ttl, sourceAsMap);
        pipeline.execute(ingestDocument);

        Map<IngestDocument.MetaData, String> metadataMap = ingestDocument.extractMetadata();
        //it's fine to set all metadata fields all the time, as ingest document holds their starting values
        //before ingestion, which might also get modified during ingestion.
        indexRequest.index(metadataMap.get(IngestDocument.MetaData.INDEX));
        indexRequest.type(metadataMap.get(IngestDocument.MetaData.TYPE));
        indexRequest.id(metadataMap.get(IngestDocument.MetaData.ID));
        indexRequest.routing(metadataMap.get(IngestDocument.MetaData.ROUTING));
        indexRequest.parent(metadataMap.get(IngestDocument.MetaData.PARENT));
        indexRequest.timestamp(metadataMap.get(IngestDocument.MetaData.TIMESTAMP));
        indexRequest.ttl(metadataMap.get(IngestDocument.MetaData.TTL));
        indexRequest.source(ingestDocument.getSourceAndMetadata());
    }

    private Pipeline getPipeline(String pipelineId) {
        Pipeline pipeline = store.get(pipelineId);
        if (pipeline == null) {
            throw new IllegalArgumentException("pipeline with id [" + pipelineId + "] does not exist");
        }
        return pipeline;
    }

    public static Settings additionalSettings(Settings nodeSettings) {
        Settings settings = nodeSettings.getAsSettings("threadpool." + THREAD_POOL_NAME);
        if (!settings.names().isEmpty()) {
            // the TP is already configured in the node settings
            // no need for additional settings
            return Settings.EMPTY;
        }
        int availableProcessors = EsExecutors.boundedNumberOfProcessors(nodeSettings);
        return Settings.builder()
                .put("threadpool." + THREAD_POOL_NAME + ".type", "fixed")
                .put("threadpool." + THREAD_POOL_NAME + ".size", availableProcessors)
                .put("threadpool." + THREAD_POOL_NAME + ".queue_size", 200)
                .build();
    }

}
