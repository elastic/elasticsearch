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
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.metrics.CounterMetric;
import org.elasticsearch.common.metrics.MeanMetric;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.ingest.core.IngestDocument;
import org.elasticsearch.ingest.core.Pipeline;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public class PipelineExecutionService implements ClusterStateListener {

    private final PipelineStore store;
    private final ThreadPool threadPool;

    private final StatsHolder totalStats = new StatsHolder();
    private volatile Map<String, StatsHolder> statsHolderPerPipeline = Collections.emptyMap();

    public PipelineExecutionService(PipelineStore store, ThreadPool threadPool) {
        this.store = store;
        this.threadPool = threadPool;
    }

    public void executeIndexRequest(IndexRequest request, Consumer<Throwable> failureHandler, Consumer<Boolean> completionHandler) {
        Pipeline pipeline = getPipeline(request.getPipeline());
        threadPool.executor(ThreadPool.Names.INDEX).execute(new AbstractRunnable() {

            @Override
            public void onFailure(Throwable t) {
                failureHandler.accept(t);
            }

            @Override
            protected void doRun() throws Exception {
                innerExecute(request, pipeline);
                completionHandler.accept(true);
            }
        });
    }

    public void executeBulkRequest(Iterable<ActionRequest<?>> actionRequests,
                                   BiConsumer<IndexRequest, Throwable> itemFailureHandler,
                                   Consumer<Throwable> completionHandler) {
        threadPool.executor(ThreadPool.Names.BULK).execute(new AbstractRunnable() {

            @Override
            public void onFailure(Throwable t) {
                completionHandler.accept(t);
            }

            @Override
            protected void doRun() throws Exception {
                for (ActionRequest actionRequest : actionRequests) {
                    if ((actionRequest instanceof IndexRequest)) {
                        IndexRequest indexRequest = (IndexRequest) actionRequest;
                        if (Strings.hasText(indexRequest.getPipeline())) {
                            try {
                                innerExecute(indexRequest, getPipeline(indexRequest.getPipeline()));
                                //this shouldn't be needed here but we do it for consistency with index api which requires it to prevent double execution
                                indexRequest.setPipeline(null);
                            } catch (Throwable e) {
                                itemFailureHandler.accept(indexRequest, e);
                            }
                        }
                    }
                }
                completionHandler.accept(null);
            }
        });
    }

    public IngestStats stats() {
        Map<String, StatsHolder> statsHolderPerPipeline = this.statsHolderPerPipeline;

        Map<String, IngestStats.Stats> statsPerPipeline = new HashMap<>(statsHolderPerPipeline.size());
        for (Map.Entry<String, StatsHolder> entry : statsHolderPerPipeline.entrySet()) {
            statsPerPipeline.put(entry.getKey(), entry.getValue().createStats());
        }

        return new IngestStats(totalStats.createStats(), statsPerPipeline);
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        IngestMetadata ingestMetadata = event.state().getMetaData().custom(IngestMetadata.TYPE);
        if (ingestMetadata != null) {
            updatePipelineStats(ingestMetadata);
        }
    }

    void updatePipelineStats(IngestMetadata ingestMetadata) {
        boolean changed = false;
        Map<String, StatsHolder> newStatsPerPipeline = new HashMap<>(statsHolderPerPipeline);
        Iterator<String> iterator = newStatsPerPipeline.keySet().iterator();
        while (iterator.hasNext()) {
            String pipeline = iterator.next();
            if (ingestMetadata.getPipelines().containsKey(pipeline) == false) {
                iterator.remove();
                changed = true;
            }
        }
        for (String pipeline : ingestMetadata.getPipelines().keySet()) {
            if (newStatsPerPipeline.containsKey(pipeline) == false) {
                newStatsPerPipeline.put(pipeline, new StatsHolder());
                changed = true;
            }
        }

        if (changed) {
            statsHolderPerPipeline = Collections.unmodifiableMap(newStatsPerPipeline);
        }
    }

    private void innerExecute(IndexRequest indexRequest, Pipeline pipeline) throws Exception {
        long startTimeInNanos = System.nanoTime();
        // the pipeline specific stat holder may not exist and that is fine:
        // (e.g. the pipeline may have been removed while we're ingesting a document
        Optional<StatsHolder> pipelineStats = Optional.ofNullable(statsHolderPerPipeline.get(pipeline.getId()));
        try {
            totalStats.preIngest();
            pipelineStats.ifPresent(StatsHolder::preIngest);
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
        } catch (Exception e) {
            totalStats.ingestFailed();
            pipelineStats.ifPresent(StatsHolder::ingestFailed);
            throw e;
        } finally {
            long ingestTimeInMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTimeInNanos);
            totalStats.postIngest(ingestTimeInMillis);
            pipelineStats.ifPresent(statsHolder -> statsHolder.postIngest(ingestTimeInMillis));
        }
    }

    private Pipeline getPipeline(String pipelineId) {
        Pipeline pipeline = store.get(pipelineId);
        if (pipeline == null) {
            throw new IllegalArgumentException("pipeline with id [" + pipelineId + "] does not exist");
        }
        return pipeline;
    }

    static class StatsHolder {

        private final MeanMetric ingestMetric = new MeanMetric();
        private final CounterMetric ingestCurrent = new CounterMetric();
        private final CounterMetric ingestFailed = new CounterMetric();

        void preIngest() {
            ingestCurrent.inc();
        }

        void postIngest(long ingestTimeInMillis) {
            ingestCurrent.dec();
            ingestMetric.inc(ingestTimeInMillis);
        }

        void ingestFailed() {
            ingestFailed.inc();
        }

        IngestStats.Stats createStats() {
            return new IngestStats.Stats(ingestMetric.count(), ingestMetric.sum(), ingestCurrent.count(), ingestFailed.count());
        }

    }

}
