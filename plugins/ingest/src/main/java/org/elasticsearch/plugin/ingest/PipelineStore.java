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

import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.ingest.Pipeline;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class PipelineStore extends AbstractLifecycleComponent {

    public final static String INDEX = ".ingest";
    public final static String TYPE = "pipeline";

    private final ThreadPool threadPool;
    private final ClusterService clusterService;
    private final TimeValue pipelineUpdateInterval;
    private final PipelineConfigDocReader configDocReader;
    private final Map<String, Processor.Builder.Factory> processorFactoryRegistry;

    private volatile Map<String, PipelineReference> pipelines = new HashMap<>();

    @Inject
    public PipelineStore(Settings settings, ThreadPool threadPool, ClusterService clusterService, PipelineConfigDocReader configDocReader, Map<String, Processor.Builder.Factory> processors) {
        super(settings);
        this.threadPool = threadPool;
        this.clusterService = clusterService;
        this.pipelineUpdateInterval = settings.getAsTime("ingest.pipeline.store.update.interval", TimeValue.timeValueSeconds(1));
        this.configDocReader = configDocReader;
        this.processorFactoryRegistry = Collections.unmodifiableMap(processors);
        clusterService.add(new PipelineStoreListener());
    }

    @Override
    protected void doStart() {
    }

    @Override
    protected void doStop() {
    }

    @Override
    protected void doClose() {
    }

    public Pipeline get(String id) {
        PipelineReference ref = pipelines.get(id);
        if (ref != null) {
            return ref.getPipeline();
        } else {
            return null;
        }
    }

    void updatePipelines() {
        int changed = 0;
        Map<String, PipelineReference> newPipelines = new HashMap<>(pipelines);
        for (SearchHit hit : configDocReader.readAll()) {
            String pipelineId = hit.getId();
            BytesReference pipelineSource = hit.getSourceRef();
            PipelineReference previous = newPipelines.get(pipelineId);
            if (previous != null) {
                if (previous.getSource().equals(pipelineSource)) {
                    continue;
                }
            }

            changed++;
            Pipeline.Builder builder = new Pipeline.Builder(hit.sourceAsMap(), processorFactoryRegistry);
            newPipelines.put(pipelineId, new PipelineReference(builder.build(), hit.getVersion(), pipelineSource));
        }

        if (changed != 0) {
            logger.debug("adding or updating [{}] pipelines", changed);
            pipelines = newPipelines;
        } else {
            logger.debug("adding no new pipelines");
        }
    }

    void startUpdateWorker() {
        if (lifecycleState() == Lifecycle.State.STARTED) {
            threadPool.schedule(pipelineUpdateInterval, ThreadPool.Names.GENERIC, new Updater());
        }
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

    static class PipelineReference {

        private final Pipeline pipeline;
        private final long version;
        private final BytesReference source;

        PipelineReference(Pipeline pipeline, long version, BytesReference source) {
            this.pipeline = pipeline;
            this.version = version;
            this.source = source;
        }

        public Pipeline getPipeline() {
            return pipeline;
        }

        public long getVersion() {
            return version;
        }

        public BytesReference getSource() {
            return source;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            PipelineReference holder = (PipelineReference) o;
            return source.equals(holder.source);
        }

        @Override
        public int hashCode() {
            return source.hashCode();
        }
    }

}
