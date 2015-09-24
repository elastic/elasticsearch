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

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.collect.CopyOnWriteHashMap;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.ingest.Pipeline;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortOrder;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

public class PipelineStore extends AbstractLifecycleComponent {

    public final static String INDEX = ".pipelines";
    public final static String TYPE = "pipeline";

    private Client client;
    private final Injector injector;

    private volatile Updater updater;
    private volatile CopyOnWriteHashMap<String, Pipeline> pipelines = new CopyOnWriteHashMap<>();

    @Inject
    public PipelineStore(Settings settings, Injector injector) {
        super(settings);
        this.injector = injector;
    }

    @Override
    protected void doStart() {
        client = injector.getInstance(Client.class);
        updater = new Updater();
        // TODO: start when local cluster state isn't blocked: ([SERVICE_UNAVAILABLE/1/state not recovered / initialized])
        updater.start();
    }

    @Override
    protected void doStop() {
        updater.shutdown();
    }

    @Override
    protected void doClose() {
    }

    public Pipeline get(String id) {
        return pipelines.get(id);
    }

    void updatePipelines() {
        Map<String, Pipeline> pipelines = new HashMap<>();
        SearchResponse searchResponse = client.prepareSearch(INDEX)
                .setScroll(TimeValue.timeValueMinutes(1))
                .addSort("_doc", SortOrder.ASC)
                .setIndicesOptions(IndicesOptions.lenientExpandOpen())
                .get();
        logger.info("Loading [{}] pipelines", searchResponse.getHits().totalHits());
        do {
            for (SearchHit hit : searchResponse.getHits()) {
                logger.info("Loading pipeline [{}] with source [{}]", hit.getId(), hit.sourceAsString());
                Pipeline.Builder builder = new Pipeline.Builder(hit.sourceAsMap());
                pipelines.put(hit.getId(), builder.build());
            }
            searchResponse = client.prepareSearchScroll(searchResponse.getScrollId()).get();
        } while (searchResponse.getHits().getHits().length != 0);
        PipelineStore.this.pipelines = PipelineStore.this.pipelines.copyAndPutAll(pipelines);
    }

    class Updater extends Thread {

        private volatile boolean running = true;
        private final CountDownLatch latch = new CountDownLatch(1);

        public Updater() {
            super(EsExecutors.threadName(settings, "[updater]"));
        }

        @Override
        public void run() {
            try {
                while (running) {
                    try {
                        Thread.sleep(3000);
                        updatePipelines();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } catch (Exception e) {
                        logger.error("update error", e);
                    }
                }
            } finally {
                latch.countDown();
            }
        }

        public void shutdown() {
            running = false;
            try {
                interrupt();
                latch.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

    }

}
