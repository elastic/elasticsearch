/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher;


import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.xpack.watcher.execution.ExecutionService;
import org.elasticsearch.xpack.watcher.support.WatcherIndexTemplateRegistry;
import org.elasticsearch.xpack.watcher.support.init.proxy.WatcherClientProxy;
import org.elasticsearch.xpack.watcher.trigger.TriggerService;
import org.elasticsearch.xpack.watcher.watch.Watch;
import org.elasticsearch.xpack.watcher.watch.WatchStoreUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.xpack.watcher.support.Exceptions.illegalState;
import static org.elasticsearch.xpack.watcher.watch.Watch.DOC_TYPE;
import static org.elasticsearch.xpack.watcher.watch.Watch.INDEX;


public class WatcherService extends AbstractComponent {

    private final TriggerService triggerService;
    private final ExecutionService executionService;
    private final WatcherIndexTemplateRegistry watcherIndexTemplateRegistry;
    // package-private for testing
    final AtomicReference<WatcherState> state = new AtomicReference<>(WatcherState.STOPPED);
    private final TimeValue scrollTimeout;
    private final int scrollSize;
    private final Watch.Parser parser;
    private final WatcherClientProxy client;

    public WatcherService(Settings settings, TriggerService triggerService, ExecutionService executionService,
                          WatcherIndexTemplateRegistry watcherIndexTemplateRegistry, Watch.Parser parser, WatcherClientProxy client) {
        super(settings);
        this.triggerService = triggerService;
        this.executionService = executionService;
        this.watcherIndexTemplateRegistry = watcherIndexTemplateRegistry;
        this.scrollTimeout = settings.getAsTime("xpack.watcher.watch.scroll.timeout", TimeValue.timeValueSeconds(30));
        this.scrollSize = settings.getAsInt("xpack.watcher.watch.scroll.size", 100);
        this.parser = parser;
        this.client = client;
    }

    public void start(ClusterState clusterState) throws Exception {
        if (state.compareAndSet(WatcherState.STOPPED, WatcherState.STARTING)) {
            try {
                logger.debug("starting watch service...");
                watcherIndexTemplateRegistry.addTemplatesIfMissing();
                executionService.start(clusterState);
                triggerService.start(loadWatches(clusterState));

                state.set(WatcherState.STARTED);
                logger.debug("watch service has started");
            } catch (Exception e) {
                state.set(WatcherState.STOPPED);
                throw e;
            }
        } else {
            logger.debug("not starting watcher, because its state is [{}] while [{}] is expected", state, WatcherState.STOPPED);
        }
    }

    public boolean validate(ClusterState state) {
        return executionService.validate(state);
    }

    public void stop() {
        if (state.compareAndSet(WatcherState.STARTED, WatcherState.STOPPING)) {
            logger.debug("stopping watch service...");
            triggerService.stop();
            executionService.stop();
            state.set(WatcherState.STOPPED);
            logger.debug("watch service has stopped");
        } else {
            logger.debug("not stopping watcher, because its state is [{}] while [{}] is expected", state, WatcherState.STARTED);
        }
    }

    /**
     * This reads all watches from the .watches index/alias and puts them into memory for a short period of time,
     * before they are fed into the trigger service.
     *
     * This is only invoked when a node becomes master, so either on start up or when a master node switches - while watcher is started up
     */
    private Collection<Watch> loadWatches(ClusterState clusterState) {
        IndexMetaData indexMetaData = WatchStoreUtils.getConcreteIndex(INDEX, clusterState.metaData());

        // no index exists, all good, we can start
        if (indexMetaData == null) {
            return Collections.emptyList();
        }

        RefreshResponse refreshResponse = client.refresh(new RefreshRequest(INDEX));
        if (refreshResponse.getSuccessfulShards() < indexMetaData.getNumberOfShards()) {
            throw illegalState("not all required shards have been refreshed");
        }

        List<Watch> watches = new ArrayList<>();
        SearchRequest searchRequest = new SearchRequest(INDEX)
                .types(DOC_TYPE)
                .scroll(scrollTimeout)
                .source(new SearchSourceBuilder()
                        .size(scrollSize)
                        .sort(SortBuilders.fieldSort("_doc"))
                        .version(true));
        SearchResponse response = client.search(searchRequest, null);
        try {
            if (response.getTotalShards() != response.getSuccessfulShards()) {
                throw new ElasticsearchException("Partial response while loading watches");
            }

            while (response.getHits().hits().length != 0) {
                for (SearchHit hit : response.getHits()) {
                    String id = hit.getId();
                    try {
                        Watch watch = parser.parse(id, true, hit.getSourceRef(), XContentType.JSON);
                        watch.version(hit.version());
                        watches.add(watch);
                    } catch (Exception e) {
                        logger.error((Supplier<?>) () -> new ParameterizedMessage("couldn't load watch [{}], ignoring it...", id), e);
                    }
                }
                response = client.searchScroll(response.getScrollId(), scrollTimeout);
            }
        } finally {
            client.clearScroll(response.getScrollId());
        }
        return watches;
    }



    public WatcherState state() {
        return state.get();
    }

    public Map<String, Object> usageStats() {
        Map<String, Object> innerMap = executionService.usageStats();
        return innerMap;
    }

    /**
     * Something deleted or closed the {@link Watch#INDEX} and thus we need to do some cleanup to prevent further execution of watches
     * as those watches cannot be updated anymore
     */
    public void watchIndexDeletedOrClosed() {
        executionService.clearExecutions();
    }
}
