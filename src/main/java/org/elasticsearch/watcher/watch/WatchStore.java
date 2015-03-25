/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.watch;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.*;
import org.elasticsearch.watcher.WatcherException;
import org.elasticsearch.watcher.support.Callback;
import org.elasticsearch.watcher.support.TemplateUtils;
import org.elasticsearch.watcher.support.init.proxy.ClientProxy;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 */
public class WatchStore extends AbstractComponent {

    public static final String INDEX = ".watches";
    public static final String INDEX_TEMPLATE = "watches";
    public static final String DOC_TYPE = "watch";

    private final ClientProxy client;
    private final TemplateUtils templateUtils;
    private final Watch.Parser watchParser;
    private final ClusterService clusterService;
    private final ThreadPool threadPool;

    private final ConcurrentMap<String, Watch> watches;
    private final AtomicBoolean started = new AtomicBoolean(false);
    private final AtomicInteger initializationRetries = new AtomicInteger();

    private final int scrollSize;
    private final TimeValue scrollTimeout;

    @Inject
    public WatchStore(Settings settings, ClientProxy client, TemplateUtils templateUtils, Watch.Parser watchParser,
                      ClusterService clusterService, ThreadPool threadPool) {
        super(settings);
        this.client = client;
        this.templateUtils = templateUtils;
        this.watchParser = watchParser;
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.watches = ConcurrentCollections.newConcurrentMap();

        this.scrollTimeout = componentSettings.getAsTime("scroll.timeout", TimeValue.timeValueSeconds(30));
        this.scrollSize = componentSettings.getAsInt("scroll.size", 100);
    }

    public void start(ClusterState state, Callback<ClusterState> callback) {
        if (started.get()) {
            callback.onSuccess(state);
            return;
        }

        IndexMetaData watchesIndexMetaData = state.getMetaData().index(INDEX);
        if (watchesIndexMetaData == null) {
            logger.trace("watches index [{}] was not found. skipping loading watches...", INDEX);
            templateUtils.ensureIndexTemplateIsLoaded(state, INDEX_TEMPLATE);
            started.set(true);
            callback.onSuccess(state);
            return;
        }

        if (state.routingTable().index(INDEX).allPrimaryShardsActive()) {
            logger.debug("watches index [{}] found with all active primary shards. loading watches...", INDEX);
            try {
                int count = loadWatches(watchesIndexMetaData.numberOfShards());
                logger.debug("loaded [{}] watches from the watches index [{}]", count, INDEX);
            } catch (Exception e) {
                logger.debug("failed to load watches for watch index [{}]. scheduled to retry watches loading...", e, INDEX);
                watches.clear();
                retry(callback);
                return;
            }
            templateUtils.ensureIndexTemplateIsLoaded(state, INDEX_TEMPLATE);
            started.set(true);
            callback.onSuccess(state);
        } else {
            logger.debug("not all primary shards of the watches index [{}] are started. scheduled to retry loading watches...", INDEX);
            retry(callback);
        }
    }

    public boolean started() {
        return started.get();
    }

    public void stop() {
        if (started.compareAndSet(true, false)) {
            watches.clear();
            logger.info("stopped watch store");
        }
    }

    /**
     * Returns the watch with the specified name otherwise <code>null</code> is returned.
     */
    public Watch get(String name) {
        ensureStarted();
        return watches.get(name);
    }

    /**
     * Creates an watch with the specified name and source. If an watch with the specified name already exists it will
     * get overwritten.
     */
    public WatchPut put(String name, BytesReference source) {
        ensureStarted();
        Watch watch = watchParser.parse(name, false, source);
        IndexRequest indexRequest = createIndexRequest(name, source);
        IndexResponse response = client.index(indexRequest);
        watch.status().version(response.getVersion());
        Watch previous = watches.put(name, watch);
        return new WatchPut(previous, watch, response);
    }

    /**
     * Updates and persists the status of the given watch
     */
    void updateStatus(Watch watch) throws IOException {
        // at the moment we store the status together with the watch,
        // so we just need to update the watch itself
        // TODO: consider storing the status in a different documment (watch_status doc) (must smaller docs... faster for frequent updates)
        update(watch);
    }

    /**
     * Updates and persists the given watch
     */
    void update(Watch watch) throws IOException {
        ensureStarted();
        assert watch == watches.get(watch.name()) : "update watch can only be applied to an already loaded watch";
        BytesReference source = JsonXContent.contentBuilder().value(watch).bytes();
        IndexResponse response = client.index(createIndexRequest(watch.name(), source));
        watch.status().version(response.getVersion());
        // Don't need to update the watches, since we are working on an instance from it.
    }

    /**
     * Deletes the watch with the specified name if exists
     */
    public WatchDelete delete(String name) {
        ensureStarted();
        Watch watch = watches.remove(name);
        // even if the watch was not found in the watch map, we should still try to delete it
        // from the index, just to make sure we don't leave traces of it
        DeleteRequest request = new DeleteRequest(INDEX, DOC_TYPE, name);
        if (watch != null) {
            request.version(watch.status().version());
        }
        DeleteResponse response = client.delete(request).actionGet();
        return new WatchDelete(response);
    }

    public ConcurrentMap<String, Watch> watches() {
        return watches;
    }

    IndexRequest createIndexRequest(String name, BytesReference source) {
        IndexRequest indexRequest = new IndexRequest(INDEX, DOC_TYPE, name);
        indexRequest.listenerThreaded(false);
        indexRequest.source(source, false);
        return indexRequest;
    }

    private void retry(final Callback<ClusterState> callback) {
        ClusterStateListener clusterStateListener = new ClusterStateListener() {
            @Override
            public void clusterChanged(ClusterChangedEvent event) {
                final ClusterState state = event.state();
                IndexMetaData watchesIndexMetaData = state.getMetaData().index(INDEX);
                if (watchesIndexMetaData != null) {
                    if (state.routingTable().index(INDEX).allPrimaryShardsActive()) {
                        // Remove listener, so that it doesn't get called on the next cluster state update:
                        assert initializationRetries.decrementAndGet() == 0 : "Only one retry can run at the time";
                        clusterService.remove(this);
                        // We fork into another thread, because start(...) is expensive and we can't call this from the cluster update thread.
                        threadPool.executor(ThreadPool.Names.GENERIC).execute(new Runnable() {
                            @Override
                            public void run() {
                                try {
                                    start(state, callback);
                                } catch (Exception e) {
                                    callback.onFailure(e);
                                }
                            }
                        });
                    }
                }
            }
        };
        clusterService.add(clusterStateListener);
        assert initializationRetries.incrementAndGet() == 1 : "Only one retry can run at the time";
    }

    /**
     * scrolls all the watch documents in the watches index, parses them, and loads them into
     * the given map.
     */
    int loadWatches(int numPrimaryShards) {
        assert watches.isEmpty() : "no watches should reside, but there are [" + watches.size() + "] watches.";
        RefreshResponse refreshResponse = client.refresh(new RefreshRequest(INDEX));
        if (refreshResponse.getSuccessfulShards() < numPrimaryShards) {
            throw new WatcherException("not all required shards have been refreshed");
        }

        int count = 0;
        SearchRequest searchRequest = new SearchRequest(INDEX)
                .types(DOC_TYPE)
                .preference("_primary")
                .searchType(SearchType.SCAN)
                .scroll(scrollTimeout)
                .source(new SearchSourceBuilder()
                        .size(scrollSize)
                        .version(true));
        SearchResponse response = client.search(searchRequest);
        try {
            if (response.getTotalShards() != response.getSuccessfulShards()) {
                throw new ElasticsearchException("Partial response while loading watches");
            }

            if (response.getHits().getTotalHits() > 0) {
                response = client.searchScroll(response.getScrollId(), scrollTimeout);
                while (response.getHits().hits().length != 0) {
                    for (SearchHit hit : response.getHits()) {
                        String name = hit.getId();
                        Watch watch = watchParser.parse(name, true, hit.getSourceRef());
                        watch.status().version(hit.version());
                        watches.put(name, watch);
                        count++;
                    }
                    response = client.searchScroll(response.getScrollId(), scrollTimeout);
                }
            }
        } finally {
            client.clearScroll(response.getScrollId());
        }
        return count;
    }

    private void ensureStarted() {
        if (!started.get()) {
            throw new ElasticsearchIllegalStateException("watch store not started");
        }
    }

    public class WatchPut {

        private final Watch previous;
        private final Watch current;
        private final IndexResponse response;

        public WatchPut(Watch previous, Watch current, IndexResponse response) {
            this.current = current;
            this.previous = previous;
            this.response = response;
        }

        public Watch current() {
            return current;
        }

        public Watch previous() {
            return previous;
        }

        public IndexResponse indexResponse() {
            return response;
        }
    }

    public class WatchDelete {

        private final DeleteResponse response;

        public WatchDelete(DeleteResponse response) {
            this.response = response;
        }

        public DeleteResponse deleteResponse() {
            return response;
        }
    }

}
