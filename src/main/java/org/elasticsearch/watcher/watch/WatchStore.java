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
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.cluster.ClusterState;
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
import org.elasticsearch.watcher.WatcherException;
import org.elasticsearch.watcher.support.TemplateUtils;
import org.elasticsearch.watcher.support.init.proxy.ClientProxy;

import java.io.IOException;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 */
public class WatchStore extends AbstractComponent {

    public static final String INDEX = ".watches";
    public static final String INDEX_TEMPLATE = "watches";
    public static final String DOC_TYPE = "watch";

    private final ClientProxy client;
    private final TemplateUtils templateUtils;
    private final Watch.Parser watchParser;

    private final ConcurrentMap<String, Watch> watches;
    private final AtomicBoolean started = new AtomicBoolean(false);

    private final int scrollSize;
    private final TimeValue scrollTimeout;

    @Inject
    public WatchStore(Settings settings, ClientProxy client, TemplateUtils templateUtils, Watch.Parser watchParser) {
        super(settings);
        this.client = client;
        this.templateUtils = templateUtils;
        this.watchParser = watchParser;
        this.watches = ConcurrentCollections.newConcurrentMap();

        this.scrollTimeout = componentSettings.getAsTime("scroll.timeout", TimeValue.timeValueSeconds(30));
        this.scrollSize = componentSettings.getAsInt("scroll.size", 100);
    }

    public void start(ClusterState state) {
        if (started.get()) {
            logger.debug("watch store already started");
            return;
        }

        IndexMetaData watchesIndexMetaData = state.getMetaData().index(INDEX);
        if (watchesIndexMetaData != null) {
            try {
                int count = loadWatches(watchesIndexMetaData.numberOfShards());
                logger.debug("loaded [{}] watches from the watches index [{}]", count, INDEX);
                templateUtils.ensureIndexTemplateIsLoaded(state, INDEX_TEMPLATE);
                started.set(true);
            } catch (Exception e) {
                logger.debug("failed to load watches for watch index [{}]", e, INDEX);
                watches.clear();
            }
        } else {
            templateUtils.ensureIndexTemplateIsLoaded(state, INDEX_TEMPLATE);
            started.set(true);
        }
    }

    public boolean validate(ClusterState state) {
        IndexMetaData watchesIndexMetaData = state.getMetaData().index(INDEX);
        if (watchesIndexMetaData == null) {
            logger.debug("watches index [{}] doesn't exist, so we can start", INDEX);
            return true;
        }
        if (state.routingTable().index(INDEX).allPrimaryShardsActive()) {
            logger.debug("watches index [{}] exists and all primary shards are started, so we can start", INDEX);
            return true;
        }
        return false;
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
    public WatchPut put(Watch watch) {
        ensureStarted();
        IndexRequest indexRequest = createIndexRequest(watch.id(), watch.getAsBytes());
        IndexResponse response = client.index(indexRequest);
        watch.status().version(response.getVersion());
        Watch previous = watches.put(watch.id(), watch);
        return new WatchPut(previous, watch, response);
    }

    /**
     * Updates and persists the status of the given watch
     */
    public void updateStatus(Watch watch) throws IOException {
        // at the moment we store the status together with the watch,
        // so we just need to update the watch itself
        // TODO: consider storing the status in a different documment (watch_status doc) (must smaller docs... faster for frequent updates)
        if (watch.status().dirty()) {
            update(watch);
        }
    }

    /**
     * Updates and persists the given watch
     */
    void update(Watch watch) throws IOException {
        ensureStarted();
        assert watch == watches.get(watch.id()) : "update watch can only be applied to an already loaded watch";
        BytesReference source = JsonXContent.contentBuilder().value(watch).bytes();
        IndexResponse response = client.index(createIndexRequest(watch.id(), source));
        watch.status().version(response.getVersion());
        watch.status().dirty(false);
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
                        try {
                            Watch watch = watchParser.parse(name, true, hit.getSourceRef());
                            watch.status().version(hit.version());
                            watches.put(name, watch);
                            count++;
                        } catch (WatcherException we) {
                            logger.error("while loading watches, failed to parse [{}]", we, name);
                            throw we;
                        }
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
