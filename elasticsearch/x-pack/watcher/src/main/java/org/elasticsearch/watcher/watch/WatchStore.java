/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.watch;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.watcher.support.init.proxy.WatcherClientProxy;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.watcher.support.Exceptions.illegalState;

/**
 */
public class WatchStore extends AbstractComponent {

    public static final String INDEX = ".watches";
    public static final String DOC_TYPE = "watch";

    private final WatcherClientProxy client;
    private final Watch.Parser watchParser;

    private final ConcurrentMap<String, Watch> watches;
    private final AtomicBoolean started = new AtomicBoolean(false);

    private final int scrollSize;
    private final TimeValue scrollTimeout;

    @Inject
    public WatchStore(Settings settings, WatcherClientProxy client, Watch.Parser watchParser) {
        super(settings);
        this.client = client;
        this.watchParser = watchParser;
        this.watches = ConcurrentCollections.newConcurrentMap();

        this.scrollTimeout = settings.getAsTime("xpack.watcher.watch.scroll.timeout", TimeValue.timeValueSeconds(30));
        this.scrollSize = settings.getAsInt("xpack.watcher.watch.scroll.size", 100);
    }

    public void start(ClusterState state) throws Exception {
        if (started.get()) {
            logger.debug("watch store already started");
            return;
        }

        IndexMetaData watchesIndexMetaData = state.getMetaData().index(INDEX);
        if (watchesIndexMetaData != null) {
            try {
                int count = loadWatches(watchesIndexMetaData.getNumberOfShards());
                logger.debug("loaded [{}] watches from the watches index [{}]", count, INDEX);
                started.set(true);
            } catch (Exception e) {
                logger.debug("failed to load watches for watch index [{}]", e, INDEX);
                watches.clear();
                throw e;
            }
        } else {
            started.set(true);
        }
    }

    public boolean validate(ClusterState state) {
        IndexMetaData watchesIndexMetaData = state.getMetaData().index(INDEX);
        if (watchesIndexMetaData == null) {
            logger.debug("index [{}] doesn't exist, so we can start", INDEX);
            return true;
        }
        if (state.routingTable().index(INDEX).allPrimaryShardsActive()) {
            logger.debug("index [{}] exists and all primary shards are started, so we can start", INDEX);
            return true;
        } else {
            logger.debug("not all primary shards active for index [{}], so we cannot start", INDEX);
            return false;
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
     * Returns the watch with the specified id otherwise <code>null</code> is returned.
     */
    public Watch get(String id) {
        ensureStarted();
        return watches.get(id);
    }

    /**
     * Creates an watch if this watch already exists it will be overwritten
     */
    public WatchPut put(Watch watch) throws IOException {
        ensureStarted();
        IndexRequest indexRequest = createIndexRequest(watch.id(), watch.getAsBytes(), Versions.MATCH_ANY);
        IndexResponse response = client.index(indexRequest, (TimeValue) null);
        watch.status().version(response.getVersion());
        watch.version(response.getVersion());
        Watch previous = watches.put(watch.id(), watch);
        return new WatchPut(previous, watch, response);
    }

    /**
     * Updates and persists the status of the given watch
     */
    public void updateStatus(Watch watch) throws IOException {
        ensureStarted();
        if (!watch.status().dirty()) {
            return;
        }

        // at the moment we store the status together with the watch,
        // so we just need to update the watch itself
        // TODO: consider storing the status in a different documment (watch_status doc) (must smaller docs... faster for frequent updates)
        XContentBuilder source = JsonXContent.contentBuilder().
                startObject()
                    .field(Watch.Field.STATUS.getPreferredName(), watch.status(), ToXContent.EMPTY_PARAMS)
                .endObject();
        UpdateRequest updateRequest = new UpdateRequest(INDEX, DOC_TYPE, watch.id());
        updateRequest.doc(source);
        updateRequest.version(watch.version());

        UpdateResponse response = client.update(updateRequest);
        watch.status().version(response.getVersion());
        watch.version(response.getVersion());
        watch.status().resetDirty();
        // Don't need to update the watches, since we are working on an instance from it.
    }

    /**
     * Deletes the watch with the specified id if exists
     */
    public WatchDelete delete(String id, boolean force) {
        ensureStarted();
        Watch watch = watches.remove(id);
        // even if the watch was not found in the watch map, we should still try to delete it
        // from the index, just to make sure we don't leave traces of it
        DeleteRequest request = new DeleteRequest(INDEX, DOC_TYPE, id);
        if (watch != null && !force) {
            request.version(watch.version());
        }
        DeleteResponse response = client.delete(request);
        // Another operation may hold the Watch instance, so lets set the version for consistency:
        if (watch != null) {
            watch.version(response.getVersion());
        }
        return new WatchDelete(response);
    }

    public Collection<Watch> watches() {
        return watches.values();
    }

    public Collection<Watch> activeWatches() {
        Set<Watch> watches = new HashSet<>();
        for (Watch watch : watches()) {
            if (watch.status().state().isActive()) {
                watches.add(watch);
            }
        }
        return watches;
    }

    public int watchCount() {
        return watches.size();
    }

    IndexRequest createIndexRequest(String id, BytesReference source, long version) {
        IndexRequest indexRequest = new IndexRequest(INDEX, DOC_TYPE, id);
        indexRequest.source(source.toBytes());
        indexRequest.version(version);
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
            throw illegalState("not all required shards have been refreshed");
        }

        int count = 0;
        SearchRequest searchRequest = new SearchRequest(INDEX)
                .types(DOC_TYPE)
                .preference("_primary")
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
                        Watch watch = watchParser.parse(id, true, hit.getSourceRef());
                        watch.status().version(hit.version());
                        watch.version(hit.version());
                        watches.put(id, watch);
                        count++;
                    } catch (Exception e) {
                        logger.error("couldn't load watch [{}], ignoring it...", e, id);
                    }
                }
                response = client.searchScroll(response.getScrollId(), scrollTimeout);
            }
        } finally {
            client.clearScroll(response.getScrollId());
        }
        return count;
    }

    private void ensureStarted() {
        if (!started.get()) {
            throw new IllegalStateException("watch store not started");
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
