/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.execution;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.Preference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.watcher.execution.TriggeredWatchStoreField;
import org.elasticsearch.xpack.core.watcher.execution.Wid;
import org.elasticsearch.xpack.core.watcher.watch.Watch;
import org.elasticsearch.xpack.watcher.watch.WatchStoreUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.core.ClientHelper.WATCHER_ORIGIN;

public class TriggeredWatchStore {

    private static final Logger logger = LogManager.getLogger(TriggeredWatchStore.class);

    private final int scrollSize;
    private final Client client;
    private final TimeValue scrollTimeout;
    private final TriggeredWatch.Parser triggeredWatchParser;

    private final TimeValue defaultBulkTimeout;
    private final TimeValue defaultSearchTimeout;
    private final BulkProcessor bulkProcessor;

    public TriggeredWatchStore(Settings settings, Client client, TriggeredWatch.Parser triggeredWatchParser, BulkProcessor bulkProcessor) {
        this.scrollSize = settings.getAsInt("xpack.watcher.execution.scroll.size", 1000);
        this.client = ClientHelper.clientWithOrigin(client, WATCHER_ORIGIN);
        this.scrollTimeout = settings.getAsTime("xpack.watcher.execution.scroll.timeout", TimeValue.timeValueMinutes(5));
        this.defaultBulkTimeout = settings.getAsTime("xpack.watcher.internal.ops.bulk.default_timeout", TimeValue.timeValueSeconds(120));
        this.defaultSearchTimeout = settings.getAsTime("xpack.watcher.internal.ops.search.default_timeout", TimeValue.timeValueSeconds(30));
        this.triggeredWatchParser = triggeredWatchParser;
        this.bulkProcessor = bulkProcessor;
    }

    public void putAll(final List<TriggeredWatch> triggeredWatches, final ActionListener<BulkResponse> listener) throws IOException {
        if (triggeredWatches.isEmpty()) {
            listener.onResponse(new BulkResponse(new BulkItemResponse[]{}, 0));
            return;
        }

        client.bulk(createBulkRequest(triggeredWatches), listener);
    }

    public BulkResponse putAll(final List<TriggeredWatch> triggeredWatches) throws IOException {
        PlainActionFuture<BulkResponse> future = PlainActionFuture.newFuture();
        putAll(triggeredWatches, future);
        return future.actionGet(defaultBulkTimeout);
    }

    /**
     * Create a bulk request from the triggered watches with a specified document type
     * @param triggeredWatches  The list of triggered watches
     * @return                  The bulk request for the triggered watches
     * @throws IOException      If a triggered watch could not be parsed to JSON, this exception is thrown
     */
    private BulkRequest createBulkRequest(final List<TriggeredWatch> triggeredWatches) throws IOException {
        BulkRequest request = new BulkRequest();
        for (TriggeredWatch triggeredWatch : triggeredWatches) {
            IndexRequest indexRequest = new IndexRequest(TriggeredWatchStoreField.INDEX_NAME).id(triggeredWatch.id().value());
            try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
                triggeredWatch.toXContent(builder, ToXContent.EMPTY_PARAMS);
                indexRequest.source(builder);
            }
            indexRequest.opType(IndexRequest.OpType.CREATE);
            request.add(indexRequest);
        }
        return request;
    }

    /**
     * Delete a triggered watch entry.
     * Note that this happens asynchronously, as these kind of requests are batched together to reduce the amount of concurrent requests.
     *
     * @param wid The ID os the triggered watch id
     */
    public void delete(Wid wid) {
        DeleteRequest request = new DeleteRequest(TriggeredWatchStoreField.INDEX_NAME, wid.value());
        bulkProcessor.add(request);
    }

    /**
     * Checks if any of the loaded watches has been put into the triggered watches index for immediate execution
     *
     * Note: This is executing a blocking call over the network, thus a potential source of problems
     *
     * @param watches       The list of watches that will be loaded here
     * @param clusterState  The current cluster state
     * @return              A list of triggered watches that have been started to execute somewhere else but not finished
     */
    public Collection<TriggeredWatch> findTriggeredWatches(Collection<Watch> watches, ClusterState clusterState) {
        if (watches.isEmpty()) {
            return Collections.emptyList();
        }

        // non existing index, return immediately
        IndexMetadata indexMetadata = WatchStoreUtils.getConcreteIndex(TriggeredWatchStoreField.INDEX_NAME, clusterState.metadata());
        if (indexMetadata == null) {
            return Collections.emptyList();
        }

        try {
            RefreshRequest request = new RefreshRequest(TriggeredWatchStoreField.INDEX_NAME);
            client.admin().indices().refresh(request).actionGet(TimeValue.timeValueSeconds(5));
        } catch (IndexNotFoundException e) {
            return Collections.emptyList();
        }

        Set<String> ids = watches.stream().map(Watch::id).collect(Collectors.toSet());
        Collection<TriggeredWatch> triggeredWatches = new ArrayList<>(ids.size());

        SearchRequest searchRequest = new SearchRequest(TriggeredWatchStoreField.INDEX_NAME)
            .scroll(scrollTimeout)
            .preference(Preference.LOCAL.toString())
            .source(new SearchSourceBuilder()
                .size(scrollSize)
                .sort(SortBuilders.fieldSort("_doc"))
                .version(true));

        SearchResponse response = null;
        try {
            response = client.search(searchRequest).actionGet(defaultSearchTimeout);
            logger.debug("trying to find triggered watches for ids {}: found [{}] docs", ids, response.getHits().getTotalHits().value);
            while (response.getHits().getHits().length != 0) {
                for (SearchHit hit : response.getHits()) {
                    Wid wid = new Wid(hit.getId());
                    if (ids.contains(wid.watchId())) {
                        TriggeredWatch triggeredWatch = triggeredWatchParser.parse(hit.getId(), hit.getVersion(), hit.getSourceRef());
                        triggeredWatches.add(triggeredWatch);
                    }
                }
                SearchScrollRequest request = new SearchScrollRequest(response.getScrollId());
                request.scroll(scrollTimeout);
                response = client.searchScroll(request).actionGet(defaultSearchTimeout);
            }
        } finally {
            if (response != null) {
                ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
                clearScrollRequest.addScrollId(response.getScrollId());
                client.clearScroll(clearScrollRequest).actionGet(scrollTimeout);
            }
        }

        return triggeredWatches;
    }

    public static boolean validate(ClusterState state) {
        IndexMetadata indexMetadata = WatchStoreUtils.getConcreteIndex(TriggeredWatchStoreField.INDEX_NAME, state.metadata());
        return indexMetadata == null || (indexMetadata.getState() == IndexMetadata.State.OPEN &&
            state.routingTable().index(indexMetadata.getIndex()).allPrimaryShardsActive());
    }
}
