/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.reindex;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.replication.ReplicationRequest;
import org.elasticsearch.client.internal.ElasticsearchClient;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import static org.elasticsearch.index.reindex.AbstractBulkByScrollRequest.DEFAULT_SCROLL_SIZE;
import static org.elasticsearch.index.reindex.AbstractBulkByScrollRequest.DEFAULT_SCROLL_TIMEOUT;

public class ReindexRequestBuilder extends AbstractBulkIndexByScrollRequestBuilder<ReindexRequest, ReindexRequestBuilder> {
    private final IndexRequestBuilder destinationBuilder;
    private RemoteInfo remoteInfo;
    private Script script;
    private Integer maxDocs;
    private Boolean abortOnVersionConflict;
    private Boolean refresh;
    private TimeValue timeout;
    private ActiveShardCount waitForActiveShards;
    private TimeValue retryBackoffInitialTime;
    private Integer maxRetries;
    private Float requestsPerSecond;
    private Boolean shouldStoreResult;
    private Integer slices;

    public ReindexRequestBuilder(ElasticsearchClient client) {
        this(client, new SearchRequestBuilder(client), new IndexRequestBuilder(client));
    }

    private ReindexRequestBuilder(ElasticsearchClient client, SearchRequestBuilder search, IndexRequestBuilder destination) {
        super(client, ReindexAction.INSTANCE, search, null);
        this.destinationBuilder = destination;
        initSourceSearchRequest();
    }

    /*
     * The following is normally done within the ReindexRequest constructor. But that constructor is not called until the request()
     * method is called once this builder is complete. Doing it there blows away changes made to the source request.
     */
    private void initSourceSearchRequest() {
        source().request().source(new SearchSourceBuilder());
        source().request().scroll(DEFAULT_SCROLL_TIMEOUT);
        source().request().source(new SearchSourceBuilder());
        source().request().source().size(DEFAULT_SCROLL_SIZE);
    }

    @Override
    protected ReindexRequestBuilder self() {
        return this;
    }

    public IndexRequestBuilder destination() {
        return destinationBuilder;
    }

    /**
     * Set the destination index.
     */
    public ReindexRequestBuilder destination(String index) {
        destinationBuilder.setIndex(index);
        return this;
    }

    /**
     * Setup reindexing from a remote cluster.
     */
    public ReindexRequestBuilder setRemoteInfo(RemoteInfo remoteInfo) {
        this.remoteInfo = remoteInfo;
        return this;
    }

    public ReindexRequestBuilder script(Script script) {
        this.script = script;
        return this;
    }

    /**
     * Maximum number of processed documents. Defaults to processing all
     * documents.
     */
    public ReindexRequestBuilder maxDocs(int maxDocs) {
        this.maxDocs = maxDocs;
        return this;
    }

    /**
     * Set whether or not version conflicts cause the action to abort.
     */
    public ReindexRequestBuilder abortOnVersionConflict(boolean abortOnVersionConflict) {
        this.abortOnVersionConflict = abortOnVersionConflict;
        return this;
    }

    /**
     * Call refresh on the indexes we've written to after the request ends?
     */
    public ReindexRequestBuilder refresh(boolean refresh) {
        this.refresh = refresh;
        return this;
    }

    /**
     * Timeout to wait for the shards on to be available for each bulk request.
     */
    public ReindexRequestBuilder timeout(TimeValue timeout) {
        this.timeout = timeout;
        return this;
    }

    /**
     * The number of shard copies that must be active before proceeding with the write.
     * See {@link ReplicationRequest#waitForActiveShards(ActiveShardCount)} for details.
     */
    public ReindexRequestBuilder waitForActiveShards(ActiveShardCount activeShardCount) {
        this.waitForActiveShards = activeShardCount;
        return this;
    }

    /**
     * Initial delay after a rejection before retrying a bulk request. With the default maxRetries the total backoff for retrying rejections
     * is about one minute per bulk request. Once the entire bulk request is successful the retry counter resets.
     */
    public ReindexRequestBuilder setRetryBackoffInitialTime(TimeValue retryBackoffInitialTime) {
        this.retryBackoffInitialTime = retryBackoffInitialTime;
        return this;
    }

    /**
     * Total number of retries attempted for rejections. There is no way to ask for unlimited retries.
     */
    public ReindexRequestBuilder setMaxRetries(int maxRetries) {
        this.maxRetries = maxRetries;
        return this;
    }

    /**
     * Set the throttle for this request in sub-requests per second. {@link Float#POSITIVE_INFINITY} means set no throttle and that is the
     * default. Throttling is done between batches, as we start the next scroll requests. That way we can increase the scroll's timeout to
     * make sure that it contains any time that we might wait.
     */
    public ReindexRequestBuilder setRequestsPerSecond(float requestsPerSecond) {
        this.requestsPerSecond = requestsPerSecond;
        return this;
    }

    /**
     * Should this task store its result after it has finished?
     */
    public ReindexRequestBuilder setShouldStoreResult(boolean shouldStoreResult) {
        this.shouldStoreResult = shouldStoreResult;
        return this;
    }

    /**
     * The number of slices this task should be divided into. Defaults to 1 meaning the task isn't sliced into subtasks.
     */
    public ReindexRequestBuilder setSlices(int slices) {
        this.slices = slices;
        return this;
    }

    @Override
    public ReindexRequest request() {
        SearchRequest source = source().request();
        try {
            IndexRequest destination = destinationBuilder.request();
            try {
                ReindexRequest reindexRequest = new ReindexRequest(source, destination, false);
                try {
                    if (remoteInfo != null) {
                        reindexRequest.setRemoteInfo(remoteInfo);
                    }
                    if (script != null) {
                        reindexRequest.setScript(script);
                    }
                    if (maxDocs != null) {
                        reindexRequest.setMaxDocs(maxDocs);
                    }
                    if (abortOnVersionConflict != null) {
                        reindexRequest.setAbortOnVersionConflict(abortOnVersionConflict);
                    }
                    if (refresh != null) {
                        reindexRequest.setRefresh(refresh);
                    }
                    if (timeout != null) {
                        reindexRequest.setTimeout(timeout);
                    }
                    if (waitForActiveShards != null) {
                        reindexRequest.setWaitForActiveShards(waitForActiveShards);
                    }
                    if (retryBackoffInitialTime != null) {
                        reindexRequest.setRetryBackoffInitialTime(retryBackoffInitialTime);
                    }
                    if (maxRetries != null) {
                        reindexRequest.setMaxRetries(maxRetries);
                    }
                    if (requestsPerSecond != null) {
                        reindexRequest.setRequestsPerSecond(requestsPerSecond);
                    }
                    if (shouldStoreResult != null) {
                        reindexRequest.setShouldStoreResult(shouldStoreResult);
                    }
                    if (slices != null) {
                        reindexRequest.setSlices(slices);
                    }
                    return reindexRequest;
                } catch (Exception e) {
                    reindexRequest.decRef();
                    throw e;
                }
            } catch (Exception e) {
                destination.decRef();
                throw e;
            }
        } catch (Exception e) {
            source.decRef();
            throw e;
        }
    }
}
