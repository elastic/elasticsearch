/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.reindex;

import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.replication.ReplicationRequest;
import org.elasticsearch.client.internal.ElasticsearchClient;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;

public abstract class AbstractBulkByScrollRequestBuilder<
    Request extends AbstractBulkByScrollRequest<Request>,
    Self extends AbstractBulkByScrollRequestBuilder<Request, Self>> extends ActionRequestBuilder<Request, BulkByScrollResponse> {
    private final SearchRequestBuilder source;

    protected AbstractBulkByScrollRequestBuilder(
        ElasticsearchClient client,
        ActionType<BulkByScrollResponse> action,
        SearchRequestBuilder source,
        Request request
    ) {
        super(client, action, request);
        this.source = source;
    }

    protected abstract Self self();

    /**
     * The search used to find documents to process.
     */
    public SearchRequestBuilder source() {
        return source;
    }

    /**
     * Set the source indices.
     */
    public Self source(String... indices) {
        source.setIndices(indices);
        return self();
    }

    /**
     * Set the query that will filter the source. Just a convenience method for
     * easy chaining.
     */
    public Self filter(QueryBuilder filter) {
        source.setQuery(filter);
        return self();
    }

    /**
     * Maximum number of processed documents. Defaults to processing all
     * documents.
     * @deprecated please use maxDocs(int) instead.
     */
    @Deprecated
    public Self size(int size) {
        return maxDocs(size);
    }

    /**
     * Maximum number of processed documents. Defaults to processing all
     * documents.
     */
    public Self maxDocs(int maxDocs) {
        request.setMaxDocs(maxDocs);
        return self();
    }

    /**
     * Set whether or not version conflicts cause the action to abort.
     */
    public Self abortOnVersionConflict(boolean abortOnVersionConflict) {
        request.setAbortOnVersionConflict(abortOnVersionConflict);
        return self();
    }

    /**
     * Call refresh on the indexes we've written to after the request ends?
     */
    public Self refresh(boolean refresh) {
        request.setRefresh(refresh);
        return self();
    }

    /**
     * Timeout to wait for the shards on to be available for each bulk request.
     */
    public Self timeout(TimeValue timeout) {
        request.setTimeout(timeout);
        return self();
    }

    /**
     * The number of shard copies that must be active before proceeding with the write.
     * See {@link ReplicationRequest#waitForActiveShards(ActiveShardCount)} for details.
     */
    public Self waitForActiveShards(ActiveShardCount activeShardCount) {
        request.setWaitForActiveShards(activeShardCount);
        return self();
    }

    /**
     * Initial delay after a rejection before retrying a bulk request. With the default maxRetries the total backoff for retrying rejections
     * is about one minute per bulk request. Once the entire bulk request is successful the retry counter resets.
     */
    public Self setRetryBackoffInitialTime(TimeValue retryBackoffInitialTime) {
        request.setRetryBackoffInitialTime(retryBackoffInitialTime);
        return self();
    }

    /**
     * Total number of retries attempted for rejections. There is no way to ask for unlimited retries.
     */
    public Self setMaxRetries(int maxRetries) {
        request.setMaxRetries(maxRetries);
        return self();
    }

    /**
     * Set the throttle for this request in sub-requests per second. {@link Float#POSITIVE_INFINITY} means set no throttle and that is the
     * default. Throttling is done between batches, as we start the next scroll requests. That way we can increase the scroll's timeout to
     * make sure that it contains any time that we might wait.
     */
    public Self setRequestsPerSecond(float requestsPerSecond) {
        request.setRequestsPerSecond(requestsPerSecond);
        return self();
    }

    /**
     * Should this task store its result after it has finished?
     */
    public Self setShouldStoreResult(boolean shouldStoreResult) {
        request.setShouldStoreResult(shouldStoreResult);
        return self();
    }

    /**
     * The number of slices this task should be divided into. Defaults to 1 meaning the task isn't sliced into subtasks.
     */
    public Self setSlices(int slices) {
        request.setSlices(slices);
        return self();
    }
}
