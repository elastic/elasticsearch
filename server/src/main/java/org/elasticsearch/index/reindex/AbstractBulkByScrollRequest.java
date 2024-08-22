/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.reindex;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.replication.ReplicationRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.core.TimeValue.timeValueMillis;
import static org.elasticsearch.core.TimeValue.timeValueMinutes;

public abstract class AbstractBulkByScrollRequest<Self extends AbstractBulkByScrollRequest<Self>> extends ActionRequest {

    public static final int MAX_DOCS_ALL_MATCHES = -1;
    public static final TimeValue DEFAULT_SCROLL_TIMEOUT = timeValueMinutes(5);
    public static final int DEFAULT_SCROLL_SIZE = 1000;

    public static final int AUTO_SLICES = 0;
    public static final String AUTO_SLICES_VALUE = "auto";
    private static final int DEFAULT_SLICES = 1;

    /**
     * The search to be executed.
     */
    private final SearchRequest searchRequest;

    /**
     * Maximum number of processed documents. Defaults to -1 meaning process all
     * documents.
     */
    private int maxDocs = MAX_DOCS_ALL_MATCHES;

    /**
     * Should version conflicts cause aborts? Defaults to true.
     */
    private boolean abortOnVersionConflict = true;

    /**
     * Call refresh on the indexes we've written to after the request ends?
     */
    private boolean refresh = false;

    /**
     * Timeout to wait for the shards on to be available for each bulk request?
     */
    private TimeValue timeout = ReplicationRequest.DEFAULT_TIMEOUT;

    /**
     * The number of shard copies that must be active before proceeding with the write.
     */
    private ActiveShardCount activeShardCount = ActiveShardCount.DEFAULT;

    /**
     * Initial delay after a rejection before retrying a bulk request. With the default maxRetries the total backoff for retrying rejections
     * is about one minute per bulk request. Once the entire bulk request is successful the retry counter resets.
     */
    private TimeValue retryBackoffInitialTime = timeValueMillis(500);

    /**
     * Total number of retries attempted for rejections. There is no way to ask for unlimited retries.
     */
    private int maxRetries = 11;

    /**
     * The throttle for this request in sub-requests per second. {@link Float#POSITIVE_INFINITY} means set no throttle and that is the
     * default. Throttling is done between batches, as we start the next scroll requests. That way we can increase the scroll's timeout to
     * make sure that it contains any time that we might wait.
     */
    private float requestsPerSecond = Float.POSITIVE_INFINITY;

    /**
     * Should this task store its result?
     */
    private boolean shouldStoreResult;

    /**
     * The number of slices this task should be divided into. Defaults to 1 meaning the task isn't sliced into subtasks.
     */
    private int slices = DEFAULT_SLICES;

    public AbstractBulkByScrollRequest(StreamInput in) throws IOException {
        super(in);
        searchRequest = new SearchRequest(in);
        abortOnVersionConflict = in.readBoolean();
        maxDocs = in.readVInt();
        refresh = in.readBoolean();
        timeout = in.readTimeValue();
        activeShardCount = ActiveShardCount.readFrom(in);
        retryBackoffInitialTime = in.readTimeValue();
        maxRetries = in.readVInt();
        requestsPerSecond = in.readFloat();
        slices = in.readVInt();
    }

    /**
     * Constructor for actual use.
     *
     * @param searchRequest the search request to execute to get the documents to process
     * @param setDefaults should this request set the defaults on the search request? Usually set to true but leave it false to support
     *        request slicing
     */
    public AbstractBulkByScrollRequest(SearchRequest searchRequest, boolean setDefaults) {
        this.searchRequest = searchRequest;

        // Set the defaults which differ from SearchRequest's defaults.
        if (setDefaults) {
            searchRequest.scroll(DEFAULT_SCROLL_TIMEOUT);
            searchRequest.source(new SearchSourceBuilder());
            searchRequest.source().size(DEFAULT_SCROLL_SIZE);
        }
    }

    /**
     * `this` cast to Self. Used for building fluent methods without cast
     * warnings.
     */
    protected abstract Self self();

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException e = searchRequest.validate();
        if (searchRequest.source().from() != -1) {
            e = addValidationError("from is not supported in this context", e);
        }
        if (searchRequest.source().storedFields() != null) {
            e = addValidationError("stored_fields is not supported in this context", e);
        }
        if (maxRetries < 0) {
            e = addValidationError("retries cannot be negative", e);
        }
        if (false == (maxDocs == -1 || maxDocs > 0)) {
            e = addValidationError(
                "maxDocs should be greater than 0 if the request is limited to some number of documents or -1 if it isn't but it was ["
                    + maxDocs
                    + "]",
                e
            );
        }
        if (searchRequest.source().slice() != null && slices != DEFAULT_SLICES) {
            e = addValidationError("can't specify both manual and automatic slicing at the same time", e);
        }
        return e;
    }

    /**
     * Maximum number of processed documents. Defaults to -1 meaning process all
     * documents.
     */
    public int getMaxDocs() {
        return maxDocs;
    }

    /**
     * Maximum number of processed documents. Defaults to -1 meaning process all
     * documents.
     */
    public Self setMaxDocs(int maxDocs) {
        if (maxDocs < 0) {
            throw new IllegalArgumentException("[max_docs] parameter cannot be negative, found [" + maxDocs + "]");
        }
        if (maxDocs < slices) {
            throw new IllegalArgumentException("[max_docs] should be >= [slices]");
        }
        this.maxDocs = maxDocs;
        return self();
    }

    /**
     * Whether or not version conflicts cause the action to abort.
     */
    public boolean isAbortOnVersionConflict() {
        return abortOnVersionConflict;
    }

    /**
     * Set whether or not version conflicts cause the action to abort.
     */
    public Self setAbortOnVersionConflict(boolean abortOnVersionConflict) {
        this.abortOnVersionConflict = abortOnVersionConflict;
        return self();
    }

    /**
     * Sets abortOnVersionConflict based on REST-friendly names.
     */
    public void setConflicts(String conflicts) {
        switch (conflicts) {
            case "proceed" -> {
                setAbortOnVersionConflict(false);
            }
            case "abort" -> {
                setAbortOnVersionConflict(true);
            }
            default -> throw new IllegalArgumentException("conflicts may only be \"proceed\" or \"abort\" but was [" + conflicts + "]");
        }
    }

    /**
     * The search request that matches the documents to process.
     */
    public SearchRequest getSearchRequest() {
        return searchRequest;
    }

    /**
     * Call refresh on the indexes we've written to after the request ends?
     */
    public boolean isRefresh() {
        return refresh;
    }

    /**
     * Call refresh on the indexes we've written to after the request ends?
     */
    public Self setRefresh(boolean refresh) {
        this.refresh = refresh;
        return self();
    }

    /**
     * Timeout to wait for the shards on to be available for each bulk request?
     */
    public TimeValue getTimeout() {
        return timeout;
    }

    /**
     * Timeout to wait for the shards on to be available for each bulk request?
     */
    public Self setTimeout(TimeValue timeout) {
        this.timeout = Objects.requireNonNull(timeout);
        return self();
    }

    /**
     * The number of shard copies that must be active before proceeding with the write.
     */
    public ActiveShardCount getWaitForActiveShards() {
        return activeShardCount;
    }

    /**
     * Sets the number of shard copies that must be active before proceeding with the write.
     * See {@link ReplicationRequest#waitForActiveShards(ActiveShardCount)} for details.
     */
    public Self setWaitForActiveShards(ActiveShardCount activeShardCount) {
        this.activeShardCount = activeShardCount;
        return self();
    }

    /**
     * A shortcut for {@link #setWaitForActiveShards(ActiveShardCount)} where the numerical
     * shard count is passed in, instead of having to first call {@link ActiveShardCount#from(int)}
     * to get the ActiveShardCount.
     */
    public Self setWaitForActiveShards(final int waitForActiveShards) {
        return setWaitForActiveShards(ActiveShardCount.from(waitForActiveShards));
    }

    /**
     * Initial delay after a rejection before retrying request.
     */
    public TimeValue getRetryBackoffInitialTime() {
        return retryBackoffInitialTime;
    }

    /**
     * Set the initial delay after a rejection before retrying request.
     */
    public Self setRetryBackoffInitialTime(TimeValue retryBackoffInitialTime) {
        this.retryBackoffInitialTime = retryBackoffInitialTime;
        return self();
    }

    /**
     * Total number of retries attempted for rejections.
     */
    public int getMaxRetries() {
        return maxRetries;
    }

    /**
     * Set the total number of retries attempted for rejections. There is no way to ask for unlimited retries.
     */
    public Self setMaxRetries(int maxRetries) {
        this.maxRetries = maxRetries;
        return self();
    }

    /**
     * The throttle for this request in sub-requests per second. {@link Float#POSITIVE_INFINITY} means set no throttle and that is the
     * default. Throttling is done between batches, as we start the next scroll requests. That way we can increase the scroll's timeout to
     * make sure that it contains any time that we might wait.
     */
    public float getRequestsPerSecond() {
        return requestsPerSecond;
    }

    /**
     * Set the throttle for this request in sub-requests per second. {@link Float#POSITIVE_INFINITY} means set no throttle and that is the
     * default. Throttling is done between batches, as we start the next scroll requests. That way we can increase the scroll's timeout to
     * make sure that it contains any time that we might wait.
     */
    public Self setRequestsPerSecond(float requestsPerSecond) {
        if (requestsPerSecond <= 0) {
            throw new IllegalArgumentException(
                "[requests_per_second] must be greater than 0. Use Float.POSITIVE_INFINITY to disable throttling."
            );
        }
        this.requestsPerSecond = requestsPerSecond;
        return self();
    }

    /**
     * Should this task store its result after it has finished?
     */
    public Self setShouldStoreResult(boolean shouldStoreResult) {
        this.shouldStoreResult = shouldStoreResult;
        return self();
    }

    @Override
    public boolean getShouldStoreResult() {
        return shouldStoreResult;
    }

    /**
     * Set scroll timeout for {@link SearchRequest}
     */
    public Self setScroll(TimeValue keepAlive) {
        searchRequest.scroll(new Scroll(keepAlive));
        return self();
    }

    /**
     * Get scroll timeout
     */
    public TimeValue getScrollTime() {
        return searchRequest.scroll().keepAlive();
    }

    /**
     * The number of slices this task should be divided into. Defaults to 1 meaning the task isn't sliced into subtasks.
     * A value of 0 is equivalent to the "auto" slices parameter of the Rest API.
     */
    public Self setSlices(int slices) {
        if (slices < 0) {
            throw new IllegalArgumentException("[slices] must be at least 0 but was [" + slices + "]");
        }
        this.slices = slices;
        return self();
    }

    /**
     * The number of slices this task should be divided into. Defaults to 1 meaning the task isn't sliced into subtasks.
     */
    public int getSlices() {
        return slices;
    }

    /**
     * Build a new request for a slice of the parent request.
     */
    public abstract Self forSlice(TaskId slicingTask, SearchRequest slice, int totalSlices);

    /**
     * Setup a clone of this request with the information needed to process a slice of it.
     */
    protected Self doForSlice(Self request, TaskId slicingTask, int totalSlices) {
        if (totalSlices < 1) {
            throw new IllegalArgumentException("Number of total slices must be at least 1 but was [" + totalSlices + "]");
        }

        request.setAbortOnVersionConflict(abortOnVersionConflict)
            .setRefresh(refresh)
            .setTimeout(timeout)
            .setWaitForActiveShards(activeShardCount)
            .setRetryBackoffInitialTime(retryBackoffInitialTime)
            .setMaxRetries(maxRetries)
            // Parent task will store result
            .setShouldStoreResult(false)
            // Split requests per second between all slices
            .setRequestsPerSecond(requestsPerSecond / totalSlices)
            // Sub requests don't have workers
            .setSlices(1);
        if (maxDocs != MAX_DOCS_ALL_MATCHES) {
            // maxDocs is split between workers. This means the maxDocs might round
            // down!
            request.setMaxDocs(maxDocs / totalSlices);
        }
        // Set the parent task so this task is cancelled if we cancel the parent
        request.setParentTask(slicingTask);
        // TODO It'd be nice not to refresh on every slice. Instead we should refresh after the sub requests finish.
        return request;
    }

    @Override
    public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
        return new BulkByScrollTask(id, type, action, getDescription(), parentTaskId, headers);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        searchRequest.writeTo(out);
        out.writeBoolean(abortOnVersionConflict);
        out.writeVInt(maxDocs);
        out.writeBoolean(refresh);
        out.writeTimeValue(timeout);
        activeShardCount.writeTo(out);
        out.writeTimeValue(retryBackoffInitialTime);
        out.writeVInt(maxRetries);
        out.writeFloat(requestsPerSecond);
        out.writeVInt(slices);
    }

    /**
     * Append a short description of the search request to a StringBuilder. Used
     * to make toString.
     */
    protected void searchToString(StringBuilder b) {
        if (searchRequest.indices() != null && searchRequest.indices().length != 0) {
            b.append(Arrays.toString(searchRequest.indices()));
        } else {
            b.append("[all indices]");
        }
    }

    @Override
    public String getDescription() {
        return this.toString();
    }
}
