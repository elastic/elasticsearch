/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.reindex;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.common.BackoffPolicy;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.reindex.ResumeInfo.WorkerResumeInfo;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;

/**
 * A source of paginated search results. Pumps data out into the passed onResponse consumer. If a scrollable search is used, then the
 * same data may come out several times in case of failures during searching (though not yet). Once the onResponse consumer is done,
 * it should call AsyncResponse.isDone(time) to receive more data (only receives one response at a time).
 * <p>
 * For more information on paginating searches, view the
 * <a href="https://www.elastic.co/docs/reference/elasticsearch/rest-apis/paginate-search-results"> ES Search documentation</a>
 */
public abstract class PaginatedHitSource {
    private final AtomicReference<String> scrollId = new AtomicReference<>();
    private final AtomicReference<Object[]> searchAfterValues = new AtomicReference<>();

    protected final Logger logger;
    protected final BackoffPolicy backoffPolicy;
    protected final ThreadPool threadPool;
    protected final Runnable countSearchRetry;
    private final Consumer<AsyncResponse> onResponse;
    protected final Consumer<Exception> fail;

    public PaginatedHitSource(
        Logger logger,
        BackoffPolicy backoffPolicy,
        ThreadPool threadPool,
        Runnable countSearchRetry,
        Consumer<AsyncResponse> onResponse,
        Consumer<Exception> fail
    ) {
        this.logger = logger;
        this.backoffPolicy = backoffPolicy;
        this.threadPool = threadPool;
        this.countSearchRetry = countSearchRetry;
        this.onResponse = onResponse;
        this.fail = fail;
    }

    public final void start() {
        doFirstSearch(createRetryListener(this::doFirstSearch));
    }

    /**
     * Resumes the hit source from previously saved state.
     * @param resumeInfo resume information
     */
    public void resume(WorkerResumeInfo resumeInfo) {
        restoreState(resumeInfo);
        requestNextBatch(TimeValue.ZERO);
    }

    protected abstract void restoreState(WorkerResumeInfo resumeInfo);

    private RetryListener<Response> createRetryListener(Consumer<RejectAwareActionListener<Response>> retryHandler) {
        Consumer<RejectAwareActionListener<Response>> countingRetryHandler = listener -> {
            countSearchRetry.run();
            retryHandler.accept(listener);
        };
        return new RetryListener<>(logger, threadPool, backoffPolicy, countingRetryHandler, ActionListener.wrap(this::onResponse, fail));
    }

    // package private for tests.
    public final void requestNextBatch(TimeValue extraKeepAlive) {
        requestNextBatch(extraKeepAlive, createRetryListener(listener -> requestNextBatch(extraKeepAlive, listener)));
    }

    private void requestNextBatch(TimeValue extraKeepAlive, RejectAwareActionListener<Response> searchListener) {
        PaginationCursor cursor;
        String sid = scrollId.get();
        if (Strings.hasLength(sid)) {
            cursor = PaginationCursor.forScroll(sid);
        } else {
            Object[] searchAfter = searchAfterValues.get();
            if (searchAfter != null) {
                cursor = PaginationCursor.forSearchAfter(searchAfter);
            } else {
                fail.accept(new IllegalStateException("No pagination cursor available for next batch"));
                return;
            }
        }
        doNextSearch(cursor, extraKeepAlive, searchListener);
    }

    private void onResponse(Response response) {
        logger.trace("search returned [{}] documents", response.getHits().size());
        setScrollId(response.getScrollId());
        setSearchAfterValues(response.getSearchAfterValues());
        onResponse.accept(new AsyncResponse() {
            private AtomicBoolean alreadyDone = new AtomicBoolean();

            @Override
            public Response response() {
                return response;
            }

            @Override
            public void done(TimeValue extraKeepAlive) {
                assert alreadyDone.compareAndSet(false, true);
                requestNextBatch(extraKeepAlive);
            }
        });
    }

    /** Clean up all resources. */
    public final void close(Runnable onCompletion) {
        releaseSearchContext(() -> cleanup(onCompletion));
    }

    public final void cleanupWithoutClosingPagination(final Runnable onCompletion) {
        cleanup(onCompletion);
    }

    // following is the SPI to be implemented.
    protected abstract void doFirstSearch(RejectAwareActionListener<Response> searchListener);

    protected abstract void doNextSearch(
        PaginationCursor cursor,
        TimeValue extraKeepAlive,
        RejectAwareActionListener<Response> searchListener
    );

    /**
     * Called to release pagination resources (e.g. clear scroll context).
     * For PIT-based pagination this is a no-op as the PIT is closed elsewhere.
     *
     * @param onCompletion implementers must call this after completing the release whether they are
     *        successful or not
     */
    protected abstract void releaseSearchContext(Runnable onCompletion);

    /**
     * Called after the process has been totally finished to clean up any resources the process
     * needed like remote connections.
     *
     * @param onCompletion implementers must call this after completing the cleanup whether they are
     *        successful or not
     */
    protected abstract void cleanup(Runnable onCompletion);

    /**
     * Set the scroll id from the last response. Used for scroll-based pagination.
     */
    public final void setScrollId(String scrollId) {
        this.scrollId.set(scrollId);
    }

    /**
     * Set the search_after values from the last response. Used for PIT-based pagination.
     */
    protected final void setSearchAfterValues(Object[] searchAfterValues) {
        this.searchAfterValues.set(searchAfterValues);
    }

    /**
     * Returns the current scroll id, if any. Used by scroll implementations in releaseSearchContext.
     */
    protected final String getScrollId() {
        return scrollId.get();
    }

    /**
     * Whether there are more batches to fetch.
     */
    public boolean hasMoreBatches() {
        return scrollId.get() != null || searchAfterValues.get() != null;
    }

    public interface AsyncResponse {
        /**
         * The response data made available.
         */
        Response response();

        /**
         * Called when done processing response to signal more data is needed.
         * @param extraKeepAlive extra time to keep underlying scroll open.
         */
        void done(TimeValue extraKeepAlive);
    }

    /**
     * Response from each search batch.
     */
    public static class Response {
        private final boolean timedOut;
        private final List<SearchFailure> failures;
        private final long totalHits;
        private final List<? extends Hit> hits;
        private final String scrollId;
        private final Object[] searchAfterValues;
        private final BytesReference pitId;

        public Response(boolean timedOut, List<SearchFailure> failures, long totalHits, List<? extends Hit> hits, String scrollId) {
            this(timedOut, failures, totalHits, hits, scrollId, null, null);
        }

        public Response(
            boolean timedOut,
            List<SearchFailure> failures,
            long totalHits,
            List<? extends Hit> hits,
            String scrollId,
            Object[] searchAfterValues
        ) {
            this(timedOut, failures, totalHits, hits, scrollId, searchAfterValues, null);
        }

        public Response(
            boolean timedOut,
            List<SearchFailure> failures,
            long totalHits,
            List<? extends Hit> hits,
            String scrollId,
            Object[] searchAfterValues,
            BytesReference pitId
        ) {
            this.timedOut = timedOut;
            this.failures = failures;
            this.totalHits = totalHits;
            this.hits = hits;
            this.scrollId = scrollId;
            this.searchAfterValues = searchAfterValues;
            this.pitId = pitId;
        }

        /**
         * Did this batch time out?
         */
        public boolean isTimedOut() {
            return timedOut;
        }

        /**
         * Where there any search failures?
         */
        public final List<SearchFailure> getFailures() {
            return failures;
        }

        /**
         * What were the total number of documents matching the search?
         */
        public long getTotalHits() {
            return totalHits;
        }

        /**
         * The documents returned in this batch.
         */
        public List<? extends Hit> getHits() {
            return hits;
        }

        /**
         * The scroll id used to fetch the next set of documents. Null for PIT-based pagination.
         */
        public String getScrollId() {
            return scrollId;
        }

        /**
         * Refreshed PIT id from the response, used for the next PIT search request. Null for scroll-based pagination.
         */
        @Nullable
        public BytesReference getPitId() {
            return pitId;
        }

        /**
         * The search_after values from the last hit, used to fetch the next batch in PIT-based pagination.
         * Null for scroll-based pagination.
         */
        @Nullable
        public Object[] getSearchAfterValues() {
            if (searchAfterValues != null) {
                return searchAfterValues;
            }
            if (hits != null && hits.isEmpty() == false) {
                Hit lastHit = hits.get(hits.size() - 1);
                return lastHit.getSortValues();
            }
            return null;
        }
    }

    /**
     * A document returned as part of the response. Think of it like {@link SearchHit} but with all the things reindex needs in convenient
     * methods.
     */
    public interface Hit {
        /**
         * The index in which the hit is stored.
         */
        String getIndex();

        /**
         * The document id of the hit.
         */
        String getId();

        /**
         * The version of the match or {@code -1} if the version wasn't requested. The {@code -1} keeps it inline with Elasticsearch's
         * internal APIs.
         */
        long getVersion();

        /**
         * The sequence number of the match or {@link SequenceNumbers#UNASSIGNED_SEQ_NO} if sequence numbers weren't requested.
         */
        long getSeqNo();

        /**
         * The primary term of the match or {@link SequenceNumbers#UNASSIGNED_PRIMARY_TERM} if sequence numbers weren't requested.
         */
        long getPrimaryTerm();

        /**
         * The source of the hit. Returns null if the source didn't come back from the search, usually because it source wasn't stored at
         * all.
         */
        @Nullable
        BytesReference getSource();

        /**
         * The content type of the hit source. Returns null if the source didn't come back from the search.
         */
        @Nullable
        XContentType getXContentType();

        /**
         * The routing on the hit if there is any or null if there isn't.
         */
        @Nullable
        String getRouting();

        /**
         * The sort values of the hit, used for search_after pagination. Null if not available.
         */
        @Nullable
        default Object[] getSortValues() {
            return null;
        }
    }

    /**
     * An implementation of {@linkplain Hit} that uses getters and setters.
     */
    public static class BasicHit implements Hit {
        private final String index;
        private final String id;
        private final long version;

        private BytesReference source;
        private XContentType xContentType;
        private String routing;
        private long seqNo;
        private long primaryTerm;
        private Object[] sortValues;

        public BasicHit(String index, String id, long version) {
            this.index = index;
            this.id = id;
            this.version = version;
        }

        @Override
        public String getIndex() {
            return index;
        }

        @Override
        public String getId() {
            return id;
        }

        @Override
        public long getVersion() {
            return version;
        }

        @Override
        public long getSeqNo() {
            return seqNo;
        }

        @Override
        public long getPrimaryTerm() {
            return primaryTerm;
        }

        @Override
        public BytesReference getSource() {
            return source;
        }

        @Override
        public XContentType getXContentType() {
            return xContentType;
        }

        @Override
        @Nullable
        public Object[] getSortValues() {
            return sortValues;
        }

        public BasicHit setSource(BytesReference source, XContentType xContentType) {
            this.source = source;
            this.xContentType = xContentType;
            return this;
        }

        @Override
        public String getRouting() {
            return routing;
        }

        public BasicHit setRouting(String routing) {
            this.routing = routing;
            return this;
        }

        public void setSeqNo(long seqNo) {
            this.seqNo = seqNo;
        }

        public void setPrimaryTerm(long primaryTerm) {
            this.primaryTerm = primaryTerm;
        }

        public BasicHit setSortValues(Object[] sortValues) {
            this.sortValues = sortValues;
            return this;
        }
    }

    /**
     * A failure during search. Like {@link ShardSearchFailure} but useful for reindex from remote as well.
     */
    public static class SearchFailure implements Writeable, ToXContentObject {
        private final Throwable reason;
        private final RestStatus status;
        @Nullable
        private final String index;
        @Nullable
        private final Integer shardId;
        @Nullable
        private final String nodeId;

        public static final String INDEX_FIELD = "index";
        public static final String SHARD_FIELD = "shard";
        public static final String NODE_FIELD = "node";
        public static final String REASON_FIELD = "reason";
        public static final String STATUS_FIELD = BulkItemResponse.Failure.STATUS_FIELD;

        public SearchFailure(Throwable reason, @Nullable String index, @Nullable Integer shardId, @Nullable String nodeId) {
            this(reason, index, shardId, nodeId, ExceptionsHelper.status(reason));
        }

        public SearchFailure(
            Throwable reason,
            @Nullable String index,
            @Nullable Integer shardId,
            @Nullable String nodeId,
            RestStatus status
        ) {
            this.index = index;
            this.shardId = shardId;
            this.reason = requireNonNull(reason, "reason cannot be null");
            this.nodeId = nodeId;
            this.status = status;
        }

        /**
         * Build a search failure that doesn't have shard information available.
         */
        public SearchFailure(Throwable reason) {
            this(reason, null, null, null);
        }

        /**
         * Read from a stream.
         */
        public SearchFailure(StreamInput in) throws IOException {
            reason = in.readException();
            index = in.readOptionalString();
            shardId = in.readOptionalVInt();
            nodeId = in.readOptionalString();
            status = ExceptionsHelper.status(reason);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeException(reason);
            out.writeOptionalString(index);
            out.writeOptionalVInt(shardId);
            out.writeOptionalString(nodeId);
        }

        public String getIndex() {
            return index;
        }

        public Integer getShardId() {
            return shardId;
        }

        public RestStatus getStatus() {
            return this.status;
        }

        public Throwable getReason() {
            return reason;
        }

        @Nullable
        public String getNodeId() {
            return nodeId;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            if (index != null) {
                builder.field(INDEX_FIELD, index);
            }
            if (shardId != null) {
                builder.field(SHARD_FIELD, shardId);
            }
            if (nodeId != null) {
                builder.field(NODE_FIELD, nodeId);
            }
            builder.field(STATUS_FIELD, status.getStatus());
            builder.field(REASON_FIELD);
            {
                builder.startObject();
                ElasticsearchException.generateThrowableXContent(builder, params, reason);
                builder.endObject();
            }
            builder.endObject();
            return builder;
        }

        @Override
        public String toString() {
            return Strings.toString(this);
        }
    }
}
