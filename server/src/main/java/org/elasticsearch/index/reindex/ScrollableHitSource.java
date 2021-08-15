/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.reindex;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;

/**
 * A scrollable source of results. Pumps data out into the passed onResponse consumer. Same data may come out several times in case
 * of failures during searching (though not yet). Once the onResponse consumer is done, it should call AsyncResponse.isDone(time) to receive
 * more data (only receives one response at a time).
 */
public abstract class ScrollableHitSource {
    private final AtomicReference<String> scrollId = new AtomicReference<>();

    protected final Logger logger;
    protected final BackoffPolicy backoffPolicy;
    protected final ThreadPool threadPool;
    protected final Runnable countSearchRetry;
    private final Consumer<AsyncResponse> onResponse;
    protected final Consumer<Exception> fail;

    public ScrollableHitSource(Logger logger, BackoffPolicy backoffPolicy, ThreadPool threadPool, Runnable countSearchRetry,
                               Consumer<AsyncResponse> onResponse, Consumer<Exception> fail) {
        this.logger = logger;
        this.backoffPolicy = backoffPolicy;
        this.threadPool = threadPool;
        this.countSearchRetry = countSearchRetry;
        this.onResponse = onResponse;
        this.fail = fail;
    }

    public final void start() {
        doStart(createRetryListener(this::doStart));
    }

    private RetryListener createRetryListener(Consumer<RejectAwareActionListener<Response>> retryHandler) {
        Consumer<RejectAwareActionListener<Response>> countingRetryHandler = listener -> {
            countSearchRetry.run();
            retryHandler.accept(listener);
        };
        return new RetryListener(logger, threadPool, backoffPolicy, countingRetryHandler,
            ActionListener.wrap(this::onResponse, fail));
    }

    // package private for tests.
    final void startNextScroll(TimeValue extraKeepAlive) {
        startNextScroll(extraKeepAlive, createRetryListener(listener -> startNextScroll(extraKeepAlive, listener)));
    }
    private void startNextScroll(TimeValue extraKeepAlive, RejectAwareActionListener<Response> searchListener) {
        doStartNextScroll(scrollId.get(), extraKeepAlive, searchListener);
    }

    private void onResponse(Response response) {
        logger.trace("scroll returned [{}] documents with a scroll id of [{}]", response.getHits().size(), response.getScrollId());
        setScroll(response.getScrollId());
        onResponse.accept(new AsyncResponse() {
            private AtomicBoolean alreadyDone = new AtomicBoolean();
            @Override
            public Response response() {
                return response;
            }

            @Override
            public void done(TimeValue extraKeepAlive) {
                assert alreadyDone.compareAndSet(false, true);
                startNextScroll(extraKeepAlive);
            }
        });
    }

    public final void close(Runnable onCompletion) {
        String scrollId = this.scrollId.get();
        if (Strings.hasLength(scrollId)) {
            clearScroll(scrollId, () -> cleanup(onCompletion));
        } else {
            cleanup(onCompletion);
        }
    }

    // following is the SPI to be implemented.
    protected abstract void doStart(RejectAwareActionListener<Response> searchListener);

    protected abstract void doStartNextScroll(String scrollId, TimeValue extraKeepAlive,
                                              RejectAwareActionListener<Response> searchListener);

    /**
     * Called to clear a scroll id.
     *
     * @param scrollId the id to clear
     * @param onCompletion implementers must call this after completing the clear whether they are
     *        successful or not
     */
    protected abstract void clearScroll(String scrollId, Runnable onCompletion);
    /**
     * Called after the process has been totally finished to clean up any resources the process
     * needed like remote connections.
     *
     * @param onCompletion implementers must call this after completing the cleanup whether they are
     *        successful or not
     */
    protected abstract void cleanup(Runnable onCompletion);

    /**
     * Set the id of the last scroll. Used for debugging.
     */
    public final void setScroll(String scrollId) {
        this.scrollId.set(scrollId);
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
     * Response from each scroll batch.
     */
    public static class Response {
        private final boolean timedOut;
        private final List<SearchFailure> failures;
        private final long totalHits;
        private final List<? extends Hit> hits;
        private final String scrollId;

        public Response(boolean timedOut, List<SearchFailure> failures, long totalHits, List<? extends Hit> hits, String scrollId) {
            this.timedOut = timedOut;
            this.failures = failures;
            this.totalHits = totalHits;
            this.hits = hits;
            this.scrollId = scrollId;
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
         * The scroll id used to fetch the next set of documents.
         */
        public String getScrollId() {
            return scrollId;
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
         * The type that the hit has.
         */
        String getType();
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
        @Nullable BytesReference getSource();
        /**
         * The content type of the hit source. Returns null if the source didn't come back from the search.
         */
        @Nullable XContentType getXContentType();
        /**
         * The routing on the hit if there is any or null if there isn't.
         */
        @Nullable String getRouting();
    }

    /**
     * An implementation of {@linkplain Hit} that uses getters and setters.
     */
    public static class BasicHit implements Hit {
        private final String index;
        private final String type;
        private final String id;
        private final long version;

        private BytesReference source;
        private XContentType xContentType;
        private String routing;
        private long seqNo;
        private long primaryTerm;

        public BasicHit(String index, String type, String id, long version) {
            this.index = index;
            this.type = type;
            this.id = id;
            this.version = version;
        }

        @Override
        public String getIndex() {
            return index;
        }

        @Override
        public String getType() {
            return type;
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

        public SearchFailure(Throwable reason, @Nullable String index, @Nullable Integer shardId, @Nullable String nodeId,
                             RestStatus status) {
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
