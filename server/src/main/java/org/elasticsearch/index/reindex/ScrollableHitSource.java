/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.reindex;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.mapper.SeqNoFieldMapper;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.ToLongFunction;

import static java.util.Objects.requireNonNull;
import static org.elasticsearch.common.util.CollectionUtils.isEmpty;

/**
 * A scrollable source of results. Pumps data out into the passed onResponse consumer. Same data may come out several times in case
 * of failures during searching. Once the onResponse consumer is done, it should call AsyncResponse.isDone(time) to receive more data
 * (only receives one response at a time).
 */
public abstract class ScrollableHitSource {
    private final AtomicReference<String> scrollId = new AtomicReference<>();

    protected final Logger logger;
    protected final BackoffPolicy backoffPolicy;
    protected final ThreadPool threadPool;
    private final Runnable countSearchRetry;
    private final Consumer<AsyncResponse> onResponse;
    private final Consumer<Exception> fail;
    private final ToLongFunction<Hit> extractRetryValueFunction;
    private long retryFromValue = Long.MIN_VALUE; // need refinement if we support descending.

    public ScrollableHitSource(Logger logger, BackoffPolicy backoffPolicy, ThreadPool threadPool, Runnable countSearchRetry,
                               Consumer<AsyncResponse> onResponse, Consumer<Exception> fail, String resumableSortingField) {
        this.logger = logger;
        this.backoffPolicy = backoffPolicy;
        this.threadPool = threadPool;
        this.countSearchRetry = countSearchRetry;
        this.onResponse = onResponse;
        this.fail = fail;
        if (resumableSortingField != null) {
            if (SeqNoFieldMapper.NAME.equals(resumableSortingField)) {
                extractRetryValueFunction = Hit::getSeqNo;
            } else {
                extractRetryValueFunction = hit -> Long.MIN_VALUE;
                // need to extract field, either from source or by asking for it explicitly.
                // also we need to handle missing values.
                // hit -> ((Number) hit.field(resumableSortingField).getValue()).longValue();
            }
        } else {
            extractRetryValueFunction = hit -> Long.MIN_VALUE;
        }
    }

    public final void start() {
        if (logger.isDebugEnabled()) {
            logger.debug("executing initial scroll against {}",
                isEmpty(indices()) ? "all indices" : indices());
        }

        doStart(new RetryListener(new TimeValue(0)));
    }

    public final void retryFromValue(long retryFromValue) {
        retryFromValue = retryFromValue;
    }

    public final long retryFromValue() {
        return retryFromValue;
    }

    public final void restart(RejectAwareActionListener<Response> searchListener) {
        String scrollId1 = this.scrollId.get();
        if (scrollId1 != null) {
            // we do not bother waiting for the scroll to be cleared, yet at least. We could implement a policy to
            // not have more than x old scrolls outstanding and wait for their timeout before continuing (we know the timeout).
            // A flaky connection could in principle lead to many scrolls within the timeout window, so is worth pursuing.
            clearScroll(scrollId1, () -> {});
            this.scrollId.set(null);
        }
        if (logger.isDebugEnabled()) {
            logger.debug("retrying scroll against {} from resume marker {}",
                isEmpty(indices()) ? "all indices" : indices(), retryFromValue());
        }
        if (retryFromValue == Long.MIN_VALUE) {
            doStart(searchListener);
        } else {
            doRestart(searchListener, retryFromValue);
        }
    }


    private void startNextScroll(TimeValue extraKeepAlive, RejectAwareActionListener<Response> searchListener) {
        doStartNextScroll(scrollId.get(), extraKeepAlive, searchListener);
    }

    private void onResponse(Response response) {
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
                retryFromValue = extractRetryFromValue(response, retryFromValue);
                startNextScroll(extraKeepAlive, new RetryListener(extraKeepAlive));
            }
        });
    }

    private long extractRetryFromValue(Response response, long defaultValue) {
        List<? extends Hit> hits = response.hits;
        if (hits.size() != 0) {
            return extractRetryValueFunction.applyAsLong(hits.get(hits.size() - 1));
        } else {
            return defaultValue;
        }
    }

    public final void close(Runnable onCompletion) {
        String scrollId = this.scrollId.get();
        if (Strings.hasLength(scrollId)) {
            clearScroll(scrollId, () -> cleanup(onCompletion));
        } else {
            cleanup(onCompletion);
        }
    }

    // following is the SPI to be implemented. We could extract this into a separate interface instead.
    protected abstract void doStart(RejectAwareActionListener<Response> searchListener);
    protected abstract void doRestart(RejectAwareActionListener<Response> searchListener, long retryFromValue);

    protected abstract void doStartNextScroll(String scrollId, TimeValue extraKeepAlive,
                                              RejectAwareActionListener<Response> searchListener);
    protected abstract String[] indices();

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
        Response response();
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

        public SearchFailure(Throwable reason, @Nullable String index, @Nullable Integer shardId, @Nullable String nodeId) {
            this.index = index;
            this.shardId = shardId;
            this.reason = requireNonNull(reason, "reason cannot be null");
            this.nodeId = nodeId;
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

    // public for testing
    public interface RejectAwareActionListener<T> extends ActionListener<T> {
        void onRejection(Exception e);

        /**
         * Return a new listener that delegates failure/reject to original but forwards response to responseHandler
         */
        default <T> RejectAwareActionListener<T> withResponseHandler(Consumer<T> responseHandler) {
            RejectAwareActionListener<?> outer = this;
            return new RejectAwareActionListener<T>() {
                @Override
                public void onRejection(Exception e) {
                    outer.onRejection(e);
                }

                @Override
                public void onResponse(T t) {
                    responseHandler.accept(t);
                }

                @Override
                public void onFailure(Exception e) {
                    outer.onFailure(e);
                }
            };
        }
    }

    // todo: figure out if/when thread-context handling is necessary.
    private class RetryListener implements RejectAwareActionListener<Response> {
        private final Iterator<TimeValue> retries = backoffPolicy.iterator();
        private volatile int retryCount = 0;
        private TimeValue extraKeepAlive;

        private RetryListener(TimeValue extraKeepAlive) {
            this.extraKeepAlive = extraKeepAlive;
        }

        @Override
        public void onResponse(Response response) {
            if (response.getFailures().isEmpty() == false) {
                // some but not all shards failed, we cannot process data, since our resume marker would progress too much.
                if (retries.hasNext()) {
                    TimeValue delay = retries.next();
                    logger.trace(
                        () -> new ParameterizedMessage("retrying rejected search after [{}] for shard failures [{}]",
                            delay,
                            response.getFailures()));

                    schedule(() -> restart(this), delay);
                    return;
                } // else respond to let action fail.
            }
            logger.debug("scroll returned [{}] documents with a scroll id of [{}]", response.getHits().size(), response.getScrollId());
            ScrollableHitSource.this.onResponse(response);
        }

        @Override
        public void onFailure(Exception e) {
            handleException(e,
                delay -> {
                    logger.trace(() -> new ParameterizedMessage("retrying rejected search after [{}]", delay), e);
                    schedule(() -> restart(this), delay);
                });
        }

        @Override
        public void onRejection(Exception e) {
            handleException(e,
                delay -> {
                    logger.trace(() -> new ParameterizedMessage("retrying rejected search after [{}]", delay), e);
                    schedule(()-> startNextScroll(extraKeepAlive, this), delay);

                });
        }

        public void handleException(Exception e, Consumer<TimeValue> action) {
            if (retries.hasNext()) {
                retryCount += 1;
                TimeValue delay = retries.next();
                countSearchRetry.run();
                action.accept(delay);
            } else {
                logger.warn(() -> new ParameterizedMessage(
                    "giving up on search because we retried [{}] times without success", retryCount), e);
                fail.accept(e);
            }
        }

        private void schedule(Runnable runnable, TimeValue delay) {
            // schedule does not preserve context so have to do this manually
            threadPool.schedule(threadPool.preserveContext(runnable), delay, ThreadPool.Names.SAME);
        }
    }
}
