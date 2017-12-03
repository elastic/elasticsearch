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
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent.Params;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;

/**
 * A scrollable source of results.
 */
public abstract class ScrollableHitSource {
    private final AtomicReference<String> scrollId = new AtomicReference<>();

    protected final Logger logger;
    protected final BackoffPolicy backoffPolicy;
    protected final ThreadPool threadPool;
    protected final Runnable countSearchRetry;
    protected final Consumer<Exception> fail;

    public ScrollableHitSource(Logger logger, BackoffPolicy backoffPolicy, ThreadPool threadPool, Runnable countSearchRetry,
            Consumer<Exception> fail) {
        this.logger = logger;
        this.backoffPolicy = backoffPolicy;
        this.threadPool = threadPool;
        this.countSearchRetry = countSearchRetry;
        this.fail = fail;
    }

    public final void start(Consumer<Response> onResponse) {
        doStart(response -> {
           setScroll(response.getScrollId());
           logger.debug("scroll returned [{}] documents with a scroll id of [{}]", response.getHits().size(), response.getScrollId());
           onResponse.accept(response);
        });
    }
    protected abstract void doStart(Consumer<? super Response> onResponse);

    public final void startNextScroll(TimeValue extraKeepAlive, Consumer<Response> onResponse) {
        doStartNextScroll(scrollId.get(), extraKeepAlive, response -> {
            setScroll(response.getScrollId());
            onResponse.accept(response);
        });
    }
    protected abstract void doStartNextScroll(String scrollId, TimeValue extraKeepAlive, Consumer<? super Response> onResponse);

    public final void close(Runnable onCompletion) {
        String scrollId = this.scrollId.get();
        if (Strings.hasLength(scrollId)) {
            clearScroll(scrollId, () -> cleanup(onCompletion));
        } else {
            cleanup(onCompletion);
        }
    }

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
         * The source of the hit. Returns null if the source didn't come back from the search, usually because it source wasn't stored at
         * all.
         */
        @Nullable BytesReference getSource();
        /**
         * The content type of the hit source. Returns null if the source didn't come back from the search.
         */
        @Nullable XContentType getXContentType();
        /**
         * The document id of the parent of the hit if there is a parent or null if there isn't.
         */
        @Nullable String getParent();
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
        private String parent;
        private String routing;

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
        public String getParent() {
            return parent;
        }

        public BasicHit setParent(String parent) {
            this.parent = parent;
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
                builder.field("index", index);
            }
            if (shardId != null) {
                builder.field("shard", shardId);
            }
            if (nodeId != null) {
                builder.field("node", nodeId);
            }
            builder.field("reason");
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
