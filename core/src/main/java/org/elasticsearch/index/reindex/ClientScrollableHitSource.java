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
import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.ClearScrollResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.ParentTaskAssigningClient;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.mapper.ParentFieldMapper;
import org.elasticsearch.index.mapper.RoutingFieldMapper;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;

import static java.util.Collections.emptyList;
import static java.util.Collections.unmodifiableList;
import static org.elasticsearch.common.unit.TimeValue.timeValueNanos;
import static org.elasticsearch.common.util.CollectionUtils.isEmpty;

/**
 * A scrollable source of hits from a {@linkplain Client} instance.
 */
public class ClientScrollableHitSource extends ScrollableHitSource {
    private final ParentTaskAssigningClient client;
    private final SearchRequest firstSearchRequest;

    public ClientScrollableHitSource(Logger logger, BackoffPolicy backoffPolicy, ThreadPool threadPool, Runnable countSearchRetry,
            Consumer<Exception> fail, ParentTaskAssigningClient client, SearchRequest firstSearchRequest) {
        super(logger, backoffPolicy, threadPool, countSearchRetry, fail);
        this.client = client;
        this.firstSearchRequest = firstSearchRequest;
    }

    @Override
    public void doStart(Consumer<? super Response> onResponse) {
        if (logger.isDebugEnabled()) {
            logger.debug("executing initial scroll against {}{}",
                    isEmpty(firstSearchRequest.indices()) ? "all indices" : firstSearchRequest.indices(),
                    isEmpty(firstSearchRequest.types()) ? "" : firstSearchRequest.types());
        }
        searchWithRetry(listener -> client.search(firstSearchRequest, listener), r -> consume(r, onResponse));
    }

    @Override
    protected void doStartNextScroll(String scrollId, TimeValue extraKeepAlive, Consumer<? super Response> onResponse) {
        searchWithRetry(listener -> {
            SearchScrollRequest request = new SearchScrollRequest();
            // Add the wait time into the scroll timeout so it won't timeout while we wait for throttling
            request.scrollId(scrollId).scroll(timeValueNanos(firstSearchRequest.scroll().keepAlive().nanos() + extraKeepAlive.nanos()));
            client.searchScroll(request, listener);
        }, r -> consume(r, onResponse));
    }

    @Override
    public void clearScroll(String scrollId, Runnable onCompletion) {
        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        clearScrollRequest.addScrollId(scrollId);
        /*
         * Unwrap the client so we don't set our task as the parent. If we *did* set our ID then the clear scroll would be cancelled as
         * if this task is cancelled. But we want to clear the scroll regardless of whether or not the main request was cancelled.
         */
        client.unwrap().clearScroll(clearScrollRequest, new ActionListener<ClearScrollResponse>() {
            @Override
            public void onResponse(ClearScrollResponse response) {
                logger.debug("Freed [{}] contexts", response.getNumFreed());
                onCompletion.run();
            }

            @Override
            public void onFailure(Exception e) {
                logger.warn((Supplier<?>) () -> new ParameterizedMessage("Failed to clear scroll [{}]", scrollId), e);
                onCompletion.run();
            }
        });
    }

    @Override
    protected void cleanup(Runnable onCompletion) {
        onCompletion.run();
    }

    /**
     * Run a search action and call onResponse when a the response comes in, retrying if the action fails with an exception caused by
     * rejected execution.
     *
     * @param action consumes a listener and starts the action. The listener it consumes is rigged to retry on failure.
     * @param onResponse consumes the response from the action
     */
    private void searchWithRetry(Consumer<ActionListener<SearchResponse>> action, Consumer<SearchResponse> onResponse) {
        /*
         * RetryHelper is both an AbstractRunnable and an ActionListener<SearchResponse> - meaning that it both starts the search and
         * handles reacts to the results. The complexity is all in onFailure which either adapts the failure to the "fail" listener or
         * retries the search. Since both AbstractRunnable and ActionListener define the onFailure method it is called for either failure
         * to run the action (either while running or before starting) and for failure on the response from the action.
         */
        class RetryHelper extends AbstractRunnable implements ActionListener<SearchResponse> {
            private final Iterator<TimeValue> retries = backoffPolicy.iterator();
            /**
             * The runnable to run that retries in the same context as the original call.
             */
            private Runnable retryWithContext;
            private volatile int retryCount = 0;

            @Override
            protected void doRun() throws Exception {
                action.accept(this);
            }

            @Override
            public void onResponse(SearchResponse response) {
                onResponse.accept(response);
            }

            @Override
            public void onFailure(Exception e) {
                if (ExceptionsHelper.unwrap(e, EsRejectedExecutionException.class) != null) {
                    if (retries.hasNext()) {
                        retryCount += 1;
                        TimeValue delay = retries.next();
                        logger.trace((Supplier<?>) () -> new ParameterizedMessage("retrying rejected search after [{}]", delay), e);
                        countSearchRetry.run();
                        threadPool.schedule(delay, ThreadPool.Names.SAME, retryWithContext);
                    } else {
                        logger.warn(
                            (Supplier<?>) () -> new ParameterizedMessage(
                                "giving up on search because we retried [{}] times without success", retryCount), e);
                        fail.accept(e);
                    }
                } else {
                    logger.warn("giving up on search because it failed with a non-retryable exception", e);
                    fail.accept(e);
                }
            }
        }
        RetryHelper helper = new RetryHelper();
        // Wrap the helper in a runnable that preserves the current context so we keep it on retry.
        helper.retryWithContext = threadPool.getThreadContext().preserveContext(helper);
        helper.run();
    }

    private void consume(SearchResponse response, Consumer<? super Response> onResponse) {
        onResponse.accept(wrap(response));
    }

    private Response wrap(SearchResponse response) {
        List<SearchFailure> failures;
        if (response.getShardFailures() == null) {
            failures = emptyList();
        } else {
            failures = new ArrayList<>(response.getShardFailures().length);
            for (ShardSearchFailure failure: response.getShardFailures()) {
                String nodeId = failure.shard() == null ? null : failure.shard().getNodeId();
                failures.add(new SearchFailure(failure.getCause(), failure.index(), failure.shardId(), nodeId));
            }
        }
        List<Hit> hits;
        if (response.getHits().getHits() == null || response.getHits().getHits().length == 0) {
            hits = emptyList();
        } else {
            hits = new ArrayList<>(response.getHits().getHits().length);
            for (SearchHit hit: response.getHits().getHits()) {
                hits.add(new ClientHit(hit));
            }
            hits = unmodifiableList(hits);
        }
        return new Response(response.isTimedOut(), failures, response.getHits().getTotalHits(),
                hits, response.getScrollId());
    }

    private static class ClientHit implements Hit {
        private final SearchHit delegate;
        private final BytesReference source;

        ClientHit(SearchHit delegate) {
            this.delegate = delegate;
            source = delegate.hasSource() ? delegate.getSourceRef() : null;
        }

        @Override
        public String getIndex() {
            return delegate.getIndex();
        }

        @Override
        public String getType() {
            return delegate.getType();
        }

        @Override
        public String getId() {
            return delegate.getId();
        }

        @Override
        public BytesReference getSource() {
            return source;
        }

        @Override
        public XContentType getXContentType() {
            return XContentFactory.xContentType(source);
        }
        @Override
        public long getVersion() {
            return delegate.getVersion();
        }

        @Override
        public String getParent() {
            return fieldValue(ParentFieldMapper.NAME);
        }

        @Override
        public String getRouting() {
            return fieldValue(RoutingFieldMapper.NAME);
        }

        private <T> T fieldValue(String fieldName) {
            DocumentField field = delegate.field(fieldName);
            return field == null ? null : field.getValue();
        }
    }
}
