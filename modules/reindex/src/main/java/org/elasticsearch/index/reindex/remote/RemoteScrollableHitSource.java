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

package org.elasticsearch.index.reindex.remote;

import org.apache.http.HttpEntity;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.ParseFieldMatcherSupplier;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.xcontent.XContent;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.reindex.ScrollableHitSource;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Consumer;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.common.unit.TimeValue.timeValueMillis;
import static org.elasticsearch.common.unit.TimeValue.timeValueNanos;
import static org.elasticsearch.index.reindex.remote.RemoteRequestBuilders.initialSearchEntity;
import static org.elasticsearch.index.reindex.remote.RemoteRequestBuilders.initialSearchParams;
import static org.elasticsearch.index.reindex.remote.RemoteRequestBuilders.initialSearchPath;
import static org.elasticsearch.index.reindex.remote.RemoteRequestBuilders.scrollEntity;
import static org.elasticsearch.index.reindex.remote.RemoteRequestBuilders.scrollParams;
import static org.elasticsearch.index.reindex.remote.RemoteRequestBuilders.scrollPath;
import static org.elasticsearch.index.reindex.remote.RemoteResponseParsers.MAIN_ACTION_PARSER;
import static org.elasticsearch.index.reindex.remote.RemoteResponseParsers.RESPONSE_PARSER;

public class RemoteScrollableHitSource extends ScrollableHitSource {
    private final AsyncClient client;
    private final BytesReference query;
    private final SearchRequest searchRequest;
    Version remoteVersion;

    public RemoteScrollableHitSource(ESLogger logger, BackoffPolicy backoffPolicy, ThreadPool threadPool, Runnable countSearchRetry,
            Consumer<Exception> fail, AsyncClient client, BytesReference query, SearchRequest searchRequest) {
        super(logger, backoffPolicy, threadPool, countSearchRetry, fail);
        this.query = query;
        this.searchRequest = searchRequest;
        this.client = client;
    }

    @Override
    public void close() {
        try {
            client.close();
        } catch (IOException e) {
            fail.accept(new IOException("couldn't close the remote connection", e));
        }
    }

    @Override
    protected void doStart(Consumer<? super Response> onResponse) {
        lookupRemoteVersion(version -> {
            remoteVersion = version;
            execute("POST", initialSearchPath(searchRequest), initialSearchParams(searchRequest, version),
                    initialSearchEntity(query), RESPONSE_PARSER, r -> onStartResponse(onResponse, r));
        });
    }

    void lookupRemoteVersion(Consumer<Version> onVersion) {
        execute("GET", "", emptyMap(), null, MAIN_ACTION_PARSER, onVersion);
        
    }

    void onStartResponse(Consumer<? super Response> onResponse, Response response) {
        if (Strings.hasLength(response.getScrollId()) && response.getHits().isEmpty()) {
            logger.debug("First response looks like a scan response. Jumping right to the second. scroll=[{}]", response.getScrollId());
            doStartNextScroll(response.getScrollId(), timeValueMillis(0), onResponse);
        } else {
            onResponse.accept(response);
        }
    }

    @Override
    protected void doStartNextScroll(String scrollId, TimeValue extraKeepAlive, Consumer<? super Response> onResponse) {
        execute("POST", scrollPath(), scrollParams(timeValueNanos(searchRequest.scroll().keepAlive().nanos() + extraKeepAlive.nanos())),
                scrollEntity(scrollId), RESPONSE_PARSER, onResponse);
    }

    @Override
    protected void clearScroll(String scrollId) {
        // Need to throw out response....
        client.performRequest("DELETE", scrollPath(), emptyMap(), scrollEntity(scrollId), new ResponseListener() {
            @Override
            public void onResponse(InputStream response) {
                logger.debug("Successfully cleared [{}]", scrollId);
            }

            @Override
            public void onRetryableFailure(Exception t) {
                onFailure(t);
            }

            @Override
            public void onFailure(Exception t) {
                logger.warn("Failed to clear scroll [{}]", t, scrollId);
            }
        });
    }

    <T> void execute(String method, String uri, Map<String, String> params, HttpEntity entity,
            BiFunction<XContentParser, ParseFieldMatcherSupplier, T> parser, Consumer<? super T> listener) {
        class RetryHelper extends AbstractRunnable {
            private final Iterator<TimeValue> retries = backoffPolicy.iterator();

            @Override
            protected void doRun() throws Exception {
                client.performRequest(method, uri, params, entity, new ResponseListener() {
                    @Override
                    public void onResponse(InputStream content) {
                        T response;
                        try {
                            XContent xContent = XContentFactory.xContentType(content).xContent();
                            try(XContentParser xContentParser = xContent.createParser(content)) {
                                response = parser.apply(xContentParser, () -> ParseFieldMatcher.STRICT);
                            }
                        } catch (IOException e) {
                            throw new ElasticsearchException("Error deserializing response", e);
                        }
                        listener.accept(response);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        fail.accept(e);
                    }

                    @Override
                    public void onRetryableFailure(Exception t) {
                        if (retries.hasNext()) {
                            TimeValue delay = retries.next();
                            logger.trace("retrying rejected search after [{}]", t, delay);
                            countSearchRetry.run();
                            threadPool.schedule(delay, ThreadPool.Names.SAME, RetryHelper.this);
                        } else {
                            fail.accept(t);
                        }
                    }
                });
            }

            @Override
            public void onFailure(Exception t) {
                fail.accept(t);
            }
        }
        new RetryHelper().run();
    }

    public interface AsyncClient extends Closeable {
        void performRequest(String method, String uri, Map<String, String> params, HttpEntity entity, ResponseListener listener);
    }

    public interface ResponseListener extends ActionListener<InputStream> {
        void onRetryableFailure(Exception t);
    }

    public static class AsynchronizingRestClient implements AsyncClient {
        private final ThreadPool threadPool;
        private final RestClient restClient;

        public AsynchronizingRestClient(ThreadPool threadPool, RestClient restClient) {
            this.threadPool = threadPool;
            this.restClient = restClient;
        }

        @Override
        public void performRequest(String method, String uri, Map<String, String> params, HttpEntity entity,
                ResponseListener listener) {
            /*
             * We use the generic thread pool here because this client is blocking the generic thread pool is sized appropriately for some
             * of the threads on it to be blocked, waiting on IO. It'd be a disaster if this ran on the listener thread pool, eating
             * valuable threads needed to handle responses. Most other thread pool would probably not mind running this either, but the
             * generic thread pool is the "most right" place for it to run. We could make our own thread pool for this but the generic
             * thread pool already has plenty of capacity.
             */
            threadPool.generic().execute(new AbstractRunnable() {
                @Override
                protected void doRun() throws Exception {
                    org.elasticsearch.client.Response response = restClient.performRequest(method, uri, params, entity);
                    InputStream markSupportedInputStream = new BufferedInputStream(response.getEntity().getContent());
                    listener.onResponse(markSupportedInputStream);
                }

                @Override
                public void onFailure(Exception t) {
                    if (t instanceof ResponseException) {
                        ResponseException re = (ResponseException) t;
                        if (RestStatus.TOO_MANY_REQUESTS.getStatus() == re.getResponse().getStatusLine().getStatusCode()) {
                            listener.onRetryableFailure(t);
                            return;
                        }
                    }
                    listener.onFailure(t);
                }
            });
        }

        @Override
        public void close() throws IOException {
            restClient.close();
        }
    }
}
