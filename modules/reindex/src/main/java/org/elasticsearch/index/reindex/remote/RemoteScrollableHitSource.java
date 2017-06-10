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

import org.apache.http.ContentTooLongException;
import org.apache.http.HttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.Version;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.index.reindex.ScrollableHitSource;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.ResponseListener;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Consumer;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.common.unit.TimeValue.timeValueMillis;
import static org.elasticsearch.common.unit.TimeValue.timeValueNanos;
import static org.elasticsearch.index.reindex.remote.RemoteRequestBuilders.clearScrollEntity;
import static org.elasticsearch.index.reindex.remote.RemoteRequestBuilders.initialSearchEntity;
import static org.elasticsearch.index.reindex.remote.RemoteRequestBuilders.initialSearchParams;
import static org.elasticsearch.index.reindex.remote.RemoteRequestBuilders.initialSearchPath;
import static org.elasticsearch.index.reindex.remote.RemoteRequestBuilders.scrollEntity;
import static org.elasticsearch.index.reindex.remote.RemoteRequestBuilders.scrollParams;
import static org.elasticsearch.index.reindex.remote.RemoteRequestBuilders.scrollPath;
import static org.elasticsearch.index.reindex.remote.RemoteResponseParsers.MAIN_ACTION_PARSER;
import static org.elasticsearch.index.reindex.remote.RemoteResponseParsers.RESPONSE_PARSER;

public class RemoteScrollableHitSource extends ScrollableHitSource {
    private final RestClient client;
    private final BytesReference query;
    private final SearchRequest searchRequest;
    Version remoteVersion;

    public RemoteScrollableHitSource(Logger logger, BackoffPolicy backoffPolicy, ThreadPool threadPool, Runnable countSearchRetry,
            Consumer<Exception> fail, RestClient client, BytesReference query, SearchRequest searchRequest) {
        super(logger, backoffPolicy, threadPool, countSearchRetry, fail);
        this.query = query;
        this.searchRequest = searchRequest;
        this.client = client;
    }

    @Override
    protected void doStart(Consumer<? super Response> onResponse) {
        lookupRemoteVersion(version -> {
            remoteVersion = version;
            execute("POST", initialSearchPath(searchRequest), initialSearchParams(searchRequest, version),
                    initialSearchEntity(searchRequest, query, remoteVersion), RESPONSE_PARSER, r -> onStartResponse(onResponse, r));
        });
    }

    void lookupRemoteVersion(Consumer<Version> onVersion) {
        execute("GET", "", emptyMap(), null, MAIN_ACTION_PARSER, onVersion);
    }

    private void onStartResponse(Consumer<? super Response> onResponse, Response response) {
        if (Strings.hasLength(response.getScrollId()) && response.getHits().isEmpty()) {
            logger.debug("First response looks like a scan response. Jumping right to the second. scroll=[{}]", response.getScrollId());
            doStartNextScroll(response.getScrollId(), timeValueMillis(0), onResponse);
        } else {
            onResponse.accept(response);
        }
    }

    @Override
    protected void doStartNextScroll(String scrollId, TimeValue extraKeepAlive, Consumer<? super Response> onResponse) {
        Map<String, String> scrollParams = scrollParams(
                timeValueNanos(searchRequest.scroll().keepAlive().nanos() + extraKeepAlive.nanos()),
                remoteVersion);
        execute("POST", scrollPath(), scrollParams, scrollEntity(scrollId, remoteVersion), RESPONSE_PARSER, onResponse);
    }

    @Override
    protected void clearScroll(String scrollId, Runnable onCompletion) {
        client.performRequestAsync("DELETE", scrollPath(), emptyMap(), clearScrollEntity(scrollId, remoteVersion), new ResponseListener() {
            @Override
            public void onSuccess(org.elasticsearch.client.Response response) {
                logger.debug("Successfully cleared [{}]", scrollId);
                onCompletion.run();
            }

            @Override
            public void onFailure(Exception e) {
                logFailure(e);
                onCompletion.run();
            }

            private void logFailure(Exception e) {
                if (e instanceof ResponseException) {
                    ResponseException re = (ResponseException) e;
                            if (remoteVersion.before(Version.fromId(2000099))
                                    && re.getResponse().getStatusLine().getStatusCode() == 404) {
                        logger.debug((Supplier<?>) () -> new ParameterizedMessage(
                                "Failed to clear scroll [{}] from pre-2.0 Elasticsearch. This is normal if the request terminated "
                                        + "normally as the scroll has already been cleared automatically.", scrollId), e);
                        return;
                    }
                }
                logger.warn((Supplier<?>) () -> new ParameterizedMessage("Failed to clear scroll [{}]", scrollId), e);
            }
        });
    }

    @Override
    protected void cleanup(Runnable onCompletion) {
        /* This is called on the RestClient's thread pool and attempting to close the client on its
         * own threadpool causes it to fail to close. So we always shutdown the RestClient
         * asynchronously on a thread in Elasticsearch's generic thread pool. */
        threadPool.generic().submit(() -> {
            try {
                client.close();
                logger.debug("Shut down remote connection");
            } catch (IOException e) {
                logger.error("Failed to shutdown the remote connection", e);
            } finally {
                onCompletion.run();
            }
        });
    }

    private <T> void execute(String method, String uri, Map<String, String> params, HttpEntity entity,
                             BiFunction<XContentParser, XContentType, T> parser, Consumer<? super T> listener) {
        // Preserve the thread context so headers survive after the call
        java.util.function.Supplier<ThreadContext.StoredContext> contextSupplier = threadPool.getThreadContext().newRestorableContext(true);
        class RetryHelper extends AbstractRunnable {
            private final Iterator<TimeValue> retries = backoffPolicy.iterator();

            @Override
            protected void doRun() throws Exception {
                client.performRequestAsync(method, uri, params, entity, new ResponseListener() {
                    @Override
                    public void onSuccess(org.elasticsearch.client.Response response) {
                        // Restore the thread context to get the precious headers
                        try (ThreadContext.StoredContext ctx = contextSupplier.get()) {
                            assert ctx != null; // eliminates compiler warning
                            T parsedResponse;
                            try {
                                HttpEntity responseEntity = response.getEntity();
                                InputStream content = responseEntity.getContent();
                                XContentType xContentType = null;
                                if (responseEntity.getContentType() != null) {
                                    final String mimeType = ContentType.parse(responseEntity.getContentType().getValue()).getMimeType();
                                    xContentType = XContentType.fromMediaType(mimeType);
                                }
                                if (xContentType == null) {
                                    try {
                                        throw new ElasticsearchException(
                                            "Response didn't include Content-Type: " + bodyMessage(response.getEntity()));
                                    } catch (IOException e) {
                                        ElasticsearchException ee = new ElasticsearchException("Error extracting body from response");
                                        ee.addSuppressed(e);
                                        throw ee;
                                    }
                                }
                                // EMPTY is safe here because we don't call namedObject
                                try (XContentParser xContentParser = xContentType.xContent().createParser(NamedXContentRegistry.EMPTY,
                                    content)) {
                                    parsedResponse = parser.apply(xContentParser, xContentType);
                                } catch (ParsingException e) {
                                /* Because we're streaming the response we can't get a copy of it here. The best we can do is hint that it
                                 * is totally wrong and we're probably not talking to Elasticsearch. */
                                    throw new ElasticsearchException(
                                        "Error parsing the response, remote is likely not an Elasticsearch instance", e);
                                }
                            } catch (IOException e) {
                                throw new ElasticsearchException(
                                    "Error deserializing response, remote is likely not an Elasticsearch instance", e);
                            }
                            listener.accept(parsedResponse);
                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        try (ThreadContext.StoredContext ctx = contextSupplier.get()) {
                            assert ctx != null; // eliminates compiler warning
                            if (e instanceof ResponseException) {
                                ResponseException re = (ResponseException) e;
                                if (RestStatus.TOO_MANY_REQUESTS.getStatus() == re.getResponse().getStatusLine().getStatusCode()) {
                                    if (retries.hasNext()) {
                                        TimeValue delay = retries.next();
                                        logger.trace(
                                            (Supplier<?>) () -> new ParameterizedMessage("retrying rejected search after [{}]", delay), e);
                                        countSearchRetry.run();
                                        threadPool.schedule(delay, ThreadPool.Names.SAME, RetryHelper.this);
                                        return;
                                    }
                                }
                                e = wrapExceptionToPreserveStatus(re.getResponse().getStatusLine().getStatusCode(),
                                    re.getResponse().getEntity(), re);
                            } else if (e instanceof ContentTooLongException) {
                                e = new IllegalArgumentException(
                                    "Remote responded with a chunk that was too large. Use a smaller batch size.", e);
                            }
                            fail.accept(e);
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

    /**
     * Wrap the ResponseException in an exception that'll preserve its status code if possible so we can send it back to the user. We might
     * not have a constant for the status code so in that case we just use 500 instead. We also extract make sure to include the response
     * body in the message so the user can figure out *why* the remote Elasticsearch service threw the error back to us.
     */
    static ElasticsearchStatusException wrapExceptionToPreserveStatus(int statusCode, @Nullable HttpEntity entity, Exception cause) {
        RestStatus status = RestStatus.fromCode(statusCode);
        String messagePrefix = "";
        if (status == null) {
            messagePrefix = "Couldn't extract status [" + statusCode + "]. ";
            status = RestStatus.INTERNAL_SERVER_ERROR;
        }
        try {
            return new ElasticsearchStatusException(messagePrefix + bodyMessage(entity), status, cause);
        } catch (IOException ioe) {
            ElasticsearchStatusException e = new ElasticsearchStatusException(messagePrefix + "Failed to extract body.", status, cause);
            e.addSuppressed(ioe);
            return e;
        }
    }

    static String bodyMessage(@Nullable HttpEntity entity) throws IOException {
        if (entity == null) {
            return "No error body.";
        } else {
            return "body=" + EntityUtils.toString(entity);
        }
    }
}
