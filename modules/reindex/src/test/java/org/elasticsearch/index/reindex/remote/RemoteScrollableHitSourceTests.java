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
import org.elasticsearch.Version;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.index.reindex.ScrollableHitSource.Response;
import org.elasticsearch.index.reindex.remote.RemoteScrollableHitSource.ResponseListener;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static org.elasticsearch.common.unit.TimeValue.timeValueMillis;
import static org.elasticsearch.common.unit.TimeValue.timeValueMinutes;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;

public class RemoteScrollableHitSourceTests extends ESTestCase {
    private final String FAKE_SCROLL_ID = "DnF1ZXJ5VGhlbkZldGNoBQAAAfakescroll";
    private int retries;
    private ThreadPool threadPool;
    private SearchRequest searchRequest;
    private int retriesAllowed;

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool(getTestName()) {
            @Override
            public Executor executor(String name) {
                return r -> r.run();
            }

            @Override
            public ScheduledFuture<?> schedule(TimeValue delay, String name, Runnable command) {
                command.run();
                return null;
            }
        };
        retries = 0;
        searchRequest = new SearchRequest();
        searchRequest.scroll(timeValueMinutes(5));
        searchRequest.source(new SearchSourceBuilder().size(10).version(true).sort("_doc").size(123));
        retriesAllowed = 0;
    }

    @After
    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        terminate(threadPool);
    }

    public void testLookupRemoteVersion() throws Exception {
        sourceWithMockedRemoteCall(false, "main/0_20_5.json").lookupRemoteVersion(v -> assertEquals(Version.fromString("0.20.5"), v));
        sourceWithMockedRemoteCall(false, "main/0_90_13.json").lookupRemoteVersion(v -> assertEquals(Version.fromString("0.90.13"), v));
        sourceWithMockedRemoteCall(false, "main/1_7_5.json").lookupRemoteVersion(v -> assertEquals(Version.fromString("1.7.5"), v));
        sourceWithMockedRemoteCall(false, "main/2_3_3.json").lookupRemoteVersion(v -> assertEquals(Version.V_2_3_3, v));
        sourceWithMockedRemoteCall(false, "main/5_0_0_alpha_3.json").lookupRemoteVersion(v -> assertEquals(Version.V_5_0_0_alpha3, v));
    }

    public void testParseStartOk() throws Exception {
        AtomicBoolean called = new AtomicBoolean();
        sourceWithMockedRemoteCall("start_ok.json").doStart(r -> {
            assertFalse(r.isTimedOut());
            assertEquals(FAKE_SCROLL_ID, r.getScrollId());
            assertEquals(4, r.getTotalHits());
            assertThat(r.getFailures(), empty());
            assertThat(r.getHits(), hasSize(1));
            assertEquals("test", r.getHits().get(0).getIndex());
            assertEquals("test", r.getHits().get(0).getType());
            assertEquals("AVToMiC250DjIiBO3yJ_", r.getHits().get(0).getId());
            assertEquals("{\"test\":\"test2\"}", r.getHits().get(0).getSource().utf8ToString());
            assertNull(r.getHits().get(0).getTTL());
            assertNull(r.getHits().get(0).getTimestamp());
            assertNull(r.getHits().get(0).getRouting());
            called.set(true);
        });
        assertTrue(called.get());
    }

    public void testParseScrollOk() throws Exception {
        AtomicBoolean called = new AtomicBoolean();
        sourceWithMockedRemoteCall("scroll_ok.json").doStartNextScroll("", timeValueMillis(0), r -> {
            assertFalse(r.isTimedOut());
            assertEquals(FAKE_SCROLL_ID, r.getScrollId());
            assertEquals(4, r.getTotalHits());
            assertThat(r.getFailures(), empty());
            assertThat(r.getHits(), hasSize(1));
            assertEquals("test", r.getHits().get(0).getIndex());
            assertEquals("test", r.getHits().get(0).getType());
            assertEquals("AVToMiDL50DjIiBO3yKA", r.getHits().get(0).getId());
            assertEquals("{\"test\":\"test3\"}", r.getHits().get(0).getSource().utf8ToString());
            assertNull(r.getHits().get(0).getTTL());
            assertNull(r.getHits().get(0).getTimestamp());
            assertNull(r.getHits().get(0).getRouting());
            called.set(true);
        });
        assertTrue(called.get());
    }

    /**
     * Test for parsing _ttl, _timestamp, and _routing.
     */
    public void testParseScrollFullyLoaded() throws Exception {
        AtomicBoolean called = new AtomicBoolean();
        sourceWithMockedRemoteCall("scroll_fully_loaded.json").doStartNextScroll("", timeValueMillis(0), r -> {
            assertEquals("AVToMiDL50DjIiBO3yKA", r.getHits().get(0).getId());
            assertEquals("{\"test\":\"test3\"}", r.getHits().get(0).getSource().utf8ToString());
            assertEquals((Long) 1234L, r.getHits().get(0).getTTL());
            assertEquals((Long) 123444L, r.getHits().get(0).getTimestamp());
            assertEquals("testrouting", r.getHits().get(0).getRouting());
            assertEquals("testparent", r.getHits().get(0).getParent());
            called.set(true);
        });
        assertTrue(called.get());
    }

    /**
     * Versions of Elasticsearch before 2.1.0 don't support sort:_doc and instead need to use search_type=scan. Scan doesn't return
     * documents the first iteration but reindex doesn't like that. So we jump start strait to the next iteration.
     */
    public void testScanJumpStart() throws Exception {
        AtomicBoolean called = new AtomicBoolean();
        sourceWithMockedRemoteCall("start_scan.json", "scroll_ok.json").doStart(r -> {
            assertFalse(r.isTimedOut());
            assertEquals(FAKE_SCROLL_ID, r.getScrollId());
            assertEquals(4, r.getTotalHits());
            assertThat(r.getFailures(), empty());
            assertThat(r.getHits(), hasSize(1));
            assertEquals("test", r.getHits().get(0).getIndex());
            assertEquals("test", r.getHits().get(0).getType());
            assertEquals("AVToMiDL50DjIiBO3yKA", r.getHits().get(0).getId());
            assertEquals("{\"test\":\"test3\"}", r.getHits().get(0).getSource().utf8ToString());
            assertNull(r.getHits().get(0).getTTL());
            assertNull(r.getHits().get(0).getTimestamp());
            assertNull(r.getHits().get(0).getRouting());
            called.set(true);
        });
        assertTrue(called.get());
    }

    public void testParseRejection() throws Exception {
        // The rejection comes through in the handler because the mocked http response isn't marked as an error
        AtomicBoolean called = new AtomicBoolean();
        // Handling a scroll rejection is the same as handling a search rejection so we reuse the verification code
        Consumer<Response> checkResponse = r -> {
            assertFalse(r.isTimedOut());
            assertEquals(FAKE_SCROLL_ID, r.getScrollId());
            assertEquals(4, r.getTotalHits());
            assertThat(r.getFailures(), hasSize(1));
            assertEquals("test", r.getFailures().get(0).getIndex());
            assertEquals((Integer) 0, r.getFailures().get(0).getShardId());
            assertEquals("87A7NvevQxSrEwMbtRCecg", r.getFailures().get(0).getNodeId());
            assertThat(r.getFailures().get(0).getReason(), instanceOf(EsRejectedExecutionException.class));
            assertEquals("rejected execution of org.elasticsearch.transport.TransportService$5@52d06af2 on "
                    + "EsThreadPoolExecutor[search, queue capacity = 1000, org.elasticsearch.common.util.concurrent."
                    + "EsThreadPoolExecutor@778ea553[Running, pool size = 7, active threads = 7, queued tasks = 1000, "
                    + "completed tasks = 4182]]", r.getFailures().get(0).getReason().getMessage());
            assertThat(r.getHits(), hasSize(1));
            assertEquals("test", r.getHits().get(0).getIndex());
            assertEquals("test", r.getHits().get(0).getType());
            assertEquals("AVToMiC250DjIiBO3yJ_", r.getHits().get(0).getId());
            assertEquals("{\"test\":\"test1\"}", r.getHits().get(0).getSource().utf8ToString());
            called.set(true);
        };
        sourceWithMockedRemoteCall("rejection.json").doStart(checkResponse);
        assertTrue(called.get());
        called.set(false);
        sourceWithMockedRemoteCall("rejection.json").doStartNextScroll("scroll", timeValueMillis(0), checkResponse);
        assertTrue(called.get());
    }

    public void testParseFailureWithStatus() throws Exception {
        // The rejection comes through in the handler because the mocked http response isn't marked as an error
        AtomicBoolean called = new AtomicBoolean();
        // Handling a scroll rejection is the same as handling a search rejection so we reuse the verification code
        Consumer<Response> checkResponse = r -> {
            assertFalse(r.isTimedOut());
            assertEquals(FAKE_SCROLL_ID, r.getScrollId());
            assertEquals(10000, r.getTotalHits());
            assertThat(r.getFailures(), hasSize(1));
            assertEquals(null, r.getFailures().get(0).getIndex());
            assertEquals(null, r.getFailures().get(0).getShardId());
            assertEquals(null, r.getFailures().get(0).getNodeId());
            assertThat(r.getFailures().get(0).getReason(), instanceOf(RuntimeException.class));
            assertEquals("Unknown remote exception with reason=[SearchContextMissingException[No search context found for id [82]]]",
                    r.getFailures().get(0).getReason().getMessage());
            assertThat(r.getHits(), hasSize(1));
            assertEquals("test", r.getHits().get(0).getIndex());
            assertEquals("test", r.getHits().get(0).getType());
            assertEquals("10000", r.getHits().get(0).getId());
            assertEquals("{\"test\":\"test10000\"}", r.getHits().get(0).getSource().utf8ToString());
            called.set(true);
        };
        sourceWithMockedRemoteCall("failure_with_status.json").doStart(checkResponse);
        assertTrue(called.get());
        called.set(false);
        sourceWithMockedRemoteCall("failure_with_status.json").doStartNextScroll("scroll", timeValueMillis(0), checkResponse);
        assertTrue(called.get());
    }

    public void testParseRequestFailure() throws Exception {
        AtomicBoolean called = new AtomicBoolean();
        Consumer<Response> checkResponse = r -> {
            assertFalse(r.isTimedOut());
            assertNull(r.getScrollId());
            assertEquals(0, r.getTotalHits());
            assertThat(r.getFailures(), hasSize(1));
            assertThat(r.getFailures().get(0).getReason(), instanceOf(ParsingException.class));
            ParsingException failure = (ParsingException) r.getFailures().get(0).getReason();
            assertEquals("Unknown key for a VALUE_STRING in [invalid].", failure.getMessage());
            assertEquals(2, failure.getLineNumber());
            assertEquals(14, failure.getColumnNumber());
            called.set(true);
        };
        sourceWithMockedRemoteCall("request_failure.json").doStart(checkResponse);
        assertTrue(called.get());
        called.set(false);
        sourceWithMockedRemoteCall("request_failure.json").doStartNextScroll("scroll", timeValueMillis(0), checkResponse);
        assertTrue(called.get());
    }

    public void testRetryAndSucceed() throws Exception {
        AtomicBoolean called = new AtomicBoolean();
        Consumer<Response> checkResponse = r -> {
            assertThat(r.getFailures(), hasSize(0));
            called.set(true);
        };
        retriesAllowed = between(1, Integer.MAX_VALUE);
        sourceWithMockedRemoteCall("fail:rejection.json", "start_ok.json").doStart(checkResponse);
        assertTrue(called.get());
        assertEquals(1, retries);
        retries = 0;
        called.set(false);
        sourceWithMockedRemoteCall("fail:rejection.json", "scroll_ok.json").doStartNextScroll("scroll", timeValueMillis(0),
                checkResponse);
        assertTrue(called.get());
        assertEquals(1, retries);
    }

    public void testRetryUntilYouRunOutOfTries() throws Exception {
        AtomicBoolean called = new AtomicBoolean();
        Consumer<Response> checkResponse = r -> called.set(true);
        retriesAllowed = between(0, 10);
        String[] paths = new String[retriesAllowed + 2];
        for (int i = 0; i < retriesAllowed + 2; i++) {
            paths[i] = "fail:rejection.json";
        }
        RuntimeException e = expectThrows(RuntimeException.class, () -> sourceWithMockedRemoteCall(paths).doStart(checkResponse));
        assertEquals("failed", e.getMessage());
        assertFalse(called.get());
        assertEquals(retriesAllowed, retries);
        retries = 0;
        e = expectThrows(RuntimeException.class,
                () -> sourceWithMockedRemoteCall(paths).doStartNextScroll("scroll", timeValueMillis(0), checkResponse));
        assertEquals("failed", e.getMessage());
        assertFalse(called.get());
        assertEquals(retriesAllowed, retries);
    }

    private RemoteScrollableHitSource sourceWithMockedRemoteCall(String... paths) throws Exception {
        return sourceWithMockedRemoteCall(true, paths);
    }

    /**
     * Creates a hit source that doesn't make the remote request and instead returns data from some files. Also requests are always returned
     * synchronously rather than asynchronously.
     */
    private RemoteScrollableHitSource sourceWithMockedRemoteCall(boolean mockRemoteVersion, String... paths) throws Exception {
        URL[] resources = new URL[paths.length];
        for (int i = 0; i < paths.length; i++) {
            resources[i] = Thread.currentThread().getContextClassLoader().getResource("responses/" + paths[i].replace("fail:", ""));
            if (resources[i] == null) {
                throw new IllegalArgumentException("Couldn't find [" + paths[i] + "]");
            }
        }
        RemoteScrollableHitSource.AsyncClient client = new RemoteScrollableHitSource.AsyncClient() {
            int responseCount = 0;
            @Override
            public void performRequest(String method, String uri, Map<String, String> params, HttpEntity entity,
                    ResponseListener listener) {
                try {
                    URL resource = resources[responseCount];
                    String path = paths[responseCount++];
                    InputStream stream = resource.openStream();
                    if (path.startsWith("fail:")) {
                        String body = Streams.copyToString(new InputStreamReader(entity.getContent(), StandardCharsets.UTF_8));
                        if (path.equals("fail:rejection.json")) {
                            listener.onRetryableFailure(new RuntimeException(body));
                        } else {
                            listener.onFailure(new RuntimeException(body));
                        }
                    } else {
                        listener.onResponse(stream);
                    }
                } catch (IOException e) {
                    listener.onFailure(e);
                }
            }

            @Override
            public void close() throws IOException {
            }
        };
        TestRemoteScrollableHitSource hitSource = new TestRemoteScrollableHitSource(client) {
            @Override
            void lookupRemoteVersion(Consumer<Version> onVersion) {
                if (mockRemoteVersion) {
                    onVersion.accept(Version.CURRENT);
                } else {
                    super.lookupRemoteVersion(onVersion);
                }
            }
        };
        if (mockRemoteVersion) {
            hitSource.remoteVersion = Version.CURRENT;
        }
        return hitSource;
    }

    private BackoffPolicy backoff() {
        return BackoffPolicy.constantBackoff(timeValueMillis(0), retriesAllowed);
    }

    private void countRetry() {
        retries += 1;
    }

    private void failRequest(Throwable t) {
        throw new RuntimeException("failed", t);
    }

    private class TestRemoteScrollableHitSource extends RemoteScrollableHitSource {
        public TestRemoteScrollableHitSource(RemoteScrollableHitSource.AsyncClient client) {
            super(RemoteScrollableHitSourceTests.this.logger, backoff(), RemoteScrollableHitSourceTests.this.threadPool,
                    RemoteScrollableHitSourceTests.this::countRetry, RemoteScrollableHitSourceTests.this::failRequest, client,
                    new BytesArray("{}"), RemoteScrollableHitSourceTests.this.searchRequest);
        }
    }
}
