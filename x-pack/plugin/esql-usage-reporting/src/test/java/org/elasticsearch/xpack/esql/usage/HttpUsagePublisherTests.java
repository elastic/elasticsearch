/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.usage;

import com.carrotsearch.randomizedtesting.ThreadFilter;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import org.apache.hc.core5.http.nio.ssl.TlsStrategy;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.ssl.SslConfiguration;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.mocksocket.MockHttpServer;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ssl.SslProfile;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLSocketFactory;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.nullValue;

/**
 * Runs the real JDK client against a local plain-HTTP server (mutual TLS and the entitlements are checked end to end by the fake
 * Usage API tests). The server decides the status from the ids in the request body, which is what lets a test make one record "bad".
 */
@SuppressForbidden(reason = "use a http server")
@ThreadLeakFilters(filters = { HttpUsagePublisherTests.JdkHttpClientThreadFilter.class })
public class HttpUsagePublisherTests extends ESTestCase {

    /** The JDK client's selector thread and the common pool workers it uses outlive the test; the production code has the same. */
    public static final class JdkHttpClientThreadFilter implements ThreadFilter {
        @Override
        public boolean reject(Thread t) {
            String name = t.getName();
            return name.startsWith("HttpClient") || name.startsWith("ForkJoinPool.commonPool-worker-");
        }
    }

    /** The body used by these tests: the ids, comma separated; the record format is tested by its own branch. */
    private static final Function<List<UsageRecord>, BytesReference> SERIALIZER = records -> new BytesArray(
        records.stream().map(UsageRecord::id).collect(Collectors.joining(","))
    );

    /** One request seen by the server. */
    private record Request(String method, String contentType, List<String> ids) {}

    private HttpServer server;
    private final List<Request> requests = new CopyOnWriteArrayList<>();
    private final CountDownLatch releaseHandlers = new CountDownLatch(1);
    /** Maps the ids of a request to the status of the answer; {@code -1} drops the connection without answering. */
    private volatile Function<List<String>, Integer> statusFor = ids -> 201;
    private TestSslProfile profile;
    private HttpUsagePublisher publisher;

    @Before
    public void startServer() throws IOException {
        server = MockHttpServer.createHttp(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        server.createContext("/api/v1/usage", this::handle);
        server.start();
        profile = new TestSslProfile();
        publisher = newPublisher(TimeValue.timeValueSeconds(10));
    }

    @After
    public void stopServer() {
        releaseHandlers.countDown();
        publisher.close();
        server.stop(0);
    }

    private HttpUsagePublisher newPublisher(TimeValue timeout) {
        URI uri = URI.create("http://" + server.getAddress().getHostString() + ":" + server.getAddress().getPort() + "/api/v1/usage");
        return new HttpUsagePublisher(uri, timeout, profile, SERIALIZER);
    }

    private void handle(HttpExchange exchange) throws IOException {
        String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
        List<String> ids = body.isEmpty() ? List.of() : Arrays.asList(body.split(","));
        requests.add(new Request(exchange.getRequestMethod(), exchange.getRequestHeaders().getFirst("Content-Type"), ids));
        int status = statusFor.apply(ids);
        if (status < 0) {
            exchange.close();
            return;
        }
        if (status == 0) {
            try {
                releaseHandlers.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            exchange.close();
            return;
        }
        byte[] answer = status >= 400 ? "{\"error\":\"nope\"}".getBytes(StandardCharsets.UTF_8) : new byte[0];
        exchange.sendResponseHeaders(status, answer.length == 0 ? -1 : answer.length);
        if (answer.length > 0) {
            exchange.getResponseBody().write(answer);
        }
        exchange.close();
    }

    private static List<UsageRecord> records(int count) {
        return IntStream.range(0, count).mapToObj(i -> record("id-" + i)).toList();
    }

    private static UsageRecord record(String id) {
        Instant now = Instant.parse("2026-11-23T10:00:00Z");
        SourceIdentity source = new SourceIdentity("node", "deployment", "cluster", "aws", "us-east-1");
        return new UsageRecord(id, now, now, UsageTypes.EXTERNAL_SOURCES_CPU, 10, 300, Map.of(), null, source);
    }

    private static Set<String> ids(List<UsageRecord> records) {
        return records.stream().map(UsageRecord::id).collect(Collectors.toSet());
    }

    public void testEmptyBatchSendsNothing() {
        PublishResult result = publisher.publish(List.of());
        assertThat(result.acceptedIds(), empty());
        assertThat(result.failures(), empty());
        assertThat(requests, empty());
    }

    public void testAcceptsWholeBatchOnAnyTwoXx() {
        for (int status : new int[] { 200, 201, 204 }) {
            requests.clear();
            statusFor = ids -> status;
            List<UsageRecord> batch = records(randomIntBetween(1, 20));
            PublishResult result = publisher.publish(batch);
            assertThat(result.acceptedIds(), equalTo(ids(batch)));
            assertThat(result.failures(), empty());
            assertThat(requests, hasSize(1));
            assertThat(requests.get(0).method(), equalTo("POST"));
            assertThat(requests.get(0).contentType(), equalTo("application/json"));
            assertThat(new HashSet<>(requests.get(0).ids()), equalTo(ids(batch)));
        }
    }

    public void testRetryableStatusesKeepTheWholeBatch() {
        for (int status : new int[] { 408, 429, 500, 502, 503, 504 }) {
            requests.clear();
            statusFor = ids -> status;
            PublishResult result = publisher.publish(records(5));
            assertWholeBatchFailure(result, PublishResult.FailureClass.RETRYABLE, String.valueOf(status));
            assertThat("no isolation for " + status, requests, hasSize(1));
        }
    }

    public void testStatusesThatAreNotKnownAreRetryableNotInvalid() {
        // A wrong url or a redirect lasts: dropping records because of it would be underbilling by mistake.
        for (int status : new int[] { 301, 404, 405, 413, 415 }) {
            requests.clear();
            statusFor = ids -> status;
            PublishResult result = publisher.publish(records(3));
            assertWholeBatchFailure(result, PublishResult.FailureClass.RETRYABLE, String.valueOf(status));
            assertThat(requests, hasSize(1));
        }
    }

    public void testAuthStatusesKeepTheWholeBatchWithoutIsolation() {
        for (int status : new int[] { 401, 403 }) {
            requests.clear();
            statusFor = ids -> status;
            PublishResult result = publisher.publish(records(8));
            assertWholeBatchFailure(result, PublishResult.FailureClass.AUTH, String.valueOf(status));
            assertThat(requests, hasSize(1));
        }
    }

    public void testDroppedConnectionIsRetryable() {
        statusFor = ids -> -1;
        assertWholeBatchFailure(publisher.publish(records(3)), PublishResult.FailureClass.RETRYABLE, null);
    }

    public void testConnectionRefusedIsRetryable() {
        server.stop(0);
        assertWholeBatchFailure(publisher.publish(records(3)), PublishResult.FailureClass.RETRYABLE, null);
    }

    public void testTimeoutIsRetryable() {
        statusFor = ids -> 0;
        try (HttpUsagePublisher slow = newPublisher(TimeValue.timeValueMillis(200))) {
            long start = System.nanoTime();
            PublishResult result = slow.publish(records(3));
            assertWholeBatchFailure(result, PublishResult.FailureClass.RETRYABLE, "timed out");
            assertThat(TimeValue.nsecToMSec(System.nanoTime() - start), lessThan(5_000L));
        }
    }

    public void testBadRecordIsIsolatedAndTheOthersAccepted() {
        int count = randomIntBetween(2, 100);
        List<UsageRecord> batch = records(count);
        String bad = randomFrom(batch).id();
        statusFor = ids -> ids.contains(bad) ? 400 : 201;

        PublishResult result = publisher.publish(batch);

        Set<String> expectedAccepted = ids(batch);
        expectedAccepted.remove(bad);
        assertThat(result.acceptedIds(), equalTo(expectedAccepted));
        assertThat(result.failures(), hasSize(1));
        PublishResult.Failure failure = result.failures().get(0);
        assertThat(failure.failureClass(), equalTo(PublishResult.FailureClass.INVALID));
        assertThat(failure.recordIds(), equalTo(Set.of(bad)));
        assertThat(requests.size(), lessThanOrEqualTo(2 * count - 1));
        assertIdsNeverChanged(batch);
    }

    public void testUnprocessableStatusIsIsolatedLikeBadRequest() {
        List<UsageRecord> batch = records(4);
        statusFor = ids -> ids.contains("id-2") ? 422 : 201;
        PublishResult result = publisher.publish(batch);
        assertThat(result.acceptedIds(), equalTo(Set.of("id-0", "id-1", "id-3")));
        assertThat(result.failures().get(0).recordIds(), equalTo(Set.of("id-2")));
    }

    public void testEveryRecordRejectedIsASystematicProblemAndNothingIsDropped() {
        int count = randomIntBetween(2, 100);
        List<UsageRecord> batch = records(count);
        int status = randomFrom(400, 422);
        statusFor = ids -> status;

        PublishResult result = publisher.publish(batch);

        // Bounded requests, but no record is reported INVALID: one retryable failure of the whole batch.
        assertWholeBatchFailure(result, PublishResult.FailureClass.RETRYABLE, "rejected every record");
        assertThat(requests, hasSize(2 * count - 1));
    }

    public void testAllButOneRejectedStillIsolatesTheGoodRecord() {
        int count = randomIntBetween(2, 50);
        List<UsageRecord> batch = records(count);
        String good = randomFrom(batch).id();
        statusFor = ids -> ids.contains(good) && ids.size() == 1 ? 201 : 400;

        PublishResult result = publisher.publish(batch);

        assertThat(result.acceptedIds(), equalTo(Set.of(good)));
        assertThat(result.failures(), hasSize(count - 1));
        for (PublishResult.Failure failure : result.failures()) {
            assertThat(failure.failureClass(), equalTo(PublishResult.FailureClass.INVALID));
            assertThat(failure.recordIds(), hasSize(1));
        }
    }

    public void testSingleRecordRejectedIsInvalid() {
        statusFor = ids -> 400;
        PublishResult result = publisher.publish(records(1));
        assertThat(result.failures(), hasSize(1));
        assertThat(result.failures().get(0).recordIds(), equalTo(Set.of("id-0")));
        assertThat(result.failures().get(0).message(), containsString("400"));
        assertThat(requests, hasSize(1));
    }

    public void testOutageDuringIsolationKeepsTheUntriedRecordsPending() {
        List<UsageRecord> batch = records(4);
        // The batch is rejected, the first half is accepted, the second half meets an outage.
        statusFor = ids -> ids.size() == 4 ? 400 : ids.contains("id-2") ? 503 : 201;

        PublishResult result = publisher.publish(batch);

        assertThat(result.acceptedIds(), equalTo(Set.of("id-0", "id-1")));
        assertThat(result.failures(), hasSize(1));
        assertThat(result.failures().get(0).failureClass(), equalTo(PublishResult.FailureClass.RETRYABLE));
        assertThat(result.failures().get(0).recordIds(), equalTo(Set.of("id-2", "id-3")));
        assertThat(requests, hasSize(3));
    }

    public void testAuthFailureDuringIsolationStopsAndKeepsTheRest() {
        List<UsageRecord> batch = records(4);
        statusFor = ids -> ids.size() == 4 ? 400 : 403;

        PublishResult result = publisher.publish(batch);

        assertThat(result.acceptedIds(), empty());
        assertThat(result.failures(), hasSize(1));
        assertThat(result.failures().get(0).failureClass(), equalTo(PublishResult.FailureClass.AUTH));
        // Nothing was resolved, so it is the whole batch.
        assertThat(result.failures().get(0).recordIds(), empty());
        assertThat(requests, hasSize(2));
    }

    public void testBadRecordAfterAcceptedHalfAndOutageReportsBothClasses() {
        List<UsageRecord> batch = records(4);
        // Batch rejected; [0,1] has the bad record 1; [0] accepted; [1] invalid; [2,3] outage.
        statusFor = ids -> {
            if (ids.size() == 4) {
                return 400;
            }
            if (ids.contains("id-2")) {
                return 503;
            }
            return ids.contains("id-1") ? 400 : 201;
        };

        PublishResult result = publisher.publish(batch);

        assertThat(result.acceptedIds(), equalTo(Set.of("id-0")));
        assertThat(result.failures(), hasSize(2));
        Set<PublishResult.FailureClass> classes = result.failures()
            .stream()
            .map(PublishResult.Failure::failureClass)
            .collect(Collectors.toSet());
        assertThat(classes, equalTo(Set.of(PublishResult.FailureClass.INVALID, PublishResult.FailureClass.RETRYABLE)));
    }

    public void testRetriesSendTheSameIds() {
        List<UsageRecord> batch = records(6);
        AtomicInteger calls = new AtomicInteger();
        statusFor = ids -> calls.getAndIncrement() == 0 ? 503 : 201;

        publisher.publish(batch);
        PublishResult second = publisher.publish(batch);

        assertThat(second.acceptedIds(), equalTo(ids(batch)));
        assertThat(requests, hasSize(2));
        assertThat(requests.get(1).ids(), equalTo(requests.get(0).ids()));
        assertThat(requests.get(0).ids(), equalTo(batch.stream().map(UsageRecord::id).toList()));
    }

    public void testSerializationFailureIsRetryableAndSendsNothing() {
        URI uri = URI.create("http://" + server.getAddress().getHostString() + ":" + server.getAddress().getPort() + "/api/v1/usage");
        try (HttpUsagePublisher failing = new HttpUsagePublisher(uri, TimeValue.timeValueSeconds(10), profile, records -> {
            throw new UnsupportedOperationException("not yet");
        })) {
            assertWholeBatchFailure(failing.publish(records(2)), PublishResult.FailureClass.RETRYABLE, "not yet");
            assertThat(requests, empty());
        }
    }

    public void testProfileReloadBuildsANewClientAndKeepsPublishing() {
        assertThat(profile.sslContextCalls.get(), equalTo(1));
        profile.reload();
        assertThat(profile.sslContextCalls.get(), equalTo(2));
        List<UsageRecord> batch = records(3);
        assertThat(publisher.publish(batch).acceptedIds(), equalTo(ids(batch)));
    }

    public void testReloadAfterCloseDoesNothing() {
        publisher.close();
        publisher.close();
        profile.reload();
        assertThat(profile.sslContextCalls.get(), equalTo(1));
    }

    public void testPublishAfterCloseIsRetryable() {
        publisher.close();
        assertWholeBatchFailure(publisher.publish(records(2)), PublishResult.FailureClass.RETRYABLE, null);
        assertThat(requests, empty());
    }

    public void testInterruptIsRetryableAndKeepsTheInterruptFlag() {
        statusFor = ids -> 0;
        Thread.currentThread().interrupt();
        try {
            assertWholeBatchFailure(publisher.publish(records(3)), PublishResult.FailureClass.RETRYABLE, "interrupted");
            assertTrue("the interrupt flag is restored", Thread.currentThread().isInterrupted());
        } finally {
            Thread.interrupted();
        }
    }

    public void testClassifyTable() {
        assertThat(HttpUsagePublisher.classify(201, "").failureClass(), nullValue());
        assertThat(HttpUsagePublisher.classify(400, "x").failureClass(), equalTo(PublishResult.FailureClass.INVALID));
        assertThat(HttpUsagePublisher.classify(422, null).failureClass(), equalTo(PublishResult.FailureClass.INVALID));
        assertThat(HttpUsagePublisher.classify(401, "").failureClass(), equalTo(PublishResult.FailureClass.AUTH));
        assertThat(HttpUsagePublisher.classify(403, "").failureClass(), equalTo(PublishResult.FailureClass.AUTH));
        assertThat(HttpUsagePublisher.classify(429, "").failureClass(), equalTo(PublishResult.FailureClass.RETRYABLE));
        assertThat(HttpUsagePublisher.classify(100, "").failureClass(), equalTo(PublishResult.FailureClass.RETRYABLE));
        String longBody = "x".repeat(1000);
        assertThat(HttpUsagePublisher.classify(500, longBody).message().length(), lessThan(300));
    }

    private void assertWholeBatchFailure(PublishResult result, PublishResult.FailureClass expected, String messagePart) {
        assertThat(result.acceptedIds(), empty());
        assertThat(result.failures(), hasSize(1));
        PublishResult.Failure failure = result.failures().get(0);
        assertThat(failure.failureClass(), equalTo(expected));
        assertThat(failure.recordIds(), empty());
        if (messagePart != null) {
            assertThat(failure.message(), containsString(messagePart));
        }
    }

    private void assertIdsNeverChanged(List<UsageRecord> batch) {
        Set<String> original = ids(batch);
        for (Request request : requests) {
            assertTrue(original.containsAll(request.ids()));
        }
    }

    /**
     * A stand-in for the real profile, which only x-pack-core can build from an SSL configuration and certificate files: the
     * publisher uses nothing but {@code sslContext()} and the reload listener, which this implements for real. The server of these
     * tests is plain HTTP, so the context is never used for a handshake.
     */
    private static final class TestSslProfile implements SslProfile {
        final AtomicInteger sslContextCalls = new AtomicInteger();
        private final List<Consumer<SslProfile>> listeners = new ArrayList<>();

        @Override
        public SSLContext sslContext() {
            sslContextCalls.incrementAndGet();
            try {
                return SSLContext.getDefault();
            } catch (NoSuchAlgorithmException e) {
                throw new AssertionError(e);
            }
        }

        @Override
        public void addReloadListener(Consumer<SslProfile> listener) {
            listeners.add(listener);
        }

        void reload() {
            listeners.forEach(l -> l.accept(this));
        }

        @Override
        public SslConfiguration configuration() {
            throw new UnsupportedOperationException();
        }

        @Override
        public SSLSocketFactory socketFactory() {
            throw new UnsupportedOperationException();
        }

        @Override
        public HostnameVerifier hostnameVerifier() {
            throw new UnsupportedOperationException();
        }

        @Override
        public TlsStrategy clientTlsStrategy() {
            throw new UnsupportedOperationException();
        }

        @Override
        public SSLEngine engine(String host, int port) {
            throw new UnsupportedOperationException();
        }
    }
}
