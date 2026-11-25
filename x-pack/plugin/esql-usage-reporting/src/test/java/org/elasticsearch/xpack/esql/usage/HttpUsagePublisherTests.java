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
import org.apache.logging.log4j.Level;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.common.ssl.DefaultJdkTrustConfig;
import org.elasticsearch.common.ssl.EmptyKeyConfig;
import org.elasticsearch.common.ssl.SslClientAuthenticationMode;
import org.elasticsearch.common.ssl.SslConfiguration;
import org.elasticsearch.common.ssl.SslVerificationMode;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.mocksocket.MockHttpServer;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.xpack.core.ssl.SslProfile;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.KeyManagementException;
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
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;
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
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

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
    /** The status that makes the server send headers and then stall. */
    private static final int STALL_BODY = -2;
    private final CountDownLatch clientDropped = new CountDownLatch(1);
    /** Counted down when the server has sent the headers and the first byte of a body that never ends. */
    private final CountDownLatch bodyStarted = new CountDownLatch(1);
    /** When positive, the size of the body of every answer. */
    private volatile int answerBytes;
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
        return new HttpUsagePublisher(uri(), timeout, profile, SERIALIZER);
    }

    /** Formatted with the brackets an IPv6 loopback address needs. */
    private URI uri() {
        return URI.create("http://" + NetworkAddress.format(server.getAddress()) + "/api/v1/usage");
    }

    private void handle(HttpExchange exchange) throws IOException {
        String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
        List<String> ids = body.isEmpty() ? List.of() : Arrays.asList(body.split(","));
        requests.add(new Request(exchange.getRequestMethod(), exchange.getRequestHeaders().getFirst("Content-Type"), ids));
        int status = statusFor.apply(ids);
        if (status == STALL_BODY) {
            // An error, then a body that trickles and never ends: the connection is dropped only if the client aborts the exchange.
            exchange.sendResponseHeaders(503, 100L * 1024 * 1024);
            try {
                byte[] chunk = new byte[1];
                while (true) {
                    exchange.getResponseBody().write(chunk);
                    exchange.getResponseBody().flush();
                    bodyStarted.countDown();
                    Thread.sleep(50);
                }
            } catch (IOException e) {
                clientDropped.countDown();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return;
        }
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
        byte[] answer = answerBytes > 0 ? "x".repeat(answerBytes).getBytes(StandardCharsets.UTF_8)
            : status >= 400 ? "{\"error\":\"nope\"}".getBytes(StandardCharsets.UTF_8)
            : new byte[0];
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

    public void testAcceptsWholeBatchOnTheSuccessStatuses() {
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
        for (int status : new int[] { 301, 404, 405, 415, 422 }) {
            requests.clear();
            statusFor = ids -> status;
            PublishResult result = publisher.publish(records(3));
            assertWholeBatchFailure(result, PublishResult.FailureClass.RETRYABLE, String.valueOf(status));
            assertThat(requests, hasSize(1));
        }
    }

    public void testATwoXxThatDoesNotSayTheBatchWasWrittenIsHeldAndWarnedAbout() throws Exception {
        // 202 is processing that is not finished and 207 can carry failures of single records: sending again is safe (ids are
        // deduplicated by the Usage API), acknowledging is not.
        for (int status : new int[] { 202, 206, 207 }) {
            requests.clear();
            statusFor = ids -> status;
            try (MockLog mockLog = MockLog.capture(HttpUsagePublisher.class)) {
                mockLog.addExpectation(
                    new MockLog.SeenEventExpectation(
                        "held " + status,
                        HttpUsagePublisher.class.getCanonicalName(),
                        Level.WARN,
                        "the Usage API answered with status [" + status + "]*the usage records stay pending"
                    )
                );
                assertWholeBatchFailure(publisher.publish(records(4)), PublishResult.FailureClass.RETRYABLE, String.valueOf(status));
                mockLog.assertAllExpectationsMatched();
            }
            assertThat(requests, hasSize(1));
        }
    }

    public void testTooLargeBatchIsSplitUntilItFitsAndNothingIsDropped() {
        List<UsageRecord> batch = records(10);
        statusFor = ids -> ids.size() > 3 ? 413 : 201;

        PublishResult result = publisher.publish(batch);

        assertThat(result.acceptedIds(), equalTo(ids(batch)));
        assertThat(result.failures(), empty());
        assertIdsNeverChanged(batch);
    }

    public void testSingleRecordThatIsTooLargeIsHeldNotDropped() {
        List<UsageRecord> batch = List.of(record("good-1"), record("good-2"), record("big"));
        statusFor = ids -> ids.contains("big") ? 413 : 201;

        PublishResult result = publisher.publish(batch);

        assertThat(result.acceptedIds(), equalTo(Set.of("good-1", "good-2")));
        assertThat(result.failures(), hasSize(1));
        assertThat(result.failures().get(0).failureClass(), equalTo(PublishResult.FailureClass.RETRYABLE));
        assertThat(result.failures().get(0).recordIds(), equalTo(Set.of("big")));

        // On its own it cannot be split either: the whole batch is held.
        assertWholeBatchFailure(publisher.publish(List.of(record("big"))), PublishResult.FailureClass.RETRYABLE, "413");
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
        // Bisection, not a scan: the root, then both halves of every level on the path to the bad record.
        int levels = 32 - Integer.numberOfLeadingZeros(count - 1);
        assertThat(requests.size(), lessThanOrEqualTo(1 + 2 * levels));
        assertIdsNeverChanged(batch);
    }

    public void testEveryRecordRejectedIsASystematicProblemAndNothingIsDropped() {
        int count = randomIntBetween(2, 100);
        List<UsageRecord> batch = records(count);
        statusFor = ids -> 400;

        PublishResult result = publisher.publish(batch);

        // Bounded requests, but no record is reported INVALID: one retryable failure of the whole batch.
        assertWholeBatchFailure(result, PublishResult.FailureClass.RETRYABLE, "rejected every record");
        assertThat(requests, hasSize(2 * count - 1));
    }

    public void testHoldingAWholeBatchIsLoggedAsAWarning() throws Exception {
        statusFor = ids -> 400;
        try (MockLog mockLog = MockLog.capture(HttpUsagePublisher.class)) {
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "batch held",
                    HttpUsagePublisher.class.getCanonicalName(),
                    Level.WARN,
                    "the Usage API rejected every record of the batch*they stay pending*status [400]*"
                )
            );
            publisher.publish(records(3));
            mockLog.assertAllExpectationsMatched();
        }
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

    public void testLoneRejectionIsHeldUntilARecordHasBeenAccepted() {
        statusFor = ids -> ids.stream().anyMatch(id -> id.startsWith("bad")) ? 400 : 201;

        // A fresh publisher has not seen a record accepted: the rejection may be ours, so the record stays.
        assertWholeBatchFailure(publisher.publish(List.of(record("bad-1"))), PublishResult.FailureClass.RETRYABLE, "only record");
        assertWholeBatchFailure(publisher.publish(List.of(record("bad-1"))), PublishResult.FailureClass.RETRYABLE, "status [400]");

        // The next batch has a record that is accepted: the bad one is isolated and dropped now.
        PublishResult mixed = publisher.publish(List.of(record("bad-1"), record("good-1")));
        assertThat(mixed.acceptedIds(), equalTo(Set.of("good-1")));
        assertThat(mixed.failures(), hasSize(1));
        assertThat(mixed.failures().get(0).failureClass(), equalTo(PublishResult.FailureClass.INVALID));
        assertThat(mixed.failures().get(0).recordIds(), equalTo(Set.of("bad-1")));

        // After an accepted record a lone rejection is dropped once, and what follows it is held again.
        PublishResult dropped = publisher.publish(List.of(record("bad-2")));
        assertThat(dropped.failures().get(0).failureClass(), equalTo(PublishResult.FailureClass.INVALID));
        assertThat(dropped.failures().get(0).recordIds(), equalTo(Set.of("bad-2")));
        assertWholeBatchFailure(publisher.publish(List.of(record("bad-3"))), PublishResult.FailureClass.RETRYABLE, "only record");

        // An accepted record clears it.
        assertThat(publisher.publish(List.of(record("good-2"))).acceptedIds(), equalTo(Set.of("good-2")));
        assertThat(
            publisher.publish(List.of(record("bad-4"))).failures().get(0).failureClass(),
            equalTo(PublishResult.FailureClass.INVALID)
        );
    }

    public void testRetryableOrAuthFailureDoesNotChangeWhetherALoneRejectionIsHeld() {
        statusFor = ids -> 201;
        assertThat(publisher.publish(List.of(record("good"))).acceptedIds(), equalTo(Set.of("good")));
        statusFor = ids -> 503;
        publisher.publish(List.of(record("later")));
        statusFor = ids -> 400;
        // Still "after an accepted record": the first lone rejection is dropped.
        assertThat(publisher.publish(List.of(record("bad"))).failures().get(0).failureClass(), equalTo(PublishResult.FailureClass.INVALID));
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
        try (HttpUsagePublisher failing = new HttpUsagePublisher(uri(), TimeValue.timeValueSeconds(10), profile, records -> {
            throw new UnsupportedOperationException("not yet");
        })) {
            assertWholeBatchFailure(failing.publish(records(2)), PublishResult.FailureClass.RETRYABLE, "not yet");
            assertThat(requests, empty());
        }
    }

    public void testProfileReloadBuildsANewClientAndKeepsPublishing() {
        SSLContext before = publisher.httpClient().sslContext();
        assertThat(before, sameInstance(profile.current));
        profile.reload();
        assertThat(publisher.httpClient().sslContext(), sameInstance(profile.current));
        assertThat(publisher.httpClient().sslContext(), not(sameInstance(before)));
        List<UsageRecord> batch = records(3);
        assertThat(publisher.publish(batch).acceptedIds(), equalTo(ids(batch)));
    }

    public void testReloadBetweenBuildingTheClientAndRegisteringTheListenerIsNotLost() {
        // The profile reloads while the listener is being registered, which is after the client was built.
        profile.rotateOnRegistration = true;
        try (HttpUsagePublisher racing = newPublisher(TimeValue.timeValueSeconds(10))) {
            assertThat(racing.httpClient().sslContext(), sameInstance(profile.current));
        }
    }

    public void testReloadAfterCloseDoesNothing() {
        SSLContext before = publisher.httpClient().sslContext();
        publisher.close();
        publisher.close();
        profile.reload();
        assertThat(publisher.httpClient().sslContext(), sameInstance(before));
    }

    public void testAProfileThatSwitchesHostNameVerificationOffIsWarnedAbout() throws Exception {
        for (SslVerificationMode mode : new SslVerificationMode[] { SslVerificationMode.CERTIFICATE, SslVerificationMode.NONE }) {
            profile.verificationMode = mode;
            try (MockLog mockLog = MockLog.capture(HttpUsagePublisher.class)) {
                mockLog.addExpectation(
                    new MockLog.SeenEventExpectation(
                        "ignored " + mode,
                        HttpUsagePublisher.class.getCanonicalName(),
                        Level.WARN,
                        "[xpack.esql.usage_reporting.ssl.verification_mode] is ["
                            + mode
                            + "] but the usage reporting client always "
                            + "verifies the host name*must match the host name of [xpack.esql.usage_reporting.url]"
                    )
                );
                newPublisher(TimeValue.timeValueSeconds(10)).close();
                mockLog.assertAllExpectationsMatched();
            }
        }
    }

    public void testNoWarningWhenTheProfileVerifiesTheHostName() throws Exception {
        try (MockLog mockLog = MockLog.capture(HttpUsagePublisher.class)) {
            mockLog.addExpectation(
                new MockLog.UnseenEventExpectation("no warning", HttpUsagePublisher.class.getCanonicalName(), Level.WARN, "*")
            );
            newPublisher(TimeValue.timeValueSeconds(10)).close();
            profile.reload();
            mockLog.assertAllExpectationsMatched();
        }
    }

    public void testProtocolsAndCipherSuitesOfTheProfileAreApplied() {
        assertThat(Arrays.asList(publisher.httpClient().sslParameters().getProtocols()), equalTo(List.of("TLSv1.3")));
        assertThat(Arrays.asList(publisher.httpClient().sslParameters().getCipherSuites()), equalTo(List.of("TLS_AES_256_GCM_SHA384")));
        profile.protocols = List.of("TLSv1.2", "TLSv1.3");
        profile.reload();
        assertThat(Arrays.asList(publisher.httpClient().sslParameters().getProtocols()), equalTo(List.of("TLSv1.2", "TLSv1.3")));
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

    public void testBodyThatNeverEndsIsAbortedAfterTheTimeout() throws Exception {
        statusFor = ids -> STALL_BODY;
        try (HttpUsagePublisher slow = newPublisher(TimeValue.timeValueSeconds(2))) {
            assertWholeBatchFailure(slow.publish(records(2)), PublishResult.FailureClass.RETRYABLE, null);
            // The timeout must have hit while the body was being received, or this checks the abort before the headers instead.
            assertTrue("the response had started before the timeout", bodyStarted.await(0, TimeUnit.SECONDS));
            assertTrue("the connection of the request is released", clientDropped.await(5, TimeUnit.SECONDS));
        }
    }

    public void testLargeAnswersDoNotPreventAcceptingOrRetrying() {
        answerBytes = 2 * 1024 * 1024;
        statusFor = ids -> 201;
        List<UsageRecord> batch = records(2);
        assertThat(publisher.publish(batch).acceptedIds(), equalTo(ids(batch)));

        statusFor = ids -> 503;
        assertWholeBatchFailure(publisher.publish(batch), PublishResult.FailureClass.RETRYABLE, "503");
    }

    public void testBodySubscriberKeepsOnlyTheStartAndCancelsOnceItHasIt() throws Exception {
        RecordingSubscription subscription = new RecordingSubscription();
        HttpUsagePublisher.BoundedBodySubscriber subscriber = new HttpUsagePublisher.BoundedBodySubscriber(10);
        subscriber.onSubscribe(subscription);
        assertThat(subscription.requested, equalTo(Long.MAX_VALUE));

        subscriber.onNext(List.of(ByteBuffer.wrap("abcdef".getBytes(StandardCharsets.UTF_8))));
        assertFalse("not enough yet", subscription.cancelled);
        ByteBuffer rest = ByteBuffer.wrap("ghijklmnop".getBytes(StandardCharsets.UTF_8));
        subscriber.onNext(List.of(rest));

        assertTrue("the rest of the body is not wanted", subscription.cancelled);
        assertThat(rest.hasRemaining(), equalTo(false));
        assertThat(subscriber.getBody().toCompletableFuture().get(), equalTo("abcdefghij"));
        subscriber.onNext(List.of(ByteBuffer.wrap("more".getBytes(StandardCharsets.UTF_8))));
        subscriber.onComplete();
        assertThat(subscriber.getBody().toCompletableFuture().get(), equalTo("abcdefghij"));
    }

    public void testBodySubscriberOfASuccessReadsNothing() throws Exception {
        RecordingSubscription subscription = new RecordingSubscription();
        HttpUsagePublisher.BoundedBodySubscriber subscriber = new HttpUsagePublisher.BoundedBodySubscriber(0);
        subscriber.onSubscribe(subscription);
        subscriber.onNext(List.of(ByteBuffer.wrap("created".getBytes(StandardCharsets.UTF_8))));
        assertTrue(subscription.cancelled);
        assertThat(subscriber.getBody().toCompletableFuture().get(), equalTo(""));
    }

    public void testBodySubscriberOfAShortBodyKeepsItAll() throws Exception {
        HttpUsagePublisher.BoundedBodySubscriber subscriber = new HttpUsagePublisher.BoundedBodySubscriber(512);
        RecordingSubscription subscription = new RecordingSubscription();
        subscriber.onSubscribe(subscription);
        subscriber.onNext(List.of(ByteBuffer.wrap("{\"error\":\"nope\"}".getBytes(StandardCharsets.UTF_8))));
        subscriber.onComplete();
        assertFalse(subscription.cancelled);
        assertThat(subscriber.getBody().toCompletableFuture().get(), equalTo("{\"error\":\"nope\"}"));
    }

    public void testClassifyTable() {
        assertThat(HttpUsagePublisher.classify(201, "").failureClass(), nullValue());
        assertThat(HttpUsagePublisher.classify(200, "").failureClass(), nullValue());
        assertThat(HttpUsagePublisher.classify(204, "").failureClass(), nullValue());
        assertThat(HttpUsagePublisher.classify(202, "").failureClass(), equalTo(PublishResult.FailureClass.RETRYABLE));
        assertThat(HttpUsagePublisher.classify(207, "").failureClass(), equalTo(PublishResult.FailureClass.RETRYABLE));
        assertThat(HttpUsagePublisher.classify(413, "").failureClass(), equalTo(PublishResult.FailureClass.RETRYABLE));
        assertThat(HttpUsagePublisher.classify(413, "").split(), equalTo(true));
        assertThat(HttpUsagePublisher.classify(400, "").split(), equalTo(true));
        assertThat(HttpUsagePublisher.classify(403, "").split(), equalTo(false));
        assertThat(HttpUsagePublisher.classify(400, "x").failureClass(), equalTo(PublishResult.FailureClass.INVALID));
        assertThat(HttpUsagePublisher.classify(422, null).failureClass(), equalTo(PublishResult.FailureClass.RETRYABLE));
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
     * The JDK client creates the subscription of a real response and gives no way to build one, so the subscriber is fed by hand: this
     * records what the subscriber asks of it.
     */
    private static final class RecordingSubscription implements Flow.Subscription {
        long requested;
        boolean cancelled;

        @Override
        public void request(long n) {
            requested = n;
        }

        @Override
        public void cancel() {
            cancelled = true;
        }
    }

    /**
     * A stand-in for the real profile, which only x-pack-core can build from an SSL configuration and certificate files: the
     * publisher uses nothing but {@code sslContext()} and the reload listener, which this implements for real. The server of these
     * tests is plain HTTP, so the context is never used for a handshake.
     */
    private static final class TestSslProfile implements SslProfile {
        volatile SSLContext current = newContext();
        volatile List<String> protocols = List.of("TLSv1.3");
        volatile SslVerificationMode verificationMode = SslVerificationMode.FULL;
        /** When set, the next listener registration is preceded by a reload, as if it raced with the constructor. */
        volatile boolean rotateOnRegistration;
        private final List<Consumer<SslProfile>> listeners = new ArrayList<>();

        private static SSLContext newContext() {
            try {
                SSLContext context = SSLContext.getInstance("TLS");
                context.init(null, null, null);
                return context;
            } catch (NoSuchAlgorithmException | KeyManagementException e) {
                throw new AssertionError(e);
            }
        }

        @Override
        public SSLContext sslContext() {
            return current;
        }

        @Override
        public void addReloadListener(Consumer<SslProfile> listener) {
            if (rotateOnRegistration) {
                rotateOnRegistration = false;
                current = newContext();
            }
            listeners.add(listener);
        }

        /** A reload: a new context, and the listeners are told. */
        void reload() {
            current = newContext();
            listeners.forEach(l -> l.accept(this));
        }

        @Override
        public SslConfiguration configuration() {
            return new SslConfiguration(
                "xpack.esql.usage_reporting.ssl",
                true,
                DefaultJdkTrustConfig.DEFAULT_INSTANCE,
                EmptyKeyConfig.INSTANCE,
                verificationMode,
                SslClientAuthenticationMode.NONE,
                // The second suite does not exist: it checks that suites the JVM does not support are left out.
                List.of("TLS_AES_256_GCM_SHA384", "NOT_A_CIPHER_SUITE"),
                protocols,
                10_000L
            );
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
