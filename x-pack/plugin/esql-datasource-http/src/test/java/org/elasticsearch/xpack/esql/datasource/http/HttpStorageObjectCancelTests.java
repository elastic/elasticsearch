/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.http;

import com.carrotsearch.randomizedtesting.ThreadFilter;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.LimitedBreaker;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.mocksocket.MockHttpServer;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.ConcurrencyLimitTestSupport;
import org.elasticsearch.xpack.esql.datasources.spi.DirectBufferFactory;
import org.elasticsearch.xpack.esql.datasources.spi.DirectReadBuffer;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

/**
 * Real JDK {@link HttpClient} + {@link MockHttpServer} coverage of async-read cancel.
 * Existing {@code testCancelInFlight*} cases mock {@code sendAsync} and never run a body,
 * so they cannot see the breaker leak: cancel of the response future does not cancel the
 * subscriber's body future, and the charged destination is dropped.
 */
@SuppressForbidden(reason = "use a http server")
@ThreadLeakFilters(filters = { HttpStorageObjectCancelTests.JdkHttpClientThreadFilter.class })
public class HttpStorageObjectCancelTests extends ESTestCase {

    public static final class JdkHttpClientThreadFilter implements ThreadFilter {
        @Override
        public boolean reject(Thread t) {
            return t.getName().startsWith("HttpClient");
        }
    }

    private static final int LENGTH = 64 * 1024;
    private static final int FIRST_CHUNK = 8 * 1024;
    private static final int SKIP = 128;
    private static final int RACE_ITERS = 200;

    private final AtomicReference<HttpHandler> handler = new AtomicReference<>(exchange -> {
        exchange.sendResponseHeaders(500, -1);
        exchange.close();
    });

    private HttpServer server;
    private ExecutorService executor;
    private HttpClient client;
    private LimitedBreaker breaker;
    private DirectBufferFactory factory;
    private StoragePath path;

    @Before
    public void startHarness() throws IOException {
        breaker = new LimitedBreaker("http-cancel", ByteSizeValue.ofMb(16));
        factory = DirectBufferFactory.forBreaker(breaker);
        executor = Executors.newCachedThreadPool(r -> {
            Thread t = new Thread(r, "http-cancel-test");
            t.setDaemon(true);
            return t;
        });
        server = MockHttpServer.createHttp(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        server.createContext("/", exchange -> handler.get().handle(exchange));
        server.start();
        client = HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_1_1)
            .executor(executor)
            .connectTimeout(Duration.ofSeconds(5))
            .build();
        path = StoragePath.of("http://127.0.0.1:" + server.getAddress().getPort() + "/file.parquet");
    }

    @After
    public void stopHarness() {
        if (client != null) {
            client.close();
        }
        if (executor != null) {
            terminate(executor);
        }
        if (server != null) {
            server.stop(0);
        }
    }

    public void testCompletedReadRefundsOnClose() throws Exception {
        byte[] payload = payload(LENGTH);
        CountDownLatch serverDone = new CountDownLatch(1);
        handler.set(exchange -> writeFull(exchange, 206, payload, serverDone, new AtomicReference<>()));

        DirectReadBuffer buffer = awaitBuffer(object(), 0, LENGTH);
        assertEquals(LENGTH, buffer.buffer().remaining());
        assertTrue(breaker.getUsed() > 0L);
        buffer.close();
        assertEquals(0L, breaker.getUsed());
        assertTrue(serverDone.await(5, TimeUnit.SECONDS));
    }

    public void testCancelBeforeHeadersRefundsCharge() throws Exception {
        byte[] payload = payload(LENGTH);
        CountDownLatch releaseHeaders = new CountDownLatch(1);
        CountDownLatch serverDone = new CountDownLatch(1);
        AtomicReference<String> outcome = new AtomicReference<>();
        handler.set(exchange -> {
            try {
                if (releaseHeaders.await(10, TimeUnit.SECONDS) == false) {
                    outcome.set("headers latch timed out");
                    return;
                }
                writeFull(exchange, 206, payload, null, outcome);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                outcome.set("interrupted");
            } finally {
                serverDone.countDown();
            }
        });

        CountDownLatch listenerDone = new CountDownLatch(1);
        AtomicReference<Exception> failure = new AtomicReference<>();
        Releasable cancel = startExpectingCancel(object(), 0, LENGTH, listenerDone, failure);
        cancel.close();
        assertTrue(listenerDone.await(5, TimeUnit.SECONDS));
        assertThat(failure.get(), instanceOf(TaskCancelledException.class));
        releaseHeaders.countDown();
        assertTrue(serverDone.await(5, TimeUnit.SECONDS));
        assertBreakerEmpty();
    }

    public void testCancelMidBody206RefundsCharge() throws Exception {
        cancelMidBodyAndAssertRefund(206, 0, payload(LENGTH));
    }

    public void testCancelMidBody200SkipRefundsCharge() throws Exception {
        byte[] full = payload(SKIP + LENGTH);
        cancelMidBodyAndAssertRefund(200, SKIP, full);
    }

    public void testCancelAfterBodyCompletedBeforeResponseFutureRefundsCharge() throws Exception {
        byte[] payload = payload(LENGTH);
        CountDownLatch serverDone = new CountDownLatch(1);
        handler.set(exchange -> writeFull(exchange, 206, payload, serverDone, new AtomicReference<>()));

        CountDownLatch bodyCompleted = new CountDownLatch(1);
        CountDownLatch releaseResponse = new CountDownLatch(1);
        HttpClient delaying = delayingClient(bodyCompleted, releaseResponse);

        CountDownLatch listenerDone = new CountDownLatch(1);
        AtomicReference<Exception> failure = new AtomicReference<>();
        Releasable cancel = new HttpStorageObject(delaying, path, config()).startReadBytesAsync(
            0,
            LENGTH,
            factory,
            Runnable::run,
            cancelListener(listenerDone, failure)
        );

        assertTrue("subscriber body must complete", bodyCompleted.await(5, TimeUnit.SECONDS));
        assertTrue("destination still charged", breaker.getUsed() > 0L);
        cancel.close();
        assertTrue(listenerDone.await(5, TimeUnit.SECONDS));
        assertThat(failure.get(), instanceOf(TaskCancelledException.class));
        releaseResponse.countDown();
        assertTrue(serverDone.await(5, TimeUnit.SECONDS));
        assertBreakerEmpty();
    }

    public void testCancelAfterListenerGotBufferDoesNotDoubleFree() throws Exception {
        byte[] payload = payload(LENGTH);
        CountDownLatch serverDone = new CountDownLatch(1);
        handler.set(exchange -> writeFull(exchange, 206, payload, serverDone, new AtomicReference<>()));

        CountDownLatch listenerDone = new CountDownLatch(1);
        AtomicReference<DirectReadBuffer> got = new AtomicReference<>();
        AtomicReference<Exception> failure = new AtomicReference<>();
        Releasable cancel = object().startReadBytesAsync(0, LENGTH, factory, Runnable::run, new ActionListener<>() {
            @Override
            public void onResponse(DirectReadBuffer buffer) {
                got.set(buffer);
                listenerDone.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                failure.set(e);
                listenerDone.countDown();
            }
        });
        assertTrue(listenerDone.await(5, TimeUnit.SECONDS));
        assertNull(failure.get());
        assertNotNull(got.get());
        cancel.close();
        got.get().close();
        assertTrue(serverDone.await(5, TimeUnit.SECONDS));
        assertBreakerEmpty();
    }

    public void testCancelAndCompleteRace() throws Exception {
        EnumSet<Outcome> seen = EnumSet.noneOf(Outcome.class);
        byte[] payload = payload(256);
        handler.set(exchange -> writeFull(exchange, 206, payload, null, new AtomicReference<>()));
        for (int i = 0; i < RACE_ITERS; i++) {
            CountDownLatch listenerDone = new CountDownLatch(1);
            AtomicReference<DirectReadBuffer> got = new AtomicReference<>();
            AtomicReference<Exception> failure = new AtomicReference<>();
            Releasable cancel = object().startReadBytesAsync(0, payload.length, factory, Runnable::run, new ActionListener<>() {
                @Override
                public void onResponse(DirectReadBuffer buffer) {
                    got.set(buffer);
                    listenerDone.countDown();
                }

                @Override
                public void onFailure(Exception e) {
                    failure.set(e);
                    listenerDone.countDown();
                }
            });
            if (randomBoolean()) {
                if (randomBoolean()) {
                    Thread.sleep(randomIntBetween(0, 2));
                }
                cancel.close();
            }
            assertTrue("iteration " + i, listenerDone.await(5, TimeUnit.SECONDS));
            if (got.get() != null) {
                assertNull("iteration " + i + " delivered a buffer and a failure", failure.get());
                got.get().close();
                seen.add(Outcome.COMPLETED);
                cancel.close();
            } else {
                assertThat("iteration " + i, failure.get(), instanceOf(TaskCancelledException.class));
                seen.add(Outcome.CANCELLED);
            }
            assertBreakerEmpty();
        }
        assertFalse("race test hit no outcomes", seen.isEmpty());
    }

    public void testCancelMidBodyAbortsExchange() throws Exception {
        byte[] payload = payload(1024 * 1024);
        int firstChunk = 64 * 1024;
        CountDownLatch firstChunkSent = new CountDownLatch(1);
        CountDownLatch releaseBody = new CountDownLatch(1);
        CountDownLatch serverDone = new CountDownLatch(1);
        AtomicReference<String> outcome = new AtomicReference<>();
        handler.set(exchange -> writePaused(exchange, 206, payload, firstChunk, firstChunkSent, releaseBody, serverDone, outcome));

        CountDownLatch listenerDone = new CountDownLatch(1);
        Releasable cancel = startExpectingCancel(object(), 0, payload.length, listenerDone, new AtomicReference<>());
        assertTrue(firstChunkSent.await(5, TimeUnit.SECONDS));
        assertBusy(() -> assertTrue(breaker.getUsed() > 0L));
        cancel.close();
        assertTrue(listenerDone.await(5, TimeUnit.SECONDS));
        releaseBody.countDown();
        assertTrue("server handler did not finish", serverDone.await(5, TimeUnit.SECONDS));
        assertBreakerEmpty();
        assertTrue(
            "server wrote the full body after cancel: " + outcome.get(),
            outcome.get() != null && outcome.get().startsWith("body write failed")
        );
    }

    public void testCancelThroughConcurrencyLimiterRefundsPermitAndCharge() throws Exception {
        byte[] payload = payload(LENGTH);
        CountDownLatch firstChunkSent = new CountDownLatch(1);
        CountDownLatch releaseBody = new CountDownLatch(1);
        CountDownLatch serverDone = new CountDownLatch(1);
        handler.set(
            exchange -> writePaused(exchange, 206, payload, FIRST_CHUNK, firstChunkSent, releaseBody, serverDone, new AtomicReference<>())
        );

        ConcurrencyLimitTestSupport limited = new ConcurrencyLimitTestSupport(object(), 1);
        assertEquals(1, limited.availablePermits());
        CountDownLatch listenerDone = new CountDownLatch(1);
        Releasable cancel = startExpectingCancel(limited.object(), 0, LENGTH, listenerDone, new AtomicReference<>());
        assertTrue(firstChunkSent.await(5, TimeUnit.SECONDS));
        assertBusy(() -> assertTrue(breaker.getUsed() > 0L));
        cancel.close();
        assertTrue(listenerDone.await(5, TimeUnit.SECONDS));
        assertEquals(1, limited.availablePermits());
        releaseBody.countDown();
        assertTrue(serverDone.await(5, TimeUnit.SECONDS));
        assertBreakerEmpty();
    }

    private void cancelMidBodyAndAssertRefund(int status, int position, byte[] payload) throws Exception {
        CountDownLatch firstChunkSent = new CountDownLatch(1);
        CountDownLatch releaseBody = new CountDownLatch(1);
        CountDownLatch serverDone = new CountDownLatch(1);
        handler.set(
            exchange -> writePaused(
                exchange,
                status,
                payload,
                FIRST_CHUNK,
                firstChunkSent,
                releaseBody,
                serverDone,
                new AtomicReference<>()
            )
        );

        CountDownLatch listenerDone = new CountDownLatch(1);
        AtomicReference<Exception> failure = new AtomicReference<>();
        Releasable cancel = startExpectingCancel(object(), position, LENGTH, listenerDone, failure);
        assertTrue(firstChunkSent.await(5, TimeUnit.SECONDS));
        assertBusy(() -> assertTrue("destination not charged yet: " + breaker.getUsed(), breaker.getUsed() > 0L));
        cancel.close();
        assertTrue(listenerDone.await(5, TimeUnit.SECONDS));
        assertThat(failure.get(), instanceOf(TaskCancelledException.class));
        releaseBody.countDown();
        assertTrue(serverDone.await(5, TimeUnit.SECONDS));
        assertBreakerEmpty();
    }

    private HttpStorageObject object() {
        return new HttpStorageObject(client, path, config());
    }

    private static HttpConfiguration config() {
        return HttpConfiguration.builder().requestTimeout(Duration.ofSeconds(15)).idleTimeout(Duration.ZERO).build();
    }

    private DirectReadBuffer awaitBuffer(StorageObject object, long position, long length) throws Exception {
        CountDownLatch done = new CountDownLatch(1);
        AtomicReference<DirectReadBuffer> got = new AtomicReference<>();
        AtomicReference<Exception> failure = new AtomicReference<>();
        object.startReadBytesAsync(position, length, factory, Runnable::run, new ActionListener<>() {
            @Override
            public void onResponse(DirectReadBuffer buffer) {
                got.set(buffer);
                done.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                failure.set(e);
                done.countDown();
            }
        });
        assertTrue(done.await(5, TimeUnit.SECONDS));
        if (failure.get() != null) {
            throw new AssertionError(failure.get());
        }
        assertNotNull(got.get());
        return got.get();
    }

    private Releasable startExpectingCancel(
        StorageObject object,
        long position,
        long length,
        CountDownLatch listenerDone,
        AtomicReference<Exception> failure
    ) {
        return object.startReadBytesAsync(position, length, factory, Runnable::run, cancelListener(listenerDone, failure));
    }

    private static ActionListener<DirectReadBuffer> cancelListener(CountDownLatch listenerDone, AtomicReference<Exception> failure) {
        return new ActionListener<>() {
            @Override
            public void onResponse(DirectReadBuffer buffer) {
                buffer.close();
                fail("expected cancellation, got a buffer");
            }

            @Override
            public void onFailure(Exception e) {
                failure.set(e);
                listenerDone.countDown();
            }
        };
    }

    private void assertBreakerEmpty() throws Exception {
        assertBusy(() -> assertEquals(0L, breaker.getUsed()), 5, TimeUnit.SECONDS);
    }

    private static byte[] payload(int length) {
        byte[] bytes = new byte[length];
        for (int i = 0; i < length; i++) {
            bytes[i] = (byte) i;
        }
        return bytes;
    }

    private static void writeFull(
        HttpExchange exchange,
        int status,
        byte[] payload,
        CountDownLatch serverDone,
        AtomicReference<String> outcome
    ) {
        writePaused(exchange, status, payload, payload.length, new CountDownLatch(0), new CountDownLatch(0), serverDone, outcome);
    }

    private static void writePaused(
        HttpExchange exchange,
        int status,
        byte[] payload,
        int firstChunk,
        CountDownLatch firstChunkSent,
        CountDownLatch releaseBody,
        CountDownLatch serverDone,
        AtomicReference<String> outcome
    ) {
        try {
            addRangeHeaders(exchange, status, payload.length);
            exchange.sendResponseHeaders(status, payload.length);
            OutputStream out = exchange.getResponseBody();
            int first = Math.min(firstChunk, payload.length);
            out.write(payload, 0, first);
            out.flush();
            firstChunkSent.countDown();
            if (first < payload.length) {
                if (releaseBody.await(10, TimeUnit.SECONDS) == false) {
                    outcome.set("release timed out");
                    return;
                }
                int offset = first;
                while (offset < payload.length) {
                    int n = Math.min(8 * 1024, payload.length - offset);
                    out.write(payload, offset, n);
                    out.flush();
                    offset += n;
                }
            }
            out.close();
            outcome.set("full body written");
        } catch (IOException e) {
            outcome.set("body write failed: " + e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            outcome.set("interrupted");
        } finally {
            exchange.close();
            if (serverDone != null) {
                serverDone.countDown();
            }
        }
    }

    private static void addRangeHeaders(HttpExchange exchange, int status, int payloadLength) {
        if (status == 206) {
            exchange.getResponseHeaders().add("Content-Range", "bytes 0-" + (payloadLength - 1) + "/" + payloadLength);
        }
        exchange.getResponseHeaders().add("ETag", "\"v1\"");
        exchange.getResponseHeaders().add("Content-Type", "application/octet-stream");
    }

    @SuppressWarnings("unchecked")
    private HttpClient delayingClient(CountDownLatch bodyCompleted, CountDownLatch releaseResponse) {
        HttpClient delaying = mock(HttpClient.class);
        doAnswer(invocation -> {
            HttpRequest request = invocation.getArgument(0);
            HttpResponse.BodyHandler<DirectReadBuffer> handler = invocation.getArgument(1);
            return client.sendAsync(request, info -> delayCompletion(handler.apply(info), bodyCompleted, releaseResponse));
        }).when(delaying).sendAsync(any(HttpRequest.class), any(HttpResponse.BodyHandler.class));
        return delaying;
    }

    private static HttpResponse.BodySubscriber<DirectReadBuffer> delayCompletion(
        HttpResponse.BodySubscriber<DirectReadBuffer> inner,
        CountDownLatch bodyCompleted,
        CountDownLatch releaseResponse
    ) {
        CompletableFuture<DirectReadBuffer> delayed = new CompletableFuture<>();
        inner.getBody().whenComplete((buf, err) -> {
            bodyCompleted.countDown();
            try {
                if (releaseResponse.await(10, TimeUnit.SECONDS) == false) {
                    delayed.completeExceptionally(new AssertionError("release timed out"));
                    return;
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                delayed.completeExceptionally(e);
                return;
            }
            if (err != null) {
                delayed.completeExceptionally(err);
            } else {
                delayed.complete(buf);
            }
        });
        return new HttpResponse.BodySubscriber<>() {
            @Override
            public CompletionStage<DirectReadBuffer> getBody() {
                return delayed;
            }

            @Override
            public void onSubscribe(Flow.Subscription subscription) {
                inner.onSubscribe(subscription);
            }

            @Override
            public void onNext(List<ByteBuffer> item) {
                inner.onNext(item);
            }

            @Override
            public void onError(Throwable throwable) {
                inner.onError(throwable);
            }

            @Override
            public void onComplete() {
                inner.onComplete();
            }
        };
    }

    private enum Outcome {
        COMPLETED,
        CANCELLED
    }
}
