/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.http;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.DirectBufferFactory;
import org.elasticsearch.xpack.esql.datasources.spi.DirectReadBuffer;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpResponse;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Reproduction of the HTTP arm of the S3 breaker-burial defect: the destination buffer for a
 * native-async HTTP read is allocated inside the {@link java.net.http.HttpClient} body handler
 * ({@code DirectByteBufferBodyHandlers.ofRangeRead}'s subscriber, on {@code onSubscribe}). When
 * the circuit breaker refuses that charge, the failure reaches
 * {@code HttpStorageObject.readBytesAsync}'s completion handler wrapped (the client surfaces
 * body-subscriber failures via {@link CompletionException}/{@link IOException}). It must surface
 * with breaker status (429), not be buried inside a status-neutral {@link IOException} that the
 * read boundary classifies as a client error (400).
 */
public class HttpStorageObjectBreakerTripStatusTests extends ESTestCase {

    public void testBreakerTripInsideBodyHandlerSurfacesWithBreakerStatus() throws Exception {
        CircuitBreaker refusing = new NoopCircuitBreaker("test") {
            @Override
            public void addEstimateBytesAndMaybeBreak(long bytes, String label) {
                throw new CircuitBreakingException(
                    "[parent] Data too large, data for [" + label + "] would be [4093440261/3.8gb]",
                    bytes,
                    0,
                    CircuitBreaker.Durability.TRANSIENT
                );
            }
        };
        DirectBufferFactory factory = DirectBufferFactory.forBreaker(refusing);

        HttpClient mockClient = mock(HttpClient.class);
        doAnswer(invocation -> driveLikeTheHttpClient(invocation.getArgument(1))).when(mockClient).sendAsync(any(), any());

        StoragePath path = StoragePath.of("https://example.com/data.parquet");
        HttpStorageObject object = new HttpStorageObject(mockClient, path, HttpConfiguration.defaults());

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Exception> error = new AtomicReference<>();
        object.readBytesAsync(0, 1024, factory, Runnable::run, new ActionListener<>() {
            @Override
            public void onResponse(DirectReadBuffer buffer) {
                fail("expected failure");
            }

            @Override
            public void onFailure(Exception e) {
                error.set(e);
                latch.countDown();
            }
        });
        assertTrue(latch.await(5, TimeUnit.SECONDS));

        // The rejection must still be in the cause chain, whatever the wrapping did.
        assertNotNull(
            "the CircuitBreakingException must survive in the cause chain",
            ExceptionsHelper.unwrap(error.get(), CircuitBreakingException.class)
        );

        // The mapped failure must carry the breaker's status, not be buried under a
        // status-neutral IOException (status 500 here, and classified 400 by the external
        // read boundary).
        assertThat(
            "a breaker rejection must surface with breaker status, not as an I/O failure",
            ExceptionsHelper.status(error.get()),
            equalTo(RestStatus.TOO_MANY_REQUESTS)
        );
        // ... and still name the object that tripped it, like every other mapped read failure.
        assertThat(error.get(), instanceOf(CircuitBreakingException.class));
        assertThat(error.get().getMessage(), containsString(path.toString()));
    }

    /**
     * Drives the body handler the way the JDK {@link HttpClient} does: applies it to the response
     * info, subscribes the body subscriber (which allocates the destination buffer and trips the
     * refusing breaker), and surfaces the body failure through the {@code sendAsync} future
     * wrapped in {@link IOException} inside a {@link CompletionException}, mirroring the client's
     * own wrapping of body-processing failures.
     */
    private static CompletableFuture<HttpResponse<DirectReadBuffer>> driveLikeTheHttpClient(
        HttpResponse.BodyHandler<DirectReadBuffer> handler
    ) throws Exception {
        HttpResponse.ResponseInfo responseInfo = mock(HttpResponse.ResponseInfo.class);
        when(responseInfo.statusCode()).thenReturn(206);
        HttpResponse.BodySubscriber<DirectReadBuffer> subscriber = handler.apply(responseInfo);
        subscriber.onSubscribe(new Flow.Subscription() {
            @Override
            public void request(long n) {}

            @Override
            public void cancel() {}
        });

        CompletableFuture<HttpResponse<DirectReadBuffer>> clientFuture = new CompletableFuture<>();
        subscriber.getBody().whenComplete((body, failure) -> {
            if (failure != null) {
                clientFuture.completeExceptionally(
                    new CompletionException(new IOException("HTTP body processing failed: " + failure.getMessage(), failure))
                );
            } else {
                fail("expected the body subscriber to fail on the refused buffer allocation");
            }
        });
        return clientFuture;
    }
}
