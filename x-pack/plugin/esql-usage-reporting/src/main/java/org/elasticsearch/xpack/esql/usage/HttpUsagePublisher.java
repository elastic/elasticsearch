/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.usage;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.ssl.SslConfiguration;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.core.ssl.SslProfile;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;

/**
 * Sends batches of usage records to the Usage API: a {@code POST} of a JSON array over mutual TLS, with the JDK client.
 * <p>
 * The rules are those of billing: a record is never lost to a transient problem and never counted twice, so the ids are never
 * touched and every failure that is not clearly the fault of one record leaves the records pending. The one exception is a batch
 * the Usage API rejects as invalid: it is split in halves, recursively, until the record that is rejected is alone, and only that
 * record is reported {@link PublishResult.FailureClass#INVALID}, so that it does not hold back the others. A batch of {@code n}
 * records needs at most {@code 2n - 1} requests. A rejection that is not followed by an accepted record is held, not dropped,
 * because it probably is not the record's fault: see {@link #publish}.
 * <p>
 * The OpenAPI specification of the Usage API lists only the {@code 201} answer. The statuses come from the documentation of the
 * usage data pipeline (201, 400, 401, 403, 500 and 503) and are mapped to failure classes by {@link #classify}, in one place so
 * that it is easy to change. A {@code 2xx} answer means that the whole batch was written: the API has no per-record answer.
 * <p>
 * It blocks, so it runs on the reporter thread only. The client is rebuilt when the SSL profile reloads, so that a renewed
 * certificate is used without a restart. The protocols and cipher suites of the profile are applied to the client; its
 * {@code verification_mode} is not: the JDK client always verifies the host name of the server, whatever the profile says.
 */
public final class HttpUsagePublisher implements UsagePublisher {

    private static final Logger logger = LogManager.getLogger(HttpUsagePublisher.class);

    /** How much of an error body goes into a failure message. */
    private static final int MAX_MESSAGE_BODY_CHARS = 200;
    /** How many bytes of an error body are read; the rest of the answer is not downloaded. */
    private static final int MAX_BODY_BYTES = 512;

    private final URI uri;
    private final TimeValue requestTimeout;
    private final SslProfile sslProfile;
    private final Function<List<UsageRecord>, BytesReference> serializer;
    private final Object clientLock = new Object();
    private volatile HttpClient client;
    /** The context the current client was built with; guarded by {@code clientLock} once the constructor has returned. */
    private volatile SSLContext clientContext;
    private boolean closed;
    /** See {@link #wholeBatchRetryable}; only the reporter thread publishes, one batch at a time. */
    private volatile boolean suspect = true;

    public HttpUsagePublisher(UsageReportingSettings settings, SslProfile sslProfile) {
        this(URI.create(settings.url()), settings.requestTimeout(), sslProfile, UsageRecordJson::toJson);
    }

    /**
     * @param serializer turns a batch into the request body; a parameter so that the tests do not depend on the record format
     */
    HttpUsagePublisher(URI uri, TimeValue requestTimeout, SslProfile sslProfile, Function<List<UsageRecord>, BytesReference> serializer) {
        this.uri = uri;
        this.requestTimeout = requestTimeout;
        this.sslProfile = sslProfile;
        this.serializer = serializer;
        this.client = newClient();
        sslProfile.addReloadListener(this::onProfileReloaded);
        // A reload between building the client and registering the listener is never notified: compare with the context in use now.
        if (sslProfile.sslContext() != clientContext) {
            onProfileReloaded(sslProfile);
        }
    }

    /** The client in use, for tests that check how it is configured. */
    HttpClient httpClient() {
        return client;
    }

    @Override
    public PublishResult publish(List<UsageRecord> records) {
        if (records.isEmpty()) {
            return PublishResult.empty();
        }
        Set<String> accepted = new HashSet<>();
        List<PublishResult.Failure> failures = new ArrayList<>();
        // Ranges still to send, in order. Splitting pushes the second half first, so the first half is sent first.
        Deque<List<UsageRecord>> ranges = new ArrayDeque<>();
        ranges.push(records);
        while (ranges.isEmpty() == false) {
            List<UsageRecord> range = ranges.pop();
            Attempt attempt = send(range);
            PublishResult.FailureClass failureClass = attempt.failureClass();
            if (failureClass == null) {
                range.forEach(r -> accepted.add(r.id()));
            } else {
                switch (failureClass) {
                    case INVALID -> {
                        if (range.size() == 1) {
                            failures.add(failure(range, failureClass, attempt.message()));
                        } else {
                            int middle = range.size() / 2;
                            ranges.push(range.subList(middle, range.size()));
                            ranges.push(range.subList(0, middle));
                        }
                    }
                    case RETRYABLE, AUTH -> {
                        // Nothing more is sent: the records of this range and of the ranges not yet tried stay pending.
                        List<UsageRecord> remaining = new ArrayList<>(range);
                        ranges.forEach(remaining::addAll);
                        boolean wholeBatch = remaining.size() == records.size();
                        failures.add(
                            wholeBatch
                                ? new PublishResult.Failure(Set.of(), failureClass, attempt.message())
                                : failure(remaining, failureClass, attempt.message())
                        );
                        ranges.clear();
                    }
                    default -> throw new AssertionError("unexpected failure class [" + failureClass + "]");
                }
            }
        }
        boolean everyRecordRejected = accepted.isEmpty()
            && failures.size() == records.size()
            && failures.stream().allMatch(f -> f.failureClass() == PublishResult.FailureClass.INVALID);
        if (accepted.isEmpty() == false) {
            suspect = false;
        }
        if (everyRecordRejected) {
            boolean hold = records.size() > 1 || suspect;
            suspect = true;
            if (hold) {
                return wholeBatchRetryable(records.size(), failures);
            }
        }
        return new PublishResult(accepted, failures);
    }

    /**
     * What a rejection that no accepted record follows is held for: a record that the Usage API refuses is almost always the sign of
     * a problem on our side or on the way (a serializer bug, a schema change, a gateway answering 400), not of one bad record, and
     * dropping it would lose the usage of every period for good. So a batch in which every record is rejected is not dropped, and
     * neither is a single rejected record while {@code suspect} is set. The flag starts set, is set again by every rejection of
     * everything, and is cleared by any accepted record: a record that is really bad is then isolated and dropped by the next batch
     * that holds an accepted one, and a systematic problem costs at most one record.
     */
    private PublishResult wholeBatchRetryable(int size, List<PublishResult.Failure> failures) {
        String message = size > 1
            ? "the Usage API rejected every record of the batch, which looks like a problem that is not the records': "
            : "the Usage API rejected the only record of the batch and no record has been accepted since the last rejection, which "
                + "looks like a problem that is not the record's: ";
        message += "they stay pending (" + failures.get(0).message() + ")";
        // The reporter logs a retryable failure at debug level only, and nothing else tells that records are held.
        logger.warn("{}", message);
        return new PublishResult(Set.of(), List.of(new PublishResult.Failure(Set.of(), PublishResult.FailureClass.RETRYABLE, message)));
    }

    /** One request. Never throws: whatever goes wrong is a retryable failure, because a record is only dropped on a clear refusal. */
    private Attempt send(List<UsageRecord> batch) {
        HttpClient current = client;
        try {
            HttpRequest request = HttpRequest.newBuilder(uri)
                .timeout(Duration.ofMillis(requestTimeout.millis()))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofByteArray(BytesReference.toBytes(serializer.apply(batch))))
                .build();
            // The request timeout of the JDK client ends when the headers arrive: the future bounds the whole exchange.
            CompletableFuture<HttpResponse<String>> future = current.sendAsync(request, HttpUsagePublisher::bodySubscriber);
            try {
                HttpResponse<String> response = future.get(requestTimeout.millis(), TimeUnit.MILLISECONDS);
                return classify(response.statusCode(), response.body());
            } catch (TimeoutException e) {
                cancelExchange(future);
                return retryable("request to the Usage API timed out after [" + requestTimeout + "]");
            } catch (InterruptedException e) {
                // Do not leave the request running after publish returns.
                cancelExchange(future);
                Thread.currentThread().interrupt();
                return retryable("interrupted while sending usage records");
            }
        } catch (ExecutionException e) {
            return retryable("request to the Usage API failed: " + describe(e.getCause()));
        } catch (Exception e) {
            // Includes a failure to serialize or to build the request: nothing was sent, the records stay pending.
            logger.debug("could not send usage records", e);
            return retryable("could not send usage records: " + describe(e));
        }
    }

    /**
     * The one place where an HTTP status becomes a failure class. Anything not listed is retryable, never invalid: a wrong url
     * (404, 405) or a redirect lasts, and dropping every record because of it would be underbilling by mistake, while keeping
     * them pending is bounded by the store. The pipeline documents 201, 400 (malformed or invalid payload), 401 and 403 (identity),
     * and 500 and 503 (transient); it does not return 422, so a 422 comes from something else on the way and is retried.
     */
    static Attempt classify(int status, String body) {
        if (status >= 200 && status < 300) {
            return new Attempt(null, null);
        }
        String message = "the Usage API answered with status [" + status + "]" + bodySuffix(body);
        return switch (status) {
            case 400 -> new Attempt(PublishResult.FailureClass.INVALID, message);
            case 401, 403 -> new Attempt(PublishResult.FailureClass.AUTH, message);
            default -> new Attempt(PublishResult.FailureClass.RETRYABLE, message);
        };
    }

    /**
     * What one request showed: {@code failureClass} null means the batch was accepted; {@code INVALID} means that the batch holds a
     * record the API rejects, and is the only class that leads to a split.
     */
    record Attempt(PublishResult.FailureClass failureClass, String message) {}

    private static Attempt retryable(String message) {
        return new Attempt(PublishResult.FailureClass.RETRYABLE, message);
    }

    private static PublishResult.Failure failure(List<UsageRecord> records, PublishResult.FailureClass failureClass, String message) {
        Set<String> ids = new HashSet<>();
        records.forEach(r -> ids.add(r.id()));
        return new PublishResult.Failure(ids, failureClass, message);
    }

    private static String bodySuffix(String body) {
        if (body == null || body.isBlank()) {
            return "";
        }
        String trimmed = body.strip();
        return ": " + (trimmed.length() > MAX_MESSAGE_BODY_CHARS ? trimmed.substring(0, MAX_MESSAGE_BODY_CHARS) + "..." : trimmed);
    }

    private static String describe(Throwable t) {
        return t == null ? "unknown error" : t.getClass().getSimpleName() + (t.getMessage() == null ? "" : ": " + t.getMessage());
    }

    private HttpClient newClient() {
        SSLContext context = sslProfile.sslContext();
        HttpClient built = HttpClient.newBuilder()
            .sslContext(context)
            .sslParameters(sslParameters(context, sslProfile.configuration()))
            .connectTimeout(Duration.ofMillis(requestTimeout.millis()))
            // One request at a time, so multiplexing has nothing to offer, and HTTP/1.1 avoids the cleartext upgrade handshake.
            .version(HttpClient.Version.HTTP_1_1)
            .build();
        clientContext = context;
        return built;
    }

    /**
     * The protocols and cipher suites the profile allows: an {@code SSLContext} alone does not restrict them. The suites the JVM does
     * not support are left out, as the SSL service does for its own connections.
     */
    private static SSLParameters sslParameters(SSLContext context, SslConfiguration configuration) {
        SSLParameters parameters = new SSLParameters();
        parameters.setProtocols(configuration.supportedProtocols().toArray(new String[0]));
        List<String> supported = List.of(context.getSupportedSSLParameters().getCipherSuites());
        String[] ciphers = configuration.getCipherSuites().stream().filter(supported::contains).toArray(String[]::new);
        if (ciphers.length > 0) {
            parameters.setCipherSuites(ciphers);
        }
        return parameters;
    }

    /**
     * A success body is not read, and an error body keeps only its first bytes, for the message: the subscription is cancelled as soon
     * as the bytes wanted have arrived, so that a large answer is neither buffered nor downloaded.
     */
    private static BoundedBodySubscriber bodySubscriber(HttpResponse.ResponseInfo info) {
        boolean success = info.statusCode() >= 200 && info.statusCode() < 300;
        return new BoundedBodySubscriber(success ? 0 : MAX_BODY_BYTES);
    }

    /**
     * Aborts an exchange that has not finished, so that a request that timed out or was interrupted does not stay in flight, before
     * or after the response headers (the stalled-body test covers the second case). The JDK client documents {@code cancel(true)}, and
     * only that, as the way to do it; {@code CompletableFuture} gives the flag no other meaning, so no thread is interrupted.
     */
    @SuppressForbidden(reason = "cancel(true) is how the JDK HttpClient is told to abort an exchange; no thread is interrupted")
    private static void cancelExchange(CompletableFuture<?> future) {
        future.cancel(true);
    }

    /** Keeps the first {@code limit} bytes of the body as text, and cancels the subscription once they have arrived. */
    static final class BoundedBodySubscriber implements HttpResponse.BodySubscriber<String> {
        private final byte[] kept;
        private int length;
        private volatile Flow.Subscription subscription;
        private final CompletableFuture<String> result = new CompletableFuture<>();

        BoundedBodySubscriber(int limit) {
            this.kept = new byte[limit];
        }

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            this.subscription = subscription;
            subscription.request(Long.MAX_VALUE);
        }

        @Override
        public void onNext(List<ByteBuffer> buffers) {
            if (result.isDone()) {
                return;
            }
            for (ByteBuffer buffer : buffers) {
                int n = Math.min(kept.length - length, buffer.remaining());
                buffer.get(kept, length, n);
                length += n;
                buffer.position(buffer.limit());
            }
            if (length >= kept.length) {
                // Enough: the rest of the body is not wanted, and cancelling lets the client release the connection.
                subscription.cancel();
                result.complete(text());
            }
        }

        @Override
        public void onError(Throwable throwable) {
            result.completeExceptionally(throwable);
        }

        @Override
        public void onComplete() {
            result.complete(text());
        }

        private String text() {
            return new String(kept, 0, length, StandardCharsets.UTF_8);
        }

        @Override
        public CompletionStage<String> getBody() {
            return result;
        }
    }

    /**
     * A reloaded profile has a new {@code SSLContext}: later requests use a new client. The old one only stops accepting requests
     * and ends when those in flight do; closing it here would block the thread that reloads the profile.
     */
    private void onProfileReloaded(SslProfile profile) {
        synchronized (clientLock) {
            if (closed) {
                return;
            }
            try {
                HttpClient replacement = newClient();
                HttpClient old = client;
                client = replacement;
                old.shutdown();
                logger.info("the usage reporting SSL profile was reloaded: new connections use the new configuration");
            } catch (Exception e) {
                logger.warn("could not apply the reloaded usage reporting SSL profile, the previous configuration stays in use", e);
            }
        }
    }

    @Override
    public void close() {
        HttpClient toClose;
        synchronized (clientLock) {
            if (closed) {
                return;
            }
            closed = true;
            toClose = client;
        }
        // Outside the lock: close() waits for requests in flight, and a profile reload must not wait for it.
        toClose.close();
    }
}
