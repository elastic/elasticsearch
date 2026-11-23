/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.usage;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.core.ssl.SslProfile;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

/**
 * Sends batches of usage records to the Usage API: a {@code POST} of a JSON array over mutual TLS, with the JDK client.
 * <p>
 * The rules are those of billing: a record is never lost to a transient problem and never counted twice, so the ids are never
 * touched and every failure that is not clearly the fault of one record leaves the records pending. The one exception is a batch
 * the Usage API rejects as invalid: it is split in halves, recursively, until the record that is rejected is alone, and only that
 * record is reported {@link PublishResult.FailureClass#INVALID}, so that it does not hold back the others. A batch of {@code n}
 * records needs at most {@code 2n - 1} requests.
 * <p>
 * The Usage API specification lists only the {@code 201} answer and no error code, so the mapping of statuses to failure classes
 * ({@link #classify}) is our inference, kept in one place so that it is easy to change. A {@code 2xx} answer means that the whole
 * batch was written: the API has no per-record answer.
 * <p>
 * It blocks, so it runs on the reporter thread only. The client is rebuilt when the SSL profile reloads, so that a renewed
 * certificate is used without a restart.
 */
public final class HttpUsagePublisher implements UsagePublisher {

    private static final Logger logger = LogManager.getLogger(HttpUsagePublisher.class);

    /** How much of an error body goes into a failure message. */
    private static final int MAX_MESSAGE_BODY_CHARS = 200;

    private final URI uri;
    private final TimeValue requestTimeout;
    private final SslProfile sslProfile;
    private final Function<List<UsageRecord>, BytesReference> serializer;
    private final Object clientLock = new Object();
    private volatile HttpClient client;
    private boolean closed;

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
        return systematicRejection(records, accepted, failures) ? wholeBatchRetryable(failures) : new PublishResult(accepted, failures);
    }

    /**
     * A batch of several records in which every single record was rejected does not look like several bad records but like a
     * problem that is not the records' (a serializer bug, a schema or url problem): dropping all of them would lose a whole batch of
     * usage for good. It is reported as a retryable failure of the whole batch instead, so that the records stay pending, bounded
     * by the store, until someone notices. A single record that is rejected is still dropped: nothing says that it is not the bad one.
     */
    private static boolean systematicRejection(List<UsageRecord> records, Set<String> accepted, List<PublishResult.Failure> failures) {
        return records.size() > 1
            && accepted.isEmpty()
            && failures.size() == records.size()
            && failures.stream().allMatch(f -> f.failureClass() == PublishResult.FailureClass.INVALID);
    }

    private static PublishResult wholeBatchRetryable(List<PublishResult.Failure> failures) {
        String message = "the Usage API rejected every record of the batch, which looks like a problem that is not the records': "
            + "they stay pending ("
            + failures.get(0).message()
            + ")";
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
            CompletableFuture<HttpResponse<String>> future = current.sendAsync(
                request,
                HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8)
            );
            try {
                HttpResponse<String> response = future.get(requestTimeout.millis(), TimeUnit.MILLISECONDS);
                return classify(response.statusCode(), response.body());
            } catch (TimeoutException e) {
                FutureUtils.cancel(future);
                return retryable("request to the Usage API timed out after [" + requestTimeout + "]");
            } catch (InterruptedException e) {
                // Do not leave the request running after publish returns.
                FutureUtils.cancel(future);
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
     * them pending is bounded by the store.
     */
    static Attempt classify(int status, String body) {
        if (status >= 200 && status < 300) {
            return new Attempt(null, null);
        }
        String message = "the Usage API answered with status [" + status + "]" + bodySuffix(body);
        return switch (status) {
            case 400, 422 -> new Attempt(PublishResult.FailureClass.INVALID, message);
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
        return HttpClient.newBuilder()
            .sslContext(sslProfile.sslContext())
            .connectTimeout(Duration.ofMillis(requestTimeout.millis()))
            // One request at a time, so multiplexing has nothing to offer, and HTTP/1.1 avoids the cleartext upgrade handshake.
            .version(HttpClient.Version.HTTP_1_1)
            .build();
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
