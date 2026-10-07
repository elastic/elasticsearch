/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NotSerializableExceptionWrapper;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.datasources.QueryFailureTelemetry.Failure;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourceUsageAccumulator;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalClientException;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalCredentialsExpiredException;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalException;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalException.Condition;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalFailures;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalObjectChangedException;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalServerException;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalUnavailableException;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.parser.ParsingException;

import java.io.IOException;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;

public class QueryFailureTelemetryTests extends ESTestCase {

    private static final StoragePath PATH = StoragePath.of("s3://bucket/dir/file.parquet");

    /** Written out independently of the implementation so that a change of the mapping has to be made twice. */
    private static final Map<Condition, String> EXPECTED_BY_CONDITION = new EnumMap<>(Condition.class);
    static {
        EXPECTED_BY_CONDITION.put(Condition.STORE_UNAVAILABLE, "storage_unavailable");
        EXPECTED_BY_CONDITION.put(Condition.LOCAL_CAPACITY, "resource_limit");
        EXPECTED_BY_CONDITION.put(Condition.STORE_THROTTLED, "storage_throttled");
        EXPECTED_BY_CONDITION.put(Condition.OBJECT_CHANGED, "storage_unavailable");
        EXPECTED_BY_CONDITION.put(Condition.ACCESS_DENIED, "storage_auth");
        EXPECTED_BY_CONDITION.put(Condition.OBJECT_NOT_FOUND, "storage_not_found");
        EXPECTED_BY_CONDITION.put(Condition.OBJECT_ARCHIVED, "storage_not_found");
        EXPECTED_BY_CONDITION.put(Condition.CREDENTIALS_EXPIRED, "storage_auth");
        EXPECTED_BY_CONDITION.put(Condition.MALFORMED_DATA, "format");
        EXPECTED_BY_CONDITION.put(Condition.METADATA_UNAVAILABLE, "discovery");
        EXPECTED_BY_CONDITION.put(Condition.LISTING_FAILED, "discovery");
        EXPECTED_BY_CONDITION.put(Condition.CLOCK_SKEW, "storage_auth");
        EXPECTED_BY_CONDITION.put(Condition.CLIENT_BUG, "other");
    }

    public void testEveryConditionHasAnExpectedErrorType() {
        for (Condition condition : Condition.values()) {
            assertThat("no expectation for " + condition, EXPECTED_BY_CONDITION.containsKey(condition), equalTo(true));
        }
    }

    public void testEveryConditionMapsToItsErrorType() {
        for (Condition condition : Condition.values()) {
            Failure failure = QueryFailureTelemetry.classify(new ExternalClientException(condition, PATH, "", ""));
            assertThat("condition " + condition, failure.errorType(), equalTo(EXPECTED_BY_CONDITION.get(condition)));
            assertThat(failure.status(), equalTo("400"));
        }
    }

    public void testStatusIsTheOneOfTheTypedException() {
        assertThat(
            QueryFailureTelemetry.classify(new ExternalServerException(Condition.CLIENT_BUG, PATH, "", "")),
            equalTo(new Failure("other", "500"))
        );
        assertThat(
            QueryFailureTelemetry.classify(new ExternalUnavailableException(Condition.STORE_UNAVAILABLE, PATH, "HTTP 503", "", false, 0L)),
            equalTo(new Failure("storage_unavailable", "503"))
        );
        assertThat(
            QueryFailureTelemetry.classify(new ExternalUnavailableException(Condition.STORE_THROTTLED, PATH, "HTTP 429", "", true, 1000L)),
            equalTo(new Failure("storage_throttled", "503"))
        );
        assertThat(
            QueryFailureTelemetry.classify(new ExternalCredentialsExpiredException(PATH, "ExpiredToken", "")),
            equalTo(new Failure("storage_auth", "400"))
        );
        assertThat(
            QueryFailureTelemetry.classify(new ExternalObjectChangedException(PATH)),
            equalTo(new Failure("storage_unavailable", "503"))
        );
    }

    /** A failure wrapped by the factory loop, the schema cache or the transport layer is still recognised. */
    public void testWrappedFailureKeepsItsTypeAndStatus() {
        ExternalClientException denied = new ExternalClientException(Condition.ACCESS_DENIED, PATH, "HTTP 403", "");
        Failure expected = new Failure("storage_auth", "400");
        assertThat(QueryFailureTelemetry.classify(new ExecutionException(denied)), equalTo(expected));
        assertThat(QueryFailureTelemetry.classify(new IllegalArgumentException("wrapper", denied)), equalTo(expected));
        assertThat(QueryFailureTelemetry.classify(new ElasticsearchException("wrapper", new RuntimeException(denied))), equalTo(expected));
    }

    public void testLegacyExternalExceptionsWithoutConditionAreOther() {
        // ExternalFailures.classify builds these with the free-text constructors, which carry no Condition.
        RuntimeException io = ExternalFailures.classify(new IOException("boom"));
        assertThat(io instanceof ExternalClientException, equalTo(true));
        assertThat(QueryFailureTelemetry.classify(io), equalTo(new Failure("other", "400")));

        RuntimeException unexpected = ExternalFailures.classify(new IllegalStateException("bug"));
        assertThat(unexpected instanceof ExternalServerException, equalTo(true));
        assertThat(QueryFailureTelemetry.classify(unexpected), equalTo(new Failure("other", "500")));
    }

    /**
     * {@link ExternalException} is not a registered exception, so a failure raised on a remote data node reaches the coordinator
     * as a {@link NotSerializableExceptionWrapper}. The condition must survive that, or every remote storage failure would be
     * counted as {@code other}.
     */
    public void testConditionSurvivesTheTransportRoundTrip() throws IOException {
        for (Condition condition : Condition.values()) {
            ExternalClientException original = new ExternalClientException(condition, PATH, "", "");
            Exception received = roundTrip(original);
            assertThat(received, instanceOf(NotSerializableExceptionWrapper.class));
            assertThat(ExternalException.conditionOf(received), equalTo(condition));
            assertThat(
                "condition " + condition,
                QueryFailureTelemetry.classify(received),
                equalTo(new Failure(EXPECTED_BY_CONDITION.get(condition), "400"))
            );
        }
        assertThat(
            QueryFailureTelemetry.classify(roundTrip(new ExternalUnavailableException(Condition.STORE_THROTTLED, PATH, "", "", true, 1L))),
            equalTo(new Failure("storage_throttled", "503"))
        );
        assertThat(
            QueryFailureTelemetry.classify(roundTrip(new ExternalServerException(Condition.CLIENT_BUG, PATH, "", ""))),
            equalTo(new Failure("other", "500"))
        );
        assertThat(
            QueryFailureTelemetry.classify(roundTrip(new ExternalObjectChangedException(PATH))),
            equalTo(new Failure("storage_unavailable", "503"))
        );
        assertThat(
            QueryFailureTelemetry.classify(roundTrip(new ExternalCredentialsExpiredException(PATH, "ExpiredToken", ""))),
            equalTo(new Failure("storage_auth", "400"))
        );
        // still found when the transport layer wraps it once more
        assertThat(
            QueryFailureTelemetry.classify(
                new ElasticsearchException("wrapper", roundTrip(new ExternalClientException(Condition.ACCESS_DENIED, PATH, "", "")))
            ),
            equalTo(new Failure("storage_auth", "400"))
        );
    }

    /** A permit timeout is this node's own limit, not a store outage: it must not point on-call at the object store. */
    public void testPermitExhaustionIsAResourceLimit() throws Exception {
        ConcurrencyLimiter limiter = new ConcurrencyLimiter("s3", new ExternalSourceSettings.BlobStoreConcurrency(1, false), 10L);
        limiter.acquire();
        try {
            ExternalUnavailableException thrown = expectThrows(ExternalUnavailableException.class, limiter::acquireChecked);
            assertThat(thrown.condition(), equalTo(Condition.LOCAL_CAPACITY));
            Failure expected = new Failure("resource_limit", "503");
            assertThat(QueryFailureTelemetry.classify(thrown), equalTo(expected));
            assertThat(QueryFailureTelemetry.classify(roundTrip(thrown)), equalTo(expected));
        } finally {
            limiter.release();
        }
    }

    /**
     * The compute layer reports its preferred failure (a client error over a server error) and attaches the others as
     * suppressed. The query is labelled after the failure the client gets, never after a merely suppressed one.
     */
    public void testSuppressedFailuresDoNotDecideTheClassification() {
        ExternalUnavailableException unavailable = new ExternalUnavailableException(
            Condition.STORE_UNAVAILABLE,
            PATH,
            "HTTP 503",
            "",
            false,
            0L
        );

        EsRejectedExecutionException rejected = new EsRejectedExecutionException("saturated");
        rejected.addSuppressed(unavailable);
        assertThat(QueryFailureTelemetry.classify(rejected), equalTo(new Failure("resource_limit", "429")));

        IllegalArgumentException badRequest = new IllegalArgumentException("bad glob");
        badRequest.addSuppressed(unavailable);
        assertThat(QueryFailureTelemetry.classify(badRequest), equalTo(new Failure("other", "400")));

        // when the external failure is the primary one, it still decides, whatever is suppressed on it
        ExternalClientException denied = new ExternalClientException(Condition.ACCESS_DENIED, PATH, "HTTP 403", "");
        denied.addSuppressed(unavailable);
        assertThat(QueryFailureTelemetry.classify(denied), equalTo(new Failure("storage_auth", "400")));
    }

    public void testWithoutCauseKeepsTheConditionMetadata() {
        ExternalClientException original = new ExternalClientException(Condition.OBJECT_NOT_FOUND, PATH, "", "", new IOException("x"));
        assertThat(ExternalException.conditionOf(original.withoutCause()), equalTo(Condition.OBJECT_NOT_FOUND));
        assertThat(original.withoutCause().getMetadata(ExternalException.CONDITION_METADATA_KEY), equalTo(List.of("object_not_found")));
    }

    /** What a node that predates the metadata, or a legacy free-text failure, sends: no condition, so {@code other}. */
    public void testWrapperWithoutAKnownConditionIsOther() throws IOException {
        Exception legacy = roundTrip(ExternalFailures.classify(new IOException("boom")));
        assertThat(legacy, instanceOf(NotSerializableExceptionWrapper.class));
        assertThat(ExternalException.isExternalFailure(legacy), equalTo(false));
        assertThat(QueryFailureTelemetry.classify(legacy), equalTo(new Failure("other", "400")));

        ExternalClientException fromNewerNode = new ExternalClientException(Condition.ACCESS_DENIED, PATH, "", "");
        fromNewerNode.addMetadata(ExternalException.CONDITION_METADATA_KEY, "a_condition_added_later");
        Exception received = roundTrip(fromNewerNode);
        assertThat(ExternalException.isExternalFailure(received), equalTo(true));
        assertThat(ExternalException.conditionOf(received), nullValue());
        assertThat(QueryFailureTelemetry.classify(received), equalTo(new Failure("other", "400")));
    }

    private static Exception roundTrip(Exception e) throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeException(e);
            return out.bytes().streamInput().readException();
        }
    }

    public void testCircuitBreaker() {
        CircuitBreakingException breaker = new CircuitBreakingException("too big", CircuitBreaker.Durability.TRANSIENT);
        assertThat(QueryFailureTelemetry.classify(breaker), equalTo(new Failure("circuit_breaker", "429")));
        assertThat(QueryFailureTelemetry.classify(new ElasticsearchException("wrapped", breaker)).errorType(), equalTo("circuit_breaker"));
    }

    public void testCircuitBreakerWinsOverExternalCause() {
        CircuitBreakingException breaker = new CircuitBreakingException("too big", CircuitBreaker.Durability.TRANSIENT);
        ExternalClientException denied = new ExternalClientException(Condition.ACCESS_DENIED, PATH, "", "", breaker);
        assertThat(QueryFailureTelemetry.classify(denied).errorType(), equalTo("circuit_breaker"));
    }

    public void testRejectedExecutionIsAResourceLimit() {
        assertThat(
            QueryFailureTelemetry.classify(new EsRejectedExecutionException("reader pool is saturated")),
            equalTo(new Failure("resource_limit", "429"))
        );
    }

    /**
     * The engine's own timeout (e.g. an inactive exchange sink) is a {@code timeout}; it is a registered exception, so it
     * survives the wire.
     */
    public void testEngineTimeoutIsATimeout() throws IOException {
        Failure expected = new Failure("timeout", "429");
        ElasticsearchTimeoutException timeout = new ElasticsearchTimeoutException("Exchange sink [x] has been inactive for [5m]");
        assertThat(QueryFailureTelemetry.classify(timeout), equalTo(expected));
        assertThat(QueryFailureTelemetry.classify(new RuntimeException("wrapped", timeout)), equalTo(expected));
        assertThat(QueryFailureTelemetry.classify(roundTrip(timeout)), equalTo(expected));
    }

    /** Not a timeout in the vocabulary's sense: nothing models a raw {@link TimeoutException} as one, so it is not guessed at. */
    public void testRawTimeoutExceptionIsOther() {
        assertThat(
            QueryFailureTelemetry.classify(new RuntimeException("wrapped", new TimeoutException("timed out"))),
            equalTo(new Failure("other", "500"))
        );
    }

    public void testAnalysisFailuresAreVerification() {
        assertThat(
            QueryFailureTelemetry.classify(new VerificationException("Unknown column [x]")),
            equalTo(new Failure("verification", "400"))
        );
        assertThat(QueryFailureTelemetry.classify(new ParsingException("mismatched input")), equalTo(new Failure("verification", "400")));
    }

    /** Untyped failures are deliberately not guessed at: the status still separates a client error from a server error. */
    public void testUntypedFailuresAreOtherWithTheirStatus() {
        assertThat(
            QueryFailureTelemetry.classify(new IllegalArgumentException("Glob pattern discovered too many files")),
            equalTo(new Failure("other", "400"))
        );
        assertThat(QueryFailureTelemetry.classify(new RuntimeException("boom")), equalTo(new Failure("other", "500")));
        assertThat(QueryFailureTelemetry.classify(new ElasticsearchException("boom")), equalTo(new Failure("other", "500")));
    }

    /** Nothing but the closed vocabulary may ever be emitted: class names and messages are unbounded label values. */
    public void testNeverEmitsAnythingOutsideTheClosedVocabulary() {
        Throwable[] failures = new Throwable[] {
            new RuntimeException(randomAlphaOfLength(20)),
            new IllegalArgumentException(randomAlphaOfLength(20)),
            new IOException(randomAlphaOfLength(20)),
            new ExternalClientException(randomFrom(Condition.values()), PATH, randomAlphaOfLength(5), ""),
            new CircuitBreakingException(randomAlphaOfLength(20), CircuitBreaker.Durability.PERMANENT),
            new VerificationException(randomAlphaOfLength(20)) };
        for (Throwable failure : failures) {
            Failure classified = QueryFailureTelemetry.classify(failure);
            assertThat(DataSourceUsageAccumulator.ERROR_TYPE_NAMES, hasItem(classified.errorType()));
            assertThat(Integer.parseInt(classified.status()) >= 400, equalTo(true));
        }
    }
}
