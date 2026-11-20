/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.eql;

import org.apache.lucene.search.TotalHits;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.io.stream.NotSerializableExceptionWrapper;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LocalCircuitBreaker;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.test.AnyOperatorTestCase;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.eql.action.EqlSearchAction;
import org.elasticsearch.xpack.eql.action.EqlSearchRequest;
import org.elasticsearch.xpack.eql.action.EqlSearchResponse;
import org.elasticsearch.xpack.eql.action.EqlSearchResponse.Event;
import org.elasticsearch.xpack.eql.action.EqlSearchResponse.Hits;
import org.elasticsearch.xpack.eql.action.EqlSearchResponse.Sequence;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.plan.logical.EqlRelation;
import org.hamcrest.Matcher;
import org.junit.After;
import org.junit.Before;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.fieldAttribute;
import static org.elasticsearch.xpack.esql.core.type.DataType.INTEGER;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.elasticsearch.xpack.esql.core.type.DataType.LONG;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

/**
 * Unit tests for {@link EqlSourceOperator}: the coordinator-local source operator that issues a single async
 * {@code EqlSearchAction} and converts the (bounded) response into one {@link Page}.
 *
 * <p>The class extends {@link AnyOperatorTestCase} so the shared source-operator contract checks (description,
 * {@code toString}, empty status, {@code canProduceMoreDataWithoutExtraInput}) run for free, and reuses the base
 * {@link #driverContext()} (a real circuit breaker with a huge limit) and {@link org.elasticsearch.compute.test.ComputeTestCase}'s
 * {@code @After}, which asserts every block was released and the breaker returned to zero. On top of that it drives the
 * operator directly through the {@link SourceOperator} contract ({@code isBlocked}/{@code getOutput}/{@code isFinished}/
 * {@code close}) against a captured async listener, exercising the failure and resource-management paths the
 * internalClusterTest happy-paths never reach.
 */
public class EqlSourceOperatorTests extends AnyOperatorTestCase {

    private static final List<Attribute> SEQUENCE_SYNTHETICS = List.of(
        new ReferenceAttribute(Source.EMPTY, "_sequence", LONG),
        new ReferenceAttribute(Source.EMPTY, "_sequence_stage", INTEGER),
        new ReferenceAttribute(Source.EMPTY, "join_keys", KEYWORD)
    );

    private ThreadPool threadPool;

    @Before
    public void startThreadPool() {
        threadPool = new TestThreadPool(getTestName());
    }

    @After
    public void stopThreadPool() {
        terminate(threadPool);
    }

    @Override
    protected Operator.OperatorFactory simple(SimpleOptions options) {
        return new EqlSourceOperator.Factory(
            new CapturingEqlClient(threadPool),
            eqlRequest(1000),
            EqlRelation.Mode.EVENT,
            List.of(fieldAttribute("process.name", KEYWORD)),
            Source.EMPTY,
            false
        );
    }

    @Override
    protected Matcher<String> expectedDescriptionOfSimple() {
        return equalTo("EqlSourceOperator[mode=EVENT, indices=logs]");
    }

    @Override
    protected Matcher<String> expectedToStringOfSimple() {
        // EqlSourceOperator does not override toString, so it is the default identity string.
        return containsString("EqlSourceOperator@");
    }

    @Override
    protected void assertStatus(Map<String, Object> map, List<Page> input, List<Page> output) {
        // EqlSourceOperator reports no Operator.Status.
        assertThat(map, nullValue());
    }

    /**
     * A hand-rolled {@link org.elasticsearch.client.internal.Client} test double. The real internal client needs a
     * live cluster and the wired EQL transport action, which a unit test cannot provide; this double instead captures
     * the {@link ActionListener} the operator registers so the test can deliver the async outcome (success, failure,
     * or none-yet) on its own schedule. That control is exactly what the close-before-response and error paths need.
     */
    private static final class CapturingEqlClient extends NoOpClient {
        private ActionListener<EqlSearchResponse> capturedListener;
        private int executions;

        CapturingEqlClient(ThreadPool threadPool) {
            super(threadPool);
        }

        @Override
        @SuppressWarnings("unchecked")
        protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
            ActionType<Response> action,
            Request request,
            ActionListener<Response> listener
        ) {
            assertSame("EqlSourceOperator must delegate to EqlSearchAction", EqlSearchAction.INSTANCE, action);
            executions++;
            // Capture rather than respond so the test decides when (and whether) the response arrives.
            capturedListener = (ActionListener<EqlSearchResponse>) listener;
        }
    }

    /** {@code Factory#get} must build an operator that has not yet issued the search — the request fires on first poll. */
    public void testFactoryGetDefersRequest() {
        CapturingEqlClient client = new CapturingEqlClient(threadPool);
        EqlSourceOperator.Factory factory = new EqlSourceOperator.Factory(
            client,
            eqlRequest(1000),
            EqlRelation.Mode.EVENT,
            List.of(fieldAttribute("process.name", KEYWORD)),
            Source.EMPTY,
            false
        );
        DriverContext driverContext = driverContext();
        try (SourceOperator operator = factory.get(driverContext)) {
            assertNotNull(operator);
            assertThat("no request until first poll", client.executions, equalTo(0));
        }
    }

    /** Happy path, event mode: the prepared response flows through to a Page whose typed columns match the converter. */
    public void testEventModeProducesTypedPage() {
        CapturingEqlClient client = new CapturingEqlClient(threadPool);
        DriverContext driverContext = driverContext();
        List<Attribute> schema = List.of(fieldAttribute("process.name", KEYWORD), fieldAttribute("process.pid", LONG), metadata("_id"));
        EqlSearchResponse response = eventResponse(
            List.of(
                envelopeEvent("logs", "id-0", null, Map.of("process.name", List.of("alpha"), "process.pid", List.of(100))),
                envelopeEvent("logs", "id-1", null, Map.of("process.name", List.of("beta"), "process.pid", List.of(200)))
            )
        );

        SourceOperator operator = operator(client, driverContext, eqlRequest(1000), EqlRelation.Mode.EVENT, schema);
        Page page = null;
        try {
            // Before the response, the operator blocks and yields nothing.
            assertBlocked(operator);
            assertThat(operator.getOutput(), equalTo(null));
            assertFalse(operator.isFinished());

            client.capturedListener.onResponse(response);

            assertNotBlocked(operator);
            page = operator.getOutput();
            assertNotNull(page);
            assertThat(page.getBlockCount(), equalTo(3));
            assertThat(page.getPositionCount(), equalTo(2));
            assertBytesRefColumn(page, 0, "alpha", "beta");
            LongBlock pid = page.getBlock(1);
            assertThat(pid.getLong(0), equalTo(100L));
            assertThat(pid.getLong(1), equalTo(200L));
            assertBytesRefColumn(page, 2, "id-0", "id-1"); // _id from the envelope

            // Single-shot: after the one page, the operator is finished and produces no more.
            assertTrue(operator.isFinished());
            assertThat(operator.getOutput(), equalTo(null));
        } finally {
            if (page != null) {
                page.releaseBlocks();
            }
            operator.close();
            response.decRef();
        }
        // The one page was the only breaker-accounted resource; releasing it must return the breaker to zero.
        assertThat(driverContext.breaker().getUsed(), equalTo(0L));
    }

    /**
     * Happy path, sequence mode: the response unnests to one row per event with the {@code _sequence}/
     * {@code _sequence_stage}/{@code join_keys} synthetics, and a missing event yields a null field row while keeping
     * its synthetics.
     */
    public void testSequenceModeProducesSyntheticsAndMissingNulls() {
        CapturingEqlClient client = new CapturingEqlClient(threadPool);
        DriverContext driverContext = driverContext();
        List<Attribute> schema = concat(SEQUENCE_SYNTHETICS, fieldAttribute("process.name", KEYWORD));
        Event present = fieldEvent("p0");
        Event missing = new Event("", "", null, null, true);
        EqlSearchResponse response = sequenceResponse(List.of(new Sequence(List.of("host-a"), List.of(present, missing))));

        SourceOperator operator = operator(client, driverContext, eqlRequest(1000), EqlRelation.Mode.SEQUENCE, schema);
        Page page = null;
        try {
            assertBlocked(operator); // first poll issues the request and captures the listener
            client.capturedListener.onResponse(response);
            page = operator.getOutput();
            assertThat(page.getBlockCount(), equalTo(4));
            assertThat(page.getPositionCount(), equalTo(2)); // one sequence * two events

            LongBlock seq = page.getBlock(0);
            IntBlock stage = page.getBlock(1);
            assertThat(seq.getLong(0), equalTo(0L));
            assertThat(stage.getInt(0), equalTo(0));
            assertThat(seq.getLong(1), equalTo(0L)); // synthetics still populated for the missing event's row
            assertThat(stage.getInt(1), equalTo(1));
            assertBytesRefColumn(page, 2, "host-a", "host-a"); // join_keys on both rows

            BytesRefBlock name = page.getBlock(3);
            assertFalse(name.isNull(0));
            assertTrue("the missing event's field must be null", name.isNull(1));
        } finally {
            if (page != null) {
                page.releaseBlocks();
            }
            operator.close();
            response.decRef();
        }
        assertThat(driverContext.breaker().getUsed(), equalTo(0L));
    }

    /** {@code isBlocked} returns a pending listener until the response arrives, then {@code NOT_BLOCKED}. */
    public void testBlockedUntilResponseThenNotBlocked() {
        CapturingEqlClient client = new CapturingEqlClient(threadPool);
        DriverContext driverContext = driverContext();
        EqlSearchResponse response = eventResponse(List.of(fieldEvent("x")));

        SourceOperator operator = operator(
            client,
            driverContext,
            eqlRequest(1000),
            EqlRelation.Mode.EVENT,
            List.of(fieldAttribute("process.name", KEYWORD))
        );
        Page page = null;
        try {
            // First poll issues exactly one request and leaves the operator blocked.
            assertBlocked(operator);
            assertThat(client.executions, equalTo(1));
            // A second poll while blocked must not re-issue the request.
            assertBlocked(operator);
            assertThat(client.executions, equalTo(1));

            client.capturedListener.onResponse(response);
            assertNotBlocked(operator);
            page = operator.getOutput();
        } finally {
            if (page != null) {
                page.releaseBlocks();
            }
            operator.close();
            response.decRef();
        }
        assertThat(driverContext.breaker().getUsed(), equalTo(0L));
    }

    /**
     * The scary path: {@code close()} runs (cancellation / sibling-operator failure) before the async response
     * arrives. When the response then lands, the operator must release the freshly-built page instead of stashing it,
     * so no breaker-accounted blocks leak.
     */
    public void testCloseBeforeResponseReleasesBuiltPage() {
        CapturingEqlClient client = new CapturingEqlClient(threadPool);
        DriverContext driverContext = driverContext();
        EqlSearchResponse response = eventResponse(
            List.of(fieldEvent("a"), fieldEvent("b"), fieldEvent("c")) // enough to allocate real blocks
        );

        SourceOperator operator = operator(
            client,
            driverContext,
            eqlRequest(1000),
            EqlRelation.Mode.EVENT,
            List.of(fieldAttribute("process.name", KEYWORD))
        );
        try {
            assertBlocked(operator); // issue the request, capture the listener
            operator.close();        // driver drains-and-closes before the response arrives

            // The response arrives after close: the callback builds a page and, seeing the operator closed, releases it.
            client.capturedListener.onResponse(response);

            assertThat("the built page must have been released, not stashed", operator.getOutput(), equalTo(null));
        } finally {
            operator.close();
            response.decRef();
        }
        // The whole point: no leak. The @After breaker assertion is the real gate; assert here too for a precise failure.
        assertThat(driverContext.breaker().getUsed(), equalTo(0L));
    }

    /**
     * {@code close()} after the response has been stashed (page built and held, never emitted) must release the held
     * page — the {@code page != null} arm of {@code close()}.
     */
    public void testCloseAfterResponseReleasesStashedPage() {
        CapturingEqlClient client = new CapturingEqlClient(threadPool);
        DriverContext driverContext = driverContext();
        EqlSearchResponse response = eventResponse(List.of(fieldEvent("a"), fieldEvent("b")));

        SourceOperator operator = operator(
            client,
            driverContext,
            eqlRequest(1000),
            EqlRelation.Mode.EVENT,
            List.of(fieldAttribute("process.name", KEYWORD))
        );
        try {
            assertBlocked(operator);
            client.capturedListener.onResponse(response); // page is now built and stashed, but getOutput() is never called
            assertThat(driverContext.breaker().getUsed(), not(equalTo(0L)));
            operator.close(); // must release the stashed page
        } finally {
            operator.close();
            response.decRef();
        }
        assertThat(driverContext.breaker().getUsed(), equalTo(0L));
    }

    /**
     * The response callback runs on the EQL response thread while the driver run loop may still own the driver's
     * {@link LocalCircuitBreaker}. Building the page must not touch that breaker, or the single-thread assertion fails
     * and kills the node.
     */
    public void testResponseOnOtherThreadDoesNotTouchLocalBreaker() throws Exception {
        CapturingEqlClient client = new CapturingEqlClient(threadPool);
        DriverContext driverContext = driverContextWithLocalBreaker();
        EqlSearchResponse response = eventResponse(List.of(fieldEvent("a"), fieldEvent("b")));
        SourceOperator operator = operator(
            client,
            driverContext,
            eqlRequest(1000),
            EqlRelation.Mode.EVENT,
            List.of(fieldAttribute("process.name", KEYWORD))
        );
        Page page = null;
        try {
            assertBlocked(operator);
            pinLocalBreakerToAnotherThread(driverContext);
            client.capturedListener.onResponse(response);
            unpinLocalBreaker(driverContext);

            page = operator.getOutput();
            assertThat(page.getPositionCount(), equalTo(2));
            assertBytesRefColumn(page, 0, "a", "b");
        } finally {
            if (page != null) {
                page.releaseBlocks();
            }
            operator.close();
            response.decRef();
            unpinLocalBreaker(driverContext);
            closeLocalBreaker(driverContext);
        }
        assertThat(driverContext.breaker().getUsed(), equalTo(0L));
    }

    /** Same as above, but the operator is closed first, so the response thread releases the page it just built. */
    public void testResponseAfterCloseOnOtherThreadDoesNotTouchLocalBreaker() throws Exception {
        CapturingEqlClient client = new CapturingEqlClient(threadPool);
        DriverContext driverContext = driverContextWithLocalBreaker();
        EqlSearchResponse response = eventResponse(List.of(fieldEvent("a"), fieldEvent("b")));
        SourceOperator operator = operator(
            client,
            driverContext,
            eqlRequest(1000),
            EqlRelation.Mode.EVENT,
            List.of(fieldAttribute("process.name", KEYWORD))
        );
        try {
            assertBlocked(operator);
            operator.close();
            pinLocalBreakerToAnotherThread(driverContext);
            client.capturedListener.onResponse(response);
            unpinLocalBreaker(driverContext);
            assertThat(operator.getOutput(), equalTo(null));
        } finally {
            operator.close();
            response.decRef();
            unpinLocalBreaker(driverContext);
            closeLocalBreaker(driverContext);
        }
        assertThat(driverContext.breaker().getUsed(), equalTo(0L));
    }

    /** Error path: the EQL search fails; {@code getOutput()} surfaces the failure as a runtime exception, holding nothing. */
    public void testErrorFromClientSurfacesAsRuntimeException() {
        CapturingEqlClient client = new CapturingEqlClient(threadPool);
        DriverContext driverContext = driverContext();

        SourceOperator operator = operator(
            client,
            driverContext,
            eqlRequest(1000),
            EqlRelation.Mode.EVENT,
            List.of(fieldAttribute("process.name", KEYWORD))
        );
        try {
            assertBlocked(operator);
            client.capturedListener.onFailure(new ElasticsearchException("eql boom"));

            assertNotBlocked(operator);
            RuntimeException e = expectThrows(RuntimeException.class, operator::getOutput);
            assertThat(e.getMessage(), containsString("eql boom"));
        } finally {
            operator.close();
        }
        // No page was ever built on the failure path, so nothing to release.
        assertThat(driverContext.breaker().getUsed(), equalTo(0L));
    }

    /**
     * A conversion failure inside the response callback (here: a schema column the converter rejects) is caught,
     * stashed as the pending failure, and surfaced by {@code getOutput()} — with the half-built page released, so the
     * breaker returns to zero.
     */
    public void testConversionFailureInCallbackSurfacesAsFailure() {
        CapturingEqlClient client = new CapturingEqlClient(threadPool);
        DriverContext driverContext = driverContext();
        // _score is a valid FROM metadata field but not an EQL envelope field; EqlPageConverter throws on it.
        List<Attribute> schema = List.of(metadata("_score"));
        EqlSearchResponse response = eventResponse(List.of(fieldEvent("a")));

        SourceOperator operator = operator(client, driverContext, eqlRequest(1000), EqlRelation.Mode.EVENT, schema);
        try {
            assertBlocked(operator);
            client.capturedListener.onResponse(response);

            assertNotBlocked(operator);
            assertFalse("a pending conversion failure must keep the operator unfinished", operator.isFinished());
            RuntimeException e = expectThrows(RuntimeException.class, operator::getOutput);
            assertThat(e.getMessage(), containsString("unexpected EQL metadata column"));
        } finally {
            operator.close();
            response.decRef();
        }
        // The converter releases its half-built blocks on failure, so nothing leaks.
        assertThat(driverContext.breaker().getUsed(), equalTo(0L));
    }

    /**
     * While a failure is pending, {@code isFinished()} stays false (so the driver keeps polling {@code getOutput()} and
     * the exception propagates) and {@code isBlocked()} reports not-blocked so the poll actually happens.
     */
    public void testFailurePendingKeepsOperatorUnfinished() {
        CapturingEqlClient client = new CapturingEqlClient(threadPool);
        DriverContext driverContext = driverContext();

        SourceOperator operator = operator(
            client,
            driverContext,
            eqlRequest(1000),
            EqlRelation.Mode.EVENT,
            List.of(fieldAttribute("process.name", KEYWORD))
        );
        try {
            assertBlocked(operator);
            client.capturedListener.onFailure(new ElasticsearchException("pending failure"));

            assertFalse("must stay unfinished while a failure is pending", operator.isFinished());
            assertNotBlocked(operator);

            expectThrows(RuntimeException.class, operator::getOutput);
        } finally {
            operator.close();
        }
        assertThat(driverContext.breaker().getUsed(), equalTo(0L));
    }

    /**
     * {@code finish()} (the driver's graceful-stop entry point) marks the operator finished without a response, and
     * {@code close()} on an un-polled operator releases nothing.
     */
    public void testFinishMarksOperatorFinished() {
        CapturingEqlClient client = new CapturingEqlClient(threadPool);
        DriverContext driverContext = driverContext();

        SourceOperator operator = operator(
            client,
            driverContext,
            eqlRequest(1000),
            EqlRelation.Mode.EVENT,
            List.of(fieldAttribute("process.name", KEYWORD))
        );
        try {
            assertFalse(operator.isFinished());
            operator.finish();
            assertTrue("finish() must mark the operator finished", operator.isFinished());
        } finally {
            operator.close();
        }
        assertThat(driverContext.breaker().getUsed(), equalTo(0L));
    }

    /** A partial EQL response (a shard failed while the enclosing query allowed partial results) surfaces as a warning. */
    public void testPartialResponseEmitsWarning() {
        DriverContext driverContext = pullEventPage(false, new ShardSearchFailure(new IllegalStateException("shard failure")));
        assertThat(
            collectWarnings(driverContext),
            equalTo(List.of("Line 1:1: EQL query returned partial results, [1] failed shards: [illegal_state_exception: shard failure]"))
        );
        assertTrue(driverContext.partial());
    }

    public void testPartialWarningNamesTheViewWithoutEchoingItsDefinition() {
        DriverContext driverContext = pullEventPage(
            new Source(1, 0, "EQL logs \"process where secret_field == \\\"secret\\\"\"").withViewName("eql_view"),
            false,
            new ShardSearchFailure(new IllegalStateException("shard failure"))
        );
        assertThat(
            collectWarnings(driverContext),
            equalTo(
                List.of(
                    "Line 1:1 (in view [eql_view]): EQL query returned partial results, [1] failed shards: "
                        + "[illegal_state_exception: shard failure]"
                )
            )
        );
    }

    public void testTimedOutResponseIsPartial() {
        DriverContext driverContext = pullEventPage(true);
        assertThat(collectWarnings(driverContext), equalTo(List.of("Line 1:1: EQL query returned partial results, the search timed out")));
        assertTrue(driverContext.partial());
    }

    public void testCompleteResponseIsNotPartial() {
        DriverContext driverContext = pullEventPage(false);
        assertThat(collectWarnings(driverContext), empty());
        assertFalse(driverContext.partial());
    }

    public void testPartialWarningListsExactlyThreeDistinctRootCausesWithoutASuffix() {
        RuntimeException cyclic = new RuntimeException("cyclic");
        cyclic.initCause(new RuntimeException("cause", cyclic));
        DriverContext driverContext = pullEventPage(
            false,
            shardFailure(new IllegalStateException(), 0, "n1"),
            shardFailure(new IllegalStateException(), 1, "n1"),
            shardFailure(new IllegalArgumentException("second"), 2, "n1"),
            shardFailure(cyclic, 3, "n1")
        );
        assertThat(
            collectWarnings(driverContext),
            equalTo(
                List.of(
                    "Line 1:1: EQL query returned partial results, [4] failed shards: "
                        + "[illegal_argument_exception: second], [illegal_state_exception], [runtime_exception: cyclic]"
                )
            )
        );
    }

    public void testPartialWarningCountsShardCopiesOnceAndListsSortedRootCausesUpToALimit() {
        DriverContext driverContext = pullEventPage(
            true,
            shardFailure(new ElasticsearchException("wrapped", new IllegalStateException("first")), 0, "n1"),
            shardFailure(new IllegalStateException("first"), 0, "n2"),
            shardFailure(
                new ElasticsearchException("remote", new NotSerializableExceptionWrapper(new NumberFormatException("second"))),
                1,
                "n1"
            ),
            shardFailure(new RuntimeException("third"), 2, "n1"),
            shardFailure(new RuntimeException("fourth"), 3, "n1"),
            shardFailure(new RuntimeException("fifth"), 4, "n1")
        );
        assertThat(
            collectWarnings(driverContext),
            equalTo(
                List.of(
                    "Line 1:1: EQL query returned partial results, the search timed out, [5] failed shards: "
                        + "[illegal_state_exception: first], [number_format_exception: second], [runtime_exception: fifth]"
                        + " and [2] more"
                )
            )
        );
        assertTrue(driverContext.partial());
    }

    public void testDefaultSizeWarningIsRegisteredWhenTheSearchIsIssued() {
        assertDefaultSizeWarning(
            new Source(1, 0, "eqlq"),
            "Line 1:1: EQL query used the default size of [10] matches. Set WITH {\"size\": n} to return more."
        );
    }

    public void testDefaultSizeWarningNamesTheViewWithoutEchoingItsDefinition() {
        assertDefaultSizeWarning(
            new Source(1, 0, "EQL logs \"process where secret_field == \\\"secret\\\"\"").withViewName("eql_view"),
            "Line 1:1 (in view [eql_view]): EQL query used the default size of [10] matches. Set WITH {\"size\": n} to return more."
        );
    }

    private void assertDefaultSizeWarning(Source source, String expectedWarning) {
        CapturingEqlClient client = new CapturingEqlClient(threadPool);
        DriverContext driverContext = driverContext();
        EqlSearchResponse response = eventResponse(List.of(fieldEvent("a")));
        SourceOperator operator = new EqlSourceOperator.Factory(
            client,
            eqlRequest(10),
            EqlRelation.Mode.EVENT,
            List.of(fieldAttribute("process.name", KEYWORD)),
            source,
            true
        ).get(driverContext);
        try {
            assertBlocked(operator);
            assertThat(collectWarnings(driverContext), equalTo(List.of(expectedWarning)));
            operator.close();
            client.capturedListener.onResponse(response);
        } finally {
            operator.close();
            response.decRef();
        }
        assertThat(driverContext.breaker().getUsed(), equalTo(0L));
    }

    // ---- helpers ----

    /** A driver context whose block factory charges a {@link LocalCircuitBreaker}, as the real {@code Driver} sets up. */
    private DriverContext driverContextWithLocalBreaker() {
        DriverContext root = driverContext();
        LocalCircuitBreaker localBreaker = new LocalCircuitBreaker(root.breaker(), 0, 0);
        return new DriverContext(root.bigArrays(), root.blockFactory().newChildFactory(localBreaker), null);
    }

    /** Releases the run-loop pin. Idempotent, so {@code finally} can call it even if the body already did. */
    private static void unpinLocalBreaker(DriverContext driverContext) {
        assertTrue(driverContext.assertEndRunLoop());
    }

    private static void closeLocalBreaker(DriverContext driverContext) {
        ((LocalCircuitBreaker) driverContext.blockFactory().breaker()).close();
    }

    /** Marks another thread as the driver run-loop owner of the local breaker, like a driver that is still running. */
    private static void pinLocalBreakerToAnotherThread(DriverContext driverContext) throws InterruptedException {
        Thread driverThread = new Thread(() -> assertTrue(driverContext.assertBeginRunLoop()), "pin-driver-breaker");
        driverThread.start();
        driverThread.join();
    }

    private SourceOperator operator(
        CapturingEqlClient client,
        DriverContext driverContext,
        EqlSearchRequest request,
        EqlRelation.Mode mode,
        List<Attribute> schema
    ) {
        return operator(client, driverContext, request, mode, Source.EMPTY, schema);
    }

    private SourceOperator operator(
        CapturingEqlClient client,
        DriverContext driverContext,
        EqlSearchRequest request,
        EqlRelation.Mode mode,
        Source source,
        List<Attribute> schema
    ) {
        return new EqlSourceOperator.Factory(client, request, mode, schema, source, false).get(driverContext);
    }

    private static ShardSearchFailure shardFailure(Exception cause, int shard, String node) {
        return new ShardSearchFailure(cause, new SearchShardTarget(node, new ShardId("logs", "_na_", shard), null));
    }

    private DriverContext pullEventPage(boolean timedOut, ShardSearchFailure... failures) {
        return pullEventPage(new Source(1, 0, "eqlq"), timedOut, failures);
    }

    private DriverContext pullEventPage(Source source, boolean timedOut, ShardSearchFailure... failures) {
        CapturingEqlClient client = new CapturingEqlClient(threadPool);
        DriverContext driverContext = driverContext();
        Hits hits = new Hits(List.of(fieldEvent("a")), null, new TotalHits(1, TotalHits.Relation.EQUAL_TO));
        EqlSearchResponse response = new EqlSearchResponse(hits, 1, timedOut, failures);
        SourceOperator operator = operator(
            client,
            driverContext,
            eqlRequest(1000),
            EqlRelation.Mode.EVENT,
            source,
            List.of(fieldAttribute("process.name", KEYWORD))
        );
        Page page = null;
        try {
            assertBlocked(operator);
            client.capturedListener.onResponse(response);
            page = operator.getOutput();
            assertThat(page.getPositionCount(), equalTo(1));
        } finally {
            if (page != null) {
                page.releaseBlocks();
            }
            operator.close();
            response.decRef();
        }
        assertThat(driverContext.breaker().getUsed(), equalTo(0L));
        return driverContext;
    }

    private static void assertBlocked(SourceOperator operator) {
        assertFalse("operator should be blocked while the search is in flight", operator.isBlocked().listener().isDone());
    }

    private static void assertNotBlocked(SourceOperator operator) {
        assertTrue("operator should be unblocked once the response arrives", operator.isBlocked().listener().isDone());
    }

    private static EqlSearchRequest eqlRequest(int size) {
        return new EqlSearchRequest().indices("logs").query("any where true").size(size);
    }

    private static MetadataAttribute metadata(String name) {
        return (MetadataAttribute) MetadataAttribute.create(Source.EMPTY, name);
    }

    private static Event fieldEvent(String name) {
        return envelopeEvent("logs", randomAlphaOfLength(4), null, Map.of("process.name", List.of(name)));
    }

    private static Event envelopeEvent(String index, String id, BytesReference source, Map<String, ? extends List<?>> fields) {
        Map<String, DocumentField> fetched = new HashMap<>();
        for (Map.Entry<String, ? extends List<?>> e : fields.entrySet()) {
            @SuppressWarnings("unchecked")
            List<Object> values = (List<Object>) e.getValue();
            fetched.put(e.getKey(), new DocumentField(e.getKey(), values));
        }
        return new Event(index, id, source, fetched, false);
    }

    private static EqlSearchResponse eventResponse(List<Event> events) {
        Hits hits = new Hits(events, null, new TotalHits(events.size(), TotalHits.Relation.EQUAL_TO));
        return new EqlSearchResponse(hits, 1, false, new ShardSearchFailure[0]);
    }

    private static EqlSearchResponse sequenceResponse(List<Sequence> sequences) {
        Hits hits = new Hits(null, sequences, new TotalHits(sequences.size(), TotalHits.Relation.EQUAL_TO));
        return new EqlSearchResponse(hits, 1, false, new ShardSearchFailure[0]);
    }

    private static List<Attribute> concat(List<Attribute> head, Attribute tail) {
        return Stream.concat(head.stream(), Stream.of(tail)).toList();
    }

    private static void assertBytesRefColumn(Page page, int blockIndex, String... expected) {
        BytesRefBlock block = page.getBlock(blockIndex);
        BytesRef scratch = new BytesRef();
        for (int i = 0; i < expected.length; i++) {
            assertThat("row " + i + " of block " + blockIndex, block.getBytesRef(i, scratch), equalTo(new BytesRef(expected[i])));
        }
    }
}
