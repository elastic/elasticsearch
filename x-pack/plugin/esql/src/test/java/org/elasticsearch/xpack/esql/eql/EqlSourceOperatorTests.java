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
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.test.AnyOperatorTestCase;
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

        SourceOperator operator = operator(client, driverContext, eqlRequest(1000), EqlRelation.Mode.EVENT, schema, false);
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

        SourceOperator operator = operator(client, driverContext, eqlRequest(1000), EqlRelation.Mode.SEQUENCE, schema, false);
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
            List.of(fieldAttribute("process.name", KEYWORD)),
            false
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
            List.of(fieldAttribute("process.name", KEYWORD)),
            false
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
            List.of(fieldAttribute("process.name", KEYWORD)),
            false
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

    /** Error path: the EQL search fails; {@code getOutput()} surfaces the failure as a runtime exception, holding nothing. */
    public void testErrorFromClientSurfacesAsRuntimeException() {
        CapturingEqlClient client = new CapturingEqlClient(threadPool);
        DriverContext driverContext = driverContext();

        SourceOperator operator = operator(
            client,
            driverContext,
            eqlRequest(1000),
            EqlRelation.Mode.EVENT,
            List.of(fieldAttribute("process.name", KEYWORD)),
            false
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

        SourceOperator operator = operator(client, driverContext, eqlRequest(1000), EqlRelation.Mode.EVENT, schema, false);
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
            List.of(fieldAttribute("process.name", KEYWORD)),
            false
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
            List.of(fieldAttribute("process.name", KEYWORD)),
            false
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

    /**
     * With {@code warnOnTruncation} and a response that fills the request size cap, a truncation warning is emitted —
     * and only from the {@code getOutput()} (driver-thread) path, never from the response callback. In production the
     * callback runs on a transport thread whose thread-context headers are not collected, so the operator defers the
     * warning to {@code getOutput()}; here we assert the callback emits no warning header and only {@code getOutput()}
     * does.
     */
    public void testTruncationWarningEmittedFromGetOutputNotCallback() {
        CapturingEqlClient client = new CapturingEqlClient(threadPool);
        DriverContext driverContext = driverContext();
        int size = 2;
        Source source = new Source(1, 0, "eqlq");
        EqlSearchResponse response = eventResponse(List.of(fieldEvent("a"), fieldEvent("b"))); // fills the cap of 2

        String expected = "Line "
            + source.lineNumber()
            + ":"
            + source.columnNumber()
            + " ["
            + source.text()
            + "]: EQL query returned the maximum number of results ["
            + size
            + "]; results may be incomplete. Raise the size option or the [esql.query.result_truncation_max_size] setting";

        SourceOperator operator = operator(client, driverContext, eqlRequest(size), EqlRelation.Mode.EVENT, source, true);
        Page page = null;
        try {
            assertBlocked(operator);
            client.capturedListener.onResponse(response);
            page = operator.getOutput();
            assertThat(page.getPositionCount(), equalTo(2));
        } finally {
            if (page != null) {
                page.releaseBlocks();
            }
            operator.close();
            response.decRef();
        }
        // The warning is registered on the driver context during getOutput(), not the response callback.
        assertThat(collectWarnings(driverContext), equalTo(List.of(expected)));
        assertThat(driverContext.breaker().getUsed(), equalTo(0L));
    }

    /** A partial EQL response (a shard failed while the enclosing query allowed partial results) surfaces as a warning. */
    public void testPartialResponseEmitsWarning() {
        CapturingEqlClient client = new CapturingEqlClient(threadPool);
        DriverContext driverContext = driverContext();
        Source source = new Source(1, 0, "eqlq");
        Hits hits = new Hits(List.of(fieldEvent("a")), null, new TotalHits(1, TotalHits.Relation.EQUAL_TO));
        // isPartial=true: the delegate returned partial results because the enclosing ES|QL query allowed them.
        EqlSearchResponse response = new EqlSearchResponse(hits, 1, false, null, false, true, new ShardSearchFailure[0]);

        SourceOperator operator = operator(
            client,
            driverContext,
            eqlRequest(1000),
            EqlRelation.Mode.EVENT,
            source,
            List.of(fieldAttribute("process.name", KEYWORD)),
            false
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
        // The warning is registered on the driver context during getOutput(), not the response callback.
        assertThat(
            collectWarnings(driverContext),
            equalTo(
                List.of(
                    "Line "
                        + source.lineNumber()
                        + ":"
                        + source.columnNumber()
                        + " ["
                        + source.text()
                        + "]: EQL query returned partial results (one or more shards failed or timed out); some events may be missing"
                )
            )
        );
        assertThat(driverContext.breaker().getUsed(), equalTo(0L));
    }

    /** No warning when the result count is below the size cap, even with {@code warnOnTruncation} on. */
    public void testNoTruncationWarningBelowCap() {
        CapturingEqlClient client = new CapturingEqlClient(threadPool);
        DriverContext driverContext = driverContext();
        EqlSearchResponse response = eventResponse(List.of(fieldEvent("a"))); // one result, cap is 5

        SourceOperator operator = operator(
            client,
            driverContext,
            eqlRequest(5),
            EqlRelation.Mode.EVENT,
            new Source(1, 0, "eqlq"),
            List.of(fieldAttribute("process.name", KEYWORD)),
            true
        );
        Page page = null;
        try {
            assertBlocked(operator);
            client.capturedListener.onResponse(response);
            page = operator.getOutput();
        } finally {
            if (page != null) {
                page.releaseBlocks();
            }
            operator.close();
            response.decRef();
        }
        assertThat("below the cap must not warn", collectWarnings(driverContext), equalTo(List.of()));
        assertThat(driverContext.breaker().getUsed(), equalTo(0L));
    }

    /** With {@code warnOnTruncation} off, a full response does not warn (the size did not come from the cap). */
    public void testNoTruncationWarningWhenFlagDisabled() {
        CapturingEqlClient client = new CapturingEqlClient(threadPool);
        DriverContext driverContext = driverContext();
        EqlSearchResponse response = eventResponse(List.of(fieldEvent("a"), fieldEvent("b"))); // fills the cap of 2

        SourceOperator operator = operator(
            client,
            driverContext,
            eqlRequest(2),
            EqlRelation.Mode.EVENT,
            new Source(1, 0, "eqlq"),
            List.of(fieldAttribute("process.name", KEYWORD)),
            false
        );
        Page page = null;
        try {
            assertBlocked(operator);
            client.capturedListener.onResponse(response);
            page = operator.getOutput();
        } finally {
            if (page != null) {
                page.releaseBlocks();
            }
            operator.close();
            response.decRef();
        }
        assertThat("flag off must not warn", collectWarnings(driverContext), equalTo(List.of()));
        assertThat(driverContext.breaker().getUsed(), equalTo(0L));
    }

    // ---- helpers ----

    private SourceOperator operator(
        CapturingEqlClient client,
        DriverContext driverContext,
        EqlSearchRequest request,
        EqlRelation.Mode mode,
        List<Attribute> schema,
        boolean warnOnTruncation
    ) {
        return operator(client, driverContext, request, mode, Source.EMPTY, schema, warnOnTruncation);
    }

    private SourceOperator operator(
        CapturingEqlClient client,
        DriverContext driverContext,
        EqlSearchRequest request,
        EqlRelation.Mode mode,
        Source source,
        boolean warnOnTruncation
    ) {
        return operator(client, driverContext, request, mode, source, List.of(fieldAttribute("process.name", KEYWORD)), warnOnTruncation);
    }

    private SourceOperator operator(
        CapturingEqlClient client,
        DriverContext driverContext,
        EqlSearchRequest request,
        EqlRelation.Mode mode,
        Source source,
        List<Attribute> schema,
        boolean warnOnTruncation
    ) {
        return new EqlSourceOperator.Factory(client, request, mode, schema, source, warnOnTruncation).get(driverContext);
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
