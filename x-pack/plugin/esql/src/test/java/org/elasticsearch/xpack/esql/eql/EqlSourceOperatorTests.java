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
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
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
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.NoSeedNodeLeftException;
import org.elasticsearch.transport.NoSuchRemoteClusterException;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.transport.RemoteTransportException;
import org.elasticsearch.xpack.eql.EqlClientException;
import org.elasticsearch.xpack.eql.action.EqlSearchAction;
import org.elasticsearch.xpack.eql.action.EqlSearchRequest;
import org.elasticsearch.xpack.eql.action.EqlSearchResponse;
import org.elasticsearch.xpack.eql.action.EqlSearchResponse.Event;
import org.elasticsearch.xpack.eql.action.EqlSearchResponse.Hits;
import org.elasticsearch.xpack.eql.action.EqlSearchResponse.Sequence;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.action.EsqlExecutionInfo;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.plan.logical.EqlRelation;
import org.elasticsearch.xpack.ql.InvalidArgumentException;
import org.elasticsearch.xpack.ql.ParsingException;
import org.elasticsearch.xpack.ql.common.Failure;
import org.elasticsearch.xpack.ql.expression.UnresolvedAttribute;
import org.hamcrest.Matcher;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.regex.MatchResult;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.fieldAttribute;
import static org.elasticsearch.xpack.esql.core.type.DataType.INTEGER;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.elasticsearch.xpack.esql.core.type.DataType.LONG;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

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

    private static final String LOCAL = RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY;
    private static final Pattern KIBANA_MARKER = Pattern.compile("line \\d+:\\d+:");
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
            Set.of(LOCAL),
            EqlRelation.Mode.EVENT,
            List.of(fieldAttribute("process.name", KEYWORD)),
            Source.EMPTY,
            false,
            executionInfo(LOCAL)
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
            Set.of(LOCAL),
            EqlRelation.Mode.EVENT,
            List.of(fieldAttribute("process.name", KEYWORD)),
            Source.EMPTY,
            false,
            executionInfo(LOCAL)
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
            ElasticsearchException failure = new ElasticsearchException("eql boom");
            client.capturedListener.onFailure(failure);

            assertNotBlocked(operator);
            RuntimeException e = expectThrows(RuntimeException.class, operator::getOutput);
            assertThat(e, sameInstance(failure));
        } finally {
            operator.close();
        }
        // No page was ever built on the failure path, so nothing to release.
        assertThat(driverContext.breaker().getUsed(), equalTo(0L));
    }

    public void testEqlFailureIsLocatedInTheEsqlQueryAndItsPositionsInTheEqlQuery() {
        assertLocatedFailure(
            new Source(2, 4, "eqlq"),
            new org.elasticsearch.xpack.eql.analysis.VerificationException(
                List.of(eqlProblem(1, 14, "Unknown column [a]"), eqlProblem(3, 0, "Unknown column [b]"), eqlProblem(-1, -2, "no position"))
            ),
            """
                line 2:5: EQL query failed with 3 problems:
                    line 1:15 of the EQL query: Unknown column [a]
                    line 3:1 of the EQL query: Unknown column [b]
                    no position"""
        );
    }

    public void testEqlParserFailureAtRunTimeIsLocatedInTheEqlQuery() {
        assertLocatedFailure(
            new Source(1, 0, "eqlq"),
            new org.elasticsearch.xpack.eql.parser.ParsingException("mismatched input", null, 1, 14),
            "line 1:1: EQL query failed: line 1:15 of the EQL query: mismatched input"
        );
    }

    public void testPositionsInAnEqlMessageAreRelabelled() {
        assertLocatedFailure(
            new Source(1, 0, "eqlq"),
            new EqlClientException("Found 2 problems\nline 1:15: Unknown column [a]\nline 1:30: Unknown column [b]"),
            """
                line 1:1: EQL query failed with 2 problems:
                    line 1:15 of the EQL query: Unknown column [a]
                    line 1:30 of the EQL query: Unknown column [b]"""
        );
    }

    public void testEqlFailureWithoutProblemsKeepsItsMessage() {
        assertLocatedFailure(
            new Source(1, 0, "eqlq"),
            new EqlClientException("unsupported option [{}]", "x"),
            "line 1:1: EQL query failed: unsupported option [x]"
        );
    }

    public void testEqlEngineParsingFailureIsLocatedInTheEqlQuery() {
        assertLocatedFailure(
            new Source(1, 0, "eqlq"),
            new ParsingException("error building [between]: expects three or four arguments", null, 1, 14),
            "line 1:1: EQL query failed: line 1:15 of the EQL query: error building [between]: expects three or four arguments"
        );
    }

    public void testEqlFailureWrappedInATransportExceptionIsLocated() {
        EqlClientException eqlFailure = new EqlClientException("unsupported option [x]");
        assertLocatedFailure(
            new Source(1, 0, "eqlq"),
            new RemoteTransportException("remote failure", eqlFailure),
            eqlFailure,
            "line 1:1: EQL query failed: unsupported option [x]"
        );
    }

    public void testEqlFailureFromARemoteClusterIsLocated() {
        Exception remoteFailure = fromRemote(
            new org.elasticsearch.xpack.eql.analysis.VerificationException(List.of(eqlProblem(1, 14, "Unknown column [a]")))
        );
        assertLocatedFailure(
            new Source(1, 0, "eqlq"),
            remoteFailure,
            remoteFailure.getCause(),
            "line 1:1: EQL query failed: line 1:15 of the EQL query: Unknown column [a]"
        );
    }

    public void testEqlFailureWithSeveralProblemsFromARemoteClusterIsLocated() {
        Exception remoteFailure = fromRemote(
            new org.elasticsearch.xpack.eql.analysis.VerificationException(
                List.of(eqlProblem(1, 14, "Unknown column [a]"), eqlProblem(2, 0, "Unknown column [b]"))
            )
        );
        assertLocatedFailure(new Source(1, 0, "eqlq"), remoteFailure, remoteFailure.getCause(), """
            line 1:1: EQL query failed with 2 problems:
                line 1:15 of the EQL query: Unknown column [a]
                line 2:1 of the EQL query: Unknown column [b]""");
    }

    public void testRemoteFailureThatIsNotAClientErrorIsPassedThrough() {
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
            Exception remoteFailure = fromRemote(new ElasticsearchException("line 1:15: remote node failed"));
            client.capturedListener.onFailure(remoteFailure);
            assertThat(expectThrows(RuntimeException.class, operator::getOutput), sameInstance(remoteFailure));
        } finally {
            operator.close();
        }
    }

    public void testEveryLocatedFailureKeepsTheEsqlPositionAsTheOnlyMarker() {
        List<Exception> failures = List.of(
            new org.elasticsearch.xpack.eql.analysis.VerificationException(
                List.of(eqlProblem(1, 14, "Unknown column [a]"), eqlProblem(2, 0, "Unknown column [b]"))
            ),
            new org.elasticsearch.xpack.eql.parser.ParsingException("mismatched input", null, 1, 14),
            new ParsingException("error building [between]", null, 1, 14),
            new EqlClientException("line 1:15: Unknown column [a]"),
            new EqlClientException("Unknown column [a] near line 2:3: x"),
            new org.elasticsearch.xpack.eql.analysis.VerificationException(List.of(eqlProblem(1, 14, "see line 2:3: x"))),
            new InvalidArgumentException("Invalid sequence - escape character is not followed by special wildcard char"),
            fromRemote(new org.elasticsearch.xpack.eql.analysis.VerificationException(List.of(eqlProblem(1, 14, "Unknown column [a]")))),
            fromRemote(new org.elasticsearch.xpack.eql.parser.ParsingException("mismatched input", null, 1, 14))
        );
        for (Exception failure : failures) {
            String message = locate(randomFrom(new Source(1, 0, "eqlq"), new Source(3, 6, "eqlq")), failure).getMessage();
            List<Integer> markers = KIBANA_MARKER.matcher(message).results().map(MatchResult::start).toList();
            assertThat(message, markers, equalTo(List.of(0)));
        }
    }

    public void testAMultiLineProblemStaysOneProblemAndAMissingPositionIsDropped() {
        String eqlText = "Found 2 problems\nline 1:15: first argument of [length(\n 1)] must be [string]\nline -1:-1: no position";
        String expected = "line 1:1: EQL query failed with 2 problems:\n"
            + "    line 1:15 of the EQL query: first argument of [length(\n"
            + "     1)] must be [string]\n"
            + "    no position";
        assertLocatedFailure(new Source(1, 0, "eqlq"), new EqlClientException(eqlText), expected);
        Exception remoteFailure = fromRemote(new EqlClientException(eqlText));
        assertLocatedFailure(new Source(1, 0, "eqlq"), remoteFailure, remoteFailure.getCause(), expected);
    }

    private static Exception fromRemote(Exception eqlFailure) {
        return new RemoteTransportException("remote failure", new NotSerializableExceptionWrapper(eqlFailure));
    }

    private void assertLocatedFailure(Source source, Exception eqlFailure, String expectedMessage) {
        assertLocatedFailure(source, eqlFailure, eqlFailure, expectedMessage);
    }

    private void assertLocatedFailure(Source source, Exception delivered, Throwable eqlFailure, String expectedMessage) {
        VerificationException e = locate(source, delivered);
        assertThat(e.getMessage(), equalTo(expectedMessage));
        assertThat(e.getSuppressed(), arrayContaining(sameInstance(eqlFailure)));
    }

    private VerificationException locate(Source source, Exception delivered) {
        CapturingEqlClient client = new CapturingEqlClient(threadPool);
        DriverContext driverContext = driverContext();
        SourceOperator operator = new EqlSourceOperator.Factory(
            client,
            eqlRequest(1000),
            EqlRelation.Mode.EVENT,
            List.of(fieldAttribute("process.name", KEYWORD)),
            source,
            false,
            executionInfo(LOCAL)
        ).get(driverContext);
        VerificationException e;
        try {
            assertBlocked(operator);
            client.capturedListener.onFailure(delivered);
            e = expectThrows(VerificationException.class, operator::getOutput);
        } finally {
            operator.close();
        }
        assertThat(driverContext.breaker().getUsed(), equalTo(0L));
        assertThat(e.getCause(), nullValue());
        assertThat(e.status(), equalTo(RestStatus.BAD_REQUEST));
        return e;
    }

    private static Failure eqlProblem(int line, int charPositionInLine, String message) {
        return new Failure(
            new UnresolvedAttribute(new org.elasticsearch.xpack.ql.tree.Source(line, charPositionInLine, "x"), "x"),
            message
        );
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

    public void testLostShardIsListedOnItsCluster() {
        EsqlExecutionInfo executionInfo = executionInfo(LOCAL);
        ShardSearchFailure lost = shardFailure(new IllegalStateException("shard failure"), 0, "n1", null);
        DriverContext driverContext = pullEventPage(executionInfo, eqlRequestOn("logs"), false, lost);
        assertThat(collectWarnings(driverContext), empty());
        assertTrue(executionInfo.isPartial());
        EsqlExecutionInfo.Cluster local = executionInfo.getCluster(LOCAL);
        assertThat(local.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.PARTIAL));
        assertThat(local.getFailures(), equalTo(List.of(lost)));
    }

    public void testTimedOutResponseIsPartial() {
        EsqlExecutionInfo executionInfo = executionInfo(LOCAL);
        DriverContext driverContext = pullEventPage(executionInfo, eqlRequestOn("logs"), true);
        assertThat(collectWarnings(driverContext), empty());
        assertTrue(executionInfo.isPartial());
        assertThat(executionInfo.getCluster(LOCAL).getFailures(), empty());
    }

    public void testCompleteResponseIsNotPartial() {
        EsqlExecutionInfo executionInfo = executionInfo(LOCAL);
        DriverContext driverContext = pullEventPage(executionInfo, eqlRequestOn("logs"), false);
        assertThat(collectWarnings(driverContext), empty());
        assertFalse(executionInfo.isPartial());
        assertThat(executionInfo.getCluster(LOCAL).getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.RUNNING));
        assertThat(executionInfo.getCluster(LOCAL).getFailures(), empty());
    }

    public void testLostShardsAreListedOnceUpToALimit() {
        EsqlExecutionInfo executionInfo = executionInfo(LOCAL);
        ShardSearchFailure listedBefore = shardFailure(new IllegalStateException("earlier branch"), 0, "n1", null);
        executionInfo.swapCluster(
            LOCAL,
            (alias, cluster) -> new EsqlExecutionInfo.Cluster.Builder(cluster).setStatus(EsqlExecutionInfo.Cluster.Status.SUCCESSFUL)
                .addFailures(List.of(listedBefore))
                .build()
        );
        IllegalStateException noShard = new IllegalStateException("no shard");
        List<ShardSearchFailure> failures = new ArrayList<>();
        failures.add(shardFailure(new IllegalStateException("again"), 0, "n2", null));
        failures.add(shardFailure(new IllegalStateException("first"), 1, "n1", null));
        failures.add(shardFailure(new IllegalStateException("other copy"), 1, "n2", null));
        failures.add(new ShardSearchFailure(noShard));
        failures.add(new ShardSearchFailure(noShard));
        for (int shard = 2; shard < 6; shard++) {
            failures.add(shardFailure(new IllegalStateException("shard " + shard), shard, "n1", null));
        }
        pullEventPage(executionInfo, eqlRequestOn("logs"), false, failures.toArray(ShardSearchFailure[]::new));
        EsqlExecutionInfo.Cluster local = executionInfo.getCluster(LOCAL);
        assertThat(local.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.PARTIAL));
        assertThat(
            local.getFailures(),
            equalTo(List.of(listedBefore, failures.get(1), failures.get(3), failures.get(5), failures.get(6), failures.get(7)))
        );
    }

    public void testLostShardsAreListedOnTheClusterTheyBelongTo() {
        EsqlExecutionInfo executionInfo = executionInfo(LOCAL, "remote1");
        ShardSearchFailure remote = shardFailure(new IllegalStateException("remote"), 0, "n1", "remote1");
        ShardSearchFailure local = shardFailure(new IllegalStateException("local"), 0, "n1", null);
        pullEventPage(executionInfo, eqlRequestOn("logs", "remote1:logs"), false, remote, local);
        assertThat(executionInfo.getCluster("remote1").getFailures(), equalTo(List.of(remote)));
        assertThat(executionInfo.getCluster(LOCAL).getFailures(), equalTo(List.of(local)));
        assertThat(executionInfo.getCluster("remote1").getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.PARTIAL));
        assertThat(executionInfo.getCluster(LOCAL).getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.PARTIAL));
    }

    public void testLostShardsOfASingleRemoteClusterSearchAreListedOnThatCluster() {
        for (String[] indices : List.of(new String[] { "remote1:logs", "remote1:other" }, new String[] { "remote*:logs" })) {
            EsqlExecutionInfo executionInfo = executionInfo(LOCAL, "remote1");
            ShardSearchFailure lost = shardFailure(new IllegalStateException("lost"), 0, "n1", null);
            pullEventPage(executionInfo, eqlRequestOn(indices), false, lost);
            assertThat(String.join(",", indices), executionInfo.getCluster("remote1").getFailures(), equalTo(List.of(lost)));
            assertThat(String.join(",", indices), executionInfo.getCluster(LOCAL).getFailures(), empty());
        }
    }

    public void testLostShardInASubplanLeavesARemoteClusterStatusToTheMainPlan() {
        EsqlExecutionInfo executionInfo = executionInfo(LOCAL, "remote1");
        executionInfo.startSubPlans(true);
        ShardSearchFailure remote = shardFailure(new IllegalStateException("remote"), 0, "n1", "remote1");
        ShardSearchFailure local = shardFailure(new IllegalStateException("local"), 0, "n1", null);
        pullEventPage(executionInfo, eqlRequestOn("logs", "remote1:logs"), false, remote, local);
        assertTrue(executionInfo.isPartial());
        assertThat(executionInfo.getCluster("remote1").getFailures(), equalTo(List.of(remote)));
        assertThat(executionInfo.getCluster("remote1").getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.RUNNING));
        assertThat(executionInfo.getCluster(LOCAL).getFailures(), equalTo(List.of(local)));
        assertThat(executionInfo.getCluster(LOCAL).getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.PARTIAL));
    }

    public void testLostShardOfAnUnlistedClusterOnlyMarksTheResultPartial() {
        EsqlExecutionInfo executionInfo = executionInfo(LOCAL);
        pullEventPage(executionInfo, eqlRequestOn("logs"), false, shardFailure(new IllegalStateException("x"), 0, "n1", "remote9"));
        assertTrue(executionInfo.isPartial());
        assertThat(executionInfo.getCluster(LOCAL).getFailures(), empty());
        assertThat(executionInfo.getCluster("remote9"), nullValue());
    }

    public void testNothingIsSearchedWithoutARequest() {
        CapturingEqlClient client = new CapturingEqlClient(threadPool);
        DriverContext driverContext = driverContext();
        SourceOperator operator = operator(client, driverContext, null, EqlRelation.Mode.EVENT, keywordSchema(), executionInfo(LOCAL));
        try {
            assertNotBlocked(operator);
            assertThat(operator.getOutput(), nullValue());
            assertTrue(operator.isFinished());
            assertThat(client.executions, equalTo(0));
        } finally {
            operator.close();
        }
    }

    public void testClustersTheSearchSkippedAreMarkedSkipped() {
        EsqlExecutionInfo executionInfo = executionInfo(LOCAL, "remote1", "remote2");
        ShardSearchFailure unavailable = new ShardSearchFailure(new NoSuchRemoteClusterException("remote1"));
        Map<String, List<ShardSearchFailure>> skipped = Map.of(
            "remote1",
            List.of(unavailable),
            LOCAL,
            List.of(new ShardSearchFailure(new IllegalStateException("local"))),
            "remote9",
            List.of(new ShardSearchFailure(new IllegalStateException("remote9")))
        );
        pullEventPage(executionInfo, eqlRequestOn("logs", "remote*:logs"), false, skipped);
        assertTrue(executionInfo.isPartial());
        EsqlExecutionInfo.Cluster remote1 = executionInfo.getCluster("remote1");
        assertThat(remote1.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SKIPPED));
        assertThat(remote1.getFailures(), equalTo(List.of(unavailable)));
        assertThat(remote1.getTotalShards(), equalTo(0));
        assertThat(executionInfo.getCluster("remote2").getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.RUNNING));
        assertThat(executionInfo.getCluster(LOCAL).getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.RUNNING));
        assertThat(executionInfo.getCluster(LOCAL).getFailures(), empty());
        assertThat(executionInfo.getCluster("remote9"), nullValue());
    }

    public void testClusterSkippedAfterItReturnedEventsIsPartial() {
        EsqlExecutionInfo executionInfo = executionInfo(LOCAL, "remote1");
        CapturingEqlClient client = new CapturingEqlClient(threadPool);
        DriverContext driverContext = driverContext();
        Event remoteEvent = envelopeEvent("remote1:logs", "1", null, Map.of("process.name", List.of("a")));
        EqlSearchResponse response = eventResponse(List.of(remoteEvent));
        ShardSearchFailure lost = new ShardSearchFailure(new NoSuchRemoteClusterException("remote1"));
        response.setSkippedClusters(Map.of("remote1", List.of(lost)));
        SourceOperator operator = operator(
            client,
            driverContext,
            eqlRequestOn("logs", "remote1:logs"),
            EqlRelation.Mode.EVENT,
            keywordSchema(),
            executionInfo
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
        assertThat(executionInfo.getCluster("remote1").getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.PARTIAL));
        assertThat(executionInfo.getCluster("remote1").getFailures(), equalTo(List.of(lost)));
    }

    public void testFailuresOfASkippedClusterAreListedUpToALimit() {
        EsqlExecutionInfo executionInfo = executionInfo(LOCAL, "remote1");
        List<ShardSearchFailure> failures = new ArrayList<>();
        for (int i = 0; i < 7; i++) {
            failures.add(new ShardSearchFailure(new IllegalStateException("failure " + i)));
        }
        pullEventPage(executionInfo, eqlRequestOn("logs", "remote1:logs"), false, Map.of("remote1", failures));
        assertThat(executionInfo.getCluster("remote1").getFailures(), equalTo(failures.subList(0, 5)));
    }

    public void testClusterSkippedOutsideTheExecutionInfoOnlyMarksTheResultPartial() {
        EsqlExecutionInfo executionInfo = executionInfo(LOCAL);
        ShardSearchFailure lost = new ShardSearchFailure(new NoSuchRemoteClusterException("remote9"));
        pullEventPage(executionInfo, eqlRequestOn("logs"), false, Map.of("remote9", List.of(lost)));
        assertTrue(executionInfo.isPartial());
        assertThat(executionInfo.getCluster("remote9"), nullValue());
        assertThat(executionInfo.getCluster(LOCAL).getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.RUNNING));
    }

    public void testUnavailableRemoteClustersAreSkippedWhenSkipUnavailableAllowsIt() {
        NoSuchRemoteClusterException noSuchCluster = new NoSuchRemoteClusterException("remote1");
        List<Exception> unavailable = List.of(
            noSuchCluster,
            new RemoteTransportException("remote", noSuchCluster),
            new ConnectTransportException(DiscoveryNodeUtils.create("n1"), "unreachable"),
            new NoSeedNodeLeftException("no seed node left"),
            new IllegalStateException("Unable to open any connections to remote cluster [remote1]")
        );
        for (Exception failure : unavailable) {
            EsqlExecutionInfo executionInfo = executionInfo(
                alias -> alias.equals("remote3") == false,
                LOCAL,
                "remote1",
                "remote2",
                "remote3"
            );
            CapturingEqlClient client = new CapturingEqlClient(threadPool);
            DriverContext driverContext = driverContext();
            SourceOperator operator = operator(
                client,
                driverContext,
                eqlRequestOn("remote*:logs", "-remote3:*"),
                EqlRelation.Mode.EVENT,
                keywordSchema(),
                executionInfo
            );
            try {
                assertBlocked(operator);
                client.capturedListener.onFailure(failure);
                assertNotBlocked(operator);
                assertThat(operator.getOutput(), nullValue());
                assertTrue(operator.isFinished());
            } finally {
                operator.close();
            }
            assertTrue(executionInfo.isPartial());
            for (String remote : List.of("remote1", "remote2")) {
                EsqlExecutionInfo.Cluster cluster = executionInfo.getCluster(remote);
                assertThat(cluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SKIPPED));
                assertThat(cluster.getFailures(), hasSize(1));
                assertThat(cluster.getFailures().get(0).getCause(), sameInstance(ExceptionsHelper.unwrapCause(failure)));
            }
            assertThat(executionInfo.getCluster("remote3").getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.RUNNING));
            assertThat(executionInfo.getCluster(LOCAL).getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.RUNNING));
        }
    }

    public void testLocalClusterSkippedWhilePlanningDoesNotStopTheSkip() {
        EsqlExecutionInfo executionInfo = executionInfo(alias -> true, LOCAL, "remote1");
        executionInfo.swapCluster(
            LOCAL,
            (alias, info) -> new EsqlExecutionInfo.Cluster.Builder(info).setStatus(EsqlExecutionInfo.Cluster.Status.SKIPPED).build()
        );
        CapturingEqlClient client = new CapturingEqlClient(threadPool);
        DriverContext driverContext = driverContext();
        SourceOperator operator = operator(
            client,
            driverContext,
            eqlRequestOn("red-local*", "remote1:logs"),
            EqlRelation.Mode.EVENT,
            keywordSchema(),
            executionInfo
        );
        try {
            assertBlocked(operator);
            client.capturedListener.onFailure(new NoSuchRemoteClusterException("remote1"));
            assertThat(operator.getOutput(), nullValue());
            assertTrue(operator.isFinished());
        } finally {
            operator.close();
        }
        assertThat(executionInfo.getCluster("remote1").getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SKIPPED));
        assertThat(executionInfo.getCluster(LOCAL).getFailures(), empty());
    }

    public void testSearchFailureIsRaisedWhenCheckingTheClustersFails() {
        NoSuchRemoteClusterException removed = new NoSuchRemoteClusterException("remote1");
        EsqlExecutionInfo executionInfo = executionInfo(alias -> { throw removed; }, LOCAL, "remote1");
        NoSuchRemoteClusterException unavailable = new NoSuchRemoteClusterException("remote1");
        CapturingEqlClient client = new CapturingEqlClient(threadPool);
        DriverContext driverContext = driverContext();
        SourceOperator operator = operator(
            client,
            driverContext,
            eqlRequestOn("remote1:logs"),
            EqlRelation.Mode.EVENT,
            keywordSchema(),
            executionInfo
        );
        try {
            assertBlocked(operator);
            client.capturedListener.onFailure(unavailable);
            assertNotBlocked(operator);
            RuntimeException e = expectThrows(RuntimeException.class, operator::getOutput);
            assertThat(e, sameInstance(unavailable));
            assertThat(e.getSuppressed(), arrayContaining((Throwable) removed));
        } finally {
            operator.close();
        }
        assertThat(executionInfo.getCluster("remote1").getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.RUNNING));
    }

    public void testSearchFailureIsRaisedWhenTheClustersCannotBeSkipped() {
        record Case(Predicate<String> skippable, Exception failure, String... indices) {}
        List<Case> cases = List.of(
            new Case(alias -> false, new NoSuchRemoteClusterException("remote1"), "remote1:logs"),
            new Case(alias -> alias.equals("remote1"), new NoSuchRemoteClusterException("remote2"), "remote1:logs", "remote2:logs"),
            new Case(alias -> true, new NoSuchRemoteClusterException("remote1"), "logs", "remote1:logs"),
            new Case(alias -> true, new ElasticsearchException("not about the cluster"), "remote1:logs"),
            new Case(
                alias -> true,
                new ElasticsearchException("lost a shard", new NoSuchRemoteClusterException("remote1")),
                "remote1:logs"
            ),
            new Case(alias -> true, new NoSuchRemoteClusterException("remote9"), "remote9:logs")
        );
        for (Case c : cases) {
            EsqlExecutionInfo executionInfo = executionInfo(c.skippable(), LOCAL, "remote1", "remote2");
            CapturingEqlClient client = new CapturingEqlClient(threadPool);
            DriverContext driverContext = driverContext();
            SourceOperator operator = operator(
                client,
                driverContext,
                eqlRequestOn(c.indices()),
                EqlRelation.Mode.EVENT,
                keywordSchema(),
                executionInfo
            );
            try {
                assertBlocked(operator);
                client.capturedListener.onFailure(c.failure());
                RuntimeException e = expectThrows(RuntimeException.class, operator::getOutput);
                assertThat(String.join(",", c.indices()), e.getMessage(), equalTo(c.failure().getMessage()));
            } finally {
                operator.close();
            }
            for (String cluster : List.of(LOCAL, "remote1", "remote2")) {
                assertThat(executionInfo.getCluster(cluster).getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.RUNNING));
            }
        }
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
            Set.of(LOCAL),
            EqlRelation.Mode.EVENT,
            List.of(fieldAttribute("process.name", KEYWORD)),
            source,
            true,
            executionInfo(LOCAL)
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
        return operator(client, driverContext, request, mode, schema, executionInfo(LOCAL));
    }

    private SourceOperator operator(
        CapturingEqlClient client,
        DriverContext driverContext,
        EqlSearchRequest request,
        EqlRelation.Mode mode,
        List<Attribute> schema,
        EsqlExecutionInfo executionInfo
    ) {
        Set<String> searched = request == null
            ? Set.of()
            : EqlRequests.searchedClusters(request.indices(), executionInfo.clusterAliases(), false);
        return new EqlSourceOperator.Factory(client, request, searched, mode, schema, Source.EMPTY, false, executionInfo).get(
            driverContext
        );
    }

    private static List<Attribute> keywordSchema() {
        return List.of(fieldAttribute("process.name", KEYWORD));
    }

    private static EsqlExecutionInfo executionInfo(String... clusters) {
        return executionInfo(alias -> false, clusters);
    }

    private static EsqlExecutionInfo executionInfo(Predicate<String> skipUnavailable, String... clusters) {
        EsqlExecutionInfo executionInfo = new EsqlExecutionInfo(skipUnavailable, EsqlExecutionInfo.IncludeExecutionMetadata.CCS_ONLY);
        for (String cluster : clusters) {
            executionInfo.initCluster(cluster, EsqlExecutionInfo.LOCAL_CLUSTER_NAME_REPRESENTATION, "logs");
        }
        return executionInfo;
    }

    private static ShardSearchFailure shardFailure(Exception cause, int shard, String node, String clusterAlias) {
        return new ShardSearchFailure(cause, new SearchShardTarget(node, new ShardId("logs", "_na_", shard), clusterAlias));
    }

    private DriverContext pullEventPage(
        EsqlExecutionInfo executionInfo,
        EqlSearchRequest request,
        boolean timedOut,
        ShardSearchFailure... failures
    ) {
        return pullEventPage(executionInfo, request, timedOut, Map.of(), failures);
    }

    private DriverContext pullEventPage(
        EsqlExecutionInfo executionInfo,
        EqlSearchRequest request,
        boolean timedOut,
        Map<String, List<ShardSearchFailure>> skippedClusters,
        ShardSearchFailure... failures
    ) {
        CapturingEqlClient client = new CapturingEqlClient(threadPool);
        DriverContext driverContext = driverContext();
        Hits hits = new Hits(List.of(fieldEvent("a")), null, new TotalHits(1, TotalHits.Relation.EQUAL_TO));
        EqlSearchResponse response = new EqlSearchResponse(hits, 1, timedOut, List.of(failures));
        response.setSkippedClusters(skippedClusters);
        SourceOperator operator = operator(client, driverContext, request, EqlRelation.Mode.EVENT, keywordSchema(), executionInfo);
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

    private static EqlSearchRequest eqlRequestOn(String... indices) {
        return new EqlSearchRequest().indices(indices).query("any where true").size(1000);
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
        return new EqlSearchResponse(hits, 1, false, List.of());
    }

    private static EqlSearchResponse sequenceResponse(List<Sequence> sequences) {
        Hits hits = new Hits(null, sequences, new TotalHits(sequences.size(), TotalHits.Relation.EQUAL_TO));
        return new EqlSearchResponse(hits, 1, false, List.of());
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
