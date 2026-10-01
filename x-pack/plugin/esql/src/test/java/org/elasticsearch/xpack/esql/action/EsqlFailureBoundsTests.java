/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.apache.logging.log4j.Level;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.compute.operator.FailureCollector;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.transport.RemoteTransportException;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;

public class EsqlFailureBoundsTests extends ESTestCase {

    private static final int RENDER_LIMIT_BYTES = 64 * 1024;
    private static final String QUERY = "FROM hits | WHERE URL LIKE \"*google*\" | SORT EventTime ASC | LIMIT 10";
    private static final String READER_MESSAGE = "[parent] Data too large, data for [parquet reader] would be [1041876704/993.6mb]";
    private static final String WINDOW_MESSAGE = "[parent] Data too large, data for [parquet sliding window] would be [1042582912/994.2mb]";

    /**
     * The graph left in the heap dump of elastic/esql-planning#2135: A suppresses B twice and B suppresses A. Rendering it
     * unbounded writes one level per distinct path and does not finish, so the test goes through {@link EsqlFailureBounds#bound}.
     */
    public void testACyclicFailureIsRebuiltAndRendersSmall() throws IOException {
        CircuitBreakingException a = new CircuitBreakingException(
            READER_MESSAGE,
            1041876704,
            1020054732,
            CircuitBreaker.Durability.TRANSIENT
        );
        CircuitBreakingException b = new CircuitBreakingException(
            WINDOW_MESSAGE,
            1042582912,
            1020054732,
            CircuitBreaker.Durability.TRANSIENT
        );
        a.addSuppressed(b);
        a.addSuppressed(b);
        b.addSuppressed(a);

        AtomicReference<Exception> result = new AtomicReference<>();
        MockLog.assertThatLogger(
            () -> result.set(EsqlFailureBounds.bound(a, QUERY)),
            EsqlFailureBounds.class,
            new MockLog.SeenEventExpectation(
                "rebuild warning",
                EsqlFailureBounds.class.getCanonicalName(),
                Level.WARN,
                "query ["
                    + QUERY
                    + "] failed with an exception graph that loops or renders more than ["
                    + EsqlFailureBounds.MAX_RENDERED_ENTRIES
                    + "] entries; rebuilding it from [2] distinct exceptions"
            )
        );
        Exception bounded = result.get();

        assertThat(bounded, not(sameInstance(a)));
        assertNoRepeats(bounded);
        assertReferencesNone(bounded, a, b);
        assertThat(ExceptionsHelper.status(bounded), equalTo(RestStatus.TOO_MANY_REQUESTS));
        assertThat(bounded, instanceOf(CircuitBreakingException.class));
        CircuitBreakingException cbe = (CircuitBreakingException) bounded;
        assertThat(cbe.getMessage(), equalTo(READER_MESSAGE));
        assertThat(cbe.getBytesWanted(), equalTo(a.getBytesWanted()));
        assertThat(cbe.getByteLimit(), equalTo(a.getByteLimit()));
        assertThat(cbe.getDurability(), equalTo(a.getDurability()));
        assertArrayEquals(a.getStackTrace(), cbe.getStackTrace());
        assertThat(bounded.getSuppressed(), arrayWithSize(1));
        assertThat(bounded.getSuppressed()[0].getMessage(), equalTo(WINDOW_MESSAGE));

        String rendered = renderCapped(bounded);
        assertThat(rendered, containsString(READER_MESSAGE));
        assertThat(rendered, containsString(WINDOW_MESSAGE));
        Map<String, Object> body = XContentHelper.convertToMap(XContentType.JSON.xContent(), rendered, false);
        @SuppressWarnings("unchecked")
        Map<String, Object> error = (Map<String, Object>) body.get("error");
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> rootCauses = (List<Map<String, Object>>) error.get("root_cause");
        assertThat(rootCauses.get(0).get("type"), equalTo("circuit_breaking_exception"));
        assertThat(error.get("type"), equalTo("circuit_breaking_exception"));
    }

    public void testAnOrdinaryFailureIsReturnedUnchanged() {
        IllegalArgumentException cause = new IllegalArgumentException("cause");
        ElasticsearchException failure = new ElasticsearchException("top", cause);
        failure.addSuppressed(new IllegalStateException("one"));
        failure.addSuppressed(new IllegalStateException("two"));

        AtomicReference<Exception> result = new AtomicReference<>();
        MockLog.assertThatLogger(
            () -> result.set(EsqlFailureBounds.bound(failure, QUERY)),
            EsqlFailureBounds.class,
            new MockLog.UnseenEventExpectation("no warning", EsqlFailureBounds.class.getCanonicalName(), Level.WARN, "*")
        );
        assertThat(result.get(), sameInstance(failure));
    }

    /**
     * A failure of any type other than {@link CircuitBreakingException} becomes a named exception with the original's
     * status, so a {@code 400} stays a {@code 400} and the rendered type is not empty.
     */
    public void testANonBreakerFailureKeepsItsStatus() throws IOException {
        IllegalArgumentException a = new IllegalArgumentException("bad input");
        IllegalStateException b = new IllegalStateException("broken");
        a.addSuppressed(b);
        b.addSuppressed(a);

        Exception bounded = EsqlFailureBounds.bound(a, QUERY);

        assertNoRepeats(bounded);
        assertReferencesNone(bounded, a, b);
        assertThat(bounded, instanceOf(EsqlFailureBounds.BoundedFailureException.class));
        assertThat(ExceptionsHelper.status(bounded), equalTo(RestStatus.BAD_REQUEST));
        assertThat(bounded.getMessage(), equalTo("[illegal_argument_exception] bad input"));
        assertArrayEquals(a.getStackTrace(), bounded.getStackTrace());
        assertThat(bounded.getSuppressed(), arrayWithSize(1));
        assertThat(ExceptionsHelper.status(bounded.getSuppressed()[0]), equalTo(RestStatus.INTERNAL_SERVER_ERROR));
        assertThat(bounded.getSuppressed()[0].getMessage(), equalTo("[illegal_state_exception] broken"));
        assertThat(renderCapped(bounded), containsString("\"type\":\"bounded_failure_exception\""));
    }

    public void testALoopThroughCausesIsRebuilt() {
        RuntimeException a = new RuntimeException("a");
        RuntimeException b = new RuntimeException("b");
        a.initCause(b);
        b.initCause(a);

        Exception bounded = EsqlFailureBounds.bound(a, QUERY);

        assertNoRepeats(bounded);
        assertReferencesNone(bounded, a, b);
        assertThat(bounded.getMessage(), equalTo("[runtime_exception] a"));
        assertThat(bounded.getCause().getMessage(), equalTo("[runtime_exception] b"));
        assertNull(bounded.getCause().getCause());
        assertThat(bounded.getSuppressed(), arrayWithSize(0));
    }

    /**
     * Each waiter on a failed footer load wraps the one shared cause in its own invalid-file exception, and a
     * {@link FailureCollector} then suppresses one wrapper onto the other. The shared cause is reachable twice, but the
     * graph does not loop and renders a handful of entries: it must reach the client exactly as it is.
     */
    public void testAFailureSharingACauseIsReturnedUnchanged() {
        RuntimeException shared = new RuntimeException("malformed footer");
        FailureCollector collector = new FailureCollector();
        collector.unwrapAndCollect(new IllegalArgumentException("Could not read [s3://b/f.parquet] as a Parquet file", shared));
        collector.unwrapAndCollect(new IllegalArgumentException("Could not read [s3://b/f.parquet] as a Parquet file", shared));
        Exception failure = collector.getFailure();
        assertThat(failure.getSuppressed(), arrayWithSize(1));

        AtomicReference<Exception> result = new AtomicReference<>();
        MockLog.assertThatLogger(
            () -> result.set(EsqlFailureBounds.bound(failure, QUERY)),
            EsqlFailureBounds.class,
            new MockLog.UnseenEventExpectation("no warning", EsqlFailureBounds.class.getCanonicalName(), Level.WARN, "*")
        );
        assertThat(result.get(), sameInstance(failure));
    }

    /**
     * No loop, but each exception suppresses the next one twice, so a renderer writes the last one 2^depth times.
     */
    public void testAFailureThatRendersTooManyEntriesIsRebuilt() {
        int depth = between(11, 20);
        RuntimeException top = new RuntimeException("level-0");
        RuntimeException current = top;
        for (int i = 1; i <= depth; i++) {
            RuntimeException next = new RuntimeException("level-" + i);
            current.addSuppressed(next);
            current.addSuppressed(next);
            current = next;
        }

        Exception bounded = EsqlFailureBounds.bound(top, QUERY);

        assertThat(bounded, not(sameInstance(top)));
        assertNoRepeats(bounded);
        assertThat(bounded.getSuppressed(), arrayWithSize(EsqlFailureBounds.MAX_ADDITIONAL_FAILURES));
    }

    /**
     * A breaker failure wrapped in another exception keeps its type through the rebuild, so the rendered
     * {@code root_cause} is still {@code circuit_breaking_exception}.
     */
    public void testAWrappedBreakerFailureKeepsItsRootCause() throws IOException {
        CircuitBreakingException a = new CircuitBreakingException(READER_MESSAGE, CircuitBreaker.Durability.TRANSIENT);
        CircuitBreakingException b = new CircuitBreakingException(WINDOW_MESSAGE, CircuitBreaker.Durability.TRANSIENT);
        a.addSuppressed(b);
        b.addSuppressed(a);
        ElasticsearchException wrapper = new ElasticsearchException("wrapper", a);

        Exception bounded = EsqlFailureBounds.bound(wrapper, QUERY);

        assertNoRepeats(bounded);
        assertReferencesNone(bounded, wrapper, a, b);
        assertThat(ExceptionsHelper.status(bounded), equalTo(ExceptionsHelper.status(wrapper)));
        assertThat(bounded.getCause(), instanceOf(CircuitBreakingException.class));
        assertThat(bounded.getCause().getMessage(), equalTo(READER_MESSAGE));
        ElasticsearchException[] rootCauses = ElasticsearchException.guessRootCauses(bounded);
        assertThat(rootCauses, arrayWithSize(1));
        assertThat(rootCauses[0], instanceOf(CircuitBreakingException.class));
        assertThat(renderCapped(bounded), containsString(WINDOW_MESSAGE));
    }

    /**
     * A failure that arrives from another node is wrapped in a {@link RemoteTransportException}, which the renderer skips
     * when it reports the error. The rebuilt failure starts at the breaker failure, so the rendered {@code type} is
     * unchanged.
     */
    public void testATransportWrappedFailureKeepsItsRenderedType() throws IOException {
        CircuitBreakingException a = new CircuitBreakingException(READER_MESSAGE, CircuitBreaker.Durability.TRANSIENT);
        CircuitBreakingException b = new CircuitBreakingException(WINDOW_MESSAGE, CircuitBreaker.Durability.TRANSIENT);
        a.addSuppressed(b);
        a.addSuppressed(b);
        b.addSuppressed(a);
        RemoteTransportException wrapper = new RemoteTransportException("[node-1][indices:data/read/esql]", a);

        Exception bounded = EsqlFailureBounds.bound(wrapper, QUERY);

        assertNoRepeats(bounded);
        assertReferencesNone(bounded, wrapper, a, b);
        assertThat(bounded, instanceOf(CircuitBreakingException.class));
        assertThat(bounded.getMessage(), equalTo(READER_MESSAGE));
        assertThat(bounded.getSuppressed(), arrayWithSize(1));
        assertThat(bounded.getSuppressed()[0].getMessage(), equalTo(WINDOW_MESSAGE));
        Map<String, Object> body = XContentHelper.convertToMap(XContentType.JSON.xContent(), renderCapped(bounded), false);
        @SuppressWarnings("unchecked")
        Map<String, Object> error = (Map<String, Object>) body.get("error");
        assertThat(error.get("type"), equalTo("circuit_breaking_exception"));
    }

    /**
     * A cause chain longer than the entry limit renders more entries than the limit, and is counted without recursing
     * once per link.
     */
    public void testAVeryLongCauseChainIsRebuilt() {
        RuntimeException chain = new RuntimeException("cause-last");
        for (int i = 0; i < 100_000; i++) {
            chain = new RuntimeException("cause", chain);
        }

        Exception bounded = EsqlFailureBounds.bound(chain, QUERY);

        assertThat(bounded, not(sameInstance(chain)));
        assertNoRepeats(bounded);
    }

    /**
     * The causes of a further failure travel inside its rebuilt copy, so they must not also take slots of their own
     * and crowd out the failures that follow.
     */
    public void testCausesOfAFurtherFailureDoNotUseItsBudget() {
        RuntimeException top = new RuntimeException("top");
        RuntimeException first = new RuntimeException("first", causeChainOf(EsqlFailureBounds.MAX_CAUSE_CHAIN));
        RuntimeException second = new RuntimeException("second");
        top.addSuppressed(first);
        top.addSuppressed(second);
        second.addSuppressed(top);

        Exception bounded = EsqlFailureBounds.bound(top, QUERY);

        assertNoRepeats(bounded);
        assertThat(bounded.getSuppressed(), arrayWithSize(2));
        assertThat(bounded.getSuppressed()[0].getMessage(), equalTo("[runtime_exception] first"));
        assertThat(bounded.getSuppressed()[1].getMessage(), equalTo("[runtime_exception] second"));
    }

    private static RuntimeException causeChainOf(int length) {
        RuntimeException chain = new RuntimeException("cause-" + length);
        for (int i = length - 1; i > 0; i--) {
            chain = new RuntimeException("cause-" + i, chain);
        }
        return chain;
    }

    public void testRebuiltFailureCarriesABoundedNumberOfFurtherFailures() {
        RuntimeException top = new RuntimeException("top");
        int distinct = between(EsqlFailureBounds.MAX_ADDITIONAL_FAILURES + 1, 50);
        RuntimeException repeated = new RuntimeException("repeated");
        top.addSuppressed(repeated);
        repeated.addSuppressed(top);
        for (int i = 1; i < distinct; i++) {
            top.addSuppressed(new RuntimeException("other-" + i));
        }

        Exception bounded = EsqlFailureBounds.bound(top, QUERY);

        assertNoRepeats(bounded);
        assertThat(bounded.getSuppressed(), arrayWithSize(EsqlFailureBounds.MAX_ADDITIONAL_FAILURES));
        assertThat(bounded.getSuppressed()[0].getMessage(), containsString("repeated"));
    }

    /**
     * The loop from {@link #testACyclicFailureIsRebuiltAndRendersSmall}, first reached at depth 99 at the end of a cause
     * chain (just above the renderer's depth limit of 100) and then again directly from the top. What is counted where
     * it is first reached must not hide what it renders where it is reached again.
     */
    public void testALoopFirstReachedDeepIsStillFound() {
        RuntimeException a = new RuntimeException("a");
        RuntimeException b = new RuntimeException("b");
        a.addSuppressed(b);
        a.addSuppressed(b);
        b.addSuppressed(a);
        RuntimeException chain = new RuntimeException("cause-98", a);
        for (int i = 97; i > 0; i--) {
            chain = new RuntimeException("cause-" + i, chain);
        }
        RuntimeException top = new RuntimeException("top", chain);
        top.addSuppressed(a);

        Exception bounded = EsqlFailureBounds.bound(top, QUERY);

        assertThat(bounded, not(sameInstance(top)));
        assertNoRepeats(bounded);
    }

    public void testRebuiltCauseChainIsBounded() {
        RuntimeException a = new RuntimeException("a");
        RuntimeException b = new RuntimeException("b");
        a.addSuppressed(b);
        b.addSuppressed(a);
        int chainLength = between(EsqlFailureBounds.MAX_CAUSE_CHAIN + 2, 30);
        RuntimeException top = a;
        for (int i = 0; i < chainLength; i++) {
            top = new RuntimeException("wrapper-" + i, top);
        }

        Exception bounded = EsqlFailureBounds.bound(top, QUERY);

        int kept = 0;
        for (Throwable t = bounded; t != null; t = t.getCause()) {
            kept++;
        }
        assertThat("the failure itself plus its first cause links", kept, equalTo(EsqlFailureBounds.MAX_CAUSE_CHAIN + 1));
        assertNoRepeats(bounded);
    }

    public void testAFailureWithoutAMessageIsNamedByItsType() {
        RuntimeException a = new RuntimeException();
        RuntimeException b = new RuntimeException("b");
        a.addSuppressed(b);
        b.addSuppressed(a);

        Exception bounded = EsqlFailureBounds.bound(a, QUERY);

        assertThat(bounded.getMessage(), equalTo("[runtime_exception]"));
    }

    public void testWrapBoundsTheFailureAndPassesResponsesThrough() {
        CircuitBreakingException a = new CircuitBreakingException(READER_MESSAGE, CircuitBreaker.Durability.TRANSIENT);
        CircuitBreakingException b = new CircuitBreakingException(WINDOW_MESSAGE, CircuitBreaker.Durability.TRANSIENT);
        a.addSuppressed(b);
        b.addSuppressed(a);

        PlainActionFuture<String> failed = new PlainActionFuture<>();
        EsqlFailureBounds.wrap(failed, QUERY).onFailure(a);
        CircuitBreakingException received = expectThrows(CircuitBreakingException.class, failed::actionGet);
        assertThat(received, not(sameInstance(a)));
        assertNoRepeats(received);

        PlainActionFuture<String> succeeded = new PlainActionFuture<>();
        ActionListener<String> wrapped = EsqlFailureBounds.wrap(succeeded, QUERY);
        wrapped.onResponse("ok");
        assertThat(succeeded.actionGet(), equalTo("ok"));
    }

    /**
     * Renders {@code failure} the way a REST error response does, with stack traces, into a stream that refuses to grow
     * past {@link #RENDER_LIMIT_BYTES}.
     */
    private static String renderCapped(Exception failure) throws IOException {
        CappedOutputStream out = new CappedOutputStream();
        ToXContent.Params params = new ToXContent.MapParams(Map.of(ElasticsearchException.REST_EXCEPTION_SKIP_STACK_TRACE, "false"));
        try (XContentBuilder builder = XContentFactory.jsonBuilder(out)) {
            builder.startObject();
            ElasticsearchException.generateFailureXContent(builder, params, failure, true);
            builder.endObject();
        }
        return out.bytes.toString(StandardCharsets.UTF_8);
    }

    private static final class CappedOutputStream extends OutputStream {
        private final ByteArrayOutputStream bytes = new ByteArrayOutputStream();

        @Override
        public void write(int b) throws IOException {
            write(new byte[] { (byte) b }, 0, 1);
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            if (bytes.size() + len > RENDER_LIMIT_BYTES) {
                throw new IOException("rendered error exceeds [" + RENDER_LIMIT_BYTES + "] bytes");
            }
            bytes.write(b, off, len);
        }
    }

    private static void assertNoRepeats(Throwable root) {
        assertNoRepeats(root, Collections.newSetFromMap(new IdentityHashMap<>()));
    }

    private static void assertNoRepeats(Throwable current, Set<Throwable> seen) {
        assertTrue("reached [" + current + "] more than once", seen.add(current));
        if (current.getCause() != null) {
            assertNoRepeats(current.getCause(), seen);
        }
        for (Throwable suppressed : current.getSuppressed()) {
            assertNoRepeats(suppressed, seen);
        }
    }

    private static void assertReferencesNone(Throwable current, Throwable... originals) {
        for (Throwable original : originals) {
            assertThat(current, not(sameInstance(original)));
        }
        if (current.getCause() != null) {
            assertReferencesNone(current.getCause(), originals);
        }
        for (Throwable suppressed : current.getSuppressed()) {
            assertReferencesNone(suppressed, originals);
        }
    }
}
