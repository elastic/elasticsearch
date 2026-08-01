/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams.derivedmetrics;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.bulk.TransportBulkAction;
import org.elasticsearch.client.internal.support.AbstractClient;
import org.elasticsearch.cluster.metadata.DataStreamDerivedMetrics;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.project.DefaultProjectResolver;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.MockPageCacheRecycler;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.datastreams.DataStreamsPlugin;
import org.elasticsearch.index.IndexingPressure;
import org.elasticsearch.index.mapper.LuceneDocument;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ExecutorBuilder;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentType;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;

/**
 * Covers the decisions the service makes about whether to emit at all, which is the part of it that sheds work and had no tests.
 *
 * <p>The emitted documents themselves are covered by {@link DerivedMetricsEmitterTests} and the accumulation by
 * {@link DerivedMetricsBufferTests}; what is exercised here is the surrounding policy — the indexing pressure ceiling, the in-flight
 * ceiling, and the flush paths — because every one of those silently drops data when it fires and none of them was verified.
 */
public class DerivedMetricsServiceTests extends ESTestCase {

    private ThreadPool threadPool;
    private BigArrays bigArrays;
    private RecordingClient client;
    /**
     * Compiled once and shared. A {@link CompiledDerivedMetrics.CompiledMetric} holds an array and a compiled predicate, both of which
     * compare by identity, so recompiling the same configuration yields metrics that are not equal and therefore land in separate tables.
     * Production compiles once per cluster state version, so sharing here is what matches it.
     */
    private CompiledDerivedMetrics compiled;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool(
            getTestName(),
            new DataStreamsPlugin(Settings.EMPTY).getExecutorBuilders(Settings.EMPTY).toArray(ExecutorBuilder<?>[]::new)
        );
        bigArrays = new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService());
        client = new RecordingClient(threadPool);
        compiled = CompiledDerivedMetrics.compile(
            new DataStreamDerivedMetrics(
                true,
                List.of("ingest.docs.count"),
                TimeValue.timeValueSeconds(10),
                null,
                List.of("service.name"),
                List.of()
            )
        );
    }

    @Override
    public void tearDown() throws Exception {
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
        super.tearDown();
    }

    /**
     * Three observations of one series are one document, not three: that collapsing is the whole point of the feature.
     */
    public void testObservationsOfOneSeriesBecomeOneDocument() throws Exception {
        try (DerivedMetricsService service = service(Settings.EMPTY, new IndexingPressure(Settings.EMPTY))) {
            observe(service, 3);
            service.flushEverything("a test asked it to");
            assertBusy(() -> assertEquals(1, client.documentsSent()));
        }
    }

    /**
     * Emitted bulks are charged to the same node-wide budget as the user writes that produced them, so a node already under pressure must
     * not have its own writes pushed toward rejection to make room for metrics about those writes.
     */
    public void testEmissionIsSkippedAndCountedWhenTheNodeIsUnderIndexingPressure() throws Exception {
        // a ceiling of zero means any pressure at all is too much, which is the condition under test
        Settings settings = Settings.builder().put(DerivedMetricsService.INDEXING_PRESSURE_CEILING.getKey(), 0.0).build();
        IndexingPressure pressure = new IndexingPressure(Settings.EMPTY);
        try (
            var reserved = pressure.markCoordinatingOperationStarted(1, 1024, false);
            DerivedMetricsService service = service(settings, pressure)
        ) {
            observe(service, 3);
            service.flushEverything("a test asked it to");
            assertBusy(() -> assertEquals(0, buffered(service)));
            assertEquals("nothing may be sent while the node is over its ceiling", 0, client.documentsSent());
        }
    }

    public void testEmissionProceedsWhenTheNodeIsNotUnderPressure() throws Exception {
        Settings settings = Settings.builder().put(DerivedMetricsService.INDEXING_PRESSURE_CEILING.getKey(), 0.9).build();
        try (DerivedMetricsService service = service(settings, new IndexingPressure(Settings.EMPTY))) {
            observe(service, 3);
            service.flushEverything("a test asked it to");
            assertBusy(() -> assertEquals(1, client.documentsSent()));
        }
    }

    /**
     * Emission is fire and forget, so without a ceiling a destination that cannot keep up would let every flush add to a queue with
     * nothing bounding it. The ceiling counts documents rather than requests, so that how the documents happened to be divided into bulks
     * does not change when it fires.
     */
    public void testTheInFlightCeilingShedsRatherThanQueues() throws Exception {
        Settings settings = Settings.builder()
            .put(DerivedMetricsService.MAX_IN_FLIGHT_BULKS.getKey(), 1)
            .put(DerivedMetricsService.BULK_SIZE.getKey(), 1)
            .build();
        client.holdResponses();
        try (DerivedMetricsService service = service(settings, new IndexingPressure(Settings.EMPTY))) {
            // several distinct series, so the flush produces more than one document and more than one bulk of size one
            for (int i = 0; i < 5; i++) {
                observeService(service, "service-" + i);
            }
            service.flushEverything("a test asked it to");
            assertBusy(() -> assertEquals("only the ceiling's worth may be outstanding", 1, client.documentsSent()));
        } finally {
            client.releaseResponses();
        }
    }

    /**
     * Something is about to stop this node observing — a shard leaving, or an orderly shutdown — so intervals that are still open have to
     * go now rather than wait for their own boundary.
     */
    public void testFlushEverythingEmitsIntervalsThatAreStillOpen() throws Exception {
        try (DerivedMetricsService service = service(Settings.EMPTY, new IndexingPressure(Settings.EMPTY))) {
            observe(service, 3);
            // the bucket has not closed, so an ordinary flush finds nothing
            service.flush();
            assertEquals(0, client.documentsSent());

            service.flushEverything("a test asked it to");
            assertBusy(() -> assertEquals(1, client.documentsSent()));
        }
    }

    public void testFlushEverythingOnAnEmptyBufferSendsNothing() throws Exception {
        try (DerivedMetricsService service = service(Settings.EMPTY, new IndexingPressure(Settings.EMPTY))) {
            service.flushEverything("nothing has been observed yet");
            assertEquals(0, client.documentsSent());
        }
    }

    /**
     * A metric that reads a field the documents do not have emits nothing. That is correct, but it is also exactly what a misspelled
     * field name looks like, and until it was counted there was no signal anywhere that a metric had been configured and never fired.
     */
    public void testAMetricWhoseValueFieldIsMissingIsCountedRatherThanSilentlySkipped() throws Exception {
        CompiledDerivedMetrics misspelled = CompiledDerivedMetrics.compile(
            new DataStreamDerivedMetrics(
                true,
                List.of(),
                TimeValue.timeValueSeconds(10),
                null,
                List.of(),
                List.of(
                    new DataStreamDerivedMetrics.Metric(
                        "queue.depth",
                        DataStreamDerivedMetrics.MetricType.GAUGE,
                        null,
                        // the documents this test indexes carry service.name, never this
                        DataStreamDerivedMetrics.MetricValue.field("queue.dpeth"),
                        DataStreamDerivedMetrics.GaugeAggregation.MAX,
                        null,
                        null
                    )
                )
            )
        );
        try (DerivedMetricsService service = service(Settings.EMPTY, new IndexingPressure(Settings.EMPTY))) {
            for (int i = 0; i < 3; i++) {
                service.record(ProjectId.DEFAULT, "logs-my_app-default", misspelled, document("checkout"), true);
            }
            service.flushEverything("a test asked it to");

            assertEquals("a metric reading a field nothing has cannot emit", 0, client.documentsSent());
            assertEquals("but it must not do so silently", 3L, service.skippedForMissingValue());
        }
    }

    /**
     * Under flush_early a refused observation is retried after making room. The buffer counts a drop on each refusal, so a retry that
     * succeeded left a loss recorded that never happened — which made the policy look worse than it is precisely when someone would be
     * looking at it.
     */
    public void testAnObservationRecoveredByAnEarlyFlushIsNotReportedAsLost() throws Exception {
        // one series per node, so the second distinct series is refused and has to be recovered by flushing the first out
        Settings settings = Settings.builder().put(DerivedMetricsService.MAX_SERIES_PER_NODE.getKey(), 1).build();
        try (DerivedMetricsService service = service(settings, new IndexingPressure(Settings.EMPTY))) {
            observeService(service, "checkout");
            observeService(service, "search");

            assertEquals("the retry succeeded, so nothing was lost", 1L, service.recoveredAfterRelief());
            assertThat(
                "and the buffer counted a drop that has to be taken back",
                service.buffer().droppedSeries(),
                greaterThanOrEqualTo(1L)
            );
        }
    }

    /** How many series are still held, so a test can wait for an asynchronous flush to have drained them. */
    private static int buffered(DerivedMetricsService service) {
        return service.buffer().size();
    }

    private DerivedMetricsService service(Settings settings, IndexingPressure pressure) {
        Settings merged = Settings.builder()
            // a long interval so a bucket only closes when the test says so, never by the clock moving on
            .put(DerivedMetricsService.FLUSH_GRACE_PERIOD.getKey(), TimeValue.timeValueHours(1))
            .put(settings)
            .build();
        return new DerivedMetricsService(merged, client, threadPool, bigArrays, pressure, MeterRegistry.NOOP, "node-id-1", "node-1");
    }

    private void observe(DerivedMetricsService service, int documents) {
        for (int i = 0; i < documents; i++) {
            observeService(service, "checkout");
        }
    }

    private void observeService(DerivedMetricsService service, String serviceName) {
        service.record(ProjectId.DEFAULT, "logs-my_app-default", compiled, document(serviceName), true);
    }

    private static ParsedDocument document(String serviceName) {
        String source = "{\"@timestamp\":\"2026-01-01T00:00:00.000Z\",\"service\":{\"name\":\"" + serviceName + "\"}}";
        return new ParsedDocument(
            null,
            null,
            "doc-1",
            null,
            List.of(new LuceneDocument()),
            SourceToParse.Source.fromBytes(new BytesArray(source), XContentType.JSON),
            null,
            0L
        );
    }

    /**
     * Records the bulks the service sends. Responses are completed immediately unless a test asks for them to be held, which is how the
     * in-flight ceiling is made observable: the ceiling only bites while requests are outstanding.
     */
    private static class RecordingClient extends AbstractClient {
        private final List<Integer> sent = new CopyOnWriteArrayList<>();
        private final List<ActionListener<?>> held = new CopyOnWriteArrayList<>();
        private volatile boolean hold;

        RecordingClient(ThreadPool threadPool) {
            super(Settings.EMPTY, threadPool, DefaultProjectResolver.INSTANCE);
        }

        void holdResponses() {
            hold = true;
        }

        void releaseResponses() {
            hold = false;
            for (ActionListener<?> listener : new ArrayList<>(held)) {
                @SuppressWarnings("unchecked")
                ActionListener<BulkResponse> bulk = (ActionListener<BulkResponse>) listener;
                bulk.onResponse(new BulkResponse(new org.elasticsearch.action.bulk.BulkItemResponse[0], 0L));
            }
            held.clear();
        }

        int documentsSent() {
            return sent.stream().mapToInt(Integer::intValue).sum();
        }

        @Override
        @SuppressWarnings("unchecked")
        protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
            ActionType<Response> action,
            Request request,
            ActionListener<Response> listener
        ) {
            if (action == TransportBulkAction.TYPE) {
                sent.add(((BulkRequest) request).numberOfActions());
                if (hold) {
                    held.add(listener);
                    return;
                }
                ((ActionListener<BulkResponse>) listener).onResponse(
                    new BulkResponse(new org.elasticsearch.action.bulk.BulkItemResponse[0], 0L)
                );
                return;
            }
            throw new UnsupportedOperationException("unexpected action [" + action.name() + "]");
        }
    }
}
