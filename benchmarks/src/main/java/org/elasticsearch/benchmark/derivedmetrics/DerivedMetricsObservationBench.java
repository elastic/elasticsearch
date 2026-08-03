/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.derivedmetrics;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.benchmark.Utils;
import org.elasticsearch.benchmark.index.mapper.MapperServiceFactory;
import org.elasticsearch.client.internal.support.AbstractClient;
import org.elasticsearch.cluster.metadata.DataStreamDerivedMetrics;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.project.DefaultProjectResolver;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.datastreams.DataStreamsPlugin;
import org.elasticsearch.datastreams.derivedmetrics.CompiledDerivedMetrics;
import org.elasticsearch.datastreams.derivedmetrics.DerivedMetricsDocumentReader;
import org.elasticsearch.datastreams.derivedmetrics.DerivedMetricsService;
import org.elasticsearch.datastreams.derivedmetrics.DerivedMetricsSourceReader;
import org.elasticsearch.index.IndexingPressure;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.threadpool.ExecutorBuilder;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentType;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Measures what observing one write costs, which is the only part of derived metrics that runs on the indexing thread.
 *
 * <p>Both numbers matter and the allocation one arguably more: this path runs once per document per metric, inside the shard's operation
 * permit, so what it allocates lands straight in the young generation of a node that is already indexing hard. Run with
 * {@code -prof gc} to get B/op alongside ns/op.
 *
 * <p>The shapes are chosen to separate the costs. {@code BUILTIN_ONLY} never touches {@code _source} at all and is therefore the floor.
 * Everything else pays for source parsing, and the difference between them is what dimensions and predicates add.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 3, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 2, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@Threads(1)
@State(Scope.Benchmark)
public class DerivedMetricsObservationBench {

    /**
     * BUILTIN_ONLY      no dimensions, no user metrics — the write path never reads _source
     * ONE_DIMENSION     the common shape: builtin ingest metrics broken down by service
     * FIVE_DIMENSIONS   five dimensions plus a predicate-guarded counter, a realistic busy configuration
     * HISTOGRAM         a histogram metric over a numeric field
     * NOT_CONFIGURED    no metrics at all — what every index that never asked for this pays on every write
     */
    @Param(
        {
            "NOT_CONFIGURED",
            "ONE_METRIC",
            "ONE_METRIC_THREE_DIMENSIONS",
            "ONE_HISTOGRAM_100_BUCKETS",
            "BUILTIN_ONLY",
            "ONE_DIMENSION",
            "FIVE_DIMENSIONS",
            "HISTOGRAM" }
    )
    String shape;

    private static final String DATA_STREAM = "logs-my_app-default";

    private ThreadPool threadPool;
    private DerivedMetricsService service;
    private CompiledDerivedMetrics compiled;
    private ParsedDocument document;
    /**
     * How each configured path can be read back from the already-parsed document, or null to force the source-parsing path. Both are
     * measured, because the whole question is what the difference is worth.
     */
    private DerivedMetricsDocumentReader.Strategies strategies;

    static {
        // the service and its collaborators grab loggers in their static initialisers, and the ES logging SPI is not wired up in a
        // plain JMH JVM
        Utils.configureBenchmarkLogging();
    }

    @Setup
    public void setUp() {
        // the service takes its executor from the pool the plugin registers, so the benchmark has to register it too
        threadPool = new TestThreadPool(
            "derived-metrics-bench",
            new DataStreamsPlugin(Settings.EMPTY).getExecutorBuilders(Settings.EMPTY).toArray(ExecutorBuilder<?>[]::new)
        );
        Settings.Builder builder = Settings.builder()
            // a budget wide enough that the benchmark never measures the cap-refusal path
            .put(DerivedMetricsService.MAX_SERIES_PER_NODE.getKey(), 100_000);
        if (shape.equals("ONE_HISTOGRAM_100_BUCKETS")) {
            builder.put(DerivedMetricsService.HISTOGRAM_BUCKETS.getKey(), 100);
        }
        Settings settings = builder.build();
        service = new DerivedMetricsService(
            settings,
            new NoOpClient(threadPool),
            threadPool,
            BigArrays.NON_RECYCLING_INSTANCE,
            new IndexingPressure(Settings.EMPTY),
            MeterRegistry.NOOP,
            "node-id-1",
            "node-1"
        );
        compiled = CompiledDerivedMetrics.compile(configFor(shape));
        // A real mapping and a real parse, so the document carries the materialised fields the reader is supposed to find. A
        // hand-assembled ParsedDocument with an empty LuceneDocument would measure a reader that finds nothing.
        MapperService mappers = MapperServiceFactory.create(MAPPING);
        document = mappers.documentMapper().parse(new SourceToParse(UUIDs.randomBase64UUID(), new BytesArray(SOURCE), XContentType.JSON));
        strategies = DerivedMetricsDocumentReader.resolve(mappers.mappingLookup(), compiled.sourcePaths().paths());
    }

    @TearDown
    public void tearDown() {
        service.close();
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
    }

    /**
     * The write path as it runs in production: values come from the document Elasticsearch already parsed whenever the mapping allows it.
     */
    @Benchmark
    public void observe() {
        service.record(ProjectId.DEFAULT, DATA_STREAM, compiled, document, true, strategies);
    }

    /**
     * The same work, forced down the source-parsing path, which is what a mapping that cannot serve every configured value falls back to.
     * The gap between this and {@link #observe()} is what reading the parsed document is worth.
     */
    @Benchmark
    public void observeByReparsingSource() {
        service.record(ProjectId.DEFAULT, DATA_STREAM, compiled, document, true, null);
    }

    /**
     * Just the filtered source parse, so the cost of reading _source can be told apart from the cost of everything the observation does
     * with what it read.
     */
    @Benchmark
    public Object readSource() {
        Object[] values = new Object[compiled.sourcePaths().size()];
        DerivedMetricsSourceReader.read(document, compiled.sourcePaths(), values);
        return values;
    }

    private static DataStreamDerivedMetrics configFor(String shape) {
        TimeValue interval = TimeValue.timeValueSeconds(10);
        return switch (shape) {
            // No metrics compile, so no trigger matches and record() returns on its first comparison. This is the floor of the floor:
            // what the feature costs an index that has nothing configured. It is not the whole cost such an index pays — the indexing
            // listener above this does a volatile read and a cluster state version comparison before it ever gets here, and that part
            // needs a real ClusterService to measure — but it is the part that is measurable in isolation.
            case "NOT_CONFIGURED" -> new DataStreamDerivedMetrics(false, List.of(), interval, null, List.of(), List.of());
            case "BUILTIN_ONLY" -> new DataStreamDerivedMetrics(true, List.of("ingest.*"), interval, null, List.of(), List.of());
            case "ONE_DIMENSION" -> new DataStreamDerivedMetrics(
                true,
                List.of("ingest.*"),
                interval,
                null,
                List.of("service.name"),
                List.of()
            );
            case "FIVE_DIMENSIONS" -> new DataStreamDerivedMetrics(
                true,
                List.of("ingest.*"),
                interval,
                null,
                List.of("service.name", "cloud.region", "host.name", "http.request.method", "http.response.status_code"),
                List.of(
                    new DataStreamDerivedMetrics.Metric(
                        "http.errors",
                        DataStreamDerivedMetrics.MetricType.COUNTER,
                        Map.of("range", Map.of("http.response.status_code", Map.of("gte", 500))),
                        null,
                        null,
                        null,
                        null
                    )
                )
            );
            // The three shapes below answer "what does adding a metric actually cost me", one increment at a time, rather than making
            // someone infer it from configurations that differ in more than one way at once.
            case "ONE_METRIC" -> new DataStreamDerivedMetrics(true, List.of(), interval, null, List.of(), List.of(oneCounter(List.of())));
            case "ONE_METRIC_THREE_DIMENSIONS" -> new DataStreamDerivedMetrics(
                true,
                List.of(),
                interval,
                null,
                List.of(),
                List.of(oneCounter(List.of("service.name", "cloud.region", "host.name")))
            );
            case "ONE_HISTOGRAM_100_BUCKETS" -> new DataStreamDerivedMetrics(
                true,
                List.of(),
                interval,
                null,
                List.of(),
                List.of(
                    new DataStreamDerivedMetrics.Metric(
                        "latency.distribution",
                        DataStreamDerivedMetrics.MetricType.HISTOGRAM,
                        null,
                        DataStreamDerivedMetrics.MetricValue.field("event.duration"),
                        null,
                        null,
                        null
                    )
                )
            );
            case "HISTOGRAM" -> new DataStreamDerivedMetrics(
                true,
                List.of(),
                interval,
                null,
                List.of("service.name"),
                List.of(
                    new DataStreamDerivedMetrics.Metric(
                        "latency.distribution",
                        DataStreamDerivedMetrics.MetricType.HISTOGRAM,
                        null,
                        DataStreamDerivedMetrics.MetricValue.field("event.duration"),
                        null,
                        null,
                        null
                    )
                )
            );
            default -> throw new IllegalArgumentException("unknown shape [" + shape + "]");
        };
    }

    /** One predicate-free counter, so the only thing varying between the marginal-cost shapes is its dimension list. */
    private static DataStreamDerivedMetrics.Metric oneCounter(List<String> dimensions) {
        return new DataStreamDerivedMetrics.Metric(
            "http.requests",
            DataStreamDerivedMetrics.MetricType.COUNTER,
            null,
            null,
            null,
            dimensions,
            null
        );
    }

    /**
     * A document of the shape these metrics are derived from. Deliberately wider than the paths any configuration reads, so that the
     * filtered parse has something to skip — which is the whole point of parsing a filtered slice.
     */
    /**
     * A document of the shape these metrics are derived from. Deliberately wider than the paths any configuration reads, so that the
     * parse has something to skip — which is the whole point of reading only what is configured.
     */
    private static final String SOURCE = """
        {
          "@timestamp": "2026-01-01T00:00:00.000Z",
          "service": { "name": "checkout" },
          "cloud": { "region": "eu-west-1", "provider": "aws", "availability_zone": "eu-west-1a" },
          "host": { "name": "host-17", "ip": "10.0.0.17", "architecture": "aarch64" },
          "http": {
            "request": { "method": "POST", "bytes": 1234 },
            "response": { "status_code": 503, "bytes": 91 }
          },
          "event": { "duration": 18374652, "outcome": "failure" },
          "message": "checkout failed while reserving inventory for order 8814-A",
          "trace": { "id": "6f1a2c9d4e8b7a3f5c0d1e2b3a4f5c6d" }
        }""";

    /**
     * Dimensions as keywords and metric values as numbers with doc values, which is what an ECS-shaped stream looks like and what lets
     * every configured path be read straight from the parsed document.
     */
    private static final String MAPPING = """
        {
          "_doc": {
            "properties": {
              "@timestamp": { "type": "date" },
              "service": { "properties": { "name": { "type": "keyword" } } },
              "cloud": {
                "properties": {
                  "region": { "type": "keyword" },
                  "provider": { "type": "keyword" },
                  "availability_zone": { "type": "keyword" }
                }
              },
              "host": {
                "properties": {
                  "name": { "type": "keyword" },
                  "ip": { "type": "ip" },
                  "architecture": { "type": "keyword" }
                }
              },
              "http": {
                "properties": {
                  "request": { "properties": { "method": { "type": "keyword" }, "bytes": { "type": "long" } } },
                  "response": { "properties": { "status_code": { "type": "long" }, "bytes": { "type": "long" } } }
                }
              },
              "event": { "properties": { "duration": { "type": "long" }, "outcome": { "type": "keyword" } } },
              "message": { "type": "text" },
              "trace": { "properties": { "id": { "type": "keyword" } } }
            }
          }
        }""";

    /**
     * The service only touches its client when it flushes, which the benchmark never does. This exists so the constructor has something
     * to wrap.
     */
    private static class NoOpClient extends AbstractClient {
        NoOpClient(ThreadPool threadPool) {
            super(Settings.EMPTY, threadPool, DefaultProjectResolver.INSTANCE);
        }

        @Override
        protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
            ActionType<Response> action,
            Request request,
            ActionListener<Response> listener
        ) {
            throw new UnsupportedOperationException("the observation benchmark must never reach the client");
        }
    }

    public static void main(String[] args) throws RunnerException {
        Options options = new OptionsBuilder().include(".*" + DerivedMetricsObservationBench.class.getSimpleName() + ".*")
            .addProfiler(GCProfiler.class)
            .build();
        new Runner(options).run();
    }
}
