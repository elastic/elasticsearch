/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.querycapture;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.appender.AsyncAppender;
import org.apache.logging.log4j.core.config.AppenderRef;
import org.apache.logging.log4j.core.config.DefaultConfiguration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.config.Property;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchCaptureContext;
import org.elasticsearch.action.search.SearchCaptureContextBuilder;
import org.elasticsearch.action.search.SearchLogProducer;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.benchmark.internal.BenchmarkLogging;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.logging.ECSJsonLayout;
import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.common.logging.activity.Log4jWriter;
import org.elasticsearch.common.logging.activity.QueryCaptureService;
import org.elasticsearch.common.logging.activity.QueryInterceptor;
import org.elasticsearch.common.logging.activity.QueryLogger;
import org.elasticsearch.common.logging.activity.QueryLogging;
import org.elasticsearch.common.logging.activity.QuerySinkRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsThreadPoolExecutor;
import org.elasticsearch.index.ActionLoggingFields;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.vectors.KnnSearchBuilder;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.threadpool.DefaultBuiltInExecutorBuilders;
import org.elasticsearch.threadpool.ThreadPool;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.BenchmarkParams;
import org.openjdk.jmh.infra.Blackhole;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Measures the response-thread cost of the search action's QueryLogger, including query serialization and Log4j writing.
 * The destination consumes ECS JSON bytes through a volatile field instead of writing to disk. No search is executed,
 * authentication fields are empty, and results do not include filesystem, rolling-file, or compression costs.
 *
 * The baseline only completes the response listener. Synchronous capture runs QueryLogger on that thread; worker capture
 * uses the real QueryCaptureService. The direct appender formats JSON on the calling thread, while the async appender
 * matches the shipped query-log configuration (256 entries, non-blocking) and formats on Log4j's worker.
 *
 * Each single-shot invocation submits 128 captures, below the capture queue capacity. Both queues are drained outside
 * the timed method and every expected log write is checked: rejected or lost captures invalidate the measurement.
 * OperationsPerInvocation normalizes results to time per capture, so do not divide the reported score by 128 again.
 * Use the default single-shot mode and one producer; average-time mode can saturate the queues and measures a different workload.
 * GC profiling includes allocations on all threads, including work drained after submission.
 */
@Fork(value = 2, jvmArgsAppend = "-Des.query_capture_async_feature_flag_enabled=true")
@Warmup(iterations = 200)
@Measurement(iterations = 100)
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Threads(1)
@State(Scope.Benchmark)
public class QueryLoggerBenchmark {
    private static final int CAPTURES_PER_INVOCATION = 128;
    private static final NamedWriteableRegistry REGISTRY = new NamedWriteableRegistry(List.of());

    static {
        BenchmarkLogging.configure();
        LogConfigurator.setNodeName("query-logger-benchmark");
        LogConfigurator.setClusterName("query-logger-benchmark");
    }

    @Param({ "baseline", "sync", "worker" })
    public String scenario;

    @Param({ "term", "knn_768" })
    public String queryType;

    @Param({ "direct", "async" })
    public String appenderMode;

    private ThreadPool pool;
    private EsThreadPoolExecutor executor;
    private LoggerContext loggingContext;
    private DiscardingAppender destination;
    private SearchRequest request;
    private SearchResponse response;
    private Task task;
    private QueryInterceptor<SearchCaptureContext> synchronous;
    private QueryCaptureService.Interceptor<SearchCaptureContext> worker;
    private ActionListener<SearchResponse> listener;
    private Consumer<ActionListener<SearchResponse>> action;
    private SearchResponse delivered;
    private long expectedWrites;

    /** Builds the same search logger components as TransportSearchAction, with an in-memory log destination. */
    @Setup
    public void setup(BenchmarkParams params) throws Exception {
        if (params.getThreads() != 1 || params.getMode() != Mode.SingleShotTime) {
            throw new IllegalArgumentException("Use one producer and single-shot mode to measure accepted captures without saturation");
        }
        if (QueryCaptureService.ASYNC_FEATURE_FLAG.isEnabled() == false) {
            throw new IllegalArgumentException("The query_capture_async feature flag must be enabled for the worker comparison");
        }
        pool = new ThreadPool(
            Settings.builder().put("node.name", "query-logger-benchmark").build(),
            MeterRegistry.NOOP,
            new DefaultBuiltInExecutorBuilders()
        );
        executor = (EsThreadPoolExecutor) pool.executor(ThreadPool.Names.QUERY_CAPTURE);
        var producer = new SearchLogProducer();
        var configuration = new DefaultConfiguration();
        destination = new DiscardingAppender(
            ECSJsonLayout.newBuilder().setConfiguration(configuration).setDataset("elasticsearch.querylog").build()
        );
        configuration.addAppender(destination);
        Appender appender = switch (appenderMode) {
            case "direct" -> destination;
            case "async" -> AsyncAppender.newBuilder()
                .setName("querylog_async")
                .setConfiguration(configuration)
                .setAppenderRefs(new AppenderRef[] { AppenderRef.createAppenderRef(destination.getName(), null, null) })
                .setBufferSize(256)
                .setBlocking(false)
                .setShutdownTimeout(1000)
                .build();
            default -> throw new IllegalArgumentException("Unknown appender mode: " + appenderMode);
        };
        configuration.addAppender(appender);
        var loggerConfig = new LoggerConfig(producer.loggerName(), Level.TRACE, false);
        loggerConfig.addAppender(appender, null, null);
        configuration.addLogger(producer.loggerName(), loggerConfig);
        loggingContext = new LoggerContext("query-logger-benchmark");
        loggingContext.start(configuration);
        var settings = ClusterSettings.createBuiltInClusterSettings(
            Settings.builder().put(QueryLogger.QUERY_LOGGER_ENABLED.getKey(), true).build()
        );
        var logger = new QueryLogger<>(
            settings,
            producer,
            name -> new Log4jWriter(loggingContext.getLogger(name)),
            context -> new ActionLoggingFields(context) {},
            new SystemIndices(List.of())::isSystemName
        );
        synchronous = new QueryInterceptor<>(List.of(logger));
        worker = new QueryCaptureService(pool, MeterRegistry.NOOP, new QuerySinkRegistry(List.of())).interceptor(logger);
        var source = new SearchSourceBuilder();
        switch (queryType) {
            case "term" -> source.query(QueryBuilders.termQuery("category", "books"));
            case "knn_768" -> {
                var random = new Random(0);
                var vector = new float[768];
                for (int i = 0; i < vector.length; i++) {
                    vector[i] = random.nextFloat();
                }
                source.knnSearch(
                    List.of(
                        new KnnSearchBuilder("vector", vector, 10, 100, null, null, null).addFilterQuery(
                            QueryBuilders.termQuery("category", "books")
                        )
                    )
                );
            }
            default -> throw new IllegalArgumentException("Unknown query type: " + queryType);
        }
        request = new SearchRequest("products").source(source);
        response = new SearchResponse(
            SearchHits.EMPTY_WITH_TOTAL_HITS,
            null,
            null,
            false,
            null,
            null,
            1,
            null,
            1,
            1,
            0,
            1,
            ShardSearchFailure.EMPTY_ARRAY,
            SearchResponse.Clusters.EMPTY
        );
        task = new Task(1, "transport", "indices:data/read/search", "", TaskId.EMPTY_TASK_ID, Map.of());
        listener = ActionListener.wrap(r -> delivered = r, e -> { throw new AssertionError(e); });
        action = l -> l.onResponse(response);
        // Prove that this logger produces a serialized query rather than silently taking a disabled/no-op path.
        synchronous.wrapAndRun(listener, new SearchCaptureContextBuilder(task, REGISTRY, request), action);
        expectedWrites = 1;
        finishIteration();
        if (new String(destination.lastEvent, StandardCharsets.UTF_8).contains("\"" + QueryLogging.QUERY_FIELD_QUERY + "\"") == false) {
            throw new AssertionError("Query missing from the serialized log event");
        }
    }

    /** Measures producer latency per capture; worker completion is deliberately outside the timed interval. */
    @Benchmark
    @OperationsPerInvocation(CAPTURES_PER_INVOCATION)
    public void log(Blackhole blackhole) {
        for (int i = 0; i < CAPTURES_PER_INVOCATION; i++) {
            switch (scenario) {
                case "baseline" -> action.accept(listener);
                case "sync" -> synchronous.wrapAndRun(listener, new SearchCaptureContextBuilder(task, REGISTRY, request), action);
                case "worker" -> worker.wrapAndRun(listener, new SearchCaptureContextBuilder(task, REGISTRY, request), action);
                default -> throw new IllegalArgumentException("Unknown scenario: " + scenario);
            }
            blackhole.consume(delivered);
        }
        if (scenario.equals("baseline") == false) {
            expectedWrites += CAPTURES_PER_INVOCATION;
        }
    }

    /** Prevents queued work from crossing iterations and rejects results containing dropped log events. */
    @TearDown(org.openjdk.jmh.annotations.Level.Iteration)
    public void finishIteration() throws Exception {
        executor.submit(() -> {}).get(30, TimeUnit.SECONDS);
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (destination.writes < expectedWrites) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("Expected " + expectedWrites + " log writes but received " + destination.writes);
            }
            Thread.sleep(1);
        }
        if (destination.writes != expectedWrites) {
            throw new AssertionError("Unexpected duplicate log writes");
        }
    }

    /** Releases the response only after worker processing has completed and stops both executors. */
    @TearDown
    public void teardown() throws Exception {
        try {
            finishIteration();
        } finally {
            if (ThreadPool.terminate(pool, 30, TimeUnit.SECONDS) == false) {
                throw new AssertionError("Capture worker did not terminate");
            }
            loggingContext.stop();
            response.decRef();
        }
    }

    /** A blackhole-like destination that forces JSON serialization without filesystem I/O or cross-thread JMH Blackhole access. */
    private static class DiscardingAppender extends AbstractAppender {
        private volatile byte[] lastEvent;
        // There is exactly one writer: the producer, capture worker, or Log4j worker, according to the selected scenario.
        private volatile long writes;

        DiscardingAppender(Layout<? extends Serializable> layout) {
            super("discarding", null, layout, false, Property.EMPTY_ARRAY);
        }

        @Override
        public void append(LogEvent event) {
            lastEvent = getLayout().toByteArray(event);
            writes++;
        }
    }
}
