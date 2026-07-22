/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.esql;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.benchmark.Utils;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.analysis.Analyzer;
import org.elasticsearch.xpack.esql.analysis.AnalyzerContext;
import org.elasticsearch.xpack.esql.analysis.EnrichResolution;
import org.elasticsearch.xpack.esql.analysis.Verifier;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.expression.promql.function.PromqlFunctionRegistry;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.IndexResolution;
import org.elasticsearch.xpack.esql.inference.InferenceResolution;
import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.LogicalPlanOptimizer;
import org.elasticsearch.xpack.esql.parser.EsqlConfig;
import org.elasticsearch.xpack.esql.parser.EsqlParser;
import org.elasticsearch.xpack.esql.plan.IndexPattern;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.telemetry.Metrics;
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
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.xpack.esql.plan.QuerySettings.UNMAPPED_FIELDS;

/**
 * Measures ES|QL analysis and logical optimization performance as a function of
 * the number of fields in the index mapping.
 *
 * <p>Motivation: a production CCS query with {@code | DROP lost.*} against a merged
 * field-caps schema of ~705 000 fields (OTel attribute explosion across 7 606 indices)
 * caused the coordinator's {@code analysis} phase to take 13 423 s (~3.73 h).
 * Root cause: {@code Analyzer.dropResolver} calls
 * {@code resolvedProjections.removeIf(resolved::contains)} where {@code resolved} is
 * a plain {@link java.util.ArrayList} of ~n matched attributes, making each
 * {@code contains()} O(n) and the overall drop resolution O(n²).
 *
 * <p>The {@code plan=keep} variant is the baseline (no wildcard DROP); {@code plan=drop}
 * adds {@code | DROP otel.*} to expose the quadratic scaling.
 *
 * <p>Run with:
 * <pre>
 *   ./gradlew :benchmarks:run \
 *     --args='AnalysisBenchmark -p fieldCount=1000,10000,100000 -p plan=keep,drop'
 * </pre>
 */
@Fork(1)
@Warmup(iterations = 3, time = 2, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 2, timeUnit = TimeUnit.SECONDS)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
public class AnalysisBenchmark {

    static {
        Utils.configureBenchmarkLogging();
    }

    /**
     * Number of dummy OTel-style fields in the schema. The 1 000 / 10 000 / 100 000
     * progression exposes how the cost scales; 700 000 reproduces the production case.
     */
    @Param({ "1000", "10000", "100000", "700000" })
    public int fieldCount;

    /**
     * Which query shape to benchmark.
     * <ul>
     *   <li>{@code keep} — simple filter + LIMIT, no wildcard expansion; baseline cost.</li>
     *   <li>{@code drop} — adds {@code | DROP otel.*} which resolves the wildcard against
     *       all ~n dummy fields and then calls {@code resolvedProjections.removeIf(resolved::contains)}.
     *       {@code resolved} is a plain {@link java.util.ArrayList}, so each {@code contains()}
     *       is O(n), making the overall drop resolution O(n²). This reproduces the production
     *       case where {@code | DROP lost.*} against a 700 000-field CCS schema caused the
     *       {@code analysis} profile phase to take 13 423 s (~3.73 h).</li>
     * </ul>
     */
    @Param({ "keep", "drop" })
    public String plan;

    private static final String KEEP_QUERY = """
        FROM test
        | WHERE service_name IN (
            "motel-ingest-collector",
            "motel-aggregation-collector",
            "motel-index-collector",
            "motel-provisioner",
            "hosted-otel-controller"
          )
          AND dropped_data_points IS NOT NULL
        | SORT @timestamp ASC
        | LIMIT 1
        """;

    private static final String DROP_QUERY = """
        FROM test
        | WHERE service_name IN (
            "motel-ingest-collector",
            "motel-aggregation-collector",
            "motel-index-collector",
            "motel-provisioner",
            "hosted-otel-controller"
          )
          AND dropped_data_points IS NOT NULL
        | DROP otel.*
        | SORT @timestamp ASC
        | LIMIT 1
        """;

    private Analyzer analyzer;
    private LogicalPlanOptimizer optimizer;
    private EsqlParser parser;
    private String query;
    /** Pre-analyzed plan used by the optimization-only benchmark. */
    private LogicalPlan analyzedPlan;

    @Setup
    public void setup() {
        EsqlFunctionRegistry functionRegistry = new EsqlFunctionRegistry();
        TransportVersion minimumVersion = TransportVersion.current();

        query = plan.equals("drop") ? DROP_QUERY : KEEP_QUERY;

        EsIndex index = buildIndex(fieldCount);

        analyzer = new Analyzer(
            new AnalyzerContext(
                EsqlTestUtils.TEST_CFG,
                functionRegistry,
                PromqlFunctionRegistry.INSTANCE,
                EsqlTestUtils.TEST_ANALYSIS_REGISTRY,
                Map.of(new IndexPattern(Source.EMPTY, index.name()), IndexResolution.valid(index)),
                emptyMap(),
                new EnrichResolution(),
                InferenceResolution.EMPTY,
                minimumVersion,
                UNMAPPED_FIELDS.defaultValue()
            ),
            new Verifier(new Metrics(functionRegistry, true, true), new XPackLicenseState(() -> 0L))
        );

        optimizer = new LogicalPlanOptimizer(new LogicalOptimizerContext(EsqlTestUtils.TEST_CFG, FoldContext.small(), minimumVersion));

        parser = new EsqlParser(new EsqlConfig(functionRegistry));

        // Pre-analyze once so the optimization benchmark starts from a clean analyzed plan.
        analyzedPlan = analyzer.analyze(parser.parseQuery(query));
    }

    /** Measures parse + analysis only. */
    @Benchmark
    public void analysis(Blackhole bh) {
        bh.consume(analyzer.analyze(parser.parseQuery(query)));
    }

    /** Measures logical optimization only (analysis is done in {@link #setup}). */
    @Benchmark
    public void logicalOptimization(Blackhole bh) {
        bh.consume(optimizer.optimize(analyzedPlan));
    }

    /** Measures the full parse → analyze → optimize pipeline. */
    @Benchmark
    public void fullPipeline(Blackhole bh) {
        LogicalPlan analyzed = analyzer.analyze(parser.parseQuery(query));
        bh.consume(optimizer.optimize(analyzed));
    }

    private static EsIndex buildIndex(int dummyFieldCount) {
        Map<String, EsField> mapping = new HashMap<>(dummyFieldCount + 8);

        // The four fields actually referenced by the query.
        mapping.put("@timestamp", new EsField("@timestamp", DataType.DATETIME, emptyMap(), true, EsField.TimeSeriesFieldType.NONE));
        mapping.put("service_name", new EsField("service_name", DataType.KEYWORD, emptyMap(), true, EsField.TimeSeriesFieldType.NONE));
        mapping.put(
            "dropped_data_points",
            new EsField("dropped_data_points", DataType.LONG, emptyMap(), true, EsField.TimeSeriesFieldType.NONE)
        );
        mapping.put("message", new EsField("message", DataType.KEYWORD, emptyMap(), true, EsField.TimeSeriesFieldType.NONE));

        // Dummy fields simulating the OTel attribute explosion from CCS across many clusters.
        for (int i = 0; i < dummyFieldCount; i++) {
            String name = "otel.attr." + i;
            mapping.put(name, new EsField(name, DataType.KEYWORD, emptyMap(), true, EsField.TimeSeriesFieldType.NONE));
        }

        return new EsIndex("test", mapping, Map.of("test", IndexMode.STANDARD), Map.of(), Map.of());
    }
}
