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
 * <p>
 *     We've had some "exciting" bugs in the planner, mostly around stuff that's O(n²)
 *     with the number of attributes. Our overview cluster once spent four hours hot
 *     on a single core running {@code | DROP lost.*} against 705,000 fields.
 * </p>
 *
 * <p>
 *     The {@code keep_many} and {@code sort_many} shapes exercise exact-name resolution of
 *     {@link #WIDE_REFERENCES} explicit references against a wide output — the path
 *     {@code ResolveRefs} now serves from a per-node name index instead of a per-reference linear
 *     scan ({@code keep_many} via {@code keepResolver}, {@code sort_many} via the
 *     {@code resolveExpressions} default branch). The scan baseline was reproduced by setting the
 *     analyzer's {@code NAME_INDEX_THRESHOLD} to {@code Integer.MAX_VALUE} so every lookup rescans
 *     the output. Measured with {@link #analysis} (parse + analyze) using CLI overrides
 *     {@code -wi 5 -i 10 -f 1} (not the class defaults), ms/op with 99.9% CI:
 * </p>
 * <pre>
 *    fields   keep_many: index / scan            sort_many: index / scan
 *    10 000    5.8±0.3 /   55.4±1.8   (~10x)     30.6±0.2 /   85.7±2.3   (~3x)
 *   100 000   67.0±1.5 / 2506±392     (~37x)     95.4±2.3 / 2568±38      (~27x)
 * </pre>
 * <p>
 *     {@code keep_many} is the cleanest attribution — its output narrows to the kept columns —
 *     while {@code sort_many} carries the full wide output downstream, so its optimized floor is
 *     higher and its ratio smaller; both baselines hit the same resolution cliff at 100k. Absolute
 *     numbers are hardware/JVM specific; the ratio growing with field count is the signal that the
 *     quadratic term is gone. The index does not touch wildcard {@code DROP}/{@code KEEP} (e.g.
 *     {@code drop_sort}), which still scan patterns.
 * </p>
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
    @Param({ "1000", "10000", "100000", "1000000" })
    public int fieldCount;

    /**
     * Which query shape to benchmark. {@code keep_many} / {@code sort_many} reference
     * {@link #WIDE_REFERENCES} explicit fields to stress exact-name resolution.
     */
    @Param({ "from", "sort", "drop_sort", "keep_many", "sort_many" })
    public String query;

    /**
     * Number of explicit field references in the {@code keep_many} / {@code sort_many}
     * shapes. These resolve one-by-one against the whole {@code fieldCount}-wide output,
     * so the pre-index analyzer was O(references × fields) here.
     */
    private static final int WIDE_REFERENCES = 1000;

    private static final Map<String, String> QUERIES = buildQueries();

    private static Map<String, String> buildQueries() {
        Map<String, String> queries = new HashMap<>();
        queries.put("from", "FROM test");
        queries.put("sort", """
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
            """);
        queries.put("drop_sort", """
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
            | LIMIT 1""");
        // KEEP <N explicit fields>: resolved in keepResolver, one lookup per projection.
        queries.put("keep_many", fieldListQuery("FROM test | KEEP ", "", WIDE_REFERENCES));
        // SORT <N explicit keys>: resolved in the ResolveRefs default branch (resolveExpressions).
        queries.put("sort_many", fieldListQuery("FROM test | SORT ", " | LIMIT 1", WIDE_REFERENCES));
        return Map.copyOf(queries);
    }

    private static String fieldListQuery(String prefix, String suffix, int references) {
        StringBuilder query = new StringBuilder(prefix);
        for (int i = 0; i < references; i++) {
            if (i > 0) {
                query.append(", ");
            }
            query.append("attr_").append(i);
        }
        return query.append(suffix).toString();
    }

    private String queryText;
    private Analyzer analyzer;
    private LogicalPlanOptimizer optimizer;
    private EsqlParser parser;
    /**
     * Pre-analyzed plan used by the optimization-only benchmark.
     */
    private LogicalPlan analyzedPlan;

    @Setup
    public void setup() {
        EsqlFunctionRegistry functionRegistry = new EsqlFunctionRegistry();
        TransportVersion minimumVersion = TransportVersion.current();

        queryText = QUERIES.get(query);
        if (queryText == null) {
            throw new IllegalArgumentException("can't find [" + query + "]");
        }

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
        analyzedPlan = analyzer.analyze(parser.parseQuery(queryText));
    }

    /** Measures parse + analysis. */
    @Benchmark
    public void analysis(Blackhole bh) {
        bh.consume(analyzer.analyze(parser.parseQuery(queryText)));
    }

    /** Measures logical optimization (analysis is done in {@link #setup}). */
    @Benchmark
    public void logicalOptimization(Blackhole bh) {
        bh.consume(optimizer.optimize(analyzedPlan));
    }

    /** Measures the full parse → analyze → optimize pipeline. */
    @Benchmark
    public void fullPipeline(Blackhole bh) {
        LogicalPlan analyzed = analyzer.analyze(parser.parseQuery(queryText));
        bh.consume(optimizer.optimize(analyzed));
    }

    private static EsIndex buildIndex(int dummyFieldCount) {
        Map<String, EsField> mapping = new HashMap<>(dummyFieldCount + WIDE_REFERENCES + 8);

        mapping.put("@timestamp", new EsField("@timestamp", DataType.DATETIME, emptyMap(), true, EsField.TimeSeriesFieldType.NONE));
        mapping.put("service_name", new EsField("service_name", DataType.KEYWORD, emptyMap(), true, EsField.TimeSeriesFieldType.NONE));
        mapping.put(
            "dropped_data_points",
            new EsField("dropped_data_points", DataType.LONG, emptyMap(), true, EsField.TimeSeriesFieldType.NONE)
        );
        mapping.put("message", new EsField("message", DataType.KEYWORD, emptyMap(), true, EsField.TimeSeriesFieldType.NONE));

        // Huge set of dummy field
        for (int i = 0; i < dummyFieldCount; i++) {
            String name = "otel.attr." + i;
            mapping.put(name, new EsField(name, DataType.KEYWORD, emptyMap(), true, EsField.TimeSeriesFieldType.NONE));
        }

        // Flat, dot-free fields referenced explicitly by the keep_many / sort_many shapes. These are
        // present for every shape (one shared index per fieldCount), so they also slightly widen the
        // from/sort/drop_sort outputs; that does not change those shapes' referenced columns.
        for (int i = 0; i < WIDE_REFERENCES; i++) {
            String name = "attr_" + i;
            mapping.put(name, new EsField(name, DataType.KEYWORD, emptyMap(), true, EsField.TimeSeriesFieldType.NONE));
        }

        return new EsIndex("test", mapping, Map.of("test", IndexMode.STANDARD), Map.of(), Map.of());
    }
}
