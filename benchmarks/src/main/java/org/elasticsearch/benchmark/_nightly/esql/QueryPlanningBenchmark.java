/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark._nightly.esql;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.benchmark.internal.BenchmarkLogging;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.analysis.Analyzer;
import org.elasticsearch.xpack.esql.analysis.AnalyzerContext;
import org.elasticsearch.xpack.esql.analysis.AnalyzerSettings;
import org.elasticsearch.xpack.esql.analysis.EnrichResolution;
import org.elasticsearch.xpack.esql.analysis.IpLocationResolution;
import org.elasticsearch.xpack.esql.analysis.Verifier;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.datasources.ExternalSourceResolution;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.expression.promql.function.PromqlFunctionRegistry;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.IndexProperties;
import org.elasticsearch.xpack.esql.index.IndexResolution;
import org.elasticsearch.xpack.esql.inference.InferenceResolution;
import org.elasticsearch.xpack.esql.inference.InferenceSettings;
import org.elasticsearch.xpack.esql.optimizer.LogicalPlanOptimizer;
import org.elasticsearch.xpack.esql.parser.EsqlConfig;
import org.elasticsearch.xpack.esql.parser.EsqlParser;
import org.elasticsearch.xpack.esql.parser.QueryParams;
import org.elasticsearch.xpack.esql.plan.IndexPattern;
import org.elasticsearch.xpack.esql.plan.ResolvedSettings;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.elasticsearch.xpack.esql.session.Configuration;
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

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.logicalOptimizerContext;
import static org.elasticsearch.xpack.esql.core.type.DataType.TEXT;
import static org.elasticsearch.xpack.esql.plan.QuerySettings.UNMAPPED_FIELDS;

@Fork(1)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 10, time = 1, timeUnit = TimeUnit.SECONDS)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
public class QueryPlanningBenchmark {

    static {
        BenchmarkLogging.configure();
    }

    /**
     * The fixed 10,000 field index that {@link #manyFields} plans against, which its nightly chart has always tracked. It is a state of
     * its own so that the FILLNULL benchmarks, which run in the same JVM, do not also build and keep this planner.
     */
    @State(Scope.Benchmark)
    public static class FixedWidth {
        private Planner planner;

        @Setup
        public void setup() {
            planner = new Planner(10_000);
        }
    }

    /**
     * Holds the width of the index for {@link #manyFieldsFillNull} only, so that the {@link #manyFields} result keeps its single
     * series. A {@code @Param} on the benchmark class itself would make JMH run every benchmark method once per value, and the
     * existing nightly chart for {@code manyFields}, which filters on the benchmark name alone, would then average the widths.
     */
    @State(Scope.Benchmark)
    public static class FillNullWidth {
        /**
         * {@code FILLNULL ... ON *} mirrors every column in a {@code COALESCE} alias, so planning cost grows with the field
         * count; the sweep follows the 1k/5k/10k shape called for in #163220.
         */
        @Param({ "1000", "5000", "10000" })
        public int fields;

        private Planner planner;

        @Setup
        public void setup() {
            planner = new Planner(fields);
        }
    }

    /**
     * An index of {@code fields} text fields with the parser, analyzer and optimizer that plan queries against it.
     */
    private static final class Planner {
        private final EsqlParser parser;
        private final Analyzer analyzer;
        private final LogicalPlanOptimizer optimizer;

        Planner(int fields) {
            var config = new Configuration(
                Instant.now(),
                Locale.US,
                null,
                null,
                QueryPragmas.EMPTY,
                AnalyzerSettings.QUERY_RESULT_TRUNCATION_MAX_SIZE.getDefault(Settings.EMPTY),
                AnalyzerSettings.QUERY_RESULT_TRUNCATION_DEFAULT_SIZE.getDefault(Settings.EMPTY),
                "",
                false,
                Map.of(),
                System.nanoTime(),
                false,
                AnalyzerSettings.QUERY_TIMESERIES_RESULT_TRUNCATION_DEFAULT_SIZE.getDefault(Settings.EMPTY),
                AnalyzerSettings.QUERY_TIMESERIES_RESULT_TRUNCATION_DEFAULT_SIZE.get(Settings.EMPTY),
                ResolvedSettings.EMPTY,
                Map.of()
            );

            var mapping = LinkedHashMap.<String, EsField>newLinkedHashMap(fields);
            for (int i = 0; i < fields; i++) {
                // We're creating a standard index, so none of these fields should be marked as dimensions.
                mapping.put("field" + i, new EsField("field-" + i, TEXT, emptyMap(), true, EsField.TimeSeriesFieldType.NONE));
            }

            var esIndex = new EsIndex("test", mapping, Map.of("test", new IndexProperties(IndexMode.STANDARD, 0)), Map.of(), Map.of());

            var functionRegistry = new EsqlFunctionRegistry();
            // FILLNULL is a dev-only command, so force the dev version on regardless of how the benchmark jar was built.
            var parser = new EsqlParser(new EsqlConfig(true, functionRegistry));

            // Assume all nodes are on the current version for the benchmark.
            TransportVersion minimumVersion = TransportVersion.current();

            var analyzer = new Analyzer(
                new AnalyzerContext(
                    config,
                    functionRegistry,
                    PromqlFunctionRegistry.INSTANCE,
                    EsqlTestUtils.TEST_ANALYSIS_REGISTRY,
                    null,
                    Map.of(new IndexPattern(Source.EMPTY, esIndex.name()), IndexResolution.valid(esIndex)),
                    Map.of(),
                    Map.of(),
                    new EnrichResolution(),
                    InferenceResolution.EMPTY,
                    ExternalSourceResolution.EMPTY,
                    minimumVersion,
                    UNMAPPED_FIELDS.defaultValue(),
                    null,
                    IpLocationResolution.SERVICE_UNAVAILABLE,
                    false,
                    Map.of(),
                    // The default flags cap FILLNULL at 1,000 columns, which fails verification of the 5,000 and 10,000 field cases.
                    EsqlFlags.withMaxFillNullFields(fields),
                    Map.of()
                ),
                new Verifier(new Metrics(functionRegistry, true, true), new XPackLicenseState(() -> 0L))
            );
            var optimizer = new LogicalPlanOptimizer(logicalOptimizerContext(config, FoldContext.small(), minimumVersion));

            this.parser = parser;
            this.analyzer = analyzer;
            this.optimizer = optimizer;
        }

        LogicalPlan plan(String query) {
            var parsed = parser.parseQuery(query, new QueryParams(), new InferenceSettings(Settings.EMPTY));
            var analyzed = analyzer.analyze(parsed);
            return optimizer.optimize(analyzed);
        }
    }

    @Benchmark
    public void manyFields(FixedWidth width, Blackhole blackhole) {
        blackhole.consume(width.planner.plan("FROM test | LIMIT 10"));
    }

    /**
     * The {@link #manyFields} query at each swept width: the baseline for {@link #manyFieldsFillNull} at the same width, since the
     * planning cost without FILLNULL also grows with the mapping.
     */
    @Benchmark
    public void manyFieldsSweepBaseline(FillNullWidth width, Blackhole blackhole) {
        blackhole.consume(width.planner.plan("FROM test | LIMIT 10"));
    }

    /**
     * A {@code FILLNULL DEFAULT ON *} added to the {@link #manyFieldsSweepBaseline} query: the delta between the two at the same width
     * is the planning cost of mirroring every column in a {@code COALESCE}, which #163220 asks to track at 1k/5k/10k fields.
     */
    @Benchmark
    public void manyFieldsFillNull(FillNullWidth width, Blackhole blackhole) {
        blackhole.consume(width.planner.plan("FROM test | FILLNULL DEFAULT ON * | LIMIT 10"));
    }
}
