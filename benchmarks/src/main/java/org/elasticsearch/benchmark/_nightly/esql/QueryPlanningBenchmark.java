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
import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.xpack.esql.analysis.Analyzer;
import org.elasticsearch.xpack.esql.analysis.AnalyzerContext;
import org.elasticsearch.xpack.esql.analysis.AnalyzerSettings;
import org.elasticsearch.xpack.esql.analysis.EnrichResolution;
import org.elasticsearch.xpack.esql.analysis.Verifier;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.util.DateUtils;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.IndexResolution;
import org.elasticsearch.xpack.esql.inference.InferenceResolution;
import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.LogicalPlanOptimizer;
import org.elasticsearch.xpack.esql.parser.EsqlParser;
import org.elasticsearch.xpack.esql.parser.QueryParams;
import org.elasticsearch.xpack.esql.plan.IndexPattern;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.elasticsearch.xpack.esql.session.Configuration;
import org.elasticsearch.xpack.esql.telemetry.Metrics;
import org.elasticsearch.xpack.esql.telemetry.PlanTelemetry;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.xpack.esql.core.type.DataType.TEXT;

@Fork(1)
@Warmup(iterations = 5)
@Measurement(iterations = 10)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
public class QueryPlanningBenchmark {

    static {
        LogConfigurator.configureESLogging();
    }

    private PlanTelemetry telemetry;
    private EsqlParser defaultParser;
    private Analyzer manyFieldsAnalyzer;
    private LogicalPlanOptimizer defaultOptimizer;
    private Configuration config;

    @Setup
    public void setup() {
        this.config = new Configuration(
            DateUtils.UTC,
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
            AnalyzerSettings.QUERY_TIMESERIES_RESULT_TRUNCATION_DEFAULT_SIZE.get(Settings.EMPTY)
        );

        var fields = 10_000;
        var mapping = LinkedHashMap.<String, EsField>newLinkedHashMap(fields);
        for (int i = 0; i < fields; i++) {
            // We're creating a standard index, so none of these fields should be marked as dimensions.
            mapping.put("field" + i, new EsField("field-" + i, TEXT, emptyMap(), true, EsField.TimeSeriesFieldType.NONE));
        }

        var esIndex = new EsIndex("test", mapping, Map.of("test", IndexMode.STANDARD), Set.of());

        var functionRegistry = new EsqlFunctionRegistry();

        // Assume all nodes are on the current version for the benchmark.
        TransportVersion minimumVersion = TransportVersion.current();

        telemetry = new PlanTelemetry(functionRegistry);
        defaultParser = new EsqlParser();
        manyFieldsAnalyzer = new Analyzer(
            new AnalyzerContext(
                config,
                functionRegistry,
                Map.of(new IndexPattern(Source.EMPTY, esIndex.name()), IndexResolution.valid(esIndex)),
                Map.of(),
                new EnrichResolution(),
                InferenceResolution.EMPTY,
                minimumVersion
            ),
            new Verifier(new Metrics(functionRegistry), new XPackLicenseState(() -> 0L))
        );
        defaultOptimizer = new LogicalPlanOptimizer(new LogicalOptimizerContext(config, FoldContext.small(), minimumVersion));
    }

    private LogicalPlan plan(EsqlParser parser, Analyzer analyzer, LogicalPlanOptimizer optimizer, String query) {
        var parsed = parser.createStatement(query, new QueryParams(), telemetry);
        var analyzed = analyzer.analyze(parsed);
        var optimized = optimizer.optimize(analyzed);
        return optimized;
    }

    @Benchmark
    public void manyFields(Blackhole blackhole) {
        blackhole.consume(plan(defaultParser, manyFieldsAnalyzer, defaultOptimizer, "FROM test | LIMIT 10"));
    }
}
