/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.analysis.Analyzer;
import org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils;
import org.elasticsearch.xpack.esql.analysis.EnrichResolution;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.EsIndexGenerator;
import org.elasticsearch.xpack.esql.parser.EsqlParser;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.session.Configuration;
import org.elasticsearch.xpack.esql.stats.SearchStats;
import org.junit.BeforeClass;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_SEARCH_STATS;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_VERIFIER;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.emptyInferenceResolution;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.emptyPolicyResolution;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.loadMapping;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.testAnalyzerContext;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.unboundLogicalOptimizerContext;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.withDefaultLimitWarning;
import static org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils.defaultLookupResolution;
import static org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils.indexResolutions;

public class AbstractLocalLogicalPlanOptimizerTests extends ESTestCase {

    protected static Analyzer analyzer;
    protected static Analyzer allTypesAnalyzer;
    protected static Analyzer tsAnalyzer;
    protected static Analyzer metricsAnalyzer;
    protected static LogicalPlanOptimizer logicalOptimizer;
    protected static Map<String, EsField> mapping;

    @BeforeClass
    protected static void init() {
        mapping = loadMapping("mapping-basic.json");
        EsIndex test = EsIndexGenerator.esIndex("test", mapping, Map.of("test", IndexMode.STANDARD));
        logicalOptimizer = new LogicalPlanOptimizer(unboundLogicalOptimizerContext());

        analyzer = new Analyzer(
            testAnalyzerContext(
                EsqlTestUtils.TEST_CFG,
                new EsqlFunctionRegistry(),
                indexResolutions(test),
                defaultLookupResolution(),
                emptyPolicyResolution(),
                emptyInferenceResolution()
            ),
            TEST_VERIFIER
        );

        var allTypesMapping = loadMapping("mapping-all-types.json");
        EsIndex testAll = EsIndexGenerator.esIndex("test_all", allTypesMapping, Map.of("test_all", IndexMode.STANDARD));
        allTypesAnalyzer = new Analyzer(
            testAnalyzerContext(
                EsqlTestUtils.TEST_CFG,
                new EsqlFunctionRegistry(),
                indexResolutions(testAll),
                emptyPolicyResolution(),
                emptyInferenceResolution()
            ),
            TEST_VERIFIER
        );

        var tsMapping = loadMapping("k8s-mappings.json");
        var tsIndex = EsIndexGenerator.esIndex("k8s", tsMapping, Map.of("k8s", IndexMode.TIME_SERIES));
        var tsDownsampledMapping = loadMapping("k8s-downsampled-mappings.json");
        var tsDownsampledIndex = EsIndexGenerator.esIndex(
            "k8s-downsampled",
            tsDownsampledMapping,
            Map.of("k8s-downsampled", IndexMode.TIME_SERIES)
        );

        tsAnalyzer = new Analyzer(
            testAnalyzerContext(
                EsqlTestUtils.TEST_CFG,
                new EsqlFunctionRegistry(),
                indexResolutions(tsIndex, tsDownsampledIndex),
                emptyPolicyResolution(),
                emptyInferenceResolution()
            ),
            TEST_VERIFIER
        );

        EnrichResolution enrichResolution = new EnrichResolution();
        AnalyzerTestUtils.loadEnrichPolicyResolution(enrichResolution, "languages_idx", "id", "languages_idx", "mapping-languages.json");

        Map<String, EsField> mapping = Map.of(
            "dimension_1",
            new EsField("dimension_1", DataType.KEYWORD, Map.of(), true, EsField.TimeSeriesFieldType.DIMENSION),
            "dimension_2",
            new EsField("dimension_2", DataType.KEYWORD, Map.of(), true, EsField.TimeSeriesFieldType.DIMENSION),
            "metric_1",
            new EsField("metric_1", DataType.LONG, Map.of(), true, EsField.TimeSeriesFieldType.METRIC),
            "metric_2",
            new EsField("metric_2", DataType.LONG, Map.of(), true, EsField.TimeSeriesFieldType.METRIC),
            "@timestamp",
            new EsField("@timestamp", DataType.DATETIME, Map.of(), true, EsField.TimeSeriesFieldType.NONE),
            "_tsid",
            new EsField("_tsid", DataType.TSID_DATA_TYPE, Map.of(), true, EsField.TimeSeriesFieldType.NONE)
        );
        var metricsIndex = EsIndexGenerator.esIndex("test", mapping, Map.of("test", IndexMode.TIME_SERIES));
        metricsAnalyzer = new Analyzer(
            testAnalyzerContext(
                EsqlTestUtils.TEST_CFG,
                new EsqlFunctionRegistry(),
                indexResolutions(metricsIndex),
                defaultLookupResolution(),
                enrichResolution,
                emptyInferenceResolution()
            ),
            TEST_VERIFIER
        );
    }

    protected LogicalPlan plan(String query) {
        return plan(query, analyzer);
    }

    protected LogicalPlan plan(String query, Analyzer analyzer) {
        var analyzed = analyzer.analyze(EsqlParser.INSTANCE.parseQuery(query));
        return logicalOptimizer.optimize(analyzed);
    }

    protected LogicalPlan localPlan(String query) {
        return localPlan(plan(query), TEST_SEARCH_STATS);
    }

    public LogicalPlan localPlan(String query, Analyzer analyzer) {
        return localPlan(plan(query, analyzer), TEST_SEARCH_STATS);
    }

    protected LogicalPlan localPlan(LogicalPlan plan, SearchStats searchStats) {
        return localPlan(plan, EsqlTestUtils.TEST_CFG, searchStats);
    }

    protected LogicalPlan localPlan(LogicalPlan plan, Configuration configuration, SearchStats searchStats) {
        var localContext = new LocalLogicalOptimizerContext(configuration, FoldContext.small(), searchStats);
        return new LocalLogicalPlanOptimizer(localContext).localOptimize(plan);
    }

    @Override
    protected List<String> filteredWarnings() {
        return withDefaultLimitWarning(super.filteredWarnings());
    }
}
