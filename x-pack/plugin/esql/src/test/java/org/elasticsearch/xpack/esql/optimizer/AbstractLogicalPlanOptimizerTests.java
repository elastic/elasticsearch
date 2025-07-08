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
import org.elasticsearch.xpack.esql.analysis.AnalyzerContext;
import org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils;
import org.elasticsearch.xpack.esql.analysis.EnrichResolution;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.IndexResolution;
import org.elasticsearch.xpack.esql.parser.EsqlParser;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.junit.BeforeClass;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_VERIFIER;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.emptyInferenceResolution;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.loadMapping;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.unboundLogicalOptimizerContext;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.withDefaultLimitWarning;
import static org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils.analyze;
import static org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils.defaultLookupResolution;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;

public abstract class AbstractLogicalPlanOptimizerTests extends ESTestCase {
    protected static EsqlParser parser;
    protected static LogicalOptimizerContext logicalOptimizerCtx;
    protected static LogicalPlanOptimizer logicalOptimizer;

    protected static Map<String, EsField> mapping;
    protected static Analyzer analyzer;
    protected static Map<String, EsField> mappingAirports;
    protected static Analyzer analyzerAirports;
    protected static Map<String, EsField> mappingTypes;
    protected static Analyzer analyzerTypes;
    protected static Map<String, EsField> mappingExtra;
    protected static Analyzer analyzerExtra;
    protected static Map<String, EsField> metricMapping;
    protected static Analyzer metricsAnalyzer;
    protected static Analyzer multiIndexAnalyzer;

    protected static EnrichResolution enrichResolution;

    public static class SubstitutionOnlyOptimizer extends LogicalPlanOptimizer {
        public static SubstitutionOnlyOptimizer INSTANCE = new SubstitutionOnlyOptimizer(unboundLogicalOptimizerContext());

        SubstitutionOnlyOptimizer(LogicalOptimizerContext optimizerContext) {
            super(optimizerContext);
        }

        @Override
        protected List<Batch<LogicalPlan>> batches() {
            return List.of(substitutions());
        }
    }

    @BeforeClass
    public static void init() {
        parser = new EsqlParser();
        logicalOptimizerCtx = unboundLogicalOptimizerContext();
        logicalOptimizer = new LogicalPlanOptimizer(logicalOptimizerCtx);
        enrichResolution = new EnrichResolution();
        AnalyzerTestUtils.loadEnrichPolicyResolution(enrichResolution, "languages_idx", "id", "languages_idx", "mapping-languages.json");

        // Most tests used data from the test index, so we load it here, and use it in the plan() function.
        mapping = loadMapping("mapping-basic.json");
        EsIndex test = new EsIndex("test", mapping, Map.of("test", IndexMode.STANDARD));
        IndexResolution getIndexResult = IndexResolution.valid(test);
        analyzer = new Analyzer(
            new AnalyzerContext(
                EsqlTestUtils.TEST_CFG,
                new EsqlFunctionRegistry(),
                getIndexResult,
                defaultLookupResolution(),
                enrichResolution,
                emptyInferenceResolution()
            ),
            TEST_VERIFIER
        );

        // Some tests use data from the airports index, so we load it here, and use it in the plan_airports() function.
        mappingAirports = loadMapping("mapping-airports.json");
        EsIndex airports = new EsIndex("airports", mappingAirports, Map.of("airports", IndexMode.STANDARD));
        IndexResolution getIndexResultAirports = IndexResolution.valid(airports);
        analyzerAirports = new Analyzer(
            new AnalyzerContext(
                EsqlTestUtils.TEST_CFG,
                new EsqlFunctionRegistry(),
                getIndexResultAirports,
                enrichResolution,
                emptyInferenceResolution()
            ),
            TEST_VERIFIER
        );

        // Some tests need additional types, so we load that index here and use it in the plan_types() function.
        mappingTypes = loadMapping("mapping-all-types.json");
        EsIndex types = new EsIndex("types", mappingTypes, Map.of("types", IndexMode.STANDARD));
        IndexResolution getIndexResultTypes = IndexResolution.valid(types);
        analyzerTypes = new Analyzer(
            new AnalyzerContext(
                EsqlTestUtils.TEST_CFG,
                new EsqlFunctionRegistry(),
                getIndexResultTypes,
                enrichResolution,
                emptyInferenceResolution()
            ),
            TEST_VERIFIER
        );

        // Some tests use mappings from mapping-extra.json to be able to test more types so we load it here
        mappingExtra = loadMapping("mapping-extra.json");
        EsIndex extra = new EsIndex("extra", mappingExtra, Map.of("extra", IndexMode.STANDARD));
        IndexResolution getIndexResultExtra = IndexResolution.valid(extra);
        analyzerExtra = new Analyzer(
            new AnalyzerContext(
                EsqlTestUtils.TEST_CFG,
                new EsqlFunctionRegistry(),
                getIndexResultExtra,
                enrichResolution,
                emptyInferenceResolution()
            ),
            TEST_VERIFIER
        );

        metricMapping = loadMapping("k8s-mappings.json");
        var metricsIndex = IndexResolution.valid(new EsIndex("k8s", metricMapping, Map.of("k8s", IndexMode.TIME_SERIES)));
        metricsAnalyzer = new Analyzer(
            new AnalyzerContext(
                EsqlTestUtils.TEST_CFG,
                new EsqlFunctionRegistry(),
                metricsIndex,
                enrichResolution,
                emptyInferenceResolution()
            ),
            TEST_VERIFIER
        );

        var multiIndexMapping = loadMapping("mapping-basic.json");
        multiIndexMapping.put("partial_type_keyword", new EsField("partial_type_keyword", KEYWORD, emptyMap(), true));
        var multiIndex = IndexResolution.valid(
            new EsIndex(
                "multi_index",
                multiIndexMapping,
                Map.of("test1", IndexMode.STANDARD, "test2", IndexMode.STANDARD),
                Set.of("partial_type_keyword")
            )
        );
        multiIndexAnalyzer = new Analyzer(
            new AnalyzerContext(
                EsqlTestUtils.TEST_CFG,
                new EsqlFunctionRegistry(),
                multiIndex,
                enrichResolution,
                emptyInferenceResolution()
            ),
            TEST_VERIFIER
        );
    }

    protected LogicalPlan optimizedPlan(String query) {
        return plan(query);
    }

    protected LogicalPlan plan(String query) {
        return plan(query, logicalOptimizer);
    }

    protected LogicalPlan plan(String query, LogicalPlanOptimizer optimizer) {
        var analyzed = analyze(analyzer, parser.createStatement(query));
        // System.out.println(analyzed);
        var optimized = optimizer.optimize(analyzed);
        // System.out.println(optimized);
        return optimized;
    }

    protected LogicalPlan planAirports(String query) {
        var analyzed = analyze(analyzerAirports, parser.createStatement(query));
        // System.out.println(analyzed);
        var optimized = logicalOptimizer.optimize(analyzed);
        // System.out.println(optimized);
        return optimized;
    }

    protected LogicalPlan planExtra(String query) {
        var analyzed = analyze(analyzerExtra, parser.createStatement(query));
        // System.out.println(analyzed);
        var optimized = logicalOptimizer.optimize(analyzed);
        // System.out.println(optimized);
        return optimized;
    }

    protected LogicalPlan planTypes(String query) {
        return logicalOptimizer.optimize(analyze(analyzerTypes, parser.createStatement(query)));
    }

    protected LogicalPlan planMultiIndex(String query) {
        return logicalOptimizer.optimize(analyze(multiIndexAnalyzer, parser.createStatement(query)));
    }

    @Override
    protected List<String> filteredWarnings() {
        return withDefaultLimitWarning(super.filteredWarnings());
    }
}
