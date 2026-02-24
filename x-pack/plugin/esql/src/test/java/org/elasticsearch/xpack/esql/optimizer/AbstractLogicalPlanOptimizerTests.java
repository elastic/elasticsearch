/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.analysis.Analyzer;
import org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils;
import org.elasticsearch.xpack.esql.analysis.EnrichResolution;
import org.elasticsearch.xpack.esql.analysis.MutableAnalyzerContext;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.type.InvalidMappedField;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.EsIndexGenerator;
import org.elasticsearch.xpack.esql.parser.EsqlParser;
import org.elasticsearch.xpack.esql.plan.logical.Enrich;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.junit.BeforeClass;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.xpack.core.enrich.EnrichPolicy.MATCH_TYPE;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_VERIFIER;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.emptyInferenceResolution;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.loadMapping;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.testAnalyzerContext;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.unboundLogicalOptimizerContext;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.withDefaultLimitWarning;
import static org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils.defaultInferenceResolution;
import static org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils.defaultLookupResolution;
import static org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils.defaultSubqueryResolution;
import static org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils.indexResolutions;
import static org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils.mergeIndexResolutions;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.hamcrest.Matchers.containsString;

public abstract class AbstractLogicalPlanOptimizerTests extends ESTestCase {
    protected static EsqlParser parser = EsqlParser.INSTANCE;
    protected static LogicalOptimizerContext logicalOptimizerCtx;
    protected static LogicalPlanOptimizer logicalOptimizer;

    protected static LogicalPlanOptimizer logicalOptimizerWithLatestVersion;

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
    protected static Analyzer unionIndexAnalyzer;
    protected static Analyzer sampleDataIndexAnalyzer;
    protected static Analyzer subqueryAnalyzer;
    protected static Map<String, EsField> mappingBaseConversion;
    protected static Analyzer baseConversionAnalyzer;

    protected static EnrichResolution enrichResolution;

    public static class TestSubstitutionOnlyOptimizer extends LogicalPlanOptimizer {
        // A static instance of this would break the EsqlNodeSubclassTests because its initialization requires a Random instance.

        public TestSubstitutionOnlyOptimizer() {
            super(unboundLogicalOptimizerContext());
        }

        @Override
        protected List<Batch<LogicalPlan>> batches() {
            return List.of(substitutions());
        }
    }

    @BeforeClass
    public static void init() {
        logicalOptimizerCtx = unboundLogicalOptimizerContext();
        logicalOptimizer = new LogicalPlanOptimizer(logicalOptimizerCtx);
        logicalOptimizerWithLatestVersion = new LogicalPlanOptimizer(
            new LogicalOptimizerContext(logicalOptimizerCtx.configuration(), logicalOptimizerCtx.foldCtx(), TransportVersion.current())
        );
        enrichResolution = new EnrichResolution();
        AnalyzerTestUtils.loadEnrichPolicyResolution(enrichResolution, "languages_idx", "id", "languages_idx", "mapping-languages.json");
        AnalyzerTestUtils.loadEnrichPolicyResolution(
            enrichResolution,
            Enrich.Mode.REMOTE,
            MATCH_TYPE,
            "languages_remote",
            "id",
            "languages_idx",
            "mapping-languages.json"
        );
        AnalyzerTestUtils.loadEnrichPolicyResolution(
            enrichResolution,
            Enrich.Mode.COORDINATOR,
            MATCH_TYPE,
            "languages_coordinator",
            "id",
            "languages_idx",
            "mapping-languages.json"
        );

        // Most tests use either "test" or "employees" as the index name, but for the same mapping
        mapping = loadMapping("mapping-basic.json");
        EsIndex test = EsIndexGenerator.esIndex("test", mapping, Map.of("test", IndexMode.STANDARD));
        EsIndex employees = EsIndexGenerator.esIndex("employees", mapping, Map.of("employees", IndexMode.STANDARD));
        analyzer = new Analyzer(
            testAnalyzerContext(
                EsqlTestUtils.TEST_CFG,
                new EsqlFunctionRegistry(),
                indexResolutions(test, employees),
                defaultLookupResolution(),
                enrichResolution,
                emptyInferenceResolution()
            ),
            TEST_VERIFIER
        );

        // Some tests use data from the airports index, so we load it here, and use it in the planAirports() function.
        mappingAirports = loadMapping("mapping-airports.json");
        EsIndex airports = EsIndexGenerator.esIndex("airports", mappingAirports, Map.of("airports", IndexMode.STANDARD));
        analyzerAirports = new Analyzer(
            testAnalyzerContext(
                EsqlTestUtils.TEST_CFG,
                new EsqlFunctionRegistry(),
                indexResolutions(airports),
                defaultLookupResolution(),
                enrichResolution,
                emptyInferenceResolution()
            ),
            TEST_VERIFIER
        );

        // Some tests need additional types, so we load that index here and use it in the plan_types() function.
        mappingTypes = loadMapping("mapping-all-types.json");
        EsIndex types = EsIndexGenerator.esIndex("types", mappingTypes, Map.of("types", IndexMode.STANDARD));
        analyzerTypes = new Analyzer(
            testAnalyzerContext(
                EsqlTestUtils.TEST_CFG,
                new EsqlFunctionRegistry(),
                indexResolutions(types),
                enrichResolution,
                defaultInferenceResolution()
            ),
            TEST_VERIFIER
        );

        // Some tests use mappings from mapping-extra.json to be able to test more types so we load it here
        mappingExtra = loadMapping("mapping-extra.json");
        EsIndex extra = EsIndexGenerator.esIndex("extra", mappingExtra, Map.of("extra", IndexMode.STANDARD));
        analyzerExtra = new Analyzer(
            testAnalyzerContext(
                EsqlTestUtils.TEST_CFG,
                new EsqlFunctionRegistry(),
                indexResolutions(extra),
                enrichResolution,
                emptyInferenceResolution()
            ),
            TEST_VERIFIER
        );

        List<EsIndex> metricIndices = new ArrayList<>();
        Map<String, EsField> expHistoMetricMapping = loadMapping("exp_histo_sample-mappings.json");
        metricIndices.add(
            EsIndexGenerator.esIndex("exp_histo_sample", expHistoMetricMapping, Map.of("exp_histo_sample", IndexMode.TIME_SERIES))
        );
        Map<String, EsField> tdigestMapping = loadMapping("tdigest_timeseries_index-mappings.json");
        metricIndices.add(
            EsIndexGenerator.esIndex("tdigest_timeseries_index", tdigestMapping, Map.of("tdigest_timeseries_index", IndexMode.TIME_SERIES))
        );
        metricMapping = loadMapping("k8s-mappings.json");
        metricIndices.add(EsIndexGenerator.esIndex("k8s", metricMapping, Map.of("k8s", IndexMode.TIME_SERIES)));
        metricsAnalyzer = new Analyzer(
            testAnalyzerContext(
                EsqlTestUtils.TEST_CFG,
                new EsqlFunctionRegistry(),
                indexResolutions(metricIndices.toArray(EsIndex[]::new)),
                enrichResolution,
                emptyInferenceResolution()
            ),
            TEST_VERIFIER
        );

        var multiIndexMapping = loadMapping("mapping-basic.json");
        multiIndexMapping.put(
            "partial_type_keyword",
            new EsField("partial_type_keyword", KEYWORD, emptyMap(), true, EsField.TimeSeriesFieldType.NONE)
        );
        var multiIndex = new EsIndex(
            "multi_index",
            multiIndexMapping,
            Map.of("test1", IndexMode.STANDARD, "test2", IndexMode.STANDARD),
            Map.of(),
            Map.of(),
            Set.of("partial_type_keyword")
        );
        multiIndexAnalyzer = new Analyzer(
            testAnalyzerContext(
                EsqlTestUtils.TEST_CFG,
                new EsqlFunctionRegistry(),
                indexResolutions(multiIndex),
                enrichResolution,
                emptyInferenceResolution()
            ),
            TEST_VERIFIER
        );

        // Create a union index with conflicting types (keyword vs integer) for field 'id'
        var typesToIndices_languages = new LinkedHashMap<String, Set<String>>();
        typesToIndices_languages.put("byte", Set.of("union_types_index"));
        typesToIndices_languages.put("integer", Set.of("union_types_index_incompatible"));
        EsField languages = new InvalidMappedField("languages", typesToIndices_languages);

        var typesToIndices_lastName = new LinkedHashMap<String, Set<String>>();
        typesToIndices_lastName.put("text", Set.of("union_types_index"));
        typesToIndices_lastName.put("keyword", Set.of("union_types_index_incompatible"));
        EsField lastName = new InvalidMappedField("last_name", typesToIndices_lastName);

        var typesToIndices_salaryChange = new LinkedHashMap<String, Set<String>>();
        typesToIndices_salaryChange.put("float", Set.of("union_types_index"));
        typesToIndices_salaryChange.put("double", Set.of("union_types_index_incompatible"));
        EsField salaryChange = new InvalidMappedField("salary_change", typesToIndices_salaryChange);

        var typesToIndices_firstName = new LinkedHashMap<String, Set<String>>();
        typesToIndices_firstName.put("text", Set.of("union_types_index"));
        typesToIndices_firstName.put("keyword", Set.of("union_types_index_incompatible"));
        EsField firstName = new InvalidMappedField("first_name", typesToIndices_firstName);

        EsField idField = new EsField("id", KEYWORD, emptyMap(), true, EsField.TimeSeriesFieldType.NONE);
        var unionIndex = new EsIndex(
            "union_types_index*",
            Map.of("languages", languages, "last_name", lastName, "salary_change", salaryChange, "first_name", firstName, "id", idField),
            Map.of("union_types_index", IndexMode.STANDARD, "union_types_index_incompatible", IndexMode.STANDARD),
            Map.of("", List.of("union_types_index*")),
            Map.of("", List.of("union_types_index_incompatible", "union_types_index")),
            Set.of()
        );
        unionIndexAnalyzer = new Analyzer(
            testAnalyzerContext(
                EsqlTestUtils.TEST_CFG,
                new EsqlFunctionRegistry(),
                indexResolutions(unionIndex),
                defaultLookupResolution(),
                enrichResolution,
                emptyInferenceResolution()
            ),
            TEST_VERIFIER
        );

        var sampleDataMapping = loadMapping("mapping-sample_data.json");
        var sampleDataIndex = new EsIndex(
            "sample_data",
            sampleDataMapping,
            Map.of("sample_data", IndexMode.STANDARD),
            Map.of(),
            Map.of(),
            Set.of()
        );
        sampleDataIndexAnalyzer = new Analyzer(
            testAnalyzerContext(
                EsqlTestUtils.TEST_CFG,
                new EsqlFunctionRegistry(),
                indexResolutions(sampleDataIndex),
                enrichResolution,
                emptyInferenceResolution()
            ),
            TEST_VERIFIER
        );

        subqueryAnalyzer = new Analyzer(
            testAnalyzerContext(
                EsqlTestUtils.TEST_CFG,
                new EsqlFunctionRegistry(),
                mergeIndexResolutions(indexResolutions(test), defaultSubqueryResolution()),
                defaultLookupResolution(),
                enrichResolution,
                emptyInferenceResolution()
            ),
            TEST_VERIFIER
        );

        // Some tests use data from the baseConversion index, so we load it here
        mappingBaseConversion = loadMapping("mapping-base_conversion.json");
        EsIndex baseConversion = new EsIndex(
            "base_conversion",
            mappingBaseConversion,
            Map.of("base_conversion", IndexMode.STANDARD),
            Map.of(),
            Map.of(),
            Set.of()
        );
        baseConversionAnalyzer = new Analyzer(
            testAnalyzerContext(
                EsqlTestUtils.TEST_CFG,
                new EsqlFunctionRegistry(),
                indexResolutions(baseConversion),
                defaultLookupResolution(),
                enrichResolution,
                emptyInferenceResolution()
            ),
            TEST_VERIFIER
        );
    }

    protected LogicalPlan optimizedPlan(String query) {
        return plan(query);
    }

    protected LogicalPlan optimizedPlan(String query, TransportVersion transportVersion) {
        MutableAnalyzerContext mutableContext = (MutableAnalyzerContext) analyzer.context();
        try (var restore = mutableContext.setTemporaryTransportVersionOnOrAfter(transportVersion)) {
            return optimizedPlan(query);
        }
    }

    protected LogicalPlan plan(String query) {
        return plan(query, logicalOptimizer);
    }

    protected LogicalPlan plan(String query, LogicalPlanOptimizer optimizer) {
        var analyzed = analyzer.analyze(parser.parseQuery(query));
        var optimized = optimizer.optimize(analyzed);
        return optimized;
    }

    protected LogicalPlan planAirports(String query) {
        var analyzed = analyzerAirports.analyze(parser.parseQuery(query));
        var optimized = logicalOptimizer.optimize(analyzed);
        return optimized;
    }

    protected LogicalPlan planExtra(String query) {
        var analyzed = analyzerExtra.analyze(parser.parseQuery(query));
        var optimized = logicalOptimizer.optimize(analyzed);
        return optimized;
    }

    protected LogicalPlan planTypes(String query) {
        return logicalOptimizer.optimize(analyzerTypes.analyze(parser.parseQuery(query)));
    }

    protected LogicalPlan planMultiIndex(String query) {
        return logicalOptimizer.optimize(multiIndexAnalyzer.analyze(parser.parseQuery(query)));
    }

    protected LogicalPlan planUnionIndex(String query) {
        return logicalOptimizer.optimize(unionIndexAnalyzer.analyze(parser.parseQuery(query)));
    }

    protected LogicalPlan planSample(String query) {
        var analyzed = sampleDataIndexAnalyzer.analyze(parser.parseQuery(query));
        return logicalOptimizer.optimize(analyzed);
    }

    protected LogicalPlan planSubquery(String query) {
        var analyzed = subqueryAnalyzer.analyze(parser.parseQuery(query));
        return logicalOptimizer.optimize(analyzed);
    }

    @Override
    protected List<String> filteredWarnings() {
        return withDefaultLimitWarning(super.filteredWarnings());
    }

    protected <T extends Throwable> void failPlan(String esql, Class<T> exceptionClass, String reason) {
        var e = expectThrows(exceptionClass, () -> plan(esql));
        assertThat(e.getMessage(), containsString(reason));
    }

    protected void failPlan(String esql, String reason) {
        failPlan(esql, VerificationException.class, reason);
    }

}
