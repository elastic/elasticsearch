/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.TestAnalyzer;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.type.InvalidMappedField;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.plan.logical.Enrich;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.junit.BeforeClass;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.xpack.core.enrich.EnrichPolicy.MATCH_TYPE;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_PARSER;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.analyzer;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.configuration;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.loadMapping;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.unboundLogicalOptimizerContext;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.withDefaultLimitWarning;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.hamcrest.Matchers.containsString;

public abstract class AbstractLogicalPlanOptimizerTests extends ESTestCase {
    protected static LogicalOptimizerContext logicalOptimizerCtx;
    protected static LogicalPlanOptimizer logicalOptimizer;

    protected static LogicalPlanOptimizer logicalOptimizerWithLatestVersion;
    protected static LogicalPlanOptimizer optimizerWithoutForkImplicitLimit;

    protected static Map<String, EsField> mapping;

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
        mapping = loadMapping("mapping-basic.json");

        var config = configuration(
            new QueryPragmas(Settings.builder().put(QueryPragmas.FORK_IMPLICIT_LIMIT.getKey().toLowerCase(Locale.ROOT), false).build())
        );
        optimizerWithoutForkImplicitLimit = new LogicalPlanOptimizer(
            new LogicalOptimizerContext(config, FoldContext.small(), TransportVersion.current())
        );
    }

    protected static TestAnalyzer analyzerWithEnrichPolicies() {
        return analyzer().addEnrichPolicy(MATCH_TYPE, "languages_idx", "id", "languages_idx", "mapping-languages.json")
            .addEnrichPolicy(Enrich.Mode.REMOTE, MATCH_TYPE, "languages_remote", "id", "languages_idx", "mapping-languages.json")
            .addEnrichPolicy(Enrich.Mode.COORDINATOR, MATCH_TYPE, "languages_coordinator", "id", "languages_idx", "mapping-languages.json");
    }

    protected static TestAnalyzer defaultAnalyzer() {
        return analyzerWithEnrichPolicies().addEmployees("test").addEmployees().addLanguagesLookup().addTestLookup().addSpatialLookup();
    }

    protected static TestAnalyzer airportsAnalyzer() {
        return analyzerWithEnrichPolicies().addAirports().addLanguagesLookup().addTestLookup().addSpatialLookup();
    }

    protected static TestAnalyzer typesAnalyzer() {
        return analyzerWithEnrichPolicies().addAnalysisTestsInferenceResolution().addIndex("types", "mapping-all-types.json");
    }

    protected static TestAnalyzer extraAnalyzer() {
        return analyzerWithEnrichPolicies().addIndex("extra", "mapping-extra.json");
    }

    protected static TestAnalyzer metricsAnalyzer() {
        return analyzerWithEnrichPolicies().addIndex("exp_histo_sample", "exp_histo_sample-mappings.json", IndexMode.TIME_SERIES)
            .addIndex("tdigest_timeseries_index", "tdigest_timeseries_index-mappings.json", IndexMode.TIME_SERIES)
            .addK8s();
    }

    protected static TestAnalyzer multiIndexAnalyzer() {
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
            Map.of("partial_type_keyword", Set.of("test2"))
        );
        return analyzerWithEnrichPolicies().addIndex(multiIndex);
    }

    protected static TestAnalyzer unionIndexAnalyzer() {
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
            Map.of()
        );
        return analyzerWithEnrichPolicies().addAnalysisTestsInferenceResolution()
            .addIndex(unionIndex)
            .addLanguagesLookup()
            .addTestLookup()
            .addSpatialLookup();
    }

    protected static TestAnalyzer sampleDataAnalyzer() {
        return analyzerWithEnrichPolicies().addSampleData();
    }

    protected static TestAnalyzer subqueryAnalyzer() {
        return analyzerWithEnrichPolicies().addEmployees("test")
            .addLanguages()
            .addSampleData()
            .addDefaultIncompatible()
            .addIndex("colors", "mapping-colors.json")
            .addK8sDownsampled()
            .addRemoteMissingIndex()
            .addEmptyIndex()
            .addNoFieldsIndex()
            .addLanguagesLookup()
            .addTestLookup()
            .addSpatialLookup();
    }

    protected static TestAnalyzer baseConversionAnalyzer() {
        return analyzerWithEnrichPolicies().addIndex("base_conversion", "mapping-base_conversion.json")
            .addLanguagesLookup()
            .addTestLookup()
            .addSpatialLookup();
    }

    protected static TestAnalyzer analyzerWithoutForkImplicitLimit() {
        var config = configuration(
            new QueryPragmas(Settings.builder().put(QueryPragmas.FORK_IMPLICIT_LIMIT.getKey().toLowerCase(Locale.ROOT), false).build())
        );
        return analyzerWithEnrichPolicies().configuration(config)
            .addEmployees("test")
            .addEmployees()
            .addLanguagesLookup()
            .addTestLookup()
            .addSpatialLookup();
    }

    protected LogicalPlan optimize(LogicalPlan plan) {
        return logicalOptimizer.optimize(plan);
    }

    protected LogicalPlan optimizedPlan(String query) {
        return plan(query);
    }

    protected LogicalPlan optimizedPlan(String query, TransportVersion transportVersion) {
        return optimize(defaultAnalyzer().minimumTransportVersion(transportVersion).buildAnalyzer().analyze(TEST_PARSER.parseQuery(query)));
    }

    protected LogicalPlan plan(String query) {
        return plan(query, logicalOptimizer);
    }

    protected LogicalPlan plan(String query, LogicalPlanOptimizer optimizer) {
        return optimizer.optimize(defaultAnalyzer().query(query));
    }

    protected LogicalPlan planAirports(String query) {
        return optimize(airportsAnalyzer().query(query));
    }

    protected LogicalPlan planExtra(String query) {
        return optimize(extraAnalyzer().query(query));
    }

    protected LogicalPlan planTypes(String query) {
        return optimize(typesAnalyzer().query(query));
    }

    protected LogicalPlan planMetrics(String query) {
        return logicalOptimizerWithLatestVersion.optimize(metricsAnalyzer().query(query));
    }

    protected LogicalPlan planMultiIndex(String query) {
        return optimize(multiIndexAnalyzer().query(query));
    }

    protected LogicalPlan planUnionIndex(String query) {
        return optimize(unionIndexAnalyzer().query(query));
    }

    protected LogicalPlan planSample(String query) {
        return optimize(sampleDataAnalyzer().query(query));
    }

    protected LogicalPlan planSubquery(String query) {
        return optimize(subqueryAnalyzer().query(query));
    }

    protected LogicalPlan planWithoutForkImplicitLimit(String query) {
        return optimizerWithoutForkImplicitLimit.optimize(analyzerWithoutForkImplicitLimit().query(query));
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
