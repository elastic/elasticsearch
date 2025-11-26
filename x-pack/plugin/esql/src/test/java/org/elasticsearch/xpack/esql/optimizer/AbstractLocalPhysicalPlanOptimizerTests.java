/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.enrich.EnrichPolicy;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.analysis.Analyzer;
import org.elasticsearch.xpack.esql.analysis.EnrichResolution;
import org.elasticsearch.xpack.esql.analysis.Verifier;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.enrich.ResolvedEnrichPolicy;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.EsIndexGenerator;
import org.elasticsearch.xpack.esql.index.IndexResolution;
import org.elasticsearch.xpack.esql.plan.logical.Enrich;
import org.elasticsearch.xpack.esql.planner.FilterTests;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.elasticsearch.xpack.esql.session.Configuration;
import org.elasticsearch.xpack.esql.telemetry.Metrics;
import org.junit.Before;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_VERIFIER;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.configuration;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.emptyInferenceResolution;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.loadMapping;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.testAnalyzerContext;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.withDefaultLimitWarning;
import static org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils.defaultLookupResolution;
import static org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils.indexResolutions;

public class AbstractLocalPhysicalPlanOptimizerTests extends MapperServiceTestCase {
    protected final Configuration config;
    protected TestPlannerOptimizer plannerOptimizer;
    protected TestPlannerOptimizer plannerOptimizerDateDateNanosUnionTypes;
    protected TestPlannerOptimizer plannerOptimizerTimeSeries;
    private Analyzer timeSeriesAnalyzer;

    private static final String PARAM_FORMATTING = "%1$s";

    @ParametersFactory(argumentFormatting = PARAM_FORMATTING)
    public static List<Object[]> readScriptSpec() {
        return settings().stream().map(t -> {
            var settings = Settings.builder().loadFromMap(t.v2()).build();
            return new Object[] { t.v1(), configuration(new QueryPragmas(settings)) };
        }).toList();
    }

    private static List<Tuple<String, Map<String, Object>>> settings() {
        return List.of(new Tuple<>("default", Map.of()));
    }

    protected static QueryBuilder wrapWithSingleQuery(String query, QueryBuilder inner, String fieldName, Source source) {
        return FilterTests.singleValueQuery(query, inner, fieldName, source);
    }

    public AbstractLocalPhysicalPlanOptimizerTests(String name, Configuration config) {
        this.config = config;
    }

    @Before
    public void init() {
        EnrichResolution enrichResolution = new EnrichResolution();
        enrichResolution.addResolvedPolicy(
            "foo",
            Enrich.Mode.ANY,
            new ResolvedEnrichPolicy(
                "fld",
                EnrichPolicy.MATCH_TYPE,
                List.of("a", "b"),
                Map.of("", "idx"),
                Map.ofEntries(
                    Map.entry("a", new EsField("a", DataType.INTEGER, Map.of(), true, EsField.TimeSeriesFieldType.NONE)),
                    Map.entry("b", new EsField("b", DataType.LONG, Map.of(), true, EsField.TimeSeriesFieldType.NONE))
                )
            )
        );
        plannerOptimizer = new TestPlannerOptimizer(config, makeAnalyzer("mapping-basic.json", enrichResolution));
        var timeSeriesMapping = loadMapping("k8s-mappings.json");
        var timeSeriesIndex = IndexResolution.valid(
            EsIndexGenerator.esIndex("k8s", timeSeriesMapping, Map.of("k8s", IndexMode.TIME_SERIES))
        );
        timeSeriesAnalyzer = new Analyzer(
            testAnalyzerContext(
                EsqlTestUtils.TEST_CFG,
                new EsqlFunctionRegistry(),
                indexResolutions(timeSeriesIndex),
                enrichResolution,
                emptyInferenceResolution()
            ),
            TEST_VERIFIER
        );
        plannerOptimizerTimeSeries = new TestPlannerOptimizer(
            config,
            timeSeriesAnalyzer,
            new LogicalPlanOptimizer(new LogicalOptimizerContext(config, FoldContext.small(), TransportVersion.current()))
        );
    }

    private Analyzer makeAnalyzer(String mappingFileName, EnrichResolution enrichResolution) {
        var mapping = loadMapping(mappingFileName);
        EsIndex test = EsIndexGenerator.esIndex("test", mapping, Map.of("test", IndexMode.STANDARD));

        return new Analyzer(
            testAnalyzerContext(
                config,
                new EsqlFunctionRegistry(),
                indexResolutions(test),
                defaultLookupResolution(),
                enrichResolution,
                emptyInferenceResolution()
            ),
            new Verifier(new Metrics(new EsqlFunctionRegistry()), new XPackLicenseState(() -> 0L))
        );
    }

    protected Analyzer makeAnalyzer(String mappingFileName) {
        return makeAnalyzer(mappingFileName, new EnrichResolution());
    }

    protected Analyzer makeAnalyzer(IndexResolution indexResolution) {
        return new Analyzer(
            testAnalyzerContext(
                config,
                new EsqlFunctionRegistry(),
                indexResolutions(indexResolution),
                new EnrichResolution(),
                emptyInferenceResolution()
            ),
            new Verifier(new Metrics(new EsqlFunctionRegistry()), new XPackLicenseState(() -> 0L))
        );
    }

    /**
     * This exempts the warning about adding the automatic limit from the warnings check in
     * {@link ESTestCase#ensureNoWarnings()}
     */
    @Override
    protected List<String> filteredWarnings() {
        return withDefaultLimitWarning(super.filteredWarnings());
    }

}
