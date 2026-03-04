/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.analysis.Analyzer;
import org.elasticsearch.xpack.esql.analysis.AnalyzerContext;
import org.elasticsearch.xpack.esql.analysis.EnrichResolution;
import org.elasticsearch.xpack.esql.analysis.UnmappedResolution;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.datasources.ExternalSourceMetadata;
import org.elasticsearch.xpack.esql.datasources.ExternalSourceResolution;
import org.elasticsearch.xpack.esql.datasources.FileSet;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.LogicalPlanOptimizer;
import org.elasticsearch.xpack.esql.optimizer.PhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.PhysicalPlanOptimizer;
import org.elasticsearch.xpack.esql.parser.EsqlParser;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.EstimatesRowSize;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeExec;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.plan.physical.LocalSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import org.elasticsearch.xpack.esql.planner.mapper.Mapper;
import org.elasticsearch.xpack.esql.session.Configuration;
import org.elasticsearch.xpack.esql.session.Versioned;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_CFG;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_SEARCH_STATS;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_VERIFIER;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.emptyInferenceResolution;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.loadMapping;
import static org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils.defaultEnrichResolution;
import static org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils.indexResolutions;
import static org.elasticsearch.xpack.esql.core.tree.Source.EMPTY;
import static org.elasticsearch.xpack.esql.index.EsIndexGenerator.esIndex;

/**
 * Side-by-side comparison of the fully resolved physical plans for ES indices vs external sources.
 * <p>
 * Each test runs the same ES|QL query through the complete planning pipeline:
 * parse → analyze → logical optimize → map → physical optimize → local plan (FragmentExec expansion)
 * <p>
 * The resulting plans show exactly what the compute engine would receive. For ES indices, the
 * FragmentExec is expanded into the localized data-node plan (e.g., AggregateExec(INITIAL) →
 * EsQueryExec). For external sources, no such expansion occurs because there is no FragmentExec.
 * <p>
 * These tests are expected to FAIL, demonstrating the gap: external sources lack the ExchangeExec
 * that enables distributed execution.
 */
public class ExternalPipelineBreakerComparisonTests extends ESTestCase {

    private static final String EXTERNAL_LOCATION = "s3://bucket/data/*.parquet";
    private static final Configuration CONFIG = TEST_CFG;

    // ======================== STATS ========================

    /**
     * {@code STATS count = COUNT(*) | LIMIT 100}
     * <p>
     * ES index:    LimitExec → AggregateExec[FINAL] → ExchangeExec → AggregateExec[INITIAL] → EsQueryExec
     * External:    LimitExec → AggregateExec[SINGLE] → ExternalSourceExec
     */
    public void testStatsShouldDistributeForExternalSource() {
        assumeTrue("requires EXTERNAL command capability", EsqlCapabilities.Cap.EXTERNAL_COMMAND.isEnabled());

        String suffix = " | STATS count = COUNT(*) | LIMIT 100";
        PhysicalPlan esPlan = planEs("FROM test" + suffix);
        PhysicalPlan extPlan = planExternal("EXTERNAL \"" + EXTERNAL_LOCATION + "\"" + suffix);

        assertPlanContainsExchange("STATS count = COUNT(*)", esPlan, extPlan);

        AggregateExec esAgg = findFirst(esPlan, AggregateExec.class);
        AggregateExec extAgg = findFirst(extPlan, AggregateExec.class);
        assertNotNull("ES plan should have AggregateExec", esAgg);
        assertNotNull("External plan should have AggregateExec", extAgg);
        assertEquals(
            formatComparison("STATS aggregator mode", esPlan, extPlan)
                + "\nES index uses FINAL (two-phase), external source should too but uses SINGLE",
            esAgg.getMode(),
            extAgg.getMode()
        );
    }

    /**
     * {@code STATS avg = AVG(salary) BY gender | LIMIT 100} — grouped aggregation with surrogate decomposition.
     */
    public void testGroupedStatsShouldDistributeForExternalSource() {
        assumeTrue("requires EXTERNAL command capability", EsqlCapabilities.Cap.EXTERNAL_COMMAND.isEnabled());

        String suffix = " | STATS avg = AVG(salary) BY gender | LIMIT 100";
        PhysicalPlan esPlan = planEs("FROM test" + suffix);
        PhysicalPlan extPlan = planExternal("EXTERNAL \"" + EXTERNAL_LOCATION + "\"" + suffix);

        assertPlanContainsExchange("STATS AVG(salary) BY gender", esPlan, extPlan);
    }

    // ======================== LIMIT ========================

    /**
     * {@code LIMIT 10}
     * <p>
     * ES index:    LimitExec → ExchangeExec → LimitExec → EsQueryExec
     * External:    LimitExec → ExternalSourceExec
     */
    public void testLimitShouldDistributeForExternalSource() {
        assumeTrue("requires EXTERNAL command capability", EsqlCapabilities.Cap.EXTERNAL_COMMAND.isEnabled());

        PhysicalPlan esPlan = planEs("FROM test | LIMIT 10");
        PhysicalPlan extPlan = planExternal("EXTERNAL \"" + EXTERNAL_LOCATION + "\" | LIMIT 10");

        assertPlanContainsExchange("LIMIT 10", esPlan, extPlan);
    }

    // ======================== TopN (SORT + LIMIT) ========================

    /**
     * {@code SORT emp_no DESC | LIMIT 10}
     * <p>
     * ES index:    TopNExec → ExchangeExec → TopNExec → EsQueryExec
     * External:    TopNExec → ExternalSourceExec
     */
    public void testTopNShouldDistributeForExternalSource() {
        assumeTrue("requires EXTERNAL command capability", EsqlCapabilities.Cap.EXTERNAL_COMMAND.isEnabled());

        PhysicalPlan esPlan = planEs("FROM test | SORT emp_no DESC | LIMIT 10");
        PhysicalPlan extPlan = planExternal("EXTERNAL \"" + EXTERNAL_LOCATION + "\" | SORT emp_no DESC | LIMIT 10");

        assertPlanContainsExchange("SORT emp_no DESC | LIMIT 10", esPlan, extPlan);
    }

    // ======================== Full planning pipeline ========================

    private PhysicalPlan planEs(String query) {
        return fullPlan(query, makeEsAnalyzer());
    }

    private PhysicalPlan planExternal(String query) {
        return fullPlan(query, makeExternalAnalyzer());
    }

    /**
     * Complete planning pipeline: parse → analyze → logical optimize → map → physical optimize → localize.
     * <p>
     * The localize step expands FragmentExec nodes into the actual data-node plan via LocalMapper,
     * producing the plan exactly as the compute engine would receive it.
     */
    private PhysicalPlan fullPlan(String query, Analyzer analyzer) {
        // 1. Parse
        LogicalPlan parsed = EsqlParser.INSTANCE.parseQuery(query);

        // 2. Analyze
        LogicalPlan analyzed = analyzer.analyze(parsed);

        // 3. Logical optimize
        TransportVersion version = analyzer.context().minimumVersion();
        LogicalPlanOptimizer logicalOptimizer = new LogicalPlanOptimizer(new LogicalOptimizerContext(CONFIG, FoldContext.small(), version));
        LogicalPlan optimized = logicalOptimizer.optimize(analyzed);

        // 4. Map (logical → physical)
        PhysicalPlan physical = new Mapper().map(new Versioned<>(optimized, version));

        // 5. Physical optimize + row size estimation
        PhysicalPlanOptimizer physicalOptimizer = new PhysicalPlanOptimizer(new PhysicalOptimizerContext(CONFIG, version));
        PhysicalPlan optimizedPhysical = EstimatesRowSize.estimateRowSize(0, physicalOptimizer.optimize(physical));

        // 6. Localize — expand FragmentExec into the actual data-node plan (LocalMapper + local optimizers).
        // This is the plan that the compute engine receives.
        PhysicalPlan localized = optimizedPhysical.transformUp(FragmentExec.class, fragment -> {
            var flags = new EsqlFlags(true);
            var localPlan = PlannerUtils.localPlan(
                PlannerSettings.DEFAULTS,
                flags,
                CONFIG,
                FoldContext.small(),
                fragment,
                TEST_SEARCH_STATS,
                null
            );
            return EstimatesRowSize.estimateRowSize(fragment.estimatedRowSize(), localPlan);
        });

        // 7. Align local reduction outputs with exchange expectations
        return localized.transformUp(ExchangeExec.class, exg -> {
            if (exg.inBetweenAggs() && exg.child() instanceof LocalSourceExec lse) {
                var output = exg.output();
                if (lse.output().equals(output) == false) {
                    return exg.replaceChild(new LocalSourceExec(lse.source(), output, lse.supplier()));
                }
            }
            return exg;
        });
    }

    // ======================== Analyzer setup ========================

    private static Analyzer makeEsAnalyzer() {
        var mapping = loadMapping("mapping-basic.json");
        var index = esIndex("test", mapping, Map.of("test", org.elasticsearch.index.IndexMode.STANDARD));
        return new Analyzer(
            new AnalyzerContext(
                CONFIG,
                new EsqlFunctionRegistry(),
                null,
                indexResolutions(index),
                Map.of(),
                defaultEnrichResolution(),
                emptyInferenceResolution(),
                ExternalSourceResolution.EMPTY,
                TransportVersion.current(),
                UnmappedResolution.FAIL
            ),
            TEST_VERIFIER
        );
    }

    private static Analyzer makeExternalAnalyzer() {
        List<Attribute> schema = List.of(
            new FieldAttribute(EMPTY, "emp_no", new EsField("emp_no", DataType.INTEGER, Map.of(), false, EsField.TimeSeriesFieldType.NONE)),
            new FieldAttribute(EMPTY, "salary", new EsField("salary", DataType.INTEGER, Map.of(), false, EsField.TimeSeriesFieldType.NONE)),
            new FieldAttribute(EMPTY, "gender", new EsField("gender", DataType.KEYWORD, Map.of(), false, EsField.TimeSeriesFieldType.NONE))
        );

        ExternalSourceMetadata metadata = new ExternalSourceMetadata() {
            @Override
            public String location() {
                return EXTERNAL_LOCATION;
            }

            @Override
            public List<Attribute> schema() {
                return schema;
            }

            @Override
            public String sourceType() {
                return "parquet";
            }
        };

        var resolvedSource = new ExternalSourceResolution.ResolvedSource(metadata, FileSet.UNRESOLVED);
        var externalResolution = new ExternalSourceResolution(Map.of(EXTERNAL_LOCATION, resolvedSource));

        return new Analyzer(
            new AnalyzerContext(
                CONFIG,
                new EsqlFunctionRegistry(),
                null,
                Map.of(),
                Map.of(),
                new EnrichResolution(),
                emptyInferenceResolution(),
                externalResolution,
                TransportVersion.current(),
                UnmappedResolution.FAIL
            ),
            TEST_VERIFIER
        );
    }

    // ======================== Assertions & formatting ========================

    private static boolean containsExchange(PhysicalPlan plan) {
        return plan.anyMatch(node -> node instanceof ExchangeExec);
    }

    private static void assertPlanContainsExchange(String query, PhysicalPlan esPlan, PhysicalPlan extPlan) {
        assertTrue(
            "BUG IN TEST: ES index plan for [" + query + "] should contain ExchangeExec but doesn't:\n" + esPlan,
            containsExchange(esPlan)
        );

        assertTrue(
            formatComparison(query, esPlan, extPlan)
                + "\nExternal source plan should contain ExchangeExec for distributed execution, like ES indices",
            containsExchange(extPlan)
        );
    }

    private static String formatComparison(String query, PhysicalPlan esPlan, PhysicalPlan extPlan) {
        return "\n========================================\n"
            + "  Query: "
            + query
            + "\n========================================\n"
            + "\n--- ES Index (FROM test) ---\n"
            + esPlan
            + "\n\n--- External Source (EXTERNAL \"s3://...\") ---\n"
            + extPlan
            + "\n";
    }

    @SuppressWarnings("unchecked")
    private static <T> T findFirst(PhysicalPlan plan, Class<T> type) {
        var holder = new Object() {
            T found = null;
        };
        plan.forEachDown(node -> {
            if (holder.found == null && type.isInstance(node)) {
                holder.found = type.cast(node);
            }
        });
        return holder.found;
    }
}
