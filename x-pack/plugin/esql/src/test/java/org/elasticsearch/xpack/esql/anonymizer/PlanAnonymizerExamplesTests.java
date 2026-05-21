/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.anonymizer;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.xpack.esql.analysis.Analyzer;
import org.elasticsearch.xpack.esql.analysis.EnrichResolution;
import org.elasticsearch.xpack.esql.analysis.Verifier;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.EsIndexGenerator;
import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.LogicalPlanOptimizer;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.planner.mapper.Mapper;
import org.elasticsearch.xpack.esql.session.Versioned;
import org.elasticsearch.xpack.esql.telemetry.Metrics;
import org.junit.Before;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_CFG;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_FUNCTION_REGISTRY;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_PARSER;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.emptyInferenceResolution;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.loadMapping;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.testAnalyzerContext;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.withDefaultLimitWarning;
import static org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils.defaultLookupResolution;
import static org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils.indexResolutions;

/**
 * Runs a broad set of ES|QL queries through the parser + analyzer + logical optimizer + mapper,
 * then anonymizes the resulting logical and physical plans. Output is captured with
 * {@code -Dtests.output=always} and embedded in the PR body. The test asserts nothing — it exists
 * to produce evidence of the anonymizer's behavior on realistic plans.
 */
public class PlanAnonymizerExamplesTests extends MapperServiceTestCase {

    private static final String CLUSTER_UUID = "00000000-0000-0000-0000-000000000001";

    private Analyzer analyzer;
    private LogicalPlanOptimizer logicalOptimizer;
    private Mapper mapper;

    @Before
    public void init() {
        Map<String, EsField> mapping = loadMapping("mapping-basic.json");
        EsIndex test = EsIndexGenerator.esIndex("employees", mapping, Map.of("employees", IndexMode.STANDARD));

        analyzer = new Analyzer(
            testAnalyzerContext(
                TEST_CFG,
                TEST_FUNCTION_REGISTRY,
                indexResolutions(test),
                defaultLookupResolution(),
                new EnrichResolution(),
                emptyInferenceResolution()
            ),
            new Verifier(new Metrics(TEST_FUNCTION_REGISTRY, true, true), new XPackLicenseState(() -> 0L))
        );
        logicalOptimizer = new LogicalPlanOptimizer(
            new LogicalOptimizerContext(TEST_CFG, FoldContext.small(), analyzer.context().minimumVersion())
        );
        mapper = new Mapper();
    }

    @Override
    protected List<String> filteredWarnings() {
        return withDefaultLimitWarning(super.filteredWarnings());
    }

    public void testDumpExamples() {
        List<String> queries = List.of(
            "FROM employees | WHERE salary > 50000 | LIMIT 10",
            "FROM employees | WHERE first_name == \"Alice\" AND salary == 50000 | KEEP emp_no, last_name | LIMIT 100",
            "FROM employees | STATS avg_salary = AVG(salary) BY gender",
            "FROM employees | SORT salary DESC, hire_date ASC | LIMIT 5",
            "FROM employees | EVAL bonus = salary * 0.1, tier = CASE(salary > 100000, \"senior\", \"junior\") | KEEP emp_no, tier, bonus | LIMIT 5",
            "FROM employees | WHERE last_name LIKE \"S*\" AND hire_date > \"2020-01-01\" | LIMIT 20"
        );

        StringBuilder out = new StringBuilder();
        out.append("\n=========== ANONYMIZED ESQL PLAN EXAMPLES ===========\n");
        for (String q : queries) {
            dump(q, out);
        }
        out.append("=========== END ===========\n");
        System.out.println(out);
    }

    private void dump(String query, StringBuilder out) {
        var anon = PlanAnonymizer.forSubmission(CLUSTER_UUID);
        LogicalPlan logical = logicalOptimizer.optimize(analyzer.analyze(TEST_PARSER.parseQuery(query)));
        PhysicalPlan physical = mapper.map(new Versioned<>(logical, analyzer.context().minimumVersion()));
        var anonymized = anon.anonymize(logical, physical);

        out.append("\n--- QUERY ---\n");
        out.append(query).append("\n");
        out.append("--- MAPPING ---\n").append(anonymized.schema());
        out.append("--- LOGICAL ---\n").append(anonymized.logicalPlan()).append("\n");
        out.append("--- PHYSICAL ---\n").append(anonymized.physicalPlan()).append("\n");
    }
}
