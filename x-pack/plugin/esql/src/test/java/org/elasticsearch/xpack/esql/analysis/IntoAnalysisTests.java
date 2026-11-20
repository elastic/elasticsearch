/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.xpack.esql.VersionMode;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Into;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.Row;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.junit.Before;

import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.analyzer;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class IntoAnalysisTests extends AnalyzerTestCase {
    public IntoAnalysisTests(VersionMode versionMode) {
        super(versionMode);
    }

    @Before
    public void requiresSnapshot() {
        assumeTrue("INTO is only enabled in snapshot builds", EsqlCapabilities.Cap.INTO_COMMAND.isEnabled());
    }

    public void testNoImplicitLimitAdded() {
        for (String query : List.of("ROW a = 1 | INTO dst", "FROM employees | KEEP emp_no | INTO dst")) {
            Into into = as(analyzer().addEmployees().query(query), Into.class);
            assertThat(into.collectFirstChildren(Limit.class::isInstance), empty());
        }
    }

    public void testDescribeAfterIntoIsRejected() {
        assumeTrue("DESCRIBE is only enabled in snapshot builds", EsqlCapabilities.Cap.DESCRIBE_COMMAND.isEnabled());
        analyzer().error("ROW a = 1 | INTO dst | DESCRIBE", containsString("INTO must be the last command of the query"));
        assertWarnings("No limit defined, adding default limit of [1000]");
    }

    public void testSyntheticAttributesStrippedFromChildOutput() {
        LogicalPlan plan = analyzer().query("ROW a = 1 | WHERE a IN (ROW b = 1) OR a > 0 | INTO dst");
        Into into = as(plan, Into.class);
        long syntheticCount = into.child()
            .output()
            .stream()
            .filter(attr -> attr.name().startsWith(Attribute.SYNTHETIC_ATTRIBUTE_NAME_PREFIX))
            .count();
        assertThat(syntheticCount, equalTo(0L));
    }

    public void testMustBeTheLastTopLevelCommand() {
        String error = "INTO must be the last command of the query";
        analyzer().stripErrorPrefix(true).error("ROW a = 1 | INTO dst | LIMIT 1", equalTo("1:13: " + error));
        analyzer().error("ROW a = 1 | FORK (INTO dst) (LIMIT 1)", containsString(error));
    }

    public void testUnsupportedSources() {
        String error = "INTO currently only supports ROW or local FROM as a source, without FORK";
        analyzer().addK8s().error("TS k8s | INTO dst", containsString(error));
        analyzer().addIndex("remote:employees", "mapping-default.json").error("FROM remote:employees | INTO dst", containsString(error));
        analyzer().error("ROW a = 1 | FORK (WHERE true) (WHERE false) | INTO dst", containsString(error));
    }

    public void testSortWithLimitIsAnEligibleSource() {
        Into into = as(analyzer().addEmployees().query("FROM employees | SORT emp_no | LIMIT 10 | INTO dst"), Into.class);
        Limit limit = as(into.child(), Limit.class);
        as(limit.child(), OrderBy.class);
        assertThat(into.collect(Limit.class), hasSize(1));
    }

    public void testUnboundedSortBeforeIntoIsRejected() {
        String error = "INTO cannot be preceded by an unbounded SORT [SORT emp_no]";
        analyzer().addEmployees().error("FROM employees | SORT emp_no | INTO dst", containsString(error));
        analyzer().addEmployees().error("FROM employees | SORT emp_no | KEEP emp_no | INTO dst", containsString(error));
        analyzer().addEmployees().error("FROM employees | SORT emp_no | LIMIT 5 | SORT emp_no | INTO dst", containsString(error));
        analyzer().error("ROW a = 1 | SORT a | INTO dst", containsString("INTO cannot be preceded by an unbounded SORT [SORT a]"));
    }

    public void testSortThatAnAggregationMakesRedundantIsAnEligibleSource() {
        as(analyzer().addEmployees().query("FROM employees | SORT emp_no | STATS c = COUNT(*) BY gender | INTO dst"), Into.class);
    }

    public void testUnsupportedSourcesBelowDescribeAreRejected() {
        assumeTrue("DESCRIBE is only enabled in snapshot builds", EsqlCapabilities.Cap.DESCRIBE_COMMAND.isEnabled());
        String error = Into.UNSUPPORTED_SOURCE;
        analyzer().error("ROW a = 1 | FORK (WHERE true) (WHERE false) | DESCRIBE | INTO dst", containsString(error));
        analyzer().addK8s().error("TS k8s | DESCRIBE | INTO dst", containsString(error));
        analyzer().addIndex("remote:employees", "mapping-default.json")
            .error("FROM remote:employees | DESCRIBE | INTO dst", containsString(error));
    }

    public void testDescribeIsAnEligibleSource() {
        assumeTrue("DESCRIBE is only enabled in snapshot builds", EsqlCapabilities.Cap.DESCRIBE_COMMAND.isEnabled());
        List<String> queries = List.of(
            "ROW a = 1 | DESCRIBE | INTO dst",
            "FROM employees | DESCRIBE | INTO dst",
            "FROM employees | KEEP emp_no, salary | DESCRIBE | INTO dst"
        );
        for (String query : queries) {
            Into into = as(analyzer().addEmployees().query(query), Into.class);
            assertThat(query, into.child().anyMatch(LocalRelation.class::isInstance), equalTo(true));
            assertThat(query, into.child().anyMatch(p -> p instanceof Row || p instanceof EsRelation), equalTo(false));
        }
    }
}
