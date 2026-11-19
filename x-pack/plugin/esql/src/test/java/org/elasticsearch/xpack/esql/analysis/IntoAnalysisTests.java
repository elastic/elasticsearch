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
import org.elasticsearch.xpack.esql.plan.logical.Row;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.junit.Before;

import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.analyzer;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;

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

    public void testDescribeOfRowRemainsEligible() {
        assumeTrue("DESCRIBE is only enabled in snapshot builds", EsqlCapabilities.Cap.DESCRIBE_COMMAND.isEnabled());
        LogicalPlan plan = analyzer().query("ROW a = 1 | DESCRIBE | INTO dst");
        Into into = as(plan, Into.class);
        assertThat(into.child().anyMatch(LocalRelation.class::isInstance), equalTo(true));
        assertThat(into.child().anyMatch(Row.class::isInstance), equalTo(false));

        Into fromInto = as(analyzer().addEmployees().query("FROM employees | DESCRIBE | INTO dst"), Into.class);
        assertThat(fromInto.child().anyMatch(LocalRelation.class::isInstance), equalTo(true));
        assertThat(fromInto.child().anyMatch(EsRelation.class::isInstance), equalTo(false));

        String error = Into.UNSUPPORTED_SOURCE;
        analyzer().addIndex("remote:employees", "mapping-default.json")
            .error("FROM remote:employees | DESCRIBE | INTO dst", containsString(error));
        analyzer().error("ROW a = 1 | FORK (WHERE true) (WHERE false) | DESCRIBE | INTO dst", containsString(error));
    }
}
