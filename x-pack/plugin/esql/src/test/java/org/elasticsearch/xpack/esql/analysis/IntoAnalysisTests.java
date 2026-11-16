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
import org.elasticsearch.xpack.esql.plan.logical.Into;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.junit.Before;

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
        LogicalPlan plan = analyzer().query("ROW a = 1 | INTO dst");
        Into into = as(plan, Into.class);
        assertThat(into.collectFirstChildren(Limit.class::isInstance), empty());
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

    public void testOnlyRowSource() {
        String error = "INTO currently only supports ROW as a source, without FORK";
        analyzer().addEmployees().error("FROM employees | INTO dst", containsString(error));
        analyzer().error("ROW a = 1 | FORK (WHERE true) (WHERE false) | INTO dst", containsString(error));
    }
}
