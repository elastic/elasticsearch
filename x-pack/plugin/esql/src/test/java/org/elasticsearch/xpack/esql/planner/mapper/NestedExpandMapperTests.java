/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner.mapper;

import org.elasticsearch.Build;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.core.type.NestedScopeField;
import org.elasticsearch.xpack.esql.optimizer.AbstractLogicalPlanOptimizerTests;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.session.Versioned;
import org.junit.Before;

import java.util.List;

import static org.elasticsearch.xpack.esql.analysis.AnalyzerNestedExpandTests.nestedAnalyzer;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class NestedExpandMapperTests extends AbstractLogicalPlanOptimizerTests {
    private static final List<String> QUERIES = List.of(
        "FROM ne | NESTED_EXPAND items | KEEP id, items.sku",
        "FROM ne | WHERE id == \"1\" | NESTED_EXPAND items | WHERE items.qty > 1 | KEEP id, items.qty",
        "FROM ne | NESTED_EXPAND items | NESTED_EXPAND events | LIMIT 5 | KEEP id",
        "FROM ne | LIMIT 3 | NESTED_EXPAND items | KEEP id, items.qty"
    );

    public NestedExpandMapperTests(VersionMode versionMode) {
        super(versionMode);
    }

    @Before
    public void requireDevelopmentGrammar() {
        assumeTrue("NESTED_EXPAND is snapshot-only", Build.current().isSnapshot());
    }

    public void testNestedExpandIsRejectedBeforeAnyFragmentIsBuilt() {
        for (String query : QUERIES) {
            LogicalPlan optimized = optimize(analyze(query));
            VerificationException e = expectThrows(VerificationException.class, () -> map(optimized));
            assertThat(query, e.getMessage(), containsString("NESTED_EXPAND is not supported yet"));
            assertThat(query, e.status(), equalTo(RestStatus.BAD_REQUEST));
        }
    }

    public void testSameQueryWithoutNestedExpandBuildsAFragment() {
        PhysicalPlan physical = map(optimize(analyze("FROM ne | KEEP id")));
        assertThat(physical.collect(FragmentExec.class::isInstance), not(empty()));
    }

    private static PhysicalPlan map(LogicalPlan optimized) {
        return new Mapper().map(new Versioned<>(optimized, TransportVersion.current()));
    }

    private LogicalPlan analyze(String query) {
        TransportVersion version = minimumVersion.supports(NestedScopeField.FIELD_CAPS_VERSION)
            ? minimumVersion
            : NestedScopeField.FIELD_CAPS_VERSION;
        return nestedAnalyzer(version).query(query);
    }
}
