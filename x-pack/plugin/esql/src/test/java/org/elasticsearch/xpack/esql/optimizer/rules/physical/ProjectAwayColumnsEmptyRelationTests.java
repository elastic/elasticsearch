/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.optimizer.PhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeExec;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;

/**
 * Nodes that predate #133405 (before 9.3) replace every plan with an empty output by an empty relation, so {@link ProjectAwayColumns}
 * must not ship them a relation without fields, e.g. when the query does not reference any field of the index. Newer nodes must not
 * get the placeholder.
 */
public class ProjectAwayColumnsEmptyRelationTests extends ESTestCase {

    public void testRelationWithoutFieldsGetsPlaceholderForOldNodes() {
        var relation = relationInFragment(rule(TransportVersion.minimumCompatible()));
        assertThat(relation.output().stream().map(a -> a.name()).toList(), contains("<no-fields>"));
    }

    public void testRelationWithoutFieldsIsKeptForNodesThatSupportZeroColumnFragments() {
        var relation = relationInFragment(rule(TransportVersion.current()));
        assertThat(relation.output(), empty());
    }

    /**
     * Nodes from 9.3 on keep a plan without output columns, and they fail on the placeholder (#125866), so they must not get it
     * even though they still need the placeholder column of {@link ProjectAwayColumns#ALL_FIELDS_PROJECTED}.
     */
    public void testRelationWithoutFieldsIsKeptForNodesFrom93() {
        var relation = relationInFragment(rule(TransportVersion.fromName("esql_plan_with_no_columns")));
        assertThat(relation.output(), empty());
    }

    private static PhysicalPlan rule(TransportVersion minimumVersion) {
        EsRelation relation = new EsRelation(Source.EMPTY, "test", IndexMode.STANDARD, Map.of(), Map.of(), Map.of(), List.of());
        Limit limit = new Limit(Source.EMPTY, Literal.integer(Source.EMPTY, 2), relation);
        PhysicalPlan plan = new ExchangeExec(Source.EMPTY, new FragmentExec(limit));
        return new ProjectAwayColumns().apply(plan, new PhysicalOptimizerContext(EsqlTestUtils.TEST_CFG, minimumVersion));
    }

    private static EsRelation relationInFragment(PhysicalPlan plan) {
        FragmentExec fragment = as(as(plan, ExchangeExec.class).child(), FragmentExec.class);
        return fragment.fragment().collect(EsRelation.class).getFirst();
    }
}
