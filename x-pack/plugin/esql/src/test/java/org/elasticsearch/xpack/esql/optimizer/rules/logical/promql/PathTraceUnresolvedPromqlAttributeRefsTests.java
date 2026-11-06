/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical.promql;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.promql.UnresolvedPromqlAttribute;

import java.util.List;
import java.util.Set;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;

/**
 * An Eval stating metadata names the metadata above it after everything it leaves out of the source's series.
 */
public class PathTraceUnresolvedPromqlAttributeRefsTests extends ESTestCase {

    public void testStatementNamesMetadataAbove() {
        EsRelation relation = EsqlTestUtils.relation(IndexMode.TIME_SERIES);
        Alias derived = new Alias(Source.EMPTY, "dst", Literal.keyword(Source.EMPTY, "value"));
        LogicalPlan eval = new Eval(Source.EMPTY, relation, List.of(derived, statement(Set.of("dst"))));

        Project project = as(new PathTraceUnresolvedPromqlAttributeRefs().apply(read(eval)), Project.class);
        Eval rest = as(project.child(), Eval.class);
        assertEquals(List.of(derived), rest.fields());
        assertSame(relation, rest.child());
        assertRead(project, Set.of("dst"), "_timeseries$dst");
    }

    /** Each statement leaves its labels out of the metadata the one before it states. */
    public void testStatementsComposeInFieldOrder() {
        EsRelation relation = EsqlTestUtils.relation(IndexMode.TIME_SERIES);
        LogicalPlan eval = new Eval(Source.EMPTY, relation, List.of(statement(Set.of("pod")), statement(Set.of("dst"))));

        Project project = as(new PathTraceUnresolvedPromqlAttributeRefs().apply(read(eval)), Project.class);
        assertSame(relation, project.child());
        assertRead(project, Set.of("pod", "dst"), "_timeseries$dst$pod");
    }

    private static Alias statement(Set<String> unset) {
        return new Alias(Source.EMPTY, MetadataAttribute.TIMESERIES, UnresolvedPromqlAttribute.metadata(Source.EMPTY, unset));
    }

    /** A projection reading the metadata of {@code child} as is. */
    private static Project read(LogicalPlan child) {
        return new Project(Source.EMPTY, child, List.of(statement(Set.of())));
    }

    private static void assertRead(Project project, Set<String> unset, String name) {
        Alias projected = as(project.projections().getFirst(), Alias.class);
        UnresolvedPromqlAttribute ref = as(projected.child(), UnresolvedPromqlAttribute.class);
        assertEquals(unset, ref.unset());
        assertEquals(name, ref.name());
    }
}
