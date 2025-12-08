/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.reflections.Reflections;
import org.reflections.scanners.Scanners;

import java.lang.reflect.Modifier;
import java.util.Set;
import java.util.stream.Collectors;

public class PruneRedundantOrderByTests extends ESTestCase {

    /**
     * See the javadoc in {@link PruneRedundantOrderBy} for an explanation of this classification, specifically the
     * {@link PruneRedundantOrderBy#isRedundantSortAgnostic(LogicalPlan)} method.
     */

    private static final Set<Class<? extends LogicalPlan>> REDUNDANT_SORT_AGNOSTIC = Set.of(
        org.elasticsearch.xpack.esql.plan.logical.ChangePoint.class,
        org.elasticsearch.xpack.esql.plan.logical.Row.class,
        org.elasticsearch.xpack.esql.plan.logical.Filter.class,
        org.elasticsearch.xpack.esql.plan.logical.Project.class,
        org.elasticsearch.xpack.esql.plan.logical.Rename.class,
        org.elasticsearch.xpack.esql.plan.logical.inference.Rerank.class,
        org.elasticsearch.xpack.esql.plan.logical.Subquery.class,
        org.elasticsearch.xpack.esql.plan.logical.Enrich.class,
        org.elasticsearch.xpack.esql.plan.logical.Grok.class,
        org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation.class,
        org.elasticsearch.xpack.esql.plan.logical.Eval.class,
        org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate.class,
        org.elasticsearch.xpack.esql.plan.logical.Insist.class,
        org.elasticsearch.xpack.esql.plan.logical.Keep.class,
        org.elasticsearch.xpack.esql.plan.logical.show.ShowInfo.class,
        org.elasticsearch.xpack.esql.plan.logical.fuse.Fuse.class,
        org.elasticsearch.xpack.esql.plan.logical.Aggregate.class,
        org.elasticsearch.xpack.esql.plan.logical.EsRelation.class,
        org.elasticsearch.xpack.esql.plan.logical.local.EsqlProject.class,
        org.elasticsearch.xpack.esql.plan.logical.join.InlineJoin.class,
        org.elasticsearch.xpack.esql.plan.logical.join.LookupJoin.class,
        org.elasticsearch.xpack.esql.plan.logical.join.StubRelation.class,
        org.elasticsearch.xpack.esql.plan.logical.inference.Completion.class,
        org.elasticsearch.xpack.esql.plan.logical.Fork.class,
        org.elasticsearch.xpack.esql.plan.logical.UnionAll.class,
        org.elasticsearch.xpack.esql.plan.logical.InlineStats.class,
        org.elasticsearch.xpack.esql.plan.logical.fuse.FuseScoreEval.class,
        org.elasticsearch.xpack.esql.plan.logical.OrderBy.class,
        org.elasticsearch.xpack.esql.plan.logical.Drop.class,
        org.elasticsearch.xpack.esql.plan.logical.Dissect.class,
        org.elasticsearch.xpack.esql.plan.logical.Sample.class,
        org.elasticsearch.xpack.esql.plan.logical.Lookup.class,
        org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation.class,
        org.elasticsearch.xpack.esql.plan.logical.TopN.class,
        org.elasticsearch.xpack.esql.plan.logical.Explain.class,
        org.elasticsearch.xpack.esql.plan.logical.MvExpand.class,
        org.elasticsearch.xpack.esql.plan.logical.join.Join.class
    );
    private static final Set<Class<? extends LogicalPlan>> REDUNDANT_SORT_BREAKER = Set.of(
        // the gnostic types
        org.elasticsearch.xpack.esql.plan.logical.Limit.class
    );

    private static final Set<Class<? extends LogicalPlan>> ALL_CONSIDERED = Set.of(REDUNDANT_SORT_AGNOSTIC, REDUNDANT_SORT_BREAKER)
        .stream()
        .flatMap(Set::stream)
        .collect(Collectors.toSet());

    /**
     * This test ensures that all LogicalPlan node types are considered by the PruneRedundantOrderBy optimizer rule.
     * If a new LogicalPlan class is added, this test will fail until the rule is updated to handle it,
     * preventing accidental omissions.
     */
    public void testAllLogicalPlanTypesAreConsidered() {
        Reflections reflections = new Reflections("org.elasticsearch.xpack.esql.plan.logical", Scanners.SubTypes);
        Set<Class<? extends LogicalPlan>> allPlanTypes = reflections.getSubTypesOf(LogicalPlan.class)
            .stream()
            .filter(c -> c.isInterface() == false && c.isAnonymousClass() == false && Modifier.isAbstract(c.getModifiers()) == false)
            .collect(Collectors.toSet());

        Set<Class<? extends LogicalPlan>> diff = Sets.difference(allPlanTypes, ALL_CONSIDERED);

        assertTrue(
            "The following LogicalPlan types have not been considered as either sort agnostic or breaker in relation to the "
                + "PruneRedundantOrderBy rule: "
                + diff
                + ". "
                + "See REDUNDANT_SORT_AGNOSTIC and REDUNDANT_SORT_BREAKER sets above.",
            diff.isEmpty()
        );
    }
}
