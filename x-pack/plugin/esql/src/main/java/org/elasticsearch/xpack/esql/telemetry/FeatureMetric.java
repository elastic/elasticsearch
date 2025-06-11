/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.telemetry;

import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.ChangePoint;
import org.elasticsearch.xpack.esql.plan.logical.Dissect;
import org.elasticsearch.xpack.esql.plan.logical.Drop;
import org.elasticsearch.xpack.esql.plan.logical.Enrich;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Grok;
import org.elasticsearch.xpack.esql.plan.logical.InlineStats;
import org.elasticsearch.xpack.esql.plan.logical.Keep;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Lookup;
import org.elasticsearch.xpack.esql.plan.logical.MvExpand;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.Rename;
import org.elasticsearch.xpack.esql.plan.logical.Row;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;
import org.elasticsearch.xpack.esql.plan.logical.inference.Completion;
import org.elasticsearch.xpack.esql.plan.logical.inference.Rerank;
import org.elasticsearch.xpack.esql.plan.logical.join.LookupJoin;
import org.elasticsearch.xpack.esql.plan.logical.local.EsqlProject;
import org.elasticsearch.xpack.esql.plan.logical.show.ShowInfo;

import java.util.BitSet;
import java.util.List;
import java.util.Locale;
import java.util.function.Predicate;

public enum FeatureMetric {
    DISSECT(Dissect.class::isInstance),
    EVAL(Eval.class::isInstance),
    GROK(Grok.class::isInstance),
    LIMIT(plan -> false), // the limit is checked in Analyzer.gatherPreAnalysisMetrics, because it has a more complex and general check
    SORT(OrderBy.class::isInstance),
    STATS(Aggregate.class::isInstance),
    WHERE(Filter.class::isInstance),
    ENRICH(Enrich.class::isInstance),
    MV_EXPAND(MvExpand.class::isInstance),
    SHOW(ShowInfo.class::isInstance),
    ROW(Row.class::isInstance),
    FROM(EsRelation.class::isInstance),
    DROP(Drop.class::isInstance),
    KEEP(Keep.class::isInstance),
    RENAME(Rename.class::isInstance),
    LOOKUP_JOIN(LookupJoin.class::isInstance),
    LOOKUP(Lookup.class::isInstance),
    CHANGE_POINT(ChangePoint.class::isInstance),
    INLINESTATS(InlineStats.class::isInstance),
    COMPLETION(Completion.class::isInstance),
    RERANK(Rerank.class::isInstance);

    /**
     * List here plans we want to exclude from telemetry
     */
    private static final List<Class<? extends LogicalPlan>> excluded = List.of(
        UnresolvedRelation.class,
        EsqlProject.class,
        Project.class,
        Limit.class // LIMIT is managed in another way, see above
    );

    private Predicate<LogicalPlan> planCheck;

    FeatureMetric(Predicate<LogicalPlan> planCheck) {
        this.planCheck = planCheck;
    }

    @Override
    public String toString() {
        return this.name().toLowerCase(Locale.ROOT);
    }

    public static void set(LogicalPlan plan, BitSet bitset) {
        for (FeatureMetric metric : FeatureMetric.values()) {
            if (set(plan, bitset, metric)) {
                return;
            }
        }
        if (explicitlyExcluded(plan) == false) {
            throw new EsqlIllegalArgumentException("Command not mapped for telemetry [{}]", plan.getClass().getSimpleName());
        }
    }

    private static boolean explicitlyExcluded(LogicalPlan plan) {
        return excluded.stream().anyMatch(x -> x.isInstance(plan));
    }

    public static boolean set(LogicalPlan plan, BitSet bitset, FeatureMetric metric) {
        var isMatch = metric.planCheck.test(plan);
        if (isMatch) {
            bitset.set(metric.ordinal());
        }
        return isMatch;
    }
}
