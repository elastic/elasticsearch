/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.stats;

import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Dissect;
import org.elasticsearch.xpack.esql.plan.logical.Drop;
import org.elasticsearch.xpack.esql.plan.logical.Enrich;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Grok;
import org.elasticsearch.xpack.esql.plan.logical.Keep;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.MvExpand;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.Rename;
import org.elasticsearch.xpack.esql.plan.logical.Row;
import org.elasticsearch.xpack.esql.plan.logical.meta.MetaFunctions;
import org.elasticsearch.xpack.esql.plan.logical.show.ShowInfo;

import java.util.BitSet;
import java.util.Locale;
import java.util.function.Predicate;

public enum FeatureMetric {
    /**
     * The order of these enum values is important, do not change it.
     * For any new values added to it, they should go at the end of the list.
     * see {@link org.elasticsearch.xpack.esql.analysis.Verifier#gatherMetrics}
     */
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
    META(MetaFunctions.class::isInstance);

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
    }

    public static boolean set(LogicalPlan plan, BitSet bitset, FeatureMetric metric) {
        var isMatch = metric.planCheck.test(plan);
        if (isMatch) {
            bitset.set(metric.ordinal());
        }
        return isMatch;
    }
}
