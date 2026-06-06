/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.MetricsInfo;
import org.elasticsearch.xpack.esql.plan.logical.TsInfo;
import org.elasticsearch.xpack.esql.rule.Rule;

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;

/**
 * Uses the standard index mode if the special index mode is not required in the query.
 */
public final class PruneUnusedIndexMode extends Rule<LogicalPlan, LogicalPlan> {

    @Override
    public LogicalPlan apply(LogicalPlan plan) {
        Set<EsRelation> infoCommandRelations = Collections.newSetFromMap(new IdentityHashMap<>());
        plan.forEachDown(p -> {
            if (p instanceof MetricsInfo || p instanceof TsInfo) {
                p.forEachDown(EsRelation.class, infoCommandRelations::add);
            }
        });
        return plan.transformUp(EsRelation.class, r -> pruneRelation(r, infoCommandRelations));
    }

    private static LogicalPlan pruneRelation(EsRelation r, Set<EsRelation> infoCommandRelations) {
        if (r.indexMode() == IndexMode.TIME_SERIES) {
            if (infoCommandRelations.contains(r) == false
                && Expressions.anyMatch(r.output(), a -> MetadataAttribute.TSID_FIELD.equals(((Attribute) a).name())) == false) {
                return r.withIndexMode(IndexMode.STANDARD);
            }
        }
        return r;
    }
}
