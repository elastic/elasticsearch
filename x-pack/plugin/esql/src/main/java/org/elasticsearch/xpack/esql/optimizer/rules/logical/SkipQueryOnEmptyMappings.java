/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.local.EmptyLocalSupplier;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;

import java.util.List;

public final class SkipQueryOnEmptyMappings extends OptimizerRules.OptimizerRule<EsRelation> {

    @Override
    protected LogicalPlan rule(EsRelation plan) {
        return hasNoIndices(plan) ? new LocalRelation(plan.source(), plan.output(), EmptyLocalSupplier.EMPTY) : plan;
    }

    private static boolean hasNoIndices(EsRelation plan) {
        for (List<String> indices : plan.concreteIndicesByRemotes().values()) {
            if (indices.isEmpty() == false) {
                return false;
            }
        }
        return true;
    }
}
