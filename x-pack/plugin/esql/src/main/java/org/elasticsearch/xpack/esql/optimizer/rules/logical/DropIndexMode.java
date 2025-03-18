/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

/**
 * Uses the standard index mode if the special index mode is not required in the query.
 */
public final class DropIndexMode extends OptimizerRules.OptimizerRule<EsRelation> {
    public DropIndexMode() {
        super(OptimizerRules.TransformDirection.UP);
    }

    @Override
    protected LogicalPlan rule(EsRelation r) {
        if (r.indexMode() == IndexMode.TIME_SERIES) {
            if (r.output().stream().anyMatch(a -> a.name().equals(MetadataAttribute.TSID_FIELD)) == false) {
                return new EsRelation(r.source(), r.indexPattern(), IndexMode.STANDARD, r.indexNameWithModes(), r.output());
            }
        }
        return r;
    }
}
