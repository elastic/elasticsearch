/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.analysis;

import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.xpack.ql.common.Failure;
import org.elasticsearch.xpack.ql.index.IndexResolution;
import org.elasticsearch.xpack.ql.plan.logical.EsRelation;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.plan.logical.UnresolvedRelation;

import java.util.Collections;

public class PreAnalyzer {

    public LogicalPlan preAnalyze(LogicalPlan plan, IndexResolution indices) {
        if (indices.isValid() == false) {
            VerificationException cause = new VerificationException(Collections.singletonList(Failure.fail(plan, indices.toString())));
            // Wrapping the verification_exception in an infe to easily distinguish it on the rest layer in case it needs rewriting
            // (see RestEqlSearchAction for its usage).
            throw new IndexNotFoundException(indices.toString(), cause);
        }
        if (plan.analyzed() == false) {
            final EsRelation esRelation = new EsRelation(plan.source(), indices.get(), false);
            // FIXME: includeFrozen needs to be set already
            plan = plan.transformUp(UnresolvedRelation.class, r -> esRelation);
            plan.forEachUp(LogicalPlan::setPreAnalyzed);
        }
        return plan;
    }
}
