/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.analysis;

import org.elasticsearch.xpack.ql.common.Failure;
import org.elasticsearch.xpack.ql.index.EsIndex;
import org.elasticsearch.xpack.ql.index.IndexResolution;
import org.elasticsearch.xpack.ql.index.MappingException;
import org.elasticsearch.xpack.ql.plan.logical.EsRelation;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.plan.logical.UnresolvedRelation;

import java.util.Collections;

public class PreAnalyzer {

    public LogicalPlan preAnalyze(LogicalPlan plan, IndexResolution indices) {
        EsIndex esIndex = null;
        // wrap a potential index_not_found_exception with a VerificationException (expected by client)
        try {
            esIndex = indices.get();
        } catch (MappingException me) {
            throw new VerificationException(Collections.singletonList(Failure.fail(plan, me.getMessage())));
        }
        if (plan.analyzed() == false) {
            final EsRelation esRelation = new EsRelation(plan.source(), esIndex, false);
            // FIXME: includeFrozen needs to be set already
            plan = plan.transformUp(r -> esRelation, UnresolvedRelation.class);
            plan.forEachUp(LogicalPlan::setPreAnalyzed);
        }
        return plan;
    }
}
