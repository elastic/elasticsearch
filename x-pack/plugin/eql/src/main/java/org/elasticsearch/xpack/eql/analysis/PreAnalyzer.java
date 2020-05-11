/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.analysis;

import org.elasticsearch.xpack.ql.index.IndexResolution;
import org.elasticsearch.xpack.ql.plan.logical.EsRelation;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.plan.logical.UnresolvedRelation;

public class PreAnalyzer {

    public LogicalPlan preAnalyze(LogicalPlan plan, IndexResolution indices) {
        if (plan.analyzed() == false) {
            // FIXME: includeFrozen needs to be set already
            plan = plan.transformUp(r -> new EsRelation(r.source(), indices.get(), false), UnresolvedRelation.class);
            plan.forEachUp(LogicalPlan::setPreAnalyzed);
        }
        return plan;
    }
}
