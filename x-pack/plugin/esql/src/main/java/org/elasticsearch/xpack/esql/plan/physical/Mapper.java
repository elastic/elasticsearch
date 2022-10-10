/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.ql.plan.logical.Aggregate;
import org.elasticsearch.xpack.ql.plan.logical.EsRelation;
import org.elasticsearch.xpack.ql.plan.logical.Limit;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.plan.logical.OrderBy;

public class Mapper {

    public PhysicalPlan map(LogicalPlan p) {
        if (p instanceof EsRelation esRelation) {
            EsQueryExec queryExec = new EsQueryExec(esRelation.source(), esRelation.index());
            return new FieldExtract(esRelation.source(), queryExec, esRelation.index(), esRelation.output(), queryExec.output());
        }

        if (p instanceof OrderBy o) {
            return new OrderExec(o.source(), map(o.child()), o.order());
        }

        if (p instanceof Limit limit) {
            return new LimitExec(limit.source(), map(limit.child()), limit.limit());
        }

        if (p instanceof Aggregate aggregate) {
            return new AggregateExec(aggregate.source(), map(aggregate.child()), aggregate.groupings(), aggregate.aggregates());
        }

        if (p instanceof Eval eval) {
            return new EvalExec(eval.source(), map(eval.child()), eval.fields());
        }

        throw new UnsupportedOperationException(p.nodeName());
    }
}
