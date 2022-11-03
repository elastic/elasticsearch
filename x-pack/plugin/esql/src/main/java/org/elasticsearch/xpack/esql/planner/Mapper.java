/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import org.elasticsearch.compute.Experimental;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Row;
import org.elasticsearch.xpack.esql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.EvalExec;
import org.elasticsearch.xpack.esql.plan.physical.FilterExec;
import org.elasticsearch.xpack.esql.plan.physical.LimitExec;
import org.elasticsearch.xpack.esql.plan.physical.OrderExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.ProjectExec;
import org.elasticsearch.xpack.esql.plan.physical.RowExec;
import org.elasticsearch.xpack.ql.plan.logical.Aggregate;
import org.elasticsearch.xpack.ql.plan.logical.EsRelation;
import org.elasticsearch.xpack.ql.plan.logical.Filter;
import org.elasticsearch.xpack.ql.plan.logical.Limit;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.plan.logical.OrderBy;
import org.elasticsearch.xpack.ql.plan.logical.Project;

@Experimental
public class Mapper {

    public PhysicalPlan map(LogicalPlan p) {
        if (p instanceof EsRelation esRelation) {
            // TODO: Fold with filter
            return new EsQueryExec(esRelation.source(), esRelation.index(), new MatchAllQueryBuilder());
        }

        if (p instanceof Filter f) {
            return new FilterExec(f.source(), map(f.child()), f.condition());
        }

        if (p instanceof Project pj) {
            return new ProjectExec(pj.source(), map(pj.child()), pj.projections());
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

        if (p instanceof Row row) {
            return new RowExec(row.source(), row.fields());
        }

        throw new UnsupportedOperationException(p.nodeName());
    }
}
