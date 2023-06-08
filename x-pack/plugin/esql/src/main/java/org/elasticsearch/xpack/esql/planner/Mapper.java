/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import org.elasticsearch.compute.ann.Experimental;
import org.elasticsearch.xpack.esql.plan.logical.Dissect;
import org.elasticsearch.xpack.esql.plan.logical.Enrich;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Grok;
import org.elasticsearch.xpack.esql.plan.logical.MvExpand;
import org.elasticsearch.xpack.esql.plan.logical.Row;
import org.elasticsearch.xpack.esql.plan.logical.TopN;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.plan.logical.show.ShowFunctions;
import org.elasticsearch.xpack.esql.plan.logical.show.ShowInfo;
import org.elasticsearch.xpack.esql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.DissectExec;
import org.elasticsearch.xpack.esql.plan.physical.EnrichExec;
import org.elasticsearch.xpack.esql.plan.physical.EsSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.EvalExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeExec;
import org.elasticsearch.xpack.esql.plan.physical.FilterExec;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.plan.physical.GrokExec;
import org.elasticsearch.xpack.esql.plan.physical.LimitExec;
import org.elasticsearch.xpack.esql.plan.physical.LocalSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.MvExpandExec;
import org.elasticsearch.xpack.esql.plan.physical.OrderExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.ProjectExec;
import org.elasticsearch.xpack.esql.plan.physical.RowExec;
import org.elasticsearch.xpack.esql.plan.physical.ShowExec;
import org.elasticsearch.xpack.esql.plan.physical.TopNExec;
import org.elasticsearch.xpack.ql.expression.function.FunctionRegistry;
import org.elasticsearch.xpack.ql.plan.logical.Aggregate;
import org.elasticsearch.xpack.ql.plan.logical.EsRelation;
import org.elasticsearch.xpack.ql.plan.logical.Filter;
import org.elasticsearch.xpack.ql.plan.logical.Limit;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.plan.logical.OrderBy;
import org.elasticsearch.xpack.ql.plan.logical.Project;
import org.elasticsearch.xpack.ql.plan.logical.UnaryPlan;

import static org.elasticsearch.xpack.esql.plan.physical.AggregateExec.Mode;
import static org.elasticsearch.xpack.esql.plan.physical.AggregateExec.Mode.FINAL;
import static org.elasticsearch.xpack.esql.plan.physical.AggregateExec.Mode.PARTIAL;

@Experimental
public class Mapper {

    private final FunctionRegistry functionRegistry;
    private final boolean localMode;

    public Mapper(FunctionRegistry functionRegistry) {
        this.functionRegistry = functionRegistry;
        localMode = false;
    }

    public Mapper(boolean localMode) {
        this.functionRegistry = null;
        this.localMode = localMode;
    }

    public PhysicalPlan map(LogicalPlan p) {
        //
        // Leaf Node
        //

        // Source
        if (p instanceof EsRelation esRelation) {
            return localMode ? new EsSourceExec(esRelation) : new FragmentExec(p);
        }

        if (p instanceof Row row) {
            return new RowExec(row.source(), row.fields());
        }

        if (p instanceof LocalRelation local) {
            return new LocalSourceExec(local.source(), local.output(), local.supplier());
        }

        // Commands
        if (p instanceof ShowFunctions showFunctions) {
            return new ShowExec(showFunctions.source(), showFunctions.output(), showFunctions.values(functionRegistry));
        }
        if (p instanceof ShowInfo showInfo) {
            return new ShowExec(showInfo.source(), showInfo.output(), showInfo.values());
        }
        if (p instanceof Enrich enrich) {
            return new EnrichExec(
                enrich.source(),
                map(enrich.child()),
                enrich.matchField(),
                enrich.policy().index().get(),
                enrich.enrichFields()
            );
        }

        //
        // Unary Plan
        //

        if (p instanceof UnaryPlan ua) {
            var child = map(ua.child());
            PhysicalPlan plan = null;
            // in case of a fragment, grow it with streaming operators
            if (child instanceof FragmentExec fragment
                && ((p instanceof Aggregate || p instanceof TopN || p instanceof Limit || p instanceof OrderBy) == false)) {
                plan = new FragmentExec(p);
            } else {
                plan = map(ua, child);
            }
            return plan;
        }

        throw new UnsupportedOperationException(p.nodeName());
    }

    private PhysicalPlan map(UnaryPlan p, PhysicalPlan child) {
        //
        // Pipeline operators
        //
        if (p instanceof Filter f) {
            return new FilterExec(f.source(), child, f.condition());
        }

        if (p instanceof Project pj) {
            return new ProjectExec(pj.source(), child, pj.projections());
        }

        if (p instanceof Eval eval) {
            return new EvalExec(eval.source(), child, eval.fields());
        }

        if (p instanceof Dissect dissect) {
            return new DissectExec(dissect.source(), child, dissect.input(), dissect.parser(), dissect.extractedFields());
        }

        if (p instanceof Grok grok) {
            return new GrokExec(grok.source(), child, grok.input(), grok.parser(), grok.extractedFields());
        }

        //
        // Pipeline breakers
        //
        if (p instanceof Limit limit) {
            return map(limit, child);
        }

        if (p instanceof OrderBy o) {
            return map(o, child);
        }

        if (p instanceof TopN topN) {
            return map(topN, child);
        }

        if (p instanceof MvExpand mvExpand) {
            return new MvExpandExec(mvExpand.source(), map(mvExpand.child()), mvExpand.target());
        }

        if (p instanceof Aggregate aggregate) {
            return map(aggregate, child);
        }

        throw new UnsupportedOperationException(p.nodeName());
    }

    private PhysicalPlan map(Aggregate aggregate, PhysicalPlan child) {
        // in local mode the only aggregate that can appear is the partial side under an exchange
        if (localMode) {
            child = aggExec(aggregate, child, PARTIAL);
        }
        // otherwise create both sides of the aggregate (for parallelism purposes), if no fragment is present
        // TODO: might be easier long term to end up with just one node and split if necessary instead of doing that always at this stage
        else {
            child = addExchangeForFragment(aggregate, child);
            // if no exchange was added, create the partial aggregate
            if (child instanceof ExchangeExec == false) {
                child = aggExec(aggregate, child, PARTIAL);
            }
            child = aggExec(aggregate, child, FINAL);
        }

        return child;
    }

    private static AggregateExec aggExec(Aggregate aggregate, PhysicalPlan child, Mode aggMode) {
        return new AggregateExec(aggregate.source(), child, aggregate.groupings(), aggregate.aggregates(), aggMode);
    }

    private PhysicalPlan map(Limit limit, PhysicalPlan child) {
        child = addExchangeForFragment(limit, child);
        return new LimitExec(limit.source(), child, limit.limit());
    }

    private PhysicalPlan map(OrderBy o, PhysicalPlan child) {
        child = addExchangeForFragment(o, child);
        return new OrderExec(o.source(), child, o.order());
    }

    private PhysicalPlan map(TopN topN, PhysicalPlan child) {
        child = addExchangeForFragment(topN, child);
        return new TopNExec(topN.source(), child, topN.order(), topN.limit());
    }

    private PhysicalPlan addExchangeForFragment(LogicalPlan logical, PhysicalPlan child) {
        // in case of fragment, preserve the streaming operator (order-by, limit or topN) for local replanning
        // no need to do it for an aggregate since it gets split
        // and clone it as a physical node along with the exchange
        if (child instanceof FragmentExec) {
            child = new FragmentExec(logical);
            child = new ExchangeExec(child.source(), child);
        }
        return child;
    }
}
