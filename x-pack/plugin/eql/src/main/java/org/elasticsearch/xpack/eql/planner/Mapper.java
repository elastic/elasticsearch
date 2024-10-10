/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.planner;

import org.elasticsearch.xpack.eql.execution.search.Limit;
import org.elasticsearch.xpack.eql.plan.logical.AbstractJoin;
import org.elasticsearch.xpack.eql.plan.logical.KeyedFilter;
import org.elasticsearch.xpack.eql.plan.logical.LimitWithOffset;
import org.elasticsearch.xpack.eql.plan.logical.Sample;
import org.elasticsearch.xpack.eql.plan.logical.Sequence;
import org.elasticsearch.xpack.eql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.eql.plan.physical.FilterExec;
import org.elasticsearch.xpack.eql.plan.physical.LimitWithOffsetExec;
import org.elasticsearch.xpack.eql.plan.physical.LocalExec;
import org.elasticsearch.xpack.eql.plan.physical.LocalRelation;
import org.elasticsearch.xpack.eql.plan.physical.OrderExec;
import org.elasticsearch.xpack.eql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.eql.plan.physical.ProjectExec;
import org.elasticsearch.xpack.eql.plan.physical.SampleExec;
import org.elasticsearch.xpack.eql.plan.physical.SequenceExec;
import org.elasticsearch.xpack.eql.plan.physical.UnplannedExec;
import org.elasticsearch.xpack.eql.querydsl.container.QueryContainer;
import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.expression.Expressions;
import org.elasticsearch.xpack.ql.expression.Foldables;
import org.elasticsearch.xpack.ql.plan.logical.EsRelation;
import org.elasticsearch.xpack.ql.plan.logical.Filter;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.plan.logical.OrderBy;
import org.elasticsearch.xpack.ql.plan.logical.Project;
import org.elasticsearch.xpack.ql.rule.Rule;
import org.elasticsearch.xpack.ql.rule.RuleExecutor;
import org.elasticsearch.xpack.ql.type.DataTypeConverter;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.elasticsearch.xpack.ql.util.ReflectionUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

class Mapper extends RuleExecutor<PhysicalPlan> {

    PhysicalPlan map(LogicalPlan plan) {
        return execute(planLater(plan));
    }

    @Override
    protected Iterable<RuleExecutor.Batch<PhysicalPlan>> batches() {
        var conversion = new Batch<>("Mapping", new SimpleExecMapper());

        return Arrays.asList(conversion);
    }

    private static PhysicalPlan planLater(LogicalPlan plan) {
        return new UnplannedExec(plan.source(), plan);
    }

    private static class SimpleExecMapper extends MapExecRule<LogicalPlan> {

        @Override
        protected PhysicalPlan map(LogicalPlan p) {
            if (p instanceof AbstractJoin join) {
                List<List<Attribute>> keys = new ArrayList<>(join.children().size());
                boolean[] missing = new boolean[join.children().size()];
                List<PhysicalPlan> matches = new ArrayList<>(keys.size());

                for (int i = 0; i < join.queries().size(); i++) {
                    KeyedFilter keyed = join.queries().get(i);
                    keys.add(Expressions.asAttributes(keyed.keys()));
                    matches.add(map(keyed.child()));
                    missing[i] = keyed.isMissingEventFilter();
                }

                if (p instanceof Sample sample) {
                    return new SampleExec(p.source(), matches, keys);
                }

                Sequence s = (Sequence) p;
                return new SequenceExec(
                    p.source(),
                    keys,
                    matches,
                    Expressions.asAttributes(s.until().keys()),
                    map(s.until().child()),
                    s.timestamp(),
                    s.tiebreaker(),
                    s.direction(),
                    s.maxSpan(),
                    missing
                );
            }

            if (p instanceof LocalRelation) {
                return new LocalExec(p.source(), ((LocalRelation) p).executable());
            }

            if (p instanceof Project pj) {
                return new ProjectExec(p.source(), map(pj.child()), pj.projections());
            }

            if (p instanceof Filter fl) {
                return new FilterExec(p.source(), map(fl.child()), fl.condition());
            }

            if (p instanceof OrderBy o) {
                return new OrderExec(p.source(), map(o.child()), o.order());
            }

            if (p instanceof LimitWithOffset l) {
                int limit = (Integer) DataTypeConverter.convert(Foldables.valueOf(l.limit()), DataTypes.INTEGER);
                return new LimitWithOffsetExec(p.source(), map(l.child()), new Limit(limit, l.offset()));
            }

            if (p instanceof EsRelation c) {
                List<Attribute> output = c.output();
                QueryContainer container = new QueryContainer();
                if (c.frozen()) {
                    container = container.withFrozen();
                }
                return new EsQueryExec(p.source(), output, container);
            }

            return planLater(p);
        }
    }

    abstract static class MapExecRule<SubPlan extends LogicalPlan> extends Rule<UnplannedExec, PhysicalPlan> {

        private final Class<SubPlan> subPlanToken = ReflectionUtils.detectSuperTypeForRuleLike(getClass());

        @Override
        public final PhysicalPlan apply(PhysicalPlan plan) {
            return plan.transformUp(UnplannedExec.class, this::rule);
        }

        @SuppressWarnings("unchecked")
        protected final PhysicalPlan rule(UnplannedExec plan) {
            LogicalPlan subPlan = plan.plan();
            if (subPlanToken.isInstance(subPlan)) {
                return map((SubPlan) subPlan);
            }
            return plan;
        }

        protected abstract PhysicalPlan map(SubPlan plan);
    }
}
