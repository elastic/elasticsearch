/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.planner;

import org.elasticsearch.xpack.eql.EqlIllegalArgumentException;
import org.elasticsearch.xpack.eql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.eql.plan.physical.FilterExec;
import org.elasticsearch.xpack.eql.plan.physical.OrderExec;
import org.elasticsearch.xpack.eql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.eql.plan.physical.ProjectExec;
import org.elasticsearch.xpack.eql.querydsl.container.QueryContainer;
import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Expressions;
import org.elasticsearch.xpack.ql.expression.FieldAttribute;
import org.elasticsearch.xpack.ql.expression.Order;
import org.elasticsearch.xpack.ql.planner.ExpressionTranslators;
import org.elasticsearch.xpack.ql.querydsl.container.AttributeSort;
import org.elasticsearch.xpack.ql.querydsl.container.Sort.Direction;
import org.elasticsearch.xpack.ql.querydsl.container.Sort.Missing;
import org.elasticsearch.xpack.ql.querydsl.query.Query;
import org.elasticsearch.xpack.ql.rule.Rule;
import org.elasticsearch.xpack.ql.rule.RuleExecutor;

import java.util.Arrays;

class QueryFolder extends RuleExecutor<PhysicalPlan> {

    PhysicalPlan fold(PhysicalPlan plan) {
        return execute(plan);
    }

    @Override
    protected Iterable<RuleExecutor<PhysicalPlan>.Batch> batches() {
        Batch fold = new Batch("Fold queries",
                new FoldProject(),
                new FoldFilter(),
                new FoldOrderBy()
        );
        Batch finish = new Batch("Finish query", Limiter.ONCE,
                new PlanOutputToQueryRef()
        );
        
        return Arrays.asList(fold, finish);
    }
    

    private static class FoldProject extends FoldingRule<ProjectExec> {

        @Override
        protected PhysicalPlan rule(ProjectExec project) {
            if (project.child() instanceof EsQueryExec) {
                EsQueryExec exec = (EsQueryExec) project.child();
                return new EsQueryExec(exec.source(), exec.index(), project.output(), exec.queryContainer());
            }
            return project;
        }
    }

    private static class FoldFilter extends FoldingRule<FilterExec> {

        @Override
        protected PhysicalPlan rule(FilterExec plan) {
            if (plan.child() instanceof EsQueryExec) {
                EsQueryExec exec = (EsQueryExec) plan.child();
                QueryContainer qContainer = exec.queryContainer();

                Query query = QueryTranslator.toQuery(plan.condition());

                if (qContainer.query() != null || query != null) {
                    query = ExpressionTranslators.and(plan.source(), qContainer.query(), query);
                }

                qContainer = qContainer.with(query);
                return exec.with(qContainer);
            }
            return plan;
        }
    }
    
    private static class FoldOrderBy extends FoldingRule<OrderExec> {
        @Override
        protected PhysicalPlan rule(OrderExec plan) {
            if (plan.child() instanceof EsQueryExec) {
                EsQueryExec exec = (EsQueryExec) plan.child();
                QueryContainer qContainer = exec.queryContainer();

                for (Order order : plan.order()) {
                    Direction direction = Direction.from(order.direction());
                    Missing missing = Missing.from(order.nullsPosition());

                    // check whether sorting is on an group (and thus nested agg) or field
                    Expression orderExpression = order.child();

                    String lookup = Expressions.id(orderExpression);

                    // field
                    if (orderExpression instanceof FieldAttribute) {
                        FieldAttribute fa = (FieldAttribute) orderExpression;
                        qContainer = qContainer.addSort(lookup, new AttributeSort(fa, direction, missing));
                    }
                    // unknown
                    else {
                        throw new EqlIllegalArgumentException("unsupported sorting expression {}", orderExpression);
                    }
                }

                return exec.with(qContainer);
            }
            return plan;
        }
    }

    private static class PlanOutputToQueryRef extends FoldingRule<EsQueryExec> {
        @Override
        protected PhysicalPlan rule(EsQueryExec exec) {
            QueryContainer qContainer = exec.queryContainer();

            for (Attribute attr : exec.output()) {
                qContainer = qContainer.addColumn(attr);
            }

            // after all attributes have been resolved
            return exec.with(qContainer);
        }
    }

    abstract static class FoldingRule<SubPlan extends PhysicalPlan> extends Rule<SubPlan, PhysicalPlan> {

        @Override
        public final PhysicalPlan apply(PhysicalPlan plan) {
            return plan.transformUp(this::rule, typeToken());
        }

        @Override
        protected abstract PhysicalPlan rule(SubPlan plan);
    }
}
