/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Exchange;
import org.elasticsearch.xpack.esql.plan.logical.FieldExtract;
import org.elasticsearch.xpack.esql.plan.logical.TopN;
import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.expression.NamedExpression;
import org.elasticsearch.xpack.ql.optimizer.OptimizerRules;
import org.elasticsearch.xpack.ql.plan.logical.Aggregate;
import org.elasticsearch.xpack.ql.plan.logical.Limit;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.plan.logical.OrderBy;
import org.elasticsearch.xpack.ql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.ql.rule.RuleExecutor;

import java.util.ArrayList;
import java.util.List;

public class Optimizer extends RuleExecutor<LogicalPlan> {

    public LogicalPlan optimize(LogicalPlan verified) {
        if (verified.optimized()) {
            return verified;
        }
        LogicalPlan plan = execute(verified);
        // ensure we always have single node at the end
        if (plan.singleNode() == false) {
            return new Exchange(plan.source(), plan, Exchange.Type.GATHER, Exchange.Partitioning.SINGLE_DISTRIBUTION);
        }
        return plan;
    }

    @Override
    protected Iterable<RuleExecutor<LogicalPlan>.Batch> batches() {
        Batch fieldExtract = new Batch(
            "Move FieldExtract upwards",
            new FieldExtractPastEval(),
            new FieldExtractPastAggregate(),
            new EmptyFieldExtractRemoval()
        );
        Batch splitNodes = new Batch("Split nodes", new SplitAggregate(), new SplitTopN());
        Batch addExchange = new Batch("Add exchange", new AddExchangeBelowAggregate());
        Batch createTopN = new Batch("Create topN", new CreateTopN());
        // TODO: add rule to prune _doc_id, _segment_id, _shard_id at the top
        // Batch addProject = new Batch("Add project", new AddProjectWhenInternalFieldNoLongerNeeded());
        // TODO: provide option to further parallelize above QueryNode
        // (i.e. always add a local exchange(REPARTITION,FIXED_ARBITRARY_DISTRIBUTION))
        return List.of(createTopN, splitNodes, fieldExtract, addExchange);
    }

    private static class FieldExtractPastEval extends OptimizerRules.OptimizerRule<Eval> {

        @Override
        protected LogicalPlan rule(Eval eval) {
            if (eval.child()instanceof FieldExtract fieldExtract) {
                // If you have an ExtractFieldNode below an EvalNode,
                // only extract the things that the eval needs, and extract the rest above eval
                return possiblySplitExtractFieldNode(eval, eval.fields(), fieldExtract, true);
            }
            return eval;
        }
    }

    private static class FieldExtractPastAggregate extends OptimizerRules.OptimizerRule<Aggregate> {

        @Override
        protected LogicalPlan rule(Aggregate aggregate) {
            if (aggregate.child()instanceof FieldExtract fieldExtract) {
                // If you have an ExtractFieldNode below an Aggregate,
                // only extract the things that the aggregate needs, and extract the rest above eval
                return possiblySplitExtractFieldNode(aggregate, aggregate.aggregates(), fieldExtract, false);
            }
            return aggregate;
        }
    }

    private static UnaryPlan possiblySplitExtractFieldNode(
        UnaryPlan parent,
        List<? extends NamedExpression> namedExpressions,
        FieldExtract fieldExtract,
        boolean preserveUnused
    ) {
        List<Attribute> attributesToKeep = new ArrayList<>();
        List<Attribute> attributesToMoveUp = new ArrayList<>();
        outer: for (Attribute fieldExtractAttribute : fieldExtract.getAttrs()) {
            if (namedExpressions.stream().anyMatch(ne -> ne.anyMatch(e -> e.semanticEquals(fieldExtractAttribute)))) {
                attributesToKeep.add(fieldExtractAttribute);
            } else {
                if (preserveUnused) {
                    attributesToMoveUp.add(fieldExtractAttribute);
                }
            }
        }
        if (attributesToKeep.size() == fieldExtract.getAttrs().size()) {
            return parent;
        }
        return new FieldExtract(
            fieldExtract.source(),
            parent.replaceChild(
                new FieldExtract(
                    fieldExtract.source(),
                    fieldExtract.child(),
                    fieldExtract.index(),
                    attributesToKeep,
                    fieldExtract.getEsQueryAttrs()
                )
            ),
            fieldExtract.index(),
            attributesToMoveUp,
            fieldExtract.getEsQueryAttrs()
        );
    }

    private static class EmptyFieldExtractRemoval extends OptimizerRules.OptimizerRule<FieldExtract> {

        @Override
        protected LogicalPlan rule(FieldExtract fieldExtract) {
            if (fieldExtract.getAttrs().isEmpty()) {
                return fieldExtract.child();
            }
            return fieldExtract;
        }
    }

    private static class SplitAggregate extends OptimizerRules.OptimizerRule<Aggregate> {

        @Override
        protected LogicalPlan rule(Aggregate aggregate) {
            if (aggregate.getMode() == Aggregate.Mode.SINGLE) {
                return new Aggregate(
                    aggregate.source(),
                    new Aggregate(
                        aggregate.source(),
                        aggregate.child(),
                        aggregate.groupings(),
                        aggregate.aggregates(),
                        Aggregate.Mode.PARTIAL
                    ),
                    aggregate.groupings(),
                    aggregate.aggregates(),
                    Aggregate.Mode.FINAL
                );
            }
            return aggregate;
        }
    }

    private static class SplitTopN extends OptimizerRules.OptimizerRule<TopN> {

        @Override
        protected LogicalPlan rule(TopN topN) {
            if (topN.getMode() == TopN.Mode.SINGLE) {
                return new TopN(
                    topN.source(),
                    new TopN(topN.source(), topN.child(), topN.order(), topN.getLimit(), TopN.Mode.PARTIAL),
                    topN.order(),
                    topN.getLimit(),
                    TopN.Mode.FINAL
                );
            }
            return topN;
        }
    }

    private static class AddExchangeBelowAggregate extends OptimizerRules.OptimizerRule<UnaryPlan> {

        @Override
        protected LogicalPlan rule(UnaryPlan parent) {
            if (parent.singleNode() && parent.child().singleNode() == false) {
                if (parent instanceof Exchange exchange
                    && exchange.getType() == Exchange.Type.GATHER
                    && exchange.getPartitioning() == Exchange.Partitioning.SINGLE_DISTRIBUTION) {
                    return parent;
                }
                return parent.replaceChild(
                    new Exchange(parent.source(), parent.child(), Exchange.Type.GATHER, Exchange.Partitioning.SINGLE_DISTRIBUTION)
                );
            }
            return parent;
        }
    }

    private static class CreateTopN extends OptimizerRules.OptimizerRule<Limit> {

        @Override
        protected LogicalPlan rule(Limit limit) {
            if (limit.child()instanceof OrderBy orderBy) {
                return new TopN(limit.source(), orderBy.child(), orderBy.order(), limit.limit());
            }
            return limit;
        }
    }
}
