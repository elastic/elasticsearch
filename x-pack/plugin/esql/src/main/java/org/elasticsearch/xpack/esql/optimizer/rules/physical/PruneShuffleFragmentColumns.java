/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.physical.BinaryExec;
import org.elasticsearch.xpack.esql.plan.physical.DistributedHashJoinExec;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.plan.physical.LeafExec;
import org.elasticsearch.xpack.esql.plan.physical.MergeExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.ShuffleExchangeExec;
import org.elasticsearch.xpack.esql.plan.physical.UnaryExec;
import org.elasticsearch.xpack.esql.planner.mapper.Mapper;
import org.elasticsearch.xpack.esql.rule.Rule;

import java.util.List;

/** Keeps shuffle fragments narrow while preserving the channel order required by exchange consumers. */
public final class PruneShuffleFragmentColumns extends Rule<PhysicalPlan, PhysicalPlan> {
    @Override
    public PhysicalPlan apply(PhysicalPlan plan) {
        if (plan.anyMatch(ShuffleExchangeExec.class::isInstance) == false) {
            return plan;
        }
        return prune(plan, plan.outputSet());
    }

    private static PhysicalPlan prune(PhysicalPlan plan, AttributeSet required) {
        if (plan instanceof FragmentExec fragment) {
            if (fragment.fragment() instanceof Aggregate) {
                return fragment;
            }
            List<Attribute> output = fragment.output().stream().filter(required::contains).toList();
            if (output.isEmpty()) {
                output = fragment.output().isEmpty() ? List.of() : List.of(fragment.output().getFirst());
            }
            return new FragmentExec(
                fragment.source(),
                new Project(fragment.fragment().source(), fragment.fragment(), output),
                fragment.esFilter(),
                fragment.estimatedRowSize(),
                fragment.isFromViewBranch()
            );
        }
        if (plan instanceof ShuffleExchangeExec exchange) {
            AttributeSet childRequired = exchange.inBetweenAggs()
                ? exchange.child().outputSet()
                : required.combine(AttributeSet.of(exchange.keys()));
            return exchange.replaceChild(prune(exchange.child(), childRequired));
        }
        if (plan instanceof DistributedHashJoinExec join) {
            AttributeSet leftRequired = required.intersect(join.left().outputSet()).combine(AttributeSet.of(join.leftFields()));
            AttributeSet joinMarkers = AttributeSet.of(join.addedFields().stream().filter(Mapper::isJoinMarker).toList());
            AttributeSet rightRequired = required.intersect(join.right().outputSet())
                .combine(AttributeSet.of(join.rightFields()))
                .combine(AttributeSet.of(join.addedFields()).subtract(joinMarkers));
            return join.replaceChildren(prune(join.left(), leftRequired), prune(join.right(), rightRequired));
        }
        if (plan instanceof MergeExec merge && merge.kind() == MergeExec.Kind.UNION) {
            return merge;
        }
        if (plan instanceof BinaryExec || plan instanceof LeafExec) {
            return plan;
        }
        if (plan instanceof UnaryExec unary) {
            AttributeSet added = unary.outputSet().subtract(unary.inputSet());
            AttributeSet childRequired = required.subtract(added).combine(unary.references());
            return unary.replaceChild(prune(unary.child(), childRequired));
        }
        return plan;
    }
}
