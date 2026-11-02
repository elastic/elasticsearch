/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical;

import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.optimizer.PhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.cbo.AggMode;
import org.elasticsearch.xpack.esql.plan.logical.Enrich;
import org.elasticsearch.xpack.esql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.BinaryExec;
import org.elasticsearch.xpack.esql.plan.physical.CompoundOutputEvalExec;
import org.elasticsearch.xpack.esql.plan.physical.DistributedHashJoinExec;
import org.elasticsearch.xpack.esql.plan.physical.EnrichExec;
import org.elasticsearch.xpack.esql.plan.physical.EvalExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeDistribution;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeExec;
import org.elasticsearch.xpack.esql.plan.physical.ExternalSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.FilterExec;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.plan.physical.HashJoinExec;
import org.elasticsearch.xpack.esql.plan.physical.LimitExec;
import org.elasticsearch.xpack.esql.plan.physical.LookupJoinExec;
import org.elasticsearch.xpack.esql.plan.physical.MergeExec;
import org.elasticsearch.xpack.esql.plan.physical.MvExpandExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.ProjectExec;
import org.elasticsearch.xpack.esql.plan.physical.RegexExtractExec;
import org.elasticsearch.xpack.esql.plan.physical.SampleExec;
import org.elasticsearch.xpack.esql.plan.physical.ShuffleExchangeExec;
import org.elasticsearch.xpack.esql.plan.physical.TopNExec;
import org.elasticsearch.xpack.esql.plan.physical.UnaryExec;
import org.elasticsearch.xpack.esql.plugin.ShuffleGate;
import org.elasticsearch.xpack.esql.rule.ParameterizedRule;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

/**
 * Places exchanges where physical operators need a particular distribution instead of assigning exchanges to individual features.
 */
public final class EnsureDistribution extends ParameterizedRule<PhysicalPlan, PhysicalPlan, PhysicalOptimizerContext> {
    @Override
    public PhysicalPlan apply(PhysicalPlan plan, PhysicalOptimizerContext context) {
        boolean hasDistributedJoin = plan.anyMatch(DistributedHashJoinExec.class::isInstance);
        if (ShuffleGate.allowed(context.configuration(), plan) == false) {
            if (hasDistributedJoin) {
                throw new IllegalStateException("bug: distributed join in a plan the shuffle gate rejects");
            }
            return plan;
        }
        if (plan.anyMatch(ShuffleExchangeExec.class::isInstance)) {
            throw new IllegalStateException("bug: shuffle exchanges must only be added by EnsureDistribution");
        }

        boolean shuffleAgg = context.configuration().pragmas().shuffleAgg();
        Predicate<AggregateExec> distributeAggregate = aggregate -> context.decisions()
            .get()
            .aggregate(aggregate.aggregates())
            .map(mode -> mode == AggMode.DISTRIBUTED)
            .orElse(shuffleAgg);
        if (hasDistributedJoin == false
            && plan.anyMatch(node -> node instanceof AggregateExec aggregate && distributeAggregate.test(aggregate)) == false) {
            return plan;
        }

        PhysicalPlan rewritten = new Walker(distributeAggregate).ensure(plan, SingleRequirement.INSTANCE).plan();
        return hasShuffleExchange(rewritten) ? rewritten : plan;
    }

    private sealed interface Requirement permits AnyRequirement, SingleRequirement, HashRequirement, BroadcastRequirement {}

    private enum AnyRequirement implements Requirement {
        INSTANCE
    }

    private enum SingleRequirement implements Requirement {
        INSTANCE
    }

    private enum BroadcastRequirement implements Requirement {
        INSTANCE
    }

    private record HashRequirement(List<Attribute> keys) implements Requirement {
        private HashRequirement {
            keys = List.copyOf(keys);
        }
    }

    private sealed interface Partitioning permits SourcePartitioning, SinglePartitioning, HashPartitioning, ArbitraryPartitioning {}

    private enum SourcePartitioning implements Partitioning {
        INSTANCE
    }

    private enum SinglePartitioning implements Partitioning {
        INSTANCE
    }

    private record HashPartitioning(List<Attribute> keys, boolean exact) implements Partitioning {
        private HashPartitioning {
            keys = List.copyOf(keys);
        }
    }

    private enum ArbitraryPartitioning implements Partitioning {
        INSTANCE
    }

    private record Result(PhysicalPlan plan, Partitioning partitioning) {}

    private static final class Walker {
        private final Predicate<AggregateExec> distributeAggregate;

        private Walker(Predicate<AggregateExec> distributeAggregate) {
            this.distributeAggregate = distributeAggregate;
        }

        private Result ensure(PhysicalPlan node, Requirement requirement) {
            if (requirement instanceof BroadcastRequirement) {
                return exchange(visit(node, AnyRequirement.INSTANCE), requirement);
            }
            Result result = visit(node, requirement);
            return satisfies(result.partitioning(), requirement) ? result : exchange(result, requirement);
        }

        private Result visit(PhysicalPlan node, Requirement requirement) {
            if (node instanceof FragmentExec || node instanceof ExternalSourceExec) {
                return new Result(node, SourcePartitioning.INSTANCE);
            }
            if (node instanceof ExchangeExec exchange) {
                return legacyExchange(exchange, requirement);
            }
            if (node instanceof AggregateExec aggregate && aggregate.getClass() == AggregateExec.class) {
                return switch (aggregate.getMode()) {
                    case FINAL -> finalAggregate(aggregate);
                    case SINGLE -> singleAggregate(aggregate);
                    case INITIAL, INTERMEDIATE -> streaming(aggregate, requirement);
                };
            }
            if (node instanceof DistributedHashJoinExec join) {
                Result left = switch (join.mode()) {
                    case PARTITIONED -> ensure(join.left(), new HashRequirement(join.leftFields()));
                    case BROADCAST -> visit(join.left(), AnyRequirement.INSTANCE);
                };
                Result right = switch (join.mode()) {
                    case PARTITIONED -> ensure(join.right(), new HashRequirement(join.rightFields()));
                    case BROADCAST -> ensure(join.right(), BroadcastRequirement.INSTANCE);
                };
                PhysicalPlan rewritten = join.replaceChildren(left.plan(), right.plan());
                Partitioning output;
                if (join.mode() == DistributedHashJoinExec.Mode.PARTITIONED) {
                    if (left.partitioning() instanceof HashPartitioning hash) {
                        output = containsIds(rewritten.output(), join.leftFields())
                            ? new HashPartitioning(join.leftFields(), hash.exact())
                            : ArbitraryPartitioning.INSTANCE;
                    } else {
                        throw new IllegalStateException("bug: partitioned join probe side is not hash partitioned");
                    }
                } else {
                    output = keepIfPresent(left.partitioning(), rewritten.output());
                }
                return new Result(rewritten, output);
            }
            if (node instanceof HashJoinExec || node instanceof LookupJoinExec) {
                BinaryExec join = (BinaryExec) node;
                Result left = visit(join.left(), AnyRequirement.INSTANCE);
                if (left.partitioning() instanceof SourcePartitioning) {
                    left = exchange(left, SingleRequirement.INSTANCE);
                }
                PhysicalPlan rewritten = join.replaceChildren(List.of(left.plan(), join.right()));
                return new Result(rewritten, keepIfPresent(left.partitioning(), rewritten.output()));
            }
            if (node instanceof LimitExec || node instanceof TopNExec) {
                UnaryExec limit = (UnaryExec) node;
                Result child = visit(limit.child(), AnyRequirement.INSTANCE);
                if (child.partitioning() instanceof SinglePartitioning) {
                    return new Result(limit.replaceChild(child.plan()), SinglePartitioning.INSTANCE);
                }
                PhysicalPlan partial = limit.replaceChild(child.plan());
                return new Result(limit.replaceChild(gather(partial, false, List.of())), SinglePartitioning.INSTANCE);
            }
            if (node instanceof MergeExec merge && merge.kind() == MergeExec.Kind.UNION) {
                return union(merge, requirement);
            }
            if (isStreaming(node)) {
                return streaming((UnaryExec) node, requirement);
            }

            List<PhysicalPlan> children = new ArrayList<>(node.children().size());
            for (PhysicalPlan child : node.children()) {
                children.add(ensure(child, SingleRequirement.INSTANCE).plan());
            }
            if (children.isEmpty()) {
                return new Result(node, SinglePartitioning.INSTANCE);
            }
            return new Result(node.replaceChildren(children), SinglePartitioning.INSTANCE);
        }

        private Result streaming(UnaryExec node, Requirement requirement) {
            Requirement childRequirement = reachesUnion(node.child()) ? pushDown(requirement, node) : AnyRequirement.INSTANCE;
            Result child = visit(node.child(), childRequirement);
            return new Result(node.replaceChild(child.plan()), translateUp(child.partitioning(), node));
        }

        private Result legacyExchange(ExchangeExec exchange, Requirement requirement) {
            Result child = visit(exchange.child(), AnyRequirement.INSTANCE);
            if (exchange.inBetweenAggs()) {
                return new Result(gather(child.plan(), true, exchange.output()), SinglePartitioning.INSTANCE);
            }
            if (requirement instanceof AnyRequirement) {
                return child;
            }
            if (requirement instanceof SingleRequirement) {
                return child.partitioning() instanceof SinglePartitioning
                    ? child
                    : new Result(gather(child.plan(), false, List.of()), SinglePartitioning.INSTANCE);
            }

            HashRequirement hash = (HashRequirement) requirement;
            if (child.partitioning() instanceof HashPartitioning childHash && sameIds(childHash.keys(), hash.keys())) {
                return child;
            }
            return containsIds(child.plan().output(), hash.keys())
                ? new Result(hash(child.plan(), hash.keys(), false, List.of()), new HashPartitioning(hash.keys(), false))
                : new Result(gather(child.plan(), false, List.of()), SinglePartitioning.INSTANCE);
        }

        private Result finalAggregate(AggregateExec aggregate) {
            if (aggregate.child() instanceof ExchangeExec exchange && exchange.inBetweenAggs()) {
                Result child = visit(exchange.child(), AnyRequirement.INSTANCE);
                int groupCount = aggregate.groupings().size();
                if (distributeAggregate.test(aggregate)
                    && ShuffleRuleUtils.hasSupportedGroupings(aggregate.groupings())
                    && exchange.output().size() >= groupCount) {
                    List<Attribute> keys = exchange.output().subList(0, groupCount);
                    PhysicalPlan exchanged = hash(child.plan(), keys, true, exchange.output());
                    AggregateExec finalAggregate = aggregate.replaceChild(exchanged);
                    return new Result(finalAggregate, groupPartitioning(finalAggregate));
                }
                return new Result(aggregate.replaceChild(gather(child.plan(), true, exchange.output())), SinglePartitioning.INSTANCE);
            }
            if (aggregate.child() instanceof AggregateExec initial && initial.getMode() == AggregatorMode.INITIAL) {
                Result child = visit(initial.child(), AnyRequirement.INSTANCE);
                PhysicalPlan rewrittenInitial = initial.replaceChild(child.plan());
                if (child.partitioning() instanceof SinglePartitioning) {
                    return new Result(aggregate.replaceChild(rewrittenInitial), SinglePartitioning.INSTANCE);
                }
                return new Result(aggregate.replaceChild(gather(rewrittenInitial, true, initial.output())), SinglePartitioning.INSTANCE);
            }
            Result child = ensure(aggregate.child(), SingleRequirement.INSTANCE);
            return new Result(aggregate.replaceChild(child.plan()), SinglePartitioning.INSTANCE);
        }

        private Result singleAggregate(AggregateExec aggregate) {
            List<Attribute> groups = groupAttributes(aggregate);
            boolean canHash = distributeAggregate.test(aggregate)
                && groups != null
                && ShuffleRuleUtils.hasSupportedGroupings(aggregate.groupings());
            Requirement pushedRequirement = canHash ? new HashRequirement(groups) : SingleRequirement.INSTANCE;
            Requirement childRequirement = reachesUnion(aggregate.child()) ? pushedRequirement : AnyRequirement.INSTANCE;
            Result child = visit(aggregate.child(), childRequirement);

            if (child.partitioning() instanceof SinglePartitioning) {
                return new Result(aggregate.replaceChild(child.plan()), SinglePartitioning.INSTANCE);
            }
            if (child.partitioning() instanceof HashPartitioning hash
                && hash.exact()
                && groups != null
                && containsIds(groups, hash.keys())) {
                return new Result(aggregate.replaceChild(child.plan()), keepIfPresent(hash, aggregate.output()));
            }
            if (ShuffleRuleUtils.hasSupportedGroupKeyTypes(aggregate.groupings()) == false) {
                return new Result(aggregate.replaceChild(gather(child.plan(), false, List.of())), SinglePartitioning.INSTANCE);
            }

            List<Attribute> intermediate = aggregate.intermediateAttributes();
            AggregateExec initial = new AggregateExec(
                aggregate.source(),
                child.plan(),
                aggregate.groupings(),
                aggregate.aggregates(),
                AggregatorMode.INITIAL,
                intermediate,
                aggregate.estimatedRowSize()
            );
            PhysicalPlan exchange;
            boolean exact;
            if (canHash && intermediate.size() >= groups.size()) {
                exchange = hash(initial, intermediate.subList(0, groups.size()), true, intermediate);
                exact = true;
            } else {
                exchange = gather(initial, true, intermediate);
                exact = false;
            }
            AggregateExec finalAggregate = new AggregateExec(
                aggregate.source(),
                exchange,
                aggregate.groupings(),
                aggregate.aggregates(),
                AggregatorMode.FINAL,
                intermediate,
                aggregate.estimatedRowSize()
            );
            return new Result(finalAggregate, exact ? groupPartitioning(finalAggregate) : SinglePartitioning.INSTANCE);
        }

        private Result union(MergeExec merge, Requirement requirement) {
            Requirement effective = requirement instanceof AnyRequirement ? SingleRequirement.INSTANCE : requirement;
            List<PhysicalPlan> branches = new ArrayList<>(merge.children().size());
            boolean exact = true;
            for (PhysicalPlan branch : merge.children()) {
                if (branch.output().size() != merge.output().size()) {
                    throw new IllegalStateException("bug: union branch is not aligned with the union output");
                }
                Requirement branchRequirement = byPosition(effective, merge.output(), branch.output());
                Result result = ensure(branch, branchRequirement);
                if (result.plan() instanceof MergeExec inner && inner.kind() == MergeExec.Kind.UNION) {
                    branches.addAll(inner.children());
                    exact &= result.partitioning() instanceof HashPartitioning hash && hash.exact();
                    continue;
                }
                if (result.plan() instanceof ShuffleExchangeExec == false) {
                    result = forceExchange(result, branchRequirement);
                }
                if (result.partitioning() instanceof HashPartitioning hash) {
                    exact &= hash.exact();
                }
                branches.add(result.plan());
            }

            Partitioning output;
            if (effective instanceof SingleRequirement) {
                output = SinglePartitioning.INSTANCE;
            } else {
                HashRequirement hash = (HashRequirement) effective;
                output = new HashPartitioning(hash.keys(), exact);
            }
            return new Result(merge.replaceChildren(branches), output);
        }

        private static Result forceExchange(Result result, Requirement requirement) {
            if (requirement instanceof SingleRequirement) {
                return new Result(gather(result.plan(), false, List.of()), SinglePartitioning.INSTANCE);
            }
            if (requirement instanceof HashRequirement hash) {
                boolean exact = result.partitioning() instanceof HashPartitioning existing
                    && existing.exact()
                    && sameIds(existing.keys(), hash.keys());
                return new Result(hash(result.plan(), hash.keys(), false, List.of()), new HashPartitioning(hash.keys(), exact));
            }
            throw new IllegalStateException("bug: union branch has no concrete exchange requirement");
        }

        private static Result exchange(Result result, Requirement requirement) {
            if (requirement instanceof SingleRequirement) {
                return new Result(gather(result.plan(), false, List.of()), SinglePartitioning.INSTANCE);
            }
            if (requirement instanceof HashRequirement hash) {
                return new Result(hash(result.plan(), hash.keys(), false, List.of()), new HashPartitioning(hash.keys(), false));
            }
            if (requirement instanceof BroadcastRequirement) {
                return new Result(broadcast(result.plan()), ArbitraryPartitioning.INSTANCE);
            }
            throw new IllegalStateException("bug: ANY requirement cannot need an exchange");
        }
    }

    private static boolean satisfies(Partitioning partitioning, Requirement requirement) {
        if (requirement instanceof AnyRequirement) {
            return true;
        }
        if (requirement instanceof SingleRequirement) {
            return partitioning instanceof SinglePartitioning;
        }
        if (requirement instanceof BroadcastRequirement) {
            return false;
        }
        HashRequirement hash = (HashRequirement) requirement;
        return partitioning instanceof HashPartitioning delivered && sameIds(delivered.keys(), hash.keys());
    }

    private static boolean isStreaming(PhysicalPlan plan) {
        if (plan instanceof EnrichExec enrich) {
            return enrich.mode() != Enrich.Mode.COORDINATOR;
        }
        if (plan instanceof AggregateExec aggregate) {
            return aggregate.getMode() == AggregatorMode.INITIAL || aggregate.getMode() == AggregatorMode.INTERMEDIATE;
        }
        return plan instanceof EvalExec
            || plan instanceof FilterExec
            || plan instanceof ProjectExec
            || plan instanceof MvExpandExec
            || plan instanceof RegexExtractExec
            || plan instanceof CompoundOutputEvalExec
            || plan instanceof SampleExec;
    }

    private static boolean reachesUnion(PhysicalPlan plan) {
        PhysicalPlan current = plan;
        while (current instanceof UnaryExec unary && isStreaming(current)) {
            current = unary.child();
        }
        return current instanceof MergeExec merge && merge.kind() == MergeExec.Kind.UNION;
    }

    private static Requirement pushDown(Requirement requirement, UnaryExec node) {
        if (requirement instanceof AnyRequirement
            || requirement instanceof SingleRequirement
            || requirement instanceof BroadcastRequirement) {
            return requirement;
        }
        HashRequirement hash = (HashRequirement) requirement;
        List<Attribute> childKeys = new ArrayList<>(hash.keys().size());
        for (Attribute key : hash.keys()) {
            Attribute childKey = null;
            if (node instanceof ProjectExec project) {
                for (NamedExpression projection : project.projections()) {
                    Attribute output = projection.toAttribute();
                    if (sameId(output, key)) {
                        if (projection instanceof Alias alias && alias.child() instanceof Attribute attribute) {
                            childKey = attribute;
                        } else if (projection instanceof Attribute attribute) {
                            childKey = attribute;
                        }
                        break;
                    }
                }
            } else {
                childKey = findById(node.child().output(), key);
            }
            if (childKey == null) {
                return AnyRequirement.INSTANCE;
            }
            childKeys.add(childKey);
        }
        return new HashRequirement(childKeys);
    }

    private static Partitioning translateUp(Partitioning partitioning, UnaryExec node) {
        if (partitioning instanceof HashPartitioning hash) {
            List<Attribute> outputKeys = new ArrayList<>(hash.keys().size());
            for (Attribute key : hash.keys()) {
                Attribute output = null;
                if (node instanceof ProjectExec project) {
                    for (NamedExpression projection : project.projections()) {
                        Attribute projected = projection.toAttribute();
                        if (sameId(projected, key)
                            || projection instanceof Alias alias
                                && alias.child() instanceof Attribute attribute
                                && sameId(attribute, key)) {
                            output = projected;
                            break;
                        }
                    }
                } else {
                    output = findById(node.output(), key);
                }
                if (output == null) {
                    return ArbitraryPartitioning.INSTANCE;
                }
                outputKeys.add(output);
            }
            return new HashPartitioning(outputKeys, hash.exact());
        }
        return partitioning;
    }

    private static Partitioning keepIfPresent(Partitioning partitioning, List<Attribute> output) {
        if (partitioning instanceof HashPartitioning hash && containsIds(output, hash.keys()) == false) {
            return ArbitraryPartitioning.INSTANCE;
        }
        return partitioning;
    }

    private static List<Attribute> groupAttributes(AggregateExec aggregate) {
        List<Attribute> result = new ArrayList<>(aggregate.groupings().size());
        for (Expression grouping : aggregate.groupings()) {
            Attribute attribute = Expressions.attribute(grouping);
            if (attribute == null) {
                return null;
            }
            result.add(attribute);
        }
        return result;
    }

    private static Partitioning groupPartitioning(AggregateExec aggregate) {
        List<Attribute> groups = groupAttributes(aggregate);
        if (groups != null && groups.isEmpty()) {
            return SinglePartitioning.INSTANCE;
        }
        if (groups == null) {
            return ArbitraryPartitioning.INSTANCE;
        }
        List<Attribute> outputKeys = new ArrayList<>(groups.size());
        for (Attribute group : groups) {
            Attribute output = findById(aggregate.output(), group);
            if (output == null) {
                return ArbitraryPartitioning.INSTANCE;
            }
            outputKeys.add(output);
        }
        return new HashPartitioning(outputKeys, true);
    }

    private static Requirement byPosition(Requirement requirement, List<Attribute> mergeOutput, List<Attribute> branchOutput) {
        if (requirement instanceof HashRequirement hash) {
            List<Attribute> branchKeys = new ArrayList<>(hash.keys().size());
            for (Attribute key : hash.keys()) {
                int index = indexOfId(mergeOutput, key);
                if (index < 0) {
                    throw new IllegalStateException("bug: hash key is not present in union output");
                }
                branchKeys.add(branchOutput.get(index));
            }
            return new HashRequirement(branchKeys);
        }
        return requirement;
    }

    private static boolean hasShuffleExchange(PhysicalPlan plan) {
        return plan.anyMatch(ShuffleExchangeExec.class::isInstance);
    }

    private static boolean containsIds(List<Attribute> attributes, List<Attribute> required) {
        for (Attribute key : required) {
            if (findById(attributes, key) == null) {
                return false;
            }
        }
        return true;
    }

    private static Attribute findById(List<Attribute> attributes, Attribute key) {
        for (Attribute attribute : attributes) {
            if (sameId(attribute, key)) {
                return attribute;
            }
        }
        return null;
    }

    private static int indexOfId(List<Attribute> attributes, Attribute key) {
        for (int i = 0; i < attributes.size(); i++) {
            if (sameId(attributes.get(i), key)) {
                return i;
            }
        }
        return -1;
    }

    private static boolean sameIds(List<Attribute> left, List<Attribute> right) {
        if (left.size() != right.size()) {
            return false;
        }
        for (int i = 0; i < left.size(); i++) {
            if (sameId(left.get(i), right.get(i)) == false) {
                return false;
            }
        }
        return true;
    }

    private static boolean sameId(Attribute left, Attribute right) {
        return left.id().equals(right.id());
    }

    private static ShuffleExchangeExec hash(PhysicalPlan child, List<Attribute> keys, boolean inBetweenAggs, List<Attribute> output) {
        if (inBetweenAggs == false && containsIds(child.output(), keys) == false) {
            throw new IllegalStateException("bug: hash exchange keys are not present in child output");
        }
        return new ShuffleExchangeExec(child.source(), output, inBetweenAggs, ExchangeDistribution.HASH, keys, child);
    }

    private static ShuffleExchangeExec gather(PhysicalPlan child, boolean inBetweenAggs, List<Attribute> output) {
        return new ShuffleExchangeExec(child.source(), output, inBetweenAggs, ExchangeDistribution.GATHER, List.of(), child);
    }

    private static ShuffleExchangeExec broadcast(PhysicalPlan child) {
        return new ShuffleExchangeExec(child.source(), List.of(), false, ExchangeDistribution.BROADCAST, List.of(), child);
    }
}
