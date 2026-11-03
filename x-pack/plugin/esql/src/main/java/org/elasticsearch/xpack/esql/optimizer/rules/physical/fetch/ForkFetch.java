/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.fetch;

import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.expression.function.aggregate.First;
import org.elasticsearch.xpack.esql.expression.function.aggregate.FirstDocId;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.fetch.FetchPhasePolicy.Outcome;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.fetch.PlanFetch.Branch;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.fetch.PlanFetch.BranchShape;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.fetch.PlanFetch.Declined;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.fetch.PlanFetch.Demand;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.fetch.PlanFetch.FilterAroundTheFetch;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.UnmappedFieldsAttribute;
import org.elasticsearch.xpack.esql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.CarriesDetachedDoc;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeExec;
import org.elasticsearch.xpack.esql.plan.physical.FilterExec;
import org.elasticsearch.xpack.esql.plan.physical.LimitExec;
import org.elasticsearch.xpack.esql.plan.physical.MergeExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.ProjectExec;
import org.elasticsearch.xpack.esql.plan.physical.TopNExec;
import org.elasticsearch.xpack.esql.plan.physical.UnaryExec;
import org.elasticsearch.xpack.esql.planner.AbstractPhysicalOperationProviders;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.xpack.esql.optimizer.rules.physical.fetch.PlanFetch.ineligible;

/**
 * Plans the fetch phase for the rows of a {@code FORK}, with or without {@code FUSE} after it, when every branch ends
 * in a cut that the data nodes and the coordinator both apply.
 * <p>
 * {@code FROM idx METADATA _id, _index, _score | FORK (WHERE MATCH(a, "x") | SORT _score DESC | LIMIT 50)
 * (WHERE MATCH(b, "x") | SORT _score DESC | LIMIT 50) | FUSE | SORT _score DESC | LIMIT 10 | KEEP a, b, _score} before:
 * <pre>{@code
 * ProjectExec[a, b, _score]
 * \_TopNExec[_score DESC, 10]
 *   \_AggregateExec[BY _id, _index: SUM(_score) AS _score, FIRST(a) AS a, FIRST(b) AS b]
 *     \_FuseScoreEvalExec[_score, _fork]
 *       \_MergeExec[a, b, _id, _index, _score, _fork]
 *         |_ProjectExec <- EvalExec[_fork = "fork1"] <- TopNExec[_score DESC, 50] <- ExchangeExec[a, b, _id, _index, _score]
 *         \_ProjectExec <- EvalExec[_fork = "fork2"] <- TopNExec[_score DESC, 50] <- ExchangeExec[a, b, _id, _index, _score]
 * }</pre>
 * and after:
 * <pre>{@code
 * ProjectExec[a, b, _score]
 * \_FetchExec[_doc, fetched=[a, b]]
 *   \_TopNExec[_score DESC, 10]
 *     \_AggregateExec[BY _id, _index: SUM(_score) AS _score, FIRST_DOC_ID(_doc) AS _doc]
 *       \_FuseScoreEvalExec[_score, _fork]
 *         \_MergeExec[_doc, _id, _index, _score, _fork]
 *           |_ProjectExec <- EvalExec[_fork = "fork1"] <- TopNExec[_score DESC, 50] <- ExchangeExec[_doc, _id, _index, _score]
 *           \_ProjectExec <- EvalExec[_fork = "fork2"] <- TopNExec[_score DESC, 50] <- ExchangeExec[_doc, _id, _index, _score]
 * }</pre>
 * Each branch sends the documents of its candidates with the columns its own cut, its commands and {@code FUSE} read.
 * The coordinator cuts every branch, fuses their rows, cuts again, and fetches the other columns for the final winners
 * only. The <em>final cut</em> is the lowest {@code TopN} or {@code LIMIT} after the merge.
 * <p>
 * {@code FUSE} groups the rows by {@code _id} and {@code _index}, so the rows of a group are one document, as the
 * branches that found it saw it. A {@link FirstDocId} keeps the {@code _doc} of the first of them. A {@code FIRST} of a
 * column of the relation picks the value of that document, which is what the fetch loads for it, so the fetch replaces
 * that {@code FIRST}. An aggregation that groups by anything that doesn't name the document keeps the plan eager.
 * <p>
 * A column of the merge is fetched when all of these hold:
 * <ul>
 *     <li>every branch fetches it on its own, see {@link PlanFetch#demand}, and reads it only to pass it on after its
 *     cut,</li>
 *     <li>it is the same field, the same metadata, like {@code _source}, or the unmapped fields of the same pattern in
 *     every branch, so the document gives the same value whatever branch found it,</li>
 *     <li>nothing between the merge and the final cut reads it, other than a {@code FIRST} of {@code FUSE},</li>
 *     <li>the final cut doesn't read it, nor the {@code FIRST} that stands for it.</li>
 * </ul>
 * Every branch must read the same indices, which authorize the fetch.
 */
final class ForkFetch {
    private ForkFetch() {}

    /**
     * A {@code FORK} the rule can rewrite.
     *
     * @param between              the execs between the final cut and the merge, from the cut down
     * @param documentAggregate    the aggregation in {@code between} that groups the rows by document, like {@code FUSE}
     * @param deferred             the positions of the merged columns the fetch replaces
     * @param fetched              the columns the fetch appends. Each takes the id of the column it replaces after the final
     *                             cut, the merged column or its {@code FIRST}
     * @param filterAroundTheFetch the filter right after the final cut, when some of its conditions run before the fetch
     */
    record Candidate(
        MergeExec merge,
        List<UnaryExec> between,
        UnaryExec finalCut,
        @Nullable AggregateExec documentAggregate,
        List<ForkBranch> branches,
        Set<Integer> deferred,
        List<Attribute> fetched,
        @Nullable FilterAroundTheFetch filterAroundTheFetch
    ) implements PlanFetch.Shape {}

    /**
     * One branch of the merge.
     *
     * @param root            the projection that ends the branch, whose output lines up with the merge's
     * @param branch          the cut of the branch, its exchange and its fragment
     * @param demand          the columns the branch would fetch on its own
     * @param readAfterTheCut the columns the branch reads between its cut and its root, other than to pass them on
     */
    record ForkBranch(ProjectExec root, Branch branch, Demand demand, AttributeSet readAfterTheCut) implements BranchShape {}

    static PlanFetch.Shape shape(PhysicalPlan plan, MergeExec merge) {
        if (merge.kind() != MergeExec.Kind.FORK) {
            return ineligible("[MergeExec] of a [" + merge.kind() + "] combines several plans");
        }
        List<UnaryExec> path = new ArrayList<>();
        PhysicalPlan node = plan;
        while (node != merge) {
            if (node instanceof UnaryExec unary) {
                path.add(unary);
                node = unary.child();
            } else {
                return ineligible("[" + node.nodeName() + "] runs after the [FORK]");
            }
        }
        int cutIndex = path.size() - 1;
        while (cutIndex >= 0 && path.get(cutIndex) instanceof TopNExec == false && path.get(cutIndex) instanceof LimitExec == false) {
            cutIndex--;
        }
        if (cutIndex < 0) {
            return ineligible("no LIMIT or TopN after the [FORK]");
        }
        UnaryExec finalCut = path.get(cutIndex);
        for (UnaryExec exec : path.subList(0, cutIndex)) {
            if (exec instanceof CarriesDetachedDoc == false) {
                return ineligible("[" + exec.nodeName() + "] runs after the cut");
            }
        }
        List<UnaryExec> between = List.copyOf(path.subList(cutIndex + 1, path.size()));
        AggregateExec documentAggregate = null;
        for (UnaryExec exec : between) {
            if (exec instanceof CarriesDetachedDoc) {
                continue;
            }
            if (exec instanceof AggregateExec aggregate && documentAggregate == null && groupsByDocument(aggregate, merge)) {
                documentAggregate = aggregate;
                continue;
            }
            return ineligible("[" + exec.nodeName() + "] runs between the cut and the [FORK]");
        }

        List<ForkBranch> branches = new ArrayList<>(merge.children().size());
        for (PhysicalPlan child : merge.children()) {
            BranchShape found = branch(child, merge.output().size());
            if (found instanceof Declined declined) {
                return declined;
            }
            branches.add((ForkBranch) found);
        }
        EsRelation relation = branches.getFirst().branch().relation();
        for (ForkBranch branch : branches) {
            EsRelation other = branch.branch().relation();
            if (other.indexPattern().equals(relation.indexPattern()) == false
                || PlanFetch.localIndices(other).equals(PlanFetch.localIndices(relation)) == false) {
                return ineligible("the branches of the [FORK] read different indices");
            }
        }

        Reads reads = Reads.of(between, finalCut, documentAggregate);
        Set<Integer> deferred = new HashSet<>();
        List<Attribute> fetched = new ArrayList<>();
        Set<NameId> replaced = new HashSet<>();
        for (int position = 0; position < merge.output().size(); position++) {
            Attribute column = sameFetchedColumn(branches, position);
            if (column == null) {
                continue;
            }
            Attribute merged = merge.output().get(position);
            if (reads.belowTheAggregate().contains(merged)) {
                continue;
            }
            if (documentAggregate == null) {
                deferred.add(position);
                fetched.add(column.withId(merged.id()));
                replaced.add(merged.id());
                continue;
            }
            List<? extends NamedExpression> uses = documentAggregate.aggregates()
                .stream()
                .filter(a -> a.references().contains(merged))
                .toList();
            if (uses.isEmpty()) {
                // nothing reads it after the merge
                deferred.add(position);
                continue;
            }
            if (uses.size() == 1 && isFirstOf(uses.getFirst(), merged)) {
                Attribute first = uses.getFirst().toAttribute();
                if (reads.aboveTheAggregate().contains(first) == false) {
                    deferred.add(position);
                    fetched.add(column.withId(first.id()));
                    replaced.add(first.id());
                }
            }
        }
        if (fetched.isEmpty()) {
            return new Declined(Outcome.INELIGIBLE_NO_DEFERRABLE_FIELDS, "the cuts read every column, or the others cannot be fetched");
        }

        AttributeSet.Builder crossing = AttributeSet.builder();
        for (Attribute attribute : finalCut.output()) {
            if (replaced.contains(attribute.id()) == false) {
                crossing.add(attribute);
            }
        }
        FilterAroundTheFetch filterAroundTheFetch = cutIndex > 0 && path.get(cutIndex - 1) instanceof FilterExec filter
            ? FilterAroundTheFetch.split(filter, crossing.build())
            : null;
        return new Candidate(merge, between, finalCut, documentAggregate, branches, deferred, fetched, filterAroundTheFetch);
    }

    static PhysicalPlan rewrite(PhysicalPlan plan, Candidate candidate) {
        Attribute docOfTheBranches = PlanFetch.newDocAttribute();
        List<PhysicalPlan> children = new ArrayList<>(candidate.branches().size());
        for (ForkBranch branch : candidate.branches()) {
            children.add(narrowBranch(branch, candidate.deferred(), docOfTheBranches));
        }
        MergeExec merge = candidate.merge();
        Attribute doc = PlanFetch.newDocAttribute();
        List<Attribute> merged = new ArrayList<>();
        merged.add(doc);
        for (int position = 0; position < merge.output().size(); position++) {
            if (candidate.deferred().contains(position) == false) {
                merged.add(merge.output().get(position));
            }
        }
        PhysicalPlan below = new MergeExec(merge.source(), children, merged, merge.kind());
        Set<NameId> fetchedIds = new HashSet<>();
        candidate.fetched().forEach(attribute -> fetchedIds.add(attribute.id()));
        for (UnaryExec exec : candidate.between().reversed()) {
            if (exec == candidate.documentAggregate()) {
                Alias firstDoc = new Alias(exec.source(), EsQueryExec.DOC_ID_FIELD.getName(), new FirstDocId(exec.source(), doc));
                below = keepTheFirstDocument(candidate.documentAggregate(), below, firstDoc, fetchedIds);
                doc = firstDoc.toAttribute();
            } else if (exec instanceof ProjectExec project) {
                List<NamedExpression> projections = new ArrayList<>(project.projections().size() + 1);
                projections.add(doc);
                projections.addAll(project.projections());
                below = new ProjectExec(project.source(), below, projections);
            } else {
                below = exec.replaceChild(below);
            }
        }
        PhysicalPlan cut = candidate.finalCut().replaceChild(below);
        return PlanFetch.fetchAfterTheCut(
            plan,
            candidate.finalCut(),
            cut,
            candidate.filterAroundTheFetch(),
            doc,
            candidate.fetched(),
            candidate.branches().getFirst().branch().relation()
        );
    }

    /**
     * Checks one branch of the merge: a projection on top, then execs that a detached {@code _doc} can pass through, then
     * a cut over an exchange that {@link PlanFetch#branch} accepts.
     */
    private static BranchShape branch(PhysicalPlan child, int mergeWidth) {
        if (child instanceof ProjectExec == false) {
            return ineligible("a [FORK] branch does not end in a projection");
        }
        ProjectExec root = (ProjectExec) child;
        if (root.output().size() != mergeWidth) {
            assert false : "branch output " + root.output() + " doesn't line up with the merge";
            return new Declined(Outcome.INCONSISTENT_PROJECTION, "a [FORK] branch doesn't line up with the merge");
        }
        AttributeSet.Builder readAfterExchange = AttributeSet.builder().addAll(root.outputSet());
        AttributeSet.Builder readAfterTheCut = AttributeSet.builder();
        UnaryExec node = root;
        while (node.child() instanceof ExchangeExec == false) {
            if (node instanceof CarriesDetachedDoc == false || node.child() instanceof UnaryExec == false) {
                return ineligible("[" + node.nodeName() + "] runs after the cut of a [FORK] branch");
            }
            readAfterExchange.addAll(node.references());
            if (node instanceof ProjectExec project) {
                for (NamedExpression projection : project.projections()) {
                    if (projection instanceof Attribute == false) {
                        readAfterTheCut.addAll(projection.references());
                    }
                }
            } else {
                readAfterTheCut.addAll(node.references());
            }
            node = (UnaryExec) node.child();
        }
        if (node instanceof TopNExec == false && node instanceof LimitExec == false) {
            return ineligible("[" + node.nodeName() + "] reads the exchange of a [FORK] branch instead of a LIMIT or TopN");
        }
        readAfterExchange.addAll(node.references());
        BranchShape found = PlanFetch.branch(node, (ExchangeExec) node.child());
        if (found instanceof Declined declined) {
            return declined;
        }
        Branch branch = (Branch) found;
        Declined inconsistent = PlanFetch.checkEveryColumnIsRead(branch, readAfterExchange.build());
        if (inconsistent != null) {
            return inconsistent;
        }
        Demand demand = PlanFetch.demand(branch.fragmentRoot(), branch.relation(), node);
        return new ForkBranch(root, branch, demand, readAfterTheCut.build());
    }

    /**
     * The column of the documents that every branch carries at {@code position} and would fetch, as the first branch
     * names it, or {@code null} when a branch reads it after its cut, computes it, or carries another column there.
     */
    @Nullable
    private static Attribute sameFetchedColumn(List<ForkBranch> branches, int position) {
        Attribute first = null;
        for (ForkBranch branch : branches) {
            Attribute column = branch.root().output().get(position);
            if (AttributeSet.of(branch.demand().deferred()).contains(column) == false || branch.readAfterTheCut().contains(column)) {
                return null;
            }
            if (first == null) {
                first = column;
            } else if (sameColumnOfTheDocument(first, column) == false) {
                return null;
            }
        }
        return first;
    }

    /**
     * Whether two columns the fetch can load give the same value for a document: the same field, the same metadata, or
     * the unmapped fields of the same pattern. Branches can carry different patterns, and the fetch loads one of them.
     */
    private static boolean sameColumnOfTheDocument(Attribute one, Attribute other) {
        if (one.getClass() != other.getClass() || one.dataType() != other.dataType()) {
            return false;
        }
        return switch (one) {
            case FieldAttribute field -> field.fieldName().equals(((FieldAttribute) other).fieldName());
            case MetadataAttribute metadata -> metadata.name().equals(other.name());
            case UnmappedFieldsAttribute unmapped -> unmapped.pattern().equals(((UnmappedFieldsAttribute) other).pattern());
            default -> throw new IllegalStateException("the fetch can't load [" + one + "]");
        };
    }

    private static boolean isFirstOf(NamedExpression aggregate, Attribute merged) {
        return aggregate instanceof Alias alias && alias.child() instanceof First first && first.field().semanticEquals(merged);
    }

    /**
     * Whether every group of {@code aggregate} is one document: its groupings include the {@code _id} and the
     * {@code _index} of the rows, as every branch of {@code merge} names them.
     */
    private static boolean groupsByDocument(AggregateExec aggregate, MergeExec merge) {
        if (aggregate.getMode() != AggregatorMode.SINGLE || aggregate.groupings().stream().allMatch(Attribute.class::isInstance) == false) {
            return false;
        }
        AttributeSet groupings = AttributeSet.of(aggregate.groupings().stream().map(Attribute.class::cast).toList());
        boolean byId = false;
        boolean byIndex = false;
        for (int position = 0; position < merge.output().size(); position++) {
            if (groupings.contains(merge.output().get(position)) == false) {
                continue;
            }
            byId |= everyBranchHasMetadata(merge, position, IdFieldMapper.NAME);
            byIndex |= everyBranchHasMetadata(merge, position, MetadataAttribute.INDEX);
        }
        return byId && byIndex;
    }

    private static boolean everyBranchHasMetadata(MergeExec merge, int position, String name) {
        for (PhysicalPlan branch : merge.children()) {
            Attribute column = branch.output().get(position);
            if (column instanceof MetadataAttribute == false || column.name().equals(name) == false) {
                return false;
            }
        }
        return true;
    }

    /**
     * {@code aggregate} with a {@link FirstDocId} that keeps the {@code _doc} of each group, and without the {@code FIRST}s
     * the fetch replaces.
     */
    private static AggregateExec keepTheFirstDocument(AggregateExec aggregate, PhysicalPlan child, Alias firstDoc, Set<NameId> fetched) {
        List<NamedExpression> aggregates = new ArrayList<>(aggregate.aggregates().size() + 1);
        for (NamedExpression expression : aggregate.aggregates()) {
            if (fetched.contains(expression.id()) == false) {
                aggregates.add(expression);
            }
        }
        aggregates.add(firstDoc);
        return new AggregateExec(
            aggregate.source(),
            child,
            aggregate.groupings(),
            aggregates,
            aggregate.getMode(),
            AbstractPhysicalOperationProviders.intermediateAttributes(aggregates, aggregate.groupings()),
            aggregate.estimatedRowSize()
        );
    }

    /**
     * {@code branch} carrying {@code doc} first, from its fragment up to its root, and without the merged columns the
     * fetch loads.
     */
    private static PhysicalPlan narrowBranch(ForkBranch branch, Set<Integer> deferred, Attribute doc) {
        Set<NameId> deferredIds = new HashSet<>();
        for (int position : deferred) {
            deferredIds.add(branch.root().output().get(position).id());
        }
        Branch cutAndExchange = branch.branch();
        List<Attribute> eager = cutAndExchange.exchange().output().stream().filter(a -> deferredIds.contains(a.id()) == false).toList();
        PhysicalPlan cut = cutAndExchange.cut().replaceChild(PlanFetch.narrowExchange(cutAndExchange, doc, eager));
        PhysicalPlan narrowed = branch.root().transformDown(PhysicalPlan.class, node -> node == cutAndExchange.cut() ? cut : node);
        return narrowed.transformUp(ProjectExec.class, project -> {
            List<NamedExpression> projections = new ArrayList<>(project.projections().size() + 1);
            projections.add(doc);
            for (NamedExpression projection : project.projections()) {
                if (deferredIds.contains(projection.id()) == false) {
                    projections.add(projection);
                }
            }
            return new ProjectExec(project.source(), project.child(), projections);
        });
    }

    /**
     * The columns the execs between the final cut and the merge read, and the final cut itself. With an aggregation by
     * document, the reads below it see the merged columns and the reads above it the columns of the aggregation.
     */
    private record Reads(AttributeSet belowTheAggregate, AttributeSet aboveTheAggregate) {
        static Reads of(List<UnaryExec> between, UnaryExec finalCut, @Nullable AggregateExec documentAggregate) {
            AttributeSet.Builder below = AttributeSet.builder();
            AttributeSet.Builder above = AttributeSet.builder().addAll(finalCut.references());
            boolean belowTheAggregate = documentAggregate == null;
            for (UnaryExec exec : between) {
                if (exec == documentAggregate) {
                    // the groupings read the merged columns, the aggregates may only read them through a FIRST the fetch replaces
                    documentAggregate.groupings().forEach(grouping -> below.addAll(grouping.references()));
                    belowTheAggregate = true;
                } else if (belowTheAggregate) {
                    below.addAll(exec.references());
                } else {
                    above.addAll(exec.references());
                }
            }
            if (documentAggregate == null) {
                below.addAll(above.build());
            }
            return new Reads(below.build(), above.build());
        }
    }
}
