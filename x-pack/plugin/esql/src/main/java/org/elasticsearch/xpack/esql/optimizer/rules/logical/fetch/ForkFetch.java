/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical.fetch;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.expression.function.aggregate.First;
import org.elasticsearch.xpack.esql.expression.function.aggregate.FirstDocId;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.fetch.FetchPhasePolicy.Outcome;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.fetch.PlanFetch.Declined;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.fetch.PlanFetch.Demand;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.fetch.PlanFetch.FilterAroundTheFetch;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.DocPreserving;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.ExecutesOn;
import org.elasticsearch.xpack.esql.plan.logical.ExecutesOn.ExecuteLocation;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Fork;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnmappedFieldsAttribute;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.xpack.esql.optimizer.rules.logical.fetch.PlanFetch.ineligible;

/**
 * Plans the fetch phase for the rows of a {@code FORK}, with or without {@code FUSE} after it, when every branch ends in
 * a cut that the data nodes and the coordinator both apply.
 * <p>
 * {@code FROM idx METADATA _id, _index, _score | FORK (WHERE MATCH(a, "x") | SORT _score DESC | LIMIT 50)
 * (WHERE MATCH(b, "x") | SORT _score DESC | LIMIT 50) | FUSE | SORT _score DESC | LIMIT 10 | KEEP a, b, _score} before:
 * <pre>{@code
 * Project[a, b, _score]
 * \_TopN[_score DESC, 10]
 *   \_Aggregate[BY _id, _index: SUM(_score) AS _score, FIRST(a) AS a, FIRST(b) AS b]
 *     \_FuseScoreEval[_score, _fork]
 *       \_Fork[a, b, _id, _index, _score, _fork]
 *         |_Project[a, b, _id, _index, _score, _fork] <- Eval[_fork = "fork1"] <- TopN[_score DESC, 50] <- ...
 *         \_Project[a, b, _id, _index, _score, _fork] <- Eval[_fork = "fork2"] <- TopN[_score DESC, 50] <- ...
 * }</pre>
 * and after:
 * <pre>{@code
 * Project[a, b, _score]
 * \_Fetch[_doc, fetched=[a, b]]
 *   \_TopN[_score DESC, 10]
 *     \_Aggregate[BY _id, _index: SUM(_score) AS _score, FIRST_DOC_ID(_doc) AS _doc]
 *       \_FuseScoreEval[_score, _fork]
 *         \_Fork[_doc, _id, _index, _score, _fork]
 *           |_Project[_doc, _id, _index, _score, _fork] <- Eval[_fork = "fork1"] <- TopN[_score DESC, 50] <- ...
 *           \_Project[_doc, _id, _index, _score, _fork] <- Eval[_fork = "fork2"] <- TopN[_score DESC, 50] <- ...
 * }</pre>
 * Each branch sends the documents of its candidates with the columns its own cut, its commands and {@code FUSE} read.
 * The mapper puts an exchange below the cut of each branch, and {@code ProjectAwayColumns} sends only what the
 * projection at the top of the branch keeps. The coordinator cuts every branch, fuses their rows, cuts again, and
 * fetches the other columns for the winners of the cut closest to the merge only.
 * <p>
 * {@code FUSE} groups the rows by {@code _id} and {@code _index}, so the rows of a group are one document, as the
 * branches that found it saw it. A {@link FirstDocId} keeps the {@code _doc} of the first of them. A {@code FIRST} of a
 * column of the relation picks the value of that document, which is what the fetch loads for it, so the fetch replaces
 * that {@code FIRST}. An aggregation that groups by anything that doesn't name the document keeps the plan eager.
 * <p>
 * A column of the merge is fetched when all of these hold:
 * <ul>
 *     <li>every branch defers it on its own, see {@link PlanFetch#demand}, and reads it only to pass it on after its
 *     cut,</li>
 *     <li>it is the same field, the same metadata, like {@code _source}, or the unmapped fields of the same pattern in
 *     every branch, so the document gives the same value whatever branch found it,</li>
 *     <li>nothing between the merge and the cut reads it, other than a {@code FIRST} of {@code FUSE},</li>
 *     <li>the cut doesn't read it, nor the {@code FIRST} that stands for it.</li>
 * </ul>
 * Every branch must read the same indices, which authorize the fetch.
 */
final class ForkFetch {
    private ForkFetch() {}

    /**
     * A {@code FORK} the rule can rewrite.
     *
     * @param between              the commands between the final cut and the merge, from the cut down
     * @param finalCut             the cut closest to the merge, above which the fetch runs
     * @param documentAggregate    the aggregation in {@code between} that groups the rows by document, like {@code FUSE}
     * @param relation             the relation of the first branch. Every branch reads the same indices.
     * @param deferred             the positions of the merged columns the fetch replaces
     * @param fetched              the columns the fetch appends. Each takes the id of the column it replaces after the final
     *                             cut, the merged column or its {@code FIRST}
     * @param filterAroundTheFetch the filter right after the final cut, when some of its conditions run before the fetch
     */
    record Candidate(
        Fork fork,
        List<LogicalPlan> between,
        UnaryPlan finalCut,
        @Nullable Aggregate documentAggregate,
        List<ForkBranch> branches,
        EsRelation relation,
        Set<Integer> deferred,
        List<Attribute> fetched,
        @Nullable FilterAroundTheFetch filterAroundTheFetch
    ) implements PlanFetch.Shape {}

    /**
     * One branch of the merge.
     *
     * @param root            the projection that ends the branch, whose output lines up with the merge's
     * @param demand          the columns the branch would defer on its own
     * @param readAfterTheCut the columns the branch reads between its cut and its root, other than to pass them on
     */
    record ForkBranch(Project root, EsRelation relation, Demand demand, AttributeSet readAfterTheCut) implements BranchShape {}

    /** What the checks of one branch found: the branch, or why the plan stays eager. */
    sealed interface BranchShape permits Declined, ForkBranch {}

    /**
     * @param path the commands from the root of the plan down to the one right above {@code fork}
     */
    static PlanFetch.Shape shape(List<LogicalPlan> path, Fork fork) {
        // the branches may send any number of rows, so the cut closest to the merge can always drop some
        int cutIndex = path.size() - 1;
        while (cutIndex >= 0 && PlanFetch.isCut(path.get(cutIndex)) == false) {
            cutIndex--;
        }
        if (cutIndex < 0) {
            return ineligible("no cut after the [FORK]");
        }
        UnaryPlan finalCut = (UnaryPlan) path.get(cutIndex);
        for (LogicalPlan node : path.subList(0, cutIndex)) {
            if (PlanFetch.isSupportedAfterCut(node) == false) {
                return ineligible("[" + node.nodeName() + "] runs after the cut");
            }
        }
        List<LogicalPlan> between = List.copyOf(path.subList(cutIndex + 1, path.size()));
        Aggregate documentAggregate = null;
        for (LogicalPlan node : between) {
            if (node instanceof Aggregate aggregate && documentAggregate == null && groupsByDocument(aggregate, fork)) {
                documentAggregate = aggregate;
                continue;
            }
            if (carriesDocOnTheCoordinator(node) == false) {
                return ineligible("[" + node.nodeName() + "] runs between the cut and the [FORK]");
            }
        }

        List<ForkBranch> branches = new ArrayList<>(fork.children().size());
        for (LogicalPlan child : fork.children()) {
            BranchShape found = branch(child, fork.output().size());
            if (found instanceof Declined declined) {
                return declined;
            }
            branches.add((ForkBranch) found);
        }
        EsRelation relation = branches.getFirst().relation();
        for (ForkBranch branch : branches) {
            EsRelation other = branch.relation();
            if (other.indexPattern().equals(relation.indexPattern()) == false
                || PlanFetch.originalIndices(other).equals(PlanFetch.originalIndices(relation)) == false) {
                return ineligible("the branches of the [FORK] read different indices");
            }
        }

        Reads reads = Reads.of(between, finalCut, documentAggregate);
        Set<Integer> deferred = new HashSet<>();
        List<Attribute> fetched = new ArrayList<>();
        Set<NameId> replaced = new HashSet<>();
        for (int position = 0; position < fork.output().size(); position++) {
            Attribute column = sameFetchedColumn(branches, position);
            if (column == null) {
                continue;
            }
            Attribute merged = fork.output().get(position);
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
        FilterAroundTheFetch filterAroundTheFetch = cutIndex > 0 && path.get(cutIndex - 1) instanceof Filter filter
            ? FilterAroundTheFetch.split(filter, crossing.build())
            : null;
        return new Candidate(fork, between, finalCut, documentAggregate, branches, relation, deferred, fetched, filterAroundTheFetch);
    }

    static LogicalPlan rewrite(LogicalPlan plan, Candidate candidate) {
        Attribute docOfTheBranches = PlanFetch.newDocAttribute();
        List<LogicalPlan> children = new ArrayList<>(candidate.branches().size());
        for (ForkBranch branch : candidate.branches()) {
            children.add(narrowBranch(branch, candidate.deferred(), docOfTheBranches));
        }
        Fork fork = candidate.fork();
        Attribute doc = PlanFetch.newDocAttribute();
        List<Attribute> merged = new ArrayList<>();
        merged.add(doc);
        for (int position = 0; position < fork.output().size(); position++) {
            if (candidate.deferred().contains(position) == false) {
                merged.add(fork.output().get(position));
            }
        }
        LogicalPlan below = fork.replaceSubPlansAndOutput(children, merged);
        Set<NameId> fetchedIds = new HashSet<>();
        candidate.fetched().forEach(attribute -> fetchedIds.add(attribute.id()));
        for (LogicalPlan node : candidate.between().reversed()) {
            if (node == candidate.documentAggregate()) {
                Alias firstDoc = new Alias(node.source(), EsQueryExec.DOC_ID_FIELD.getName(), new FirstDocId(node.source(), doc));
                below = keepTheFirstDocument(candidate.documentAggregate(), below, firstDoc, fetchedIds);
                doc = firstDoc.toAttribute();
            } else if (node instanceof Project project) {
                List<NamedExpression> projections = new ArrayList<>(project.projections().size() + 1);
                projections.add(doc);
                projections.addAll(project.projections());
                below = new Project(project.source(), below, projections);
            } else {
                below = ((UnaryPlan) node).replaceChild(below);
            }
        }
        return PlanFetch.fetchAfterTheCut(
            plan,
            candidate.finalCut(),
            candidate.finalCut().replaceChild(below),
            candidate.filterAroundTheFetch(),
            doc,
            candidate.fetched(),
            candidate.relation()
        );
    }

    /**
     * Whether a detached {@code _doc} passes through {@code command} on the coordinator, above the cut of a branch or the
     * merge: a command of one input that keeps every row tied to its document, see {@link DocPreserving}.
     */
    private static boolean carriesDocOnTheCoordinator(LogicalPlan command) {
        return command instanceof UnaryPlan
            && command instanceof DocPreserving
            && (command instanceof ExecutesOn executesOn && executesOn.executesOn() == ExecuteLocation.REMOTE) == false;
    }

    /**
     * Checks one branch of the merge: a projection on top, then commands that a detached {@code _doc} can pass through,
     * then a cut and, below it, the commands and the relation a plan of its own would need, see {@link PlanFetch}.
     *
     * @return the branch, or why the plan stays eager
     */
    private static BranchShape branch(LogicalPlan child, int forkWidth) {
        if (child instanceof Project == false) {
            return ineligible("a [FORK] branch does not end in a projection");
        }
        Project root = (Project) child;
        if (root.output().size() != forkWidth) {
            assert false : "branch output " + root.output() + " doesn't line up with the merge";
            return new Declined(Outcome.INCONSISTENT_PROJECTION, "a [FORK] branch doesn't line up with the merge");
        }
        List<LogicalPlan> chain = new ArrayList<>();
        LogicalPlan leaf = root;
        for (LogicalPlan docInput = PlanFetch.docInput(leaf); docInput != null; docInput = PlanFetch.docInput(leaf)) {
            chain.add(leaf);
            leaf = docInput;
        }
        if (leaf instanceof EsRelation == false) {
            return ineligible("a [FORK] branch reads [" + leaf.nodeName() + "] instead of an index relation");
        }
        EsRelation relation = (EsRelation) leaf;
        if (relation.indexMode() != IndexMode.STANDARD && relation.indexMode() != IndexMode.TIME_SERIES) {
            return ineligible("[" + relation.indexMode() + "] relation");
        }
        int cutIndex = chain.size() - 1;
        while (cutIndex >= 0 && PlanFetch.isCut(chain.get(cutIndex)) == false) {
            cutIndex--;
        }
        if (cutIndex < 0) {
            return ineligible("a [FORK] branch has no cut");
        }
        AttributeSet.Builder readAfterTheCut = AttributeSet.builder();
        for (LogicalPlan node : chain.subList(0, cutIndex)) {
            if (carriesDocOnTheCoordinator(node) == false) {
                return ineligible("[" + node.nodeName() + "] runs after the cut of a [FORK] branch");
            }
            if (node instanceof Project project) {
                for (NamedExpression projection : project.projections()) {
                    if (projection instanceof Attribute == false) {
                        readAfterTheCut.addAll(projection.references());
                    }
                }
            } else {
                readAfterTheCut.addAll(node.references());
            }
        }
        for (LogicalPlan node : chain.subList(cutIndex + 1, chain.size())) {
            if (PlanFetch.isSupportedBeforeCut(node) == false) {
                return ineligible("[" + node.nodeName() + "] runs before the cut of a [FORK] branch");
            }
        }
        UnaryPlan cut = (UnaryPlan) chain.get(cutIndex);
        return new ForkBranch(root, relation, PlanFetch.demand(root, cut, relation), readAfterTheCut.build());
    }

    /**
     * The column of the documents that every branch carries at {@code position} and would defer, as the first branch
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
     * {@code _index} of the rows, as every branch of {@code fork} names them.
     */
    private static boolean groupsByDocument(Aggregate aggregate, Fork fork) {
        if (aggregate.getClass() != Aggregate.class || aggregate.groupings().stream().allMatch(Attribute.class::isInstance) == false) {
            return false;
        }
        AttributeSet groupings = AttributeSet.of(aggregate.groupings().stream().map(Attribute.class::cast).toList());
        boolean byId = false;
        boolean byIndex = false;
        for (int position = 0; position < fork.output().size(); position++) {
            if (groupings.contains(fork.output().get(position)) == false) {
                continue;
            }
            byId |= everyBranchHasMetadata(fork, position, IdFieldMapper.NAME);
            byIndex |= everyBranchHasMetadata(fork, position, MetadataAttribute.INDEX);
        }
        return byId && byIndex;
    }

    private static boolean everyBranchHasMetadata(Fork fork, int position, String name) {
        for (LogicalPlan branch : fork.children()) {
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
    private static Aggregate keepTheFirstDocument(Aggregate aggregate, LogicalPlan child, Alias firstDoc, Set<NameId> fetched) {
        List<NamedExpression> aggregates = new ArrayList<>(aggregate.aggregates().size() + 1);
        for (NamedExpression expression : aggregate.aggregates()) {
            if (fetched.contains(expression.id()) == false) {
                aggregates.add(expression);
            }
        }
        aggregates.add(firstDoc);
        return aggregate.with(child, aggregate.groupings(), aggregates);
    }

    /**
     * {@code branch} carrying {@code doc} from its relation up to its root, and without the merged columns the fetch loads.
     * The commands of the branch keep the deferred columns, and {@code ProjectAwayColumns} leaves them on the data nodes,
     * since the root no longer passes them on.
     */
    private static LogicalPlan narrowBranch(ForkBranch branch, Set<Integer> deferred, Attribute doc) {
        Project root = branch.root();
        Project carried = (Project) PlanFetch.carryDoc(root, doc);
        List<NamedExpression> projections = new ArrayList<>(root.projections().size() + 1);
        projections.add(doc);
        for (int position = 0; position < root.projections().size(); position++) {
            if (deferred.contains(position) == false) {
                projections.add(root.projections().get(position));
            }
        }
        return new Project(root.source(), carried.child(), projections);
    }

    /**
     * The columns the commands between the final cut and the merge read, and the final cut itself. With an aggregation by
     * document, the reads below it see the merged columns and the reads above it the columns of the aggregation.
     */
    private record Reads(AttributeSet belowTheAggregate, AttributeSet aboveTheAggregate) {
        static Reads of(List<LogicalPlan> between, UnaryPlan finalCut, @Nullable Aggregate documentAggregate) {
            AttributeSet.Builder below = AttributeSet.builder();
            AttributeSet.Builder above = AttributeSet.builder().addAll(finalCut.references());
            boolean belowTheAggregate = documentAggregate == null;
            for (LogicalPlan node : between) {
                if (node == documentAggregate) {
                    // the groupings read the merged columns, the aggregates may only read them through a FIRST the fetch replaces
                    for (Expression grouping : documentAggregate.groupings()) {
                        below.addAll(grouping.references());
                    }
                    belowTheAggregate = true;
                } else if (belowTheAggregate) {
                    below.addAll(node.references());
                } else {
                    above.addAll(node.references());
                }
            }
            if (documentAggregate == null) {
                below.addAll(above.build());
            }
            return new Reads(below.build(), above.build());
        }
    }
}
