/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.fetch;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.util.CollectionUtils;
import org.elasticsearch.xpack.esql.expression.function.blockloader.BlockLoaderExpression;
import org.elasticsearch.xpack.esql.expression.predicate.Predicates;
import org.elasticsearch.xpack.esql.optimizer.PhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.fetch.FetchPhasePolicy.Outcome;
import org.elasticsearch.xpack.esql.plan.logical.CarriesDocOnDataNodes;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LimitBy;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.TopN;
import org.elasticsearch.xpack.esql.plan.logical.TopNBy;
import org.elasticsearch.xpack.esql.plan.logical.join.Join;
import org.elasticsearch.xpack.esql.plan.physical.CarriesDetachedDoc;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeExec;
import org.elasticsearch.xpack.esql.plan.physical.FetchExec;
import org.elasticsearch.xpack.esql.plan.physical.FilterExec;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.plan.physical.LimitByExec;
import org.elasticsearch.xpack.esql.plan.physical.LimitExec;
import org.elasticsearch.xpack.esql.plan.physical.MergeExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.ProjectExec;
import org.elasticsearch.xpack.esql.plan.physical.TopNByExec;
import org.elasticsearch.xpack.esql.plan.physical.TopNExec;
import org.elasticsearch.xpack.esql.plan.physical.UnaryExec;
import org.elasticsearch.xpack.esql.rule.ParameterizedRule;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.transport.RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY;
import static org.elasticsearch.transport.RemoteClusterAware.isRemoteIndexName;

/**
 * Plans the fetch phase for a {@code LIMIT} or {@code TopN} that the data nodes and the coordinator both apply.
 * <p>
 * A <em>cut</em> is a {@code LIMIT} or a {@code TopN}: a pipeline breaker that only decides which rows survive. The
 * global top N is always among the top N rows of every part, so the data nodes cut their own rows and the coordinator
 * cuts what they send. Only the coordinator's cut knows the global winners.
 * <p>
 * Without the fetch phase every data node loads each column the query returns for every row that survives its own
 * cut, and the coordinator keeps only the global winners. With it the data nodes send the {@code _doc} of each row
 * plus the columns the cut needs, the coordinator cuts, and then loads the other columns for the winners only, from the
 * nodes that own them.
 * <p>
 * {@code FROM idx | WHERE x > 1 | SORT ts DESC | LIMIT 10 | KEEP a, b, ts} before:
 * <pre>{@code
 * ProjectExec[a, b, ts]
 * \_TopNExec[ts DESC, 10]
 *   \_ExchangeExec[a, b, ts]
 *     \_FragmentExec[Project[a, b, ts] <- TopN[ts DESC, 10] <- Filter[x > 1] <- EsRelation[idx]]
 * }</pre>
 * and after:
 * <pre>{@code
 * ProjectExec[a, b, ts]
 * \_FetchExec[_doc, fetched=[a, b]]
 *   \_TopNExec[ts DESC, 10]
 *     \_ExchangeExec[_doc, ts]
 *       \_FragmentExec[Project[_doc, ts] <- TopN[ts DESC, 10] <- Filter[x > 1] <- EsRelation[idx]]
 * }</pre>
 * The data nodes plan the fragment as they plan any other. Node-level late materialization cuts the rows of a node's
 * shards once more, and loads the eager columns the cut doesn't read for the survivors only. The rows leave the node
 * with their {@code _doc}, which keeps naming the documents after it left.
 * <p>
 * A filter right after the cut runs before the fetch when its conditions read only columns that cross the exchange,
 * like {@code WHERE ts > "2026-01-01"} right after the {@code LIMIT} above. The fetch then loads {@code a} and {@code b}
 * only for the rows that pass.
 * <p>
 * {@link #demand} decides which columns cross the exchange as values. {@link #shape} lists the plans the rule accepts.
 * Every other plan keeps loading its columns eagerly, unchanged, and the reason is recorded in
 * {@link FetchPhaseOutcomes}.
 */
public final class PlanFetch extends ParameterizedRule<PhysicalPlan, PhysicalPlan, PhysicalOptimizerContext> {
    private static final Logger logger = LogManager.getLogger(PlanFetch.class);

    private final FetchPhaseOutcomes outcomes;

    public PlanFetch(FetchPhaseOutcomes outcomes) {
        this.outcomes = outcomes;
    }

    @Override
    public PhysicalPlan apply(PhysicalPlan plan, PhysicalOptimizerContext context) {
        Outcome gate = FetchPhasePolicy.from(context).decide();
        if (gate != Outcome.ENABLED) {
            outcomes.record(gate, null);
            return plan;
        }
        return switch (shape(plan)) {
            case Declined declined -> {
                outcomes.record(declined.outcome(), declined.reason());
                logger.debug("fetch phase not planned: [{}] {}", declined.outcome(), declined.reason());
                yield plan;
            }
            case Candidate candidate -> {
                PhysicalPlan rewritten = rewrite(plan, candidate);
                outcomes.record(Outcome.APPLIED, null);
                logger.debug("fetch phase planned: eager {}, deferred {}", candidate.demand().eager(), candidate.demand().deferred());
                yield rewritten;
            }
            case ForkFetch.Candidate fork -> {
                PhysicalPlan rewritten = ForkFetch.rewrite(plan, fork);
                outcomes.record(Outcome.APPLIED, null);
                logger.debug("fetch phase planned through [FORK]: fetched {}", fork.fetched());
                yield rewritten;
            }
        };
    }

    /**
     * The split of the columns that cross the exchange.
     *
     * @param eager    columns that keep crossing the exchange as values, in exchange order
     * @param deferred columns the fetch loads after the cut, in exchange order
     */
    record Demand(List<Attribute> eager, List<Attribute> deferred) {}

    sealed interface Shape permits Declined, Candidate, ForkFetch.Candidate {}

    /**
     * What the checks of one exchange found: the exchange with its cut and fragment, or why the plan stays eager.
     */
    sealed interface BranchShape permits Declined, Branch, ForkFetch.ForkBranch {}

    record Declined(Outcome outcome, String reason) implements Shape, BranchShape {}

    /**
     * An exchange over a fragment, with the coordinator half of the cut that ends the fragment right above it.
     *
     * @param cut          the coordinator half of the cut, the parent of the exchange
     * @param fragmentRoot the projection {@code ProjectAwayColumns} put on top of the fragment
     */
    record Branch(UnaryExec cut, ExchangeExec exchange, FragmentExec fragment, Project fragmentRoot, EsRelation relation)
        implements
            BranchShape {}

    /**
     * A plan with one exchange the rule can rewrite.
     *
     * @param chain                the execs between the coordinator cut and the fetch, from the bottom up, the final cut last.
     *                             Empty when the coordinator cut is the final cut
     * @param filterAroundTheFetch the filter right after the final cut, when some of its conditions run before the fetch
     */
    private record Candidate(Branch branch, List<UnaryExec> chain, @Nullable FilterAroundTheFetch filterAroundTheFetch, Demand demand)
        implements
            Shape {}

    /**
     * A filter right after the coordinator cut, split around the fetch. The conditions that read only columns crossing the
     * exchange run before the fetch, so it loads the deferred columns only for the rows that pass. The others read a
     * fetched column and run after it.
     *
     * @param conditionsBeforeTheFetch never empty
     * @param conditionsAfterTheFetch  empty when every condition runs before the fetch
     */
    record FilterAroundTheFetch(FilterExec filter, List<Expression> conditionsBeforeTheFetch, List<Expression> conditionsAfterTheFetch) {
        /**
         * The split of {@code filter}, or {@code null} when every condition reads a fetched column, and the whole filter
         * stays after the fetch.
         *
         * @param crossing the columns that cross the exchange as values
         */
        @Nullable
        static FilterAroundTheFetch split(FilterExec filter, AttributeSet crossing) {
            List<Expression> beforeTheFetch = new ArrayList<>();
            List<Expression> afterTheFetch = new ArrayList<>();
            for (Expression condition : Predicates.splitAnd(filter.condition())) {
                (crossing.containsAll(condition.references()) ? beforeTheFetch : afterTheFetch).add(condition);
            }
            return beforeTheFetch.isEmpty() ? null : new FilterAroundTheFetch(filter, beforeTheFetch, afterTheFetch);
        }

        /** The rows the fetch reads: the cut, without the rows the conditions before the fetch drop. */
        PhysicalPlan beforeTheFetch(PhysicalPlan cut) {
            if (conditionsAfterTheFetch.isEmpty()) {
                return filter.replaceChild(cut);
            }
            return new FilterExec(filter.source(), cut, Predicates.combineAnd(conditionsBeforeTheFetch));
        }

        /** What takes the place of the filter in the plan: the fetch, and the conditions that read a fetched column. */
        PhysicalPlan afterTheFetch(FetchExec fetch) {
            if (conditionsAfterTheFetch.isEmpty()) {
                return fetch;
            }
            return new FilterExec(filter.source(), fetch, Predicates.combineAnd(conditionsAfterTheFetch));
        }
    }

    /**
     * The plans the fetch phase supports, checked in this order:
     * <ol>
     *     <li>one cluster exchange, over a fragment, without aggregation and without {@code FORK} or {@code UNION},</li>
     *     <li>between the coordinator cut and the root, only execs that a detached {@code _doc} can pass through, the
     *     {@link CarriesDetachedDoc} ones, other cuts included,</li>
     *     <li>the coordinator cut reads the exchange and mirrors the cut that ends the fragment, a {@code TopN} or a
     *     {@code LIMIT}, of the whole input or {@code BY} groups,</li>
     *     <li>between the fragment cut and the relation, only commands that {@code _doc} can pass through on the data
     *     nodes, the {@link CarriesDocOnDataNodes} ones, down the input each of them names,</li>
     *     <li>one relation on that path, on the local cluster,</li>
     *     <li>at least one column the query only needs after the cut, and that can be fetched.</li>
     * </ol>
     * Each command answers for itself, so the rule keeps no list of commands. A command that doesn't opt in keeps the
     * plan eager, which is always correct, so a new command needs no change here. Opting in needs a reason why
     * {@code _doc} still names the document of every row after the command. {@code MV_EXPAND} and {@code LOOKUP JOIN}
     * repeat rows, so the cut can hold several rows of one document, which the fetch loads once. The columns a
     * {@code LOOKUP JOIN} adds come from another index and keep crossing as values.
     * <p>
     * The fetch runs right above the final cut, the last cut from the coordinator cut up that can still drop rows, see
     * {@link #finalCut}, so it loads the columns for the rows the query returns only. In
     * {@code SORT a | LIMIT 100 | SORT b | LIMIT 10} it loads them for 10 rows, and {@code b} crosses the exchange
     * because the second cut reads it. A {@code LIMIT BY} or a {@code SORT} with {@code LIMIT BY} keeps the implicit
     * {@code LIMIT} of the query above it, which is then the final cut. The commands after the final cut see the fetched
     * columns. The exception is a filter right after it: its
     * conditions that read no fetched column run between the cut and the fetch, see {@link FilterAroundTheFetch}. The
     * optimizer pushes filters down as far as they go and combines the filters that follow each other, so the conditions
     * that can move end up in that filter.
     * <p>
     * A {@code LIMIT} after the cut only drops more rows. It shows up above a {@code WHERE} that follows the cut, because
     * the implicit limit of the query cannot move below a {@code WHERE}. {@code HIGHLIGHT} computes its columns from the
     * values of the row alone, so it highlights the fetched text of the winners. The optimizer moves a sort and a limit
     * below it unless the sort reads a generated highlight column, so it usually follows the cut.
     */
    private static Shape shape(PhysicalPlan plan) {
        List<MergeExec> merges = plan.collect(MergeExec.class);
        if (merges.isEmpty() == false) {
            return merges.size() == 1 ? ForkFetch.shape(plan, merges.getFirst()) : ineligible("more than one [MergeExec]");
        }
        List<ExchangeExec> exchanges = plan.collect(ExchangeExec.class);
        if (exchanges.size() != 1) {
            return ineligible(exchanges.isEmpty() ? "no exchange" : "more than one exchange");
        }
        ExchangeExec exchange = exchanges.getFirst();
        Declined unsupportedExchange = checkExchange(exchange);
        if (unsupportedExchange != null) {
            return unsupportedExchange;
        }

        AttributeSet.Builder readAfterExchange = AttributeSet.builder().addAll(plan.outputSet());
        // from the root down to the coordinator cut, which reads the exchange
        List<UnaryExec> afterTheCut = new ArrayList<>();
        PhysicalPlan node = plan;
        while (node.children().size() != 1 || node.children().getFirst() != exchange) {
            if (node instanceof UnaryExec unary && unary instanceof CarriesDetachedDoc) {
                readAfterExchange.addAll(unary.references());
                afterTheCut.add(unary);
                node = unary.child();
            } else {
                return ineligible("[" + node.nodeName() + "] runs after the cut");
            }
        }
        if (isCut(node) == false) {
            return ineligible("[" + node.nodeName() + "] reads the exchange instead of a cut");
        }
        UnaryExec coordinatorCut = (UnaryExec) node;
        readAfterExchange.addAll(coordinatorCut.references());

        // without a later cut that can drop rows, the coordinator cut is the final cut
        int foundFinalCut = finalCut(afterTheCut, rowsAfter(coordinatorCut));
        int finalCutIndex = foundFinalCut < 0 ? afterTheCut.size() : foundFinalCut;
        List<UnaryExec> chain = List.copyOf(afterTheCut.subList(finalCutIndex, afterTheCut.size()).reversed());
        UnaryExec afterTheFinalCut = finalCutIndex > 0 ? afterTheCut.get(finalCutIndex - 1) : null;
        AttributeSet.Builder readBeforeTheFetch = AttributeSet.builder().addAll(coordinatorCut.references());
        for (UnaryExec exec : chain) {
            readBeforeTheFetch.addAll(readsOf(exec));
        }

        BranchShape found = branch(coordinatorCut, exchange);
        if (found instanceof Declined declined) {
            return declined;
        }
        Branch branch = (Branch) found;
        Declined inconsistent = checkEveryColumnIsRead(branch, readAfterExchange.build());
        if (inconsistent != null) {
            return inconsistent;
        }

        Demand demand = demand(branch.fragmentRoot(), branch.relation(), readBeforeTheFetch.build());
        if (demand.deferred().isEmpty()) {
            return new Declined(Outcome.INELIGIBLE_NO_DEFERRABLE_FIELDS, "the cut reads every column, or the others cannot be fetched");
        }
        FilterAroundTheFetch filterAroundTheFetch = afterTheFinalCut instanceof FilterExec filter
            ? FilterAroundTheFetch.split(filter, AttributeSet.of(demand.eager()))
            : null;
        return new Candidate(branch, chain, filterAroundTheFetch, demand);
    }

    /**
     * Whether {@code exec} keeps at most a number of rows, of its whole input or of each group. A cut over the exchange
     * repeats the cut that ends the fragment. Another one after it cuts the rows further.
     */
    static boolean isCut(PhysicalPlan exec) {
        return exec instanceof TopNExec || exec instanceof LimitExec || exec instanceof TopNByExec || exec instanceof LimitByExec;
    }

    /**
     * The final cut among {@code rootFirst}: the last one, from the bottom up, that can still drop rows. A cut can when its
     * limit, of the whole input or of each group, is below the number of rows the cuts under it let through. The implicit
     * {@code LIMIT} of the query above a {@code WHERE} that follows a cut of 10 rows keeps every row, so it is not the
     * final cut, and the {@code WHERE} can read fetched columns.
     *
     * @param rootFirst  the execs above the rows, from the root down
     * @param rowsBelow  how many rows reach the lowest of them, {@link Long#MAX_VALUE} when no cut bounds them
     * @return the index of the final cut in {@code rootFirst}, or {@code -1} when none of them can drop rows
     */
    static int finalCut(List<UnaryExec> rootFirst, long rowsBelow) {
        int finalCut = -1;
        long rows = rowsBelow;
        for (int i = rootFirst.size() - 1; i >= 0; i--) {
            UnaryExec exec = rootFirst.get(i);
            if (isCut(exec) == false) {
                continue;
            }
            Long limit = limitOf(exec);
            if (limit == null || limit < rows) {
                finalCut = i;
                if (limit != null && (exec instanceof TopNExec || exec instanceof LimitExec)) {
                    rows = limit;
                }
            }
        }
        return finalCut;
    }

    /**
     * How many rows {@code cut} lets through, a cut of the whole input in all and a cut by groups in each group, or
     * {@code null} when the plan doesn't say.
     */
    @Nullable
    private static Long limitOf(PhysicalPlan cut) {
        Expression limit = switch (cut) {
            case TopNExec topN -> topN.limit();
            case LimitExec limitExec -> limitExec.limit();
            case TopNByExec topNBy -> topNBy.limitPerGroup();
            case LimitByExec limitBy -> limitBy.limitPerGroup();
            default -> throw new IllegalArgumentException("[" + cut.nodeName() + "] is not a cut");
        };
        return limit instanceof Literal literal && literal.value() instanceof Number number ? number.longValue() : null;
    }

    /**
     * How many rows the coordinator cut lets through in all, {@link Long#MAX_VALUE} for a cut by groups.
     */
    private static long rowsAfter(UnaryExec coordinatorCut) {
        Long limit = coordinatorCut instanceof TopNExec || coordinatorCut instanceof LimitExec ? limitOf(coordinatorCut) : null;
        return limit == null ? Long.MAX_VALUE : limit;
    }

    /**
     * The columns whose values {@code exec} reads. A projection only reads the inputs of the columns it computes or
     * renames: the ones it passes on as they are stay deferrable, and the fetch adds them back above it.
     */
    private static AttributeSet readsOf(UnaryExec exec) {
        if (exec instanceof ProjectExec project) {
            AttributeSet.Builder reads = AttributeSet.builder();
            for (NamedExpression projection : project.projections()) {
                if (projection instanceof Attribute == false) {
                    reads.addAll(projection.references());
                }
            }
            return reads.build();
        }
        return exec.references();
    }

    static Declined ineligible(String reason) {
        return new Declined(Outcome.INELIGIBLE_SHAPE, reason);
    }

    /**
     * Why the rule can't plan the fetch for the rows of {@code exchange}, or {@code null} when it can.
     */
    @Nullable
    private static Declined checkExchange(ExchangeExec exchange) {
        if (exchange.inBetweenAggs()) {
            return ineligible("the exchange carries an aggregation");
        }
        if (exchange.child() instanceof FragmentExec == false) {
            return ineligible("the exchange does not read a fragment");
        }
        return null;
    }

    /**
     * Checks the exchange below {@code cut}, the fragment it reads and the commands of that fragment: the fragment must
     * end in the data node half of {@code cut}, carry {@code _doc} down to its relation, and read local indices only.
     */
    static BranchShape branch(UnaryExec cut, ExchangeExec exchange) {
        Declined unsupportedExchange = checkExchange(exchange);
        if (unsupportedExchange != null) {
            return unsupportedExchange;
        }
        FragmentExec fragment = (FragmentExec) exchange.child();
        if (fragment.fragment() instanceof Project == false) {
            return ineligible("the fragment does not end in a projection");
        }
        Project fragmentRoot = (Project) fragment.fragment();
        LogicalPlan fragmentCut = fragmentRoot.child();
        if (mirrors(cut, fragmentCut) == false) {
            return ineligible("[" + cut.nodeName() + "] does not repeat the cut that ends the fragment");
        }

        // from the cut itself, since the data nodes carry _doc through it too
        LogicalPlan step = fragmentCut;
        while (step instanceof EsRelation == false) {
            LogicalPlan docInput = step instanceof CarriesDocOnDataNodes carrier ? carrier.docInput() : null;
            if (docInput == null) {
                return ineligible("[" + step.nodeName() + "] runs before the cut");
            }
            step = docInput;
        }
        EsRelation relation = (EsRelation) step;
        if (relation.indexMode() != IndexMode.STANDARD && relation.indexMode() != IndexMode.TIME_SERIES) {
            return ineligible("[" + relation.indexMode() + "] relation");
        }
        if (isLocal(relation) == false) {
            return new Declined(Outcome.INELIGIBLE_REMOTE_CLUSTER, "the relation reads a remote cluster");
        }
        return new Branch(cut, exchange, fragment, fragmentRoot, relation);
    }

    /**
     * Every column that crosses the exchange must be read above it, otherwise the analysis is wrong about the plan.
     *
     * @param readAfterExchange the columns the plan reads above the exchange, and the ones it returns
     */
    @Nullable
    static Declined checkEveryColumnIsRead(Branch branch, AttributeSet readAfterExchange) {
        List<Attribute> exchanged = branch.exchange().output();
        List<Attribute> unread = exchanged.stream().filter(a -> readAfterExchange.contains(a) == false).toList();
        if (unread.isEmpty() == false || exchanged.equals(branch.fragmentRoot().output()) == false) {
            assert false : "exchange output " + exchanged + " differs from what the fragment produces and the coordinator reads";
            return new Declined(Outcome.INCONSISTENT_PROJECTION, "the exchange carries columns nothing reads " + unread);
        }
        return null;
    }

    /**
     * Splits the columns that cross the exchange into the ones the coordinator needs as values to cut and the ones it
     * only needs for the rows that survive. A column is deferred when all of these hold:
     * <ul>
     *     <li>the relation produces it, so the document is enough to load it,</li>
     *     <li>{@link Fetchability#isFetchable} accepts it,</li>
     *     <li>nothing between the exchange and the fetch reads it: the cuts and what runs between them. A sort key crosses
     *     the exchange even when the query does not return it.</li>
     *     <li>no {@link Eval} of the fragment reads its values, and no {@link Join} looks it up. The data node loads those
     *     for every row the command sees, so the fetch would read them a second time. In
     *     {@code SORT a + 1 | LIMIT 10 | KEEP a}, the sort key is an {@code Eval} of {@code a}, so {@code a} is eager.</li>
     * </ul>
     * Everything else stays eager, which keeps the fetch partial rather than all or nothing: {@code _score} and values
     * computed inside the fragment keep crossing as values. Renames are not followed: a rename after the cut reads its
     * source column after the cut, so the source column is deferred.
     *
     * @param fragmentRoot       the {@link Project} at the root of the fragment. Its output is what crosses the exchange.
     * @param readBeforeTheFetch the columns the coordinator reads between the exchange and the fetch
     */
    static Demand demand(Project fragmentRoot, EsRelation relation, AttributeSet readBeforeTheFetch) {
        AttributeSet relationOutput = relation.outputSet();
        AttributeSet loadedBeforeTheCut = loadedBeforeTheCut(fragmentRoot.child());
        List<Attribute> eager = new ArrayList<>();
        List<Attribute> deferred = new ArrayList<>();
        for (Attribute attribute : fragmentRoot.output()) {
            boolean defer = relationOutput.contains(attribute)
                && Fetchability.isFetchable(attribute)
                && readBeforeTheFetch.contains(attribute) == false
                && loadedBeforeTheCut.contains(attribute) == false;
            (defer ? deferred : eager).add(attribute);
        }
        return new Demand(List.copyOf(eager), List.copyOf(deferred));
    }

    /**
     * The attributes whose values the data node loads before the cut: the ones an {@link Eval} of the fragment reads, and
     * the keys a {@link Join} looks up.
     * <p>
     * A field passed straight to a function the loader may compute itself, like {@code LENGTH(message)}, doesn't count.
     * The data node may load the length and never the message, and only the data node can tell. Filters don't count
     * either: Lucene usually answers them without loading values. Where these guesses are wrong, the column is read
     * twice, once before the cut and once by the fetch.
     */
    private static AttributeSet loadedBeforeTheCut(LogicalPlan fragmentBody) {
        AttributeSet.Builder loaded = AttributeSet.builder();
        fragmentBody.forEachDown(Eval.class, eval -> {
            for (Alias field : eval.fields()) {
                collectValueReads(field.child(), loaded);
            }
        });
        fragmentBody.forEachDown(Join.class, join -> {
            loaded.addAll(join.leftReferences());
            if (join.config().joinOnConditions() != null) {
                collectValueReads(join.config().joinOnConditions(), loaded);
            }
        });
        return loaded.build();
    }

    private static void collectValueReads(Expression expression, AttributeSet.Builder reads) {
        if (expression instanceof Attribute attribute) {
            reads.add(attribute);
            return;
        }
        boolean mayFuseIntoTheLoad = expression instanceof BlockLoaderExpression;
        for (Expression child : expression.children()) {
            if (mayFuseIntoTheLoad && child instanceof FieldAttribute) {
                continue;
            }
            collectValueReads(child, reads);
        }
    }

    /**
     * The fragment must end in the data node half of the coordinator cut. A local {@code LIMIT} or {@code TopN} only
     * cuts the rows of one node, so it does not count. The top rows of a group are always among the top rows of that
     * group on every node, so a cut by groups repeats on the coordinator like one without.
     */
    private static boolean mirrors(UnaryExec coordinatorCut, LogicalPlan fragmentCut) {
        if (coordinatorCut instanceof TopNExec topN && fragmentCut instanceof TopN fragmentTopN) {
            return fragmentTopN.local() == false && topN.order().equals(fragmentTopN.order()) && topN.limit().equals(fragmentTopN.limit());
        }
        if (coordinatorCut instanceof LimitExec limit && fragmentCut instanceof Limit fragmentLimit) {
            return fragmentLimit.local() == false && limit.limit().equals(fragmentLimit.limit());
        }
        if (coordinatorCut instanceof TopNByExec topNBy && fragmentCut instanceof TopNBy fragmentTopNBy) {
            return topNBy.order().equals(fragmentTopNBy.order())
                && topNBy.limitPerGroup().equals(fragmentTopNBy.limitPerGroup())
                && topNBy.groupings().equals(fragmentTopNBy.groupings());
        }
        if (coordinatorCut instanceof LimitByExec limitBy && fragmentCut instanceof LimitBy fragmentLimitBy) {
            return limitBy.limitPerGroup().equals(fragmentLimitBy.limitPerGroup())
                && limitBy.groupings().equals(fragmentLimitBy.groupings());
        }
        return false;
    }

    private static boolean isLocal(EsRelation relation) {
        if (relation.concreteIndices().isEmpty()) {
            return Arrays.stream(relation.indexPattern().split(",")).map(String::trim).noneMatch(PlanFetch::isRemoteIndexExpression);
        }
        return relation.concreteIndices().size() == 1
            && relation.concreteIndices().containsKey(LOCAL_CLUSTER_GROUP_KEY)
            && relation.concreteIndices().get(LOCAL_CLUSTER_GROUP_KEY).isEmpty() == false;
    }

    private static boolean isRemoteIndexExpression(String indexExpression) {
        return indexExpression.startsWith("-") ? isRemoteIndexName(indexExpression.substring(1)) : isRemoteIndexName(indexExpression);
    }

    private static PhysicalPlan rewrite(PhysicalPlan plan, Candidate candidate) {
        Attribute doc = newDocAttribute();
        Branch branch = candidate.branch();
        PhysicalPlan cut = branch.cut().replaceChild(narrowExchange(branch, doc, candidate.demand().eager()));
        AttributeSet deferred = AttributeSet.of(candidate.demand().deferred());
        for (UnaryExec exec : candidate.chain()) {
            cut = exec instanceof ProjectExec project ? keepingDoc(project, cut, doc, deferred) : exec.replaceChild(cut);
        }
        UnaryExec finalCut = candidate.chain().isEmpty() ? branch.cut() : candidate.chain().getLast();
        return fetchAfterTheCut(
            plan,
            finalCut,
            cut,
            candidate.filterAroundTheFetch(),
            doc,
            candidate.demand().deferred(),
            branch.relation()
        );
    }

    /**
     * {@code project} over {@code child}, passing {@code doc} on instead of the {@code deferred} columns, which the fetch
     * adds back above it.
     */
    private static ProjectExec keepingDoc(ProjectExec project, PhysicalPlan child, Attribute doc, AttributeSet deferred) {
        List<NamedExpression> projections = new ArrayList<>(project.projections().size() + 1);
        projections.add(doc);
        for (NamedExpression projection : project.projections()) {
            if (deferred.contains(projection.toAttribute()) == false) {
                projections.add(projection);
            }
        }
        return new ProjectExec(project.source(), child, projections);
    }

    /**
     * A {@code _doc} column of its own.
     */
    static Attribute newDocAttribute() {
        return new FieldAttribute(Source.EMPTY, null, null, EsQueryExec.DOC_ID_FIELD.getName(), EsQueryExec.DOC_ID_FIELD);
    }

    /**
     * The exchange of {@code branch} that carries {@code doc} and the {@code eager} columns only. The data drivers produce
     * {@code doc} next to the eager columns, and every projection inside the fragment keeps it.
     */
    static ExchangeExec narrowExchange(Branch branch, Attribute doc, List<Attribute> eager) {
        LogicalPlan body = carryDoc(branch.fragmentRoot().child(), doc);
        List<Attribute> exchanged = CollectionUtils.prependToCopy(doc, eager);
        FragmentExec fragment = branch.fragment().withFragment(new Project(branch.fragmentRoot().source(), body, exchanged));
        return new ExchangeExec(branch.exchange().source(), exchanged, false, fragment);
    }

    /**
     * {@code plan} with {@code doc} from the relation up to {@code plan}, through the inputs that
     * {@link CarriesDocOnDataNodes#docInput} names: the relation produces it, and every projection on the way keeps it.
     * The other inputs, like the lookup index of a join, stay as they are.
     */
    private static LogicalPlan carryDoc(LogicalPlan plan, Attribute doc) {
        if (plan instanceof EsRelation relation) {
            return relation.withAttributes(CollectionUtils.prependToCopy(doc, relation.output()));
        }
        LogicalPlan docInput = ((CarriesDocOnDataNodes) plan).docInput();
        LogicalPlan carried = carryDoc(docInput, doc);
        LogicalPlan rebuilt = plan.replaceChildren(plan.children().stream().map(child -> child == docInput ? carried : child).toList());
        if (rebuilt instanceof Project project) {
            List<NamedExpression> projections = new ArrayList<>(project.projections().size() + 1);
            projections.add(doc);
            projections.addAll(project.projections());
            return project.withProjections(projections);
        }
        return rebuilt;
    }

    /**
     * Puts the fetch of {@code fetched} right above {@code cut}, which takes the place of {@code originalCut} in
     * {@code plan}. The conditions of {@code filter} that read no fetched column run between them.
     *
     * @param doc      the {@code _doc} column of the rows of {@code cut}
     * @param relation the relation the documents come from, which authorizes the fetch
     */
    static PhysicalPlan fetchAfterTheCut(
        PhysicalPlan plan,
        UnaryExec originalCut,
        PhysicalPlan cut,
        @Nullable FilterAroundTheFetch filter,
        Attribute doc,
        List<Attribute> fetched,
        EsRelation relation
    ) {
        FetchExec fetch = new FetchExec(
            originalCut.source(),
            filter == null ? cut : filter.beforeTheFetch(cut),
            doc,
            fetched,
            relation.indexPattern(),
            localIndices(relation),
            null
        );
        PhysicalPlan replaced = filter == null ? originalCut : filter.filter();
        PhysicalPlan replacement = filter == null ? fetch : filter.afterTheFetch(fetch);

        // the coordinator returns what it returned before, never _doc
        PhysicalPlan rewritten = plan.transformDown(PhysicalPlan.class, node -> node == replaced ? replacement : node);
        if (sameIds(rewritten.output(), plan.output()) == false) {
            rewritten = new ProjectExec(plan.source(), rewritten, plan.output());
        }
        return rewritten;
    }

    static List<String> localIndices(EsRelation relation) {
        List<String> local = relation.originalIndices().get(LOCAL_CLUSTER_GROUP_KEY);
        return local == null ? List.of() : List.copyOf(local);
    }

    private static boolean sameIds(List<Attribute> left, List<Attribute> right) {
        if (left.size() != right.size()) {
            return false;
        }
        for (int i = 0; i < left.size(); i++) {
            if (left.get(i).id().equals(right.get(i).id()) == false) {
                return false;
            }
        }
        return true;
    }
}
