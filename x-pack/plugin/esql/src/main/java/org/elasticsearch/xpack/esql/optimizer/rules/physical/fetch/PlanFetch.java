/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.fetch;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.util.CollectionUtils;
import org.elasticsearch.xpack.esql.expression.function.blockloader.BlockLoaderExpression;
import org.elasticsearch.xpack.esql.optimizer.PhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.TopN;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.EvalExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeExec;
import org.elasticsearch.xpack.esql.plan.physical.FetchExec;
import org.elasticsearch.xpack.esql.plan.physical.FilterExec;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.plan.physical.HighlightExec;
import org.elasticsearch.xpack.esql.plan.physical.LimitExec;
import org.elasticsearch.xpack.esql.plan.physical.MergeExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.ProjectExec;
import org.elasticsearch.xpack.esql.plan.physical.TopNExec;
import org.elasticsearch.xpack.esql.plan.physical.UnaryExec;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags;
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
 * {@link #demand} decides which columns cross the exchange as values. {@link #shape} lists the plans the rule accepts.
 * Every other plan keeps loading its columns eagerly, unchanged, and the rule logs why at debug level.
 * <p>
 * The rule runs only when {@link EsqlFlags#fetchPhaseMode()} enables it.
 */
public final class PlanFetch extends ParameterizedRule<PhysicalPlan, PhysicalPlan, PhysicalOptimizerContext> {
    private static final Logger logger = LogManager.getLogger(PlanFetch.class);

    @Override
    public PhysicalPlan apply(PhysicalPlan plan, PhysicalOptimizerContext context) {
        if (context.flags().fetchPhaseMode() != EsqlFlags.FetchPhaseMode.ENABLED) {
            return plan;
        }
        return switch (shape(plan)) {
            case Declined declined -> {
                logger.debug("fetch phase not planned: {}", declined.reason());
                yield plan;
            }
            case Candidate candidate -> {
                PhysicalPlan rewritten = rewrite(plan, candidate);
                logger.debug("fetch phase planned: eager {}, deferred {}", candidate.demand().eager(), candidate.demand().deferred());
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

    private sealed interface Shape permits Declined, Candidate {}

    private record Declined(String reason) implements Shape {}

    /**
     * A plan the rule can rewrite.
     *
     * @param coordinatorCut the coordinator half of the cut, the parent of the exchange
     * @param fragmentRoot   the projection {@code ProjectAwayColumns} put on top of the fragment
     */
    private record Candidate(
        ExchangeExec exchange,
        FragmentExec fragment,
        Project fragmentRoot,
        UnaryExec coordinatorCut,
        EsRelation relation,
        Demand demand
    ) implements Shape {}

    /**
     * The plans the fetch phase supports, checked in this order:
     * <ol>
     *     <li>one cluster exchange, over a fragment, without aggregation and without {@code FORK} or {@code UNION},</li>
     *     <li>only {@code KEEP}, {@code DROP}, {@code RENAME}, {@code EVAL}, {@code WHERE}, {@code LIMIT} and
     *     {@code HIGHLIGHT} between the coordinator cut and the root,</li>
     *     <li>the coordinator cut reads the exchange and mirrors the cut that ends the fragment, a {@code TopN} or a
     *     {@code LIMIT},</li>
     *     <li>only {@code WHERE}, {@code EVAL}, projections and local copies of the cut between the fragment cut and the
     *     relation,</li>
     *     <li>one relation, on the local cluster,</li>
     *     <li>at least one column the query only needs after the cut, and that can be fetched.</li>
     * </ol>
     * The lists are deliberately short. Any other command keeps the plan eager, which is always correct, so a new
     * command needs no change here. Adding a command to a list needs a reason why {@code _doc} still names the document
     * of every row after it. Commands that duplicate rows, join them or build them from several documents need that
     * reason one by one.
     */
    private static Shape shape(PhysicalPlan plan) {
        List<ExchangeExec> exchanges = plan.collect(ExchangeExec.class);
        if (exchanges.size() != 1) {
            return new Declined(exchanges.isEmpty() ? "no exchange" : "more than one exchange");
        }
        if (plan.anyMatch(MergeExec.class::isInstance)) {
            return new Declined("[MergeExec] combines several plans");
        }
        ExchangeExec exchange = exchanges.getFirst();
        if (exchange.inBetweenAggs()) {
            return new Declined("the exchange carries an aggregation");
        }
        if (exchange.child() instanceof FragmentExec == false) {
            return new Declined("the exchange does not read a fragment");
        }
        FragmentExec fragment = (FragmentExec) exchange.child();

        AttributeSet.Builder readAfterExchange = AttributeSet.builder().addAll(plan.outputSet());
        PhysicalPlan node = plan;
        while (node.children().size() != 1 || node.children().getFirst() != exchange) {
            if (isSupportedAfterCut(node) == false) {
                return new Declined("[" + node.nodeName() + "] runs after the cut");
            }
            readAfterExchange.addAll(node.references());
            node = ((UnaryExec) node).child();
        }
        if (node instanceof TopNExec == false && node instanceof LimitExec == false) {
            return new Declined("[" + node.nodeName() + "] reads the exchange instead of a LIMIT or TopN");
        }
        UnaryExec coordinatorCut = (UnaryExec) node;
        readAfterExchange.addAll(coordinatorCut.references());

        if (fragment.fragment() instanceof Project == false) {
            return new Declined("the fragment does not end in a projection");
        }
        Project fragmentRoot = (Project) fragment.fragment();
        LogicalPlan fragmentCut = fragmentRoot.child();
        if (mirrors(coordinatorCut, fragmentCut) == false) {
            return new Declined("[" + coordinatorCut.nodeName() + "] does not repeat the cut that ends the fragment");
        }

        LogicalPlan step = ((UnaryPlan) fragmentCut).child();
        while (step instanceof EsRelation == false) {
            if (isSupportedBeforeCut(step) == false) {
                return new Declined("[" + step.nodeName() + "] runs before the cut");
            }
            step = ((UnaryPlan) step).child();
        }
        EsRelation relation = (EsRelation) step;
        if (relation.indexMode() != IndexMode.STANDARD && relation.indexMode() != IndexMode.TIME_SERIES) {
            return new Declined("[" + relation.indexMode() + "] relation");
        }
        if (isLocal(relation) == false) {
            return new Declined("the relation reads a remote cluster");
        }

        // every column that crosses the exchange must be read above it, otherwise the analysis is wrong about the plan
        List<Attribute> unread = exchange.output().stream().filter(a -> readAfterExchange.contains(a) == false).toList();
        if (unread.isEmpty() == false || exchange.output().equals(fragmentRoot.output()) == false) {
            assert false : "exchange output " + exchange.output() + " differs from what the fragment produces and the coordinator reads";
            return new Declined("the exchange carries columns nothing reads " + unread);
        }

        Demand demand = demand(fragmentRoot, relation, coordinatorCut);
        if (demand.deferred().isEmpty()) {
            return new Declined("the cut reads every column, or the others cannot be fetched");
        }
        return new Candidate(exchange, fragment, fragmentRoot, coordinatorCut, relation, demand);
    }

    /**
     * Splits the columns that cross the exchange into the ones the coordinator needs as values to cut and the ones it
     * only needs for the rows that survive. A column is deferred when all of these hold:
     * <ul>
     *     <li>the relation produces it, so the document is enough to load it,</li>
     *     <li>{@link Fetchability#isFetchable} accepts it,</li>
     *     <li>the coordinator cut does not read it. A sort key crosses the exchange even when the query does not return
     *     it.</li>
     *     <li>no {@link Eval} of the fragment reads its values. The data node loads those for every row the {@code Eval}
     *     sees, so the fetch would read them a second time. In {@code SORT a + 1 | LIMIT 10 | KEEP a}, the sort key is
     *     an {@code Eval} of {@code a}, so {@code a} is eager.</li>
     * </ul>
     * Everything else stays eager, which keeps the fetch partial rather than all or nothing: {@code _score}, values
     * computed inside the fragment, and fields that may be unmapped all keep crossing as values. Renames are not
     * followed: a rename after the cut reads its source column after the cut, so the source column is deferred.
     *
     * @param fragmentRoot   the {@link Project} at the root of the fragment. Its output is what crosses the exchange.
     * @param coordinatorCut the coordinator half of the cut, the parent of the exchange
     */
    static Demand demand(Project fragmentRoot, EsRelation relation, PhysicalPlan coordinatorCut) {
        AttributeSet relationOutput = relation.outputSet();
        AttributeSet readByCut = coordinatorCut.references();
        AttributeSet loadedBeforeTheCut = loadedByEvals(fragmentRoot.child());
        List<Attribute> eager = new ArrayList<>();
        List<Attribute> deferred = new ArrayList<>();
        for (Attribute attribute : fragmentRoot.output()) {
            boolean defer = relationOutput.contains(attribute)
                && Fetchability.isFetchable(attribute)
                && readByCut.contains(attribute) == false
                && loadedBeforeTheCut.contains(attribute) == false;
            (defer ? deferred : eager).add(attribute);
        }
        return new Demand(List.copyOf(eager), List.copyOf(deferred));
    }

    /**
     * The attributes whose values an {@link Eval} of the fragment reads, so the data node loads them before the cut.
     * <p>
     * A field passed straight to a function the loader may compute itself, like {@code LENGTH(message)}, doesn't count.
     * The data node may load the length and never the message, and only the data node can tell. Filters don't count
     * either: Lucene usually answers them without loading values. Where these guesses are wrong, the column is read
     * twice, once before the cut and once by the fetch.
     */
    private static AttributeSet loadedByEvals(LogicalPlan fragmentBody) {
        AttributeSet.Builder loaded = AttributeSet.builder();
        fragmentBody.forEachDown(Eval.class, eval -> {
            for (Alias field : eval.fields()) {
                collectValueReads(field.child(), loaded);
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
     * cuts the rows of one node, so it does not count.
     */
    private static boolean mirrors(UnaryExec coordinatorCut, LogicalPlan fragmentCut) {
        if (coordinatorCut instanceof TopNExec topN && fragmentCut instanceof TopN fragmentTopN) {
            return fragmentTopN.local() == false && topN.order().equals(fragmentTopN.order()) && topN.limit().equals(fragmentTopN.limit());
        }
        if (coordinatorCut instanceof LimitExec limit && fragmentCut instanceof Limit fragmentLimit) {
            return fragmentLimit.local() == false && limit.limit().equals(fragmentLimit.limit());
        }
        return false;
    }

    /**
     * The fetch runs right above the coordinator cut, so everything after it sees the fetched columns. A {@code LIMIT}
     * after the cut only drops more rows. It shows up above a {@code WHERE} that follows the cut, because the implicit
     * limit of the query cannot move below a {@code WHERE}.
     * <p>
     * {@code HIGHLIGHT} appends one column per field it highlights, computed from the values of the row alone, so it
     * highlights the fetched text of the winners. The optimizer moves a sort and a limit below it unless the sort reads
     * a generated highlight column, so it usually follows the cut.
     */
    private static boolean isSupportedAfterCut(PhysicalPlan node) {
        return node instanceof ProjectExec
            || node instanceof EvalExec
            || node instanceof FilterExec
            || node instanceof LimitExec
            || node instanceof HighlightExec;
    }

    private static boolean isSupportedBeforeCut(LogicalPlan step) {
        return step instanceof Filter
            || step instanceof Eval
            || step instanceof Project
            || (step instanceof Limit limit && limit.local())
            || (step instanceof TopN topN && topN.local());
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
        // the data drivers produce _doc next to the eager columns, and every projection inside the fragment keeps it
        Attribute doc = new FieldAttribute(Source.EMPTY, null, null, EsQueryExec.DOC_ID_FIELD.getName(), EsQueryExec.DOC_ID_FIELD);
        LogicalPlan body = candidate.fragmentRoot().child();
        body = body.transformUp(
            EsRelation.class,
            relation -> relation.indexMode() == IndexMode.LOOKUP
                ? relation
                : relation.withAttributes(CollectionUtils.prependToCopy(doc, relation.output()))
        );
        body = body.transformUp(Project.class, project -> {
            List<NamedExpression> projections = new ArrayList<>(project.projections().size() + 1);
            projections.add(doc);
            projections.addAll(project.projections());
            return project.withProjections(projections);
        });
        List<Attribute> exchanged = CollectionUtils.prependToCopy(doc, candidate.demand().eager());
        FragmentExec fragment = candidate.fragment().withFragment(new Project(candidate.fragmentRoot().source(), body, exchanged));
        ExchangeExec exchange = new ExchangeExec(candidate.exchange().source(), exchanged, false, fragment);

        // the coordinator cuts the narrow rows, then fetches the deferred columns of the winners
        UnaryExec coordinatorCut = candidate.coordinatorCut();
        FetchExec fetch = new FetchExec(
            coordinatorCut.source(),
            coordinatorCut.replaceChild(exchange),
            doc,
            candidate.demand().deferred(),
            candidate.relation().indexPattern(),
            localIndices(candidate.relation()),
            null
        );

        // the coordinator returns what it returned before, never _doc
        PhysicalPlan rewritten = plan.transformDown(PhysicalPlan.class, node -> node == coordinatorCut ? fetch : node);
        if (sameIds(rewritten.output(), plan.output()) == false) {
            rewritten = new ProjectExec(plan.source(), rewritten, plan.output());
        }
        return rewritten;
    }

    private static List<String> localIndices(EsRelation relation) {
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
