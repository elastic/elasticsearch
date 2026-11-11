/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical.fetch;

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
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.CollectionUtils;
import org.elasticsearch.xpack.esql.expression.function.blockloader.BlockLoaderExpression;
import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Fetch;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Highlight;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.TopN;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags.FetchPhaseMode;
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
 * mapper runs the cut on every data node and once more on the coordinator over what they send. The global top N is
 * always among the top N rows of every part, but only the coordinator's cut knows the global winners.
 * <p>
 * Without the fetch phase every data node loads each column the query returns for every row that survives its own
 * cut, and the coordinator keeps only the global winners. With it the data nodes send the {@code _doc} of each row
 * plus the columns the cut needs, the coordinator cuts, and then loads the other columns for the winners only, from the
 * nodes that own them.
 * <p>
 * {@code FROM idx | WHERE x > 1 | SORT ts DESC | LIMIT 10 | KEEP a, b, ts} before:
 * <pre>{@code
 * Project[a, b, ts]
 * \_TopN[ts DESC, 10]
 *   \_Filter[x > 1]
 *     \_EsRelation[idx][a, b, ts, x]
 * }</pre>
 * and after:
 * <pre>{@code
 * Project[a, b, ts]
 * \_Fetch[_doc, fetched=[a, b]]
 *   \_Project[_doc, ts]
 *     \_TopN[ts DESC, 10]
 *       \_Filter[x > 1]
 *         \_EsRelation[idx][_doc, a, b, ts, x]
 * }</pre>
 * Nothing above the projection under {@link Fetch} reads {@code a} or {@code b} from the cut. The mapper puts the
 * exchange right below the cut, as for any cut over a relation, and {@code ProjectAwayColumns} sends only {@code _doc}
 * and {@code ts} across it. The data nodes plan their side as they plan any other. Node-level late materialization cuts
 * the rows of a node's shards once more, and loads the eager columns the cut doesn't read for the survivors only.
 * <p>
 * {@link #demand} decides which columns cross the exchange as values. {@link #shape} lists the plans the rule accepts.
 * Both are about correctness only. {@link #paysOff} then decides whether an accepted plan is likely faster with the
 * fetch phase. Every other plan keeps loading its columns eagerly, unchanged, and the rule logs why at debug level.
 * <p>
 * {@link EsqlFlags#fetchPhaseMode()} turns the rule on. {@link FetchPhaseMode#ALWAYS} skips {@link #paysOff}. The rule
 * runs in the last batch of the logical optimizer, so no other logical rule needs to know {@link Fetch}.
 */
public final class PlanFetch extends ParameterizedRule<LogicalPlan, LogicalPlan, LogicalOptimizerContext> {
    private static final Logger logger = LogManager.getLogger(PlanFetch.class);

    @Override
    public LogicalPlan apply(LogicalPlan plan, LogicalOptimizerContext context) {
        FetchPhaseMode mode = context.flags().fetchPhaseMode();
        if (mode == FetchPhaseMode.DISABLED) {
            return plan;
        }
        Shape shape = shape(plan);
        if (mode == FetchPhaseMode.AUTO && shape instanceof Candidate candidate) {
            shape = paysOff(candidate);
        }
        return switch (shape) {
            case Declined declined -> {
                logger.debug("fetch phase not planned: {}", declined.reason());
                yield plan;
            }
            case Candidate candidate -> {
                LogicalPlan rewritten = rewrite(plan, candidate);
                logger.debug("fetch phase planned: eager {}, deferred {}", candidate.demand().eager(), candidate.demand().deferred());
                yield rewritten;
            }
        };
    }

    /**
     * The split of the columns that cross the exchange.
     *
     * @param eager    columns that keep crossing the exchange as values, in the order of the cut's output
     * @param deferred columns the fetch loads after the cut, in the order of the cut's output
     */
    record Demand(List<Attribute> eager, List<Attribute> deferred) {}

    private sealed interface Shape permits Declined, Candidate {}

    private record Declined(String reason) implements Shape {}

    /**
     * A plan the rule can rewrite.
     *
     * @param cut the cut the data nodes and the coordinator both apply
     */
    private record Candidate(UnaryPlan cut, EsRelation relation, Demand demand) implements Shape {}

    /**
     * The plans the fetch phase supports. From the root down, the plan is one chain of commands over one relation:
     * <ol>
     *     <li>only {@code KEEP}, {@code DROP}, {@code RENAME}, {@code EVAL}, {@code WHERE}, {@code LIMIT} and
     *     {@code HIGHLIGHT} above the cut,</li>
     *     <li>the cut, the {@code TopN} or {@code LIMIT} closest to the relation that isn't local,</li>
     *     <li>only {@code WHERE}, {@code EVAL}, projections and local cuts between the cut and the relation,</li>
     *     <li>one {@code STANDARD} or {@code TIME_SERIES} relation, on the local cluster,</li>
     *     <li>at least one column the query only needs after the cut, and that can be fetched.</li>
     * </ol>
     * Nothing between the cut and the relation breaks the pipeline, so the mapper copies the cut to the data nodes and
     * puts the only exchange of the plan right below the coordinator's copy.
     * <p>
     * The lists are deliberately short. Any other command keeps the plan eager, which is always correct, so a new
     * command needs no change here. Adding a command to a list needs a reason why {@code _doc} still names the document
     * of every row after it. Commands that duplicate rows, join them or build them from several documents need that
     * reason one by one.
     */
    private static Shape shape(LogicalPlan plan) {
        List<UnaryPlan> chain = new ArrayList<>();
        LogicalPlan leaf = plan;
        while (leaf instanceof UnaryPlan unary) {
            chain.add(unary);
            leaf = unary.child();
        }
        if (leaf.children().isEmpty() == false) {
            return new Declined("[" + leaf.nodeName() + "] combines several plans");
        }
        if (leaf instanceof EsRelation == false) {
            return new Declined("[" + leaf.nodeName() + "] is not an index relation");
        }
        EsRelation relation = (EsRelation) leaf;

        int cutIndex = chain.size() - 1;
        while (cutIndex >= 0 && isCut(chain.get(cutIndex)) == false) {
            cutIndex--;
        }
        if (cutIndex < 0) {
            return new Declined("no [LIMIT] or [TopN] cuts the rows");
        }
        for (UnaryPlan node : chain.subList(0, cutIndex)) {
            if (isSupportedAfterCut(node) == false) {
                return new Declined("[" + node.nodeName() + "] runs after the cut");
            }
        }
        for (UnaryPlan node : chain.subList(cutIndex + 1, chain.size())) {
            if (isSupportedBeforeCut(node) == false) {
                return new Declined("[" + node.nodeName() + "] runs before the cut");
            }
        }
        if (relation.indexMode() != IndexMode.STANDARD && relation.indexMode() != IndexMode.TIME_SERIES) {
            return new Declined("[" + relation.indexMode() + "] relation");
        }
        if (isLocal(relation) == false) {
            return new Declined("the relation reads a remote cluster");
        }

        UnaryPlan cut = chain.get(cutIndex);
        Demand demand = demand(plan, cut, relation);
        if (demand.deferred().isEmpty()) {
            return new Declined("the cut reads every column, or the others cannot be fetched");
        }
        return new Candidate(cut, relation, demand);
    }

    /**
     * Whether the fetch phase is likely faster than loading the deferred columns eagerly. The fetch saves loading and
     * sending the deferred columns of the rows the data nodes keep and the coordinator drops. It costs one more round
     * trip after the coordinator cut, which nothing overlaps. With few or small deferred columns the round trip costs
     * more than it saves.
     * <ul>
     *     <li>The cut must be a {@code TopN}. A data node stops reading after {@code LIMIT} rows, and without a
     *     filter the coordinator queries only a few nodes at a time, so an eager {@code LIMIT} loads few rows that the
     *     coordinator drops.</li>
     *     <li>The fetch must defer {@code _source}. It is usually the largest column per row and the most expensive to
     *     load, stored or synthetic, so skipping it for the dropped rows is what pays for the round trip.</li>
     * </ul>
     * Once the round trip is paid, the fetch defers every column {@link #demand} allows, not only {@code _source}.
     */
    private static Shape paysOff(Candidate candidate) {
        if (candidate.cut() instanceof TopN == false) {
            return new Declined("a [LIMIT] cut leaves too few dropped rows to pay for the fetch");
        }
        if (candidate.demand().deferred().stream().noneMatch(attribute -> attribute.dataType() == DataType.SOURCE)) {
            return new Declined("the fetch defers no [_source], so it saves too little to pay for its round trip");
        }
        return candidate;
    }

    /**
     * Splits the columns that cross the exchange into the ones the coordinator needs as values to cut and the ones it
     * only needs for the rows that survive. A column of the cut's output crosses the exchange when the cut or a command
     * above it reads it, or the query returns it. It is deferred when all of these hold:
     * <ul>
     *     <li>the relation produces it, so the document is enough to load it,</li>
     *     <li>{@link Fetchability#isFetchable} accepts it,</li>
     *     <li>the cut does not read it. A sort key crosses the exchange even when the query does not return it.</li>
     *     <li>no {@link Eval} before the cut reads its values. The data node loads those for every row the {@code Eval}
     *     sees, so the fetch would read them a second time. In {@code SORT a + 1 | LIMIT 10 | KEEP a}, the sort key is
     *     an {@code Eval} of {@code a}, so {@code a} is eager.</li>
     * </ul>
     * Everything else stays eager, which keeps the fetch partial rather than all or nothing: {@code _score}, values
     * computed before the cut, and fields that may be unmapped all keep crossing as values. Renames are not followed: a
     * rename after the cut reads its source column after the cut, so the source column is deferred.
     *
     * @param plan the whole plan. Its output and the commands above the cut read the columns that cross the exchange.
     * @param cut  the cut, a command of {@code plan}
     */
    static Demand demand(LogicalPlan plan, UnaryPlan cut, EsRelation relation) {
        AttributeSet readAfterTheCut = readAfter(plan, cut);
        AttributeSet relationOutput = relation.outputSet();
        AttributeSet readByCut = cut.references();
        AttributeSet loadedBeforeTheCut = loadedByEvals(cut.child());
        List<Attribute> eager = new ArrayList<>();
        List<Attribute> deferred = new ArrayList<>();
        for (Attribute attribute : cut.output()) {
            if (readAfterTheCut.contains(attribute) == false && readByCut.contains(attribute) == false) {
                continue;
            }
            boolean defer = relationOutput.contains(attribute)
                && Fetchability.isFetchable(attribute)
                && readByCut.contains(attribute) == false
                && loadedBeforeTheCut.contains(attribute) == false;
            (defer ? deferred : eager).add(attribute);
        }
        return new Demand(List.copyOf(eager), List.copyOf(deferred));
    }

    private static AttributeSet readAfter(LogicalPlan plan, UnaryPlan cut) {
        AttributeSet.Builder read = AttributeSet.builder().addAll(plan.outputSet());
        for (LogicalPlan node = plan; node != cut; node = ((UnaryPlan) node).child()) {
            read.addAll(node.references());
        }
        return read.build();
    }

    /**
     * The attributes whose values an {@link Eval} before the cut reads, so the data node loads them before the cut.
     * <p>
     * A field passed straight to a function the loader may compute itself, like {@code LENGTH(message)}, doesn't count.
     * The data node may load the length and never the message, and only the data node can tell. Filters don't count
     * either: Lucene usually answers them without loading values. Where these guesses are wrong, the column is read
     * twice, once before the cut and once by the fetch.
     */
    private static AttributeSet loadedByEvals(LogicalPlan beforeTheCut) {
        AttributeSet.Builder loaded = AttributeSet.builder();
        beforeTheCut.forEachDown(Eval.class, eval -> {
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

    /** A local {@code LIMIT} or {@code TopN} only cuts the rows of one node, so it doesn't pick the global winners. */
    private static boolean isCut(LogicalPlan node) {
        return (node instanceof TopN topN && topN.local() == false) || (node instanceof Limit limit && limit.local() == false);
    }

    /**
     * The fetch runs right above the cut, so everything after it sees the fetched columns. A {@code LIMIT} after the cut
     * only drops more rows. It shows up above a {@code WHERE} that follows the cut, because the implicit limit of the
     * query cannot move below a {@code WHERE}.
     * <p>
     * {@code HIGHLIGHT} appends one column per field it highlights, computed from the values of the row alone, so it
     * highlights the fetched text of the winners. The optimizer moves a sort and a limit below it unless the sort reads
     * a generated highlight column, so it usually follows the cut.
     */
    private static boolean isSupportedAfterCut(LogicalPlan node) {
        return node instanceof Project
            || node instanceof Eval
            || node instanceof Filter
            || node instanceof Limit
            || node instanceof Highlight;
    }

    private static boolean isSupportedBeforeCut(LogicalPlan node) {
        return node instanceof Filter
            || node instanceof Eval
            || node instanceof Project
            || (node instanceof Limit limit && limit.local())
            || (node instanceof TopN topN && topN.local());
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

    private static LogicalPlan rewrite(LogicalPlan plan, Candidate candidate) {
        // the data drivers produce _doc next to the eager columns, and every projection before the cut keeps it
        Attribute doc = new FieldAttribute(Source.EMPTY, null, null, EsQueryExec.DOC_ID_FIELD.getName(), EsQueryExec.DOC_ID_FIELD);
        UnaryPlan cut = candidate.cut();
        LogicalPlan beforeTheCut = cut.child()
            .transformUp(EsRelation.class, relation -> relation.withAttributes(CollectionUtils.prependToCopy(doc, relation.output())));
        beforeTheCut = beforeTheCut.transformUp(Project.class, project -> {
            List<NamedExpression> projections = new ArrayList<>(project.projections().size() + 1);
            projections.add(doc);
            projections.addAll(project.projections());
            return project.withProjections(projections);
        });

        // nothing above the narrowed cut reads the deferred columns from it, so they don't cross the exchange
        Project narrowed = new Project(
            cut.source(),
            cut.replaceChild(beforeTheCut),
            CollectionUtils.prependToCopy(doc, candidate.demand().eager())
        );
        Fetch fetch = new Fetch(
            cut.source(),
            narrowed,
            doc,
            candidate.demand().deferred(),
            candidate.relation().indexPattern(),
            localIndices(candidate.relation())
        );

        // the query returns what it returned before, never _doc
        LogicalPlan rewritten = plan.transformDown(LogicalPlan.class, node -> node == cut ? fetch : node);
        if (sameIds(rewritten.output(), plan.output()) == false) {
            rewritten = new Project(plan.source(), rewritten, plan.output());
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
