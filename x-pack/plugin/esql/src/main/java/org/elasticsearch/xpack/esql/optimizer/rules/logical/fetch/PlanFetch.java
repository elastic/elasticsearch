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
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.CollectionUtils;
import org.elasticsearch.xpack.esql.expression.function.blockloader.BlockLoaderExpression;
import org.elasticsearch.xpack.esql.index.IndexProperties;
import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;
import org.elasticsearch.xpack.esql.plan.logical.DocPreserving;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.ExecutesOn;
import org.elasticsearch.xpack.esql.plan.logical.ExecutesOn.ExecuteLocation;
import org.elasticsearch.xpack.esql.plan.logical.Fetch;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.PipelineBreaker;
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
            case Declined declined -> declined(plan, declined.reason());
            case Candidate candidate -> rewrite(plan, candidate);
        };
    }

    private static LogicalPlan declined(LogicalPlan plan, String reason) {
        logger.debug("fetch phase not planned: {}", reason);
        return plan;
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
     *     <li>commands above the cut that can run on the coordinator, see {@link #isSupportedAfterCut},</li>
     *     <li>the cut, the {@code TopN} or {@code LIMIT} closest to the relation that isn't local,</li>
     *     <li>commands between the cut and the relation that keep the document of each row and run on the data nodes,
     *     see {@link #isSupportedBeforeCut},</li>
     *     <li>one {@code STANDARD} or {@code TIME_SERIES} relation, on the local cluster,</li>
     *     <li>no column named {@code _doc}, the name of the document column the rule adds,</li>
     *     <li>at least one column the query only needs after the cut, and that can be fetched.</li>
     * </ol>
     * Nothing between the cut and the relation breaks the pipeline, so the mapper copies the cut to the data nodes and
     * puts the only exchange of the plan right below the coordinator's copy. A plan that already fetches is left as it
     * is. Any other plan keeps loading its columns eagerly, which is always correct.
     */
    private static Shape shape(LogicalPlan plan) {
        List<UnaryPlan> chain = new ArrayList<>();
        LogicalPlan leaf = plan;
        while (leaf instanceof UnaryPlan unary) {
            if (unary instanceof Fetch) {
                return new Declined("the plan already fetches");
            }
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
        if (namesADocColumn(plan)) {
            return new Declined("the query names a column [_doc]");
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
     *     <li>The relation must have more than one shard. The data node that holds a single shard cuts its rows down to
     *     the global winners, and node-level late materialization already loads the deferred columns of those only.</li>
     *     <li>The fetch must defer {@code _source}. It is usually the largest column per row and the most expensive to
     *     load, stored or synthetic, so skipping it for the dropped rows is what pays for the round trip.</li>
     * </ul>
     * Once the round trip is paid, the fetch defers every column {@link #demand} allows, not only {@code _source}.
     */
    private static Shape paysOff(Candidate candidate) {
        if (candidate.cut() instanceof TopN == false) {
            return new Declined("a [LIMIT] cut leaves too few dropped rows to pay for the fetch");
        }
        if (hasOneShard(candidate.relation())) {
            return new Declined("the relation has one shard, whose node loads the deferred columns of the winners only");
        }
        if (candidate.demand().deferred().stream().noneMatch(attribute -> attribute.dataType() == DataType.SOURCE)) {
            return new Declined("the fetch defers no [_source], so it saves too little to pay for its round trip");
        }
        return candidate;
    }

    /** Whether the shards of the relation add up to one. An index that doesn't report its shards counts as 0, unknown. */
    private static boolean hasOneShard(EsRelation relation) {
        int shards = 0;
        for (IndexProperties properties : relation.indexProperties().values()) {
            if (properties.numberOfShards() <= 0) {
                return false;
            }
            shards += properties.numberOfShards();
        }
        return shards == 1;
    }

    /**
     * Splits the columns that cross the exchange into the ones the coordinator needs as values to cut and the ones it
     * only needs for the rows that survive. A column of the cut's output crosses the exchange when the cut or a command
     * above it reads it, or the query returns it. It is deferred when all of these hold:
     * <ul>
     *     <li>the relation produces it, so the document is enough to load it,</li>
     *     <li>{@link Fetchability#isFetchable} accepts it,</li>
     *     <li>the cut does not read it. A sort key crosses the exchange even when the query does not return it.</li>
     *     <li>no command before the cut reads its values. The data node loads those for every row the command sees, so
     *     the fetch would read them a second time. In {@code SORT a + 1 | LIMIT 10 | KEEP a}, the sort key is an
     *     {@code EVAL} of {@code a}, so {@code a} is eager.</li>
     * </ul>
     * Everything else stays eager, which keeps the fetch partial rather than all or nothing: {@code _score}, values
     * computed before the cut, and fields that may be unmapped all keep crossing as values. Renames are not followed: a
     * rename after the cut reads its source column after the cut, so the source column is deferred.
     *
     * @param plan the whole plan. Its output and the commands above the cut read the columns that cross the exchange.
     * @param cut  the cut, a command of {@code plan}
     */
    static Demand demand(LogicalPlan plan, UnaryPlan cut, EsRelation relation) {
        AttributeSet requiredAfterCut = requiredAfterCut(plan, cut);
        AttributeSet relationOutput = relation.outputSet();
        AttributeSet readByCut = cut.references();
        AttributeSet loadedBeforeTheCut = loadedBeforeTheCut(cut.child());
        List<Attribute> eager = new ArrayList<>();
        List<Attribute> deferred = new ArrayList<>();
        for (Attribute attribute : cut.output()) {
            if (requiredAfterCut.contains(attribute) == false && readByCut.contains(attribute) == false) {
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

    /** The columns the query returns, and the columns the commands above the cut read. */
    private static AttributeSet requiredAfterCut(LogicalPlan plan, UnaryPlan cut) {
        AttributeSet.Builder required = AttributeSet.builder().addAll(plan.outputSet());
        for (LogicalPlan node = plan; node != cut; node = ((UnaryPlan) node).child()) {
            required.addAll(node.references());
        }
        return required.build();
    }

    /**
     * The attributes whose values a command before the cut reads, so the data node loads them before the cut.
     * <p>
     * Filters don't count: Lucene usually answers them without loading values. Projections don't count either: they only
     * pass columns on. A field passed straight to a function the loader may compute itself, like {@code LENGTH(message)},
     * doesn't count. The data node may load the length and never the message, and only the data node can tell. Where
     * these guesses are wrong, the column is read twice, once before the cut and once by the fetch.
     */
    private static AttributeSet loadedBeforeTheCut(LogicalPlan beforeTheCut) {
        AttributeSet.Builder loaded = AttributeSet.builder();
        beforeTheCut.forEachDown(UnaryPlan.class, command -> {
            if (command instanceof Filter || command instanceof Project) {
                return;
            }
            for (Expression expression : command.expressions()) {
                collectValueReads(expression, loaded);
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
     * The fetch runs right above the cut, and every command after it runs on the coordinator over the rows the fetch
     * completed, so any command can follow. A command that only runs on the data nodes can't follow a cut.
     */
    private static boolean isSupportedAfterCut(LogicalPlan command) {
        return (command instanceof ExecutesOn executesOn && executesOn.executesOn() == ExecuteLocation.REMOTE) == false;
    }

    /**
     * A command before the cut runs on the data nodes, where {@code _doc} names the document of each row. It must keep
     * that tie, see {@link DocPreserving}. {@code MV_EXPAND} repeats a row for each value, and every copy keeps the
     * {@code _doc} of its document. The mapper must also send the command to the data nodes with the relation, see
     * {@code Mapper#mapUnary}: not a command that only runs on the coordinator, and no pipeline breaker but the local
     * cuts the optimizer copies there.
     */
    private static boolean isSupportedBeforeCut(LogicalPlan command) {
        if (command instanceof ExecutesOn executesOn && executesOn.executesOn() == ExecuteLocation.COORDINATOR) {
            return false;
        }
        if (command instanceof DocPreserving == false) {
            return false;
        }
        return command instanceof PipelineBreaker == false
            || (command instanceof Limit limit && limit.local())
            || (command instanceof TopN topN && topN.local());
    }

    /** The rule adds a column named {@code _doc}. A column of the query with the same name would clash with it. */
    private static boolean namesADocColumn(LogicalPlan plan) {
        String docName = EsQueryExec.DOC_ID_FIELD.getName();
        return plan.anyMatch(node -> node.output().stream().anyMatch(attribute -> attribute.name().equals(docName)));
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
        if (beforeTheCut.outputSet().contains(doc) == false) {
            assert false : "a command before the cut drops [_doc]: " + beforeTheCut;
            return declined(plan, "a command before the cut drops [_doc]");
        }

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
        logger.debug("fetch phase planned: eager {}, deferred {}", candidate.demand().eager(), candidate.demand().deferred());
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
