/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.fetch;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.CollectionUtils;
import org.elasticsearch.xpack.esql.optimizer.PhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.fetch.FetchPhasePolicy.Outcome;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.TopN;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.esql.plan.physical.DocRefEncodeExec;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.EvalExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeExec;
import org.elasticsearch.xpack.esql.plan.physical.FetchExec;
import org.elasticsearch.xpack.esql.plan.physical.FetchSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.FieldExtractExec;
import org.elasticsearch.xpack.esql.plan.physical.FilterExec;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.plan.physical.LimitExec;
import org.elasticsearch.xpack.esql.plan.physical.MergeExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.ProjectExec;
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
 * A <em>cut</em> is a {@code LIMIT} or a {@code TopN}: a pipeline breaker that only decides which rows survive. Both are
 * decomposable. The global top N is always among the top N rows of every part, so the plan applies the same cut at up
 * to three levels:
 * <ul>
 *     <li>the <em>fragment cut</em> ends the fragment. It runs per shard and is often pushed into Lucene.</li>
 *     <li>the <em>node cut</em> runs once per data node, in the node reduce stage, over the results of its shards.</li>
 *     <li>the <em>coordinator cut</em> runs once, over the results of every node. Only this one knows the global
 *     winners, so the fetch runs right after it.</li>
 * </ul>
 * <p>
 * Without the fetch phase every data node loads each column the query returns for every row that survives its own
 * cut, and the coordinator keeps only the global winners. With it the data nodes send a document reference plus the
 * columns the cut needs, the coordinator cuts, and then loads the other columns for the winners only, from the nodes
 * that own them.
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
 * \_FetchExec[$$doc_ref, fetched=[a, b]]
 *   |_TopNExec[ts DESC, 10]                                    coordinator cut
 *   | \_ExchangeExec[$$doc_ref, ts]                            CLUSTER scope
 *   |   \_DocRefEncodeExec[_doc -> $$doc_ref]                  node reduce stage, planned here
 *   |     \_TopNExec[ts DESC, 10]                              node cut
 *   |       \_ExchangeExec[_doc, ts]                           NODE scope
 *   |         \_FragmentExec[Project[_doc, ts] <- TopN[ts DESC, 10] <- Filter[x > 1] <- EsRelation[idx]]
 *   \_ProjectExec[a, b]                                        fetch plan, runs where the documents are
 *     \_FieldExtractExec[a, b]
 *       \_FetchSourceExec[_doc]
 * }</pre>
 * {@link AttributeDemand} decides which columns cross the exchange as values. {@link #shape} lists the plans the rule
 * accepts. Every other plan keeps loading its columns eagerly, unchanged, and the reason is recorded in
 * {@link FetchPhaseOutcomes}.
 */
public final class PlanFetch extends ParameterizedRule<PhysicalPlan, PhysicalPlan, PhysicalOptimizerContext> {
    private static final Logger logger = LogManager.getLogger(PlanFetch.class);

    /** Name of the document reference column. Nothing matches on it, the column is recognized by its type. */
    public static final String DOC_REF_NAME = Attribute.rawTemporaryName("doc_ref");

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
                PhysicalPlan rewritten = rewrite(plan, candidate, context.configuration().pragmas().nodeLevelReduction());
                outcomes.record(Outcome.APPLIED, null);
                logger.debug("fetch phase planned: eager {}, deferred {}", candidate.demand().eager(), candidate.demand().deferred());
                yield rewritten;
            }
        };
    }

    private sealed interface Shape permits Declined, Candidate {}

    private record Declined(Outcome outcome, String reason) implements Shape {}

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
        AttributeDemand.Demand demand
    ) implements Shape {}

    /**
     * The plans the fetch phase supports, checked in this order:
     * <ol>
     *     <li>one cluster exchange, over a fragment, without aggregation and without {@code FORK} or {@code UNION},</li>
     *     <li>only {@code KEEP}, {@code DROP}, {@code RENAME}, {@code EVAL}, {@code WHERE} and {@code LIMIT} between the
     *     coordinator cut and the root,</li>
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
            return ineligible(exchanges.isEmpty() ? "no exchange" : "more than one exchange");
        }
        if (plan.anyMatch(MergeExec.class::isInstance)) {
            return ineligible("[MergeExec] combines several plans");
        }
        ExchangeExec exchange = exchanges.getFirst();
        if (exchange.inBetweenAggs()) {
            return ineligible("the exchange carries an aggregation");
        }
        if (exchange.child() instanceof FragmentExec == false) {
            return ineligible("the exchange does not read a fragment");
        }
        FragmentExec fragment = (FragmentExec) exchange.child();

        AttributeSet.Builder readAfterExchange = AttributeSet.builder().addAll(plan.outputSet());
        PhysicalPlan node = plan;
        while (node.children().size() != 1 || node.children().getFirst() != exchange) {
            if (isSupportedAfterCut(node) == false) {
                return ineligible("[" + node.nodeName() + "] runs after the cut");
            }
            readAfterExchange.addAll(node.references());
            node = ((UnaryExec) node).child();
        }
        if (node instanceof TopNExec == false && node instanceof LimitExec == false) {
            return ineligible("[" + node.nodeName() + "] reads the exchange instead of a LIMIT or TopN");
        }
        UnaryExec coordinatorCut = (UnaryExec) node;
        readAfterExchange.addAll(coordinatorCut.references());

        if (fragment.fragment() instanceof Project == false) {
            return ineligible("the fragment does not end in a projection");
        }
        Project fragmentRoot = (Project) fragment.fragment();
        LogicalPlan fragmentCut = fragmentRoot.child();
        if (mirrors(coordinatorCut, fragmentCut) == false) {
            return ineligible("[" + coordinatorCut.nodeName() + "] does not repeat the cut that ends the fragment");
        }

        LogicalPlan step = ((UnaryPlan) fragmentCut).child();
        while (step instanceof EsRelation == false) {
            if (isSupportedBeforeCut(step) == false) {
                return ineligible("[" + step.nodeName() + "] runs before the cut");
            }
            step = ((UnaryPlan) step).child();
        }
        EsRelation relation = (EsRelation) step;
        if (relation.indexMode() != IndexMode.STANDARD && relation.indexMode() != IndexMode.TIME_SERIES) {
            return ineligible("[" + relation.indexMode() + "] relation");
        }
        if (isLocal(relation) == false) {
            return new Declined(Outcome.INELIGIBLE_REMOTE_CLUSTER, "the relation reads a remote cluster");
        }

        // every column that crosses the exchange must be read above it, otherwise the analysis is wrong about the plan
        List<Attribute> unread = exchange.output().stream().filter(a -> readAfterExchange.contains(a) == false).toList();
        if (unread.isEmpty() == false || exchange.output().equals(fragmentRoot.output()) == false) {
            assert false : "exchange output " + exchange.output() + " differs from what the fragment produces and the coordinator reads";
            return new Declined(Outcome.INCONSISTENT_PROJECTION, "the exchange carries columns nothing reads " + unread);
        }

        AttributeDemand.Demand demand = AttributeDemand.analyze(fragmentRoot, relation, coordinatorCut);
        if (demand.deferred().isEmpty()) {
            return new Declined(Outcome.INELIGIBLE_NO_DEFERRABLE_FIELDS, "the cut reads every column, or the others cannot be fetched");
        }
        return new Candidate(exchange, fragment, fragmentRoot, coordinatorCut, relation, demand);
    }

    private static Declined ineligible(String reason) {
        return new Declined(Outcome.INELIGIBLE_SHAPE, reason);
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
     */
    private static boolean isSupportedAfterCut(PhysicalPlan node) {
        return node instanceof ProjectExec || node instanceof EvalExec || node instanceof FilterExec || node instanceof LimitExec;
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

    private static PhysicalPlan rewrite(PhysicalPlan plan, Candidate candidate, boolean nodeLevelReduction) {
        // 1. the data drivers produce _doc next to the eager columns, and every projection inside the fragment keeps it
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
        List<Attribute> dataOutput = CollectionUtils.prependToCopy(doc, candidate.demand().eager());
        FragmentExec fragment = candidate.fragment().withFragment(new Project(candidate.fragmentRoot().source(), body, dataOutput));

        // 2. the node reduce stage: the node cut over the node exchange, then _doc becomes a document reference
        UnaryExec coordinatorCut = candidate.coordinatorCut();
        PhysicalPlan nodeStage = new ExchangeExec(candidate.exchange().source(), dataOutput, false, ExchangeExec.Scope.NODE, fragment);
        if (nodeLevelReduction) {
            nodeStage = switch (coordinatorCut) {
                // each data driver emits its rows sorted, so the node cut merges sorted streams
                case TopNExec topN -> new TopNExec(topN.source(), nodeStage, topN.order(), topN.limit(), null).withSortedInput();
                case LimitExec limit -> new LimitExec(limit.source(), nodeStage, limit.limit(), null);
                default -> throw new IllegalStateException("unexpected cut [" + coordinatorCut.nodeName() + "]");
            };
        }
        ReferenceAttribute docRef = new ReferenceAttribute(
            Source.EMPTY,
            null,
            DOC_REF_NAME,
            DataType.DOC_REF,
            Nullability.FALSE,
            null,
            true
        );
        DocRefEncodeExec encode = new DocRefEncodeExec(Source.EMPTY, nodeStage, doc, docRef);
        ExchangeExec clusterExchange = new ExchangeExec(
            candidate.exchange().source(),
            encode.output(),
            false,
            ExchangeExec.Scope.CLUSTER,
            encode
        );

        // 3. the coordinator cuts on the narrow rows, then fetches the deferred columns of the winners
        List<Attribute> deferred = candidate.demand().deferred();
        Attribute fetchDoc = new FieldAttribute(Source.EMPTY, null, null, EsQueryExec.DOC_ID_FIELD.getName(), EsQueryExec.DOC_ID_FIELD);
        PhysicalPlan fetchPlan = new ProjectExec(
            Source.EMPTY,
            new FieldExtractExec(
                Source.EMPTY,
                new FetchSourceExec(Source.EMPTY, fetchDoc, null),
                deferred,
                MappedFieldType.FieldExtractPreference.NONE
            ),
            deferred
        );
        FetchExec fetch = new FetchExec(
            coordinatorCut.source(),
            coordinatorCut.replaceChild(clusterExchange),
            fetchPlan,
            docRef,
            deferred,
            1,
            candidate.relation().indexPattern(),
            localIndices(candidate.relation()),
            null
        );

        // 4. the coordinator returns what it returned before, never the document reference
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
