/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Sample;
import org.elasticsearch.xpack.esql.expression.function.inference.InferenceFunction;
import org.elasticsearch.xpack.esql.expression.function.scalar.approximate.Random;
import org.elasticsearch.xpack.esql.expression.function.scalar.date.Now;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeSinkExec;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;

import java.util.List;
import java.util.Set;

/**
 * Decides whether a node request's plan may be served from, or stored in, the shard result cache. Default deny: a plan
 * is cacheable only if every node in it is on the allowlist, so a construct nobody has thought about yet is refused
 * rather than silently admitted.
 * <p>
 * The verdict is a property of the plan and the request, not of any shard, so it is computed once per node request. It
 * carries a reason for the refusal, which is what makes a low hit rate diagnosable rather than mysterious.
 */
final class ShardResultCacheVerifier {

    /**
     * Exact classes, not {@code instanceof}: {@code TimeSeriesAggregate} and {@code SampledAggregate} extend
     * {@link Aggregate} and neither is admissible, so a subtype must not inherit its parent's verdict.
     */
    private static final Set<Class<? extends LogicalPlan>> ALLOWED_FRAGMENT_NODES = Set.of(
        Aggregate.class,
        Filter.class,
        Eval.class,
        Project.class,
        EsRelation.class
    );

    /**
     * Expressions whose value is not a function of the shard's data alone.
     * <ul>
     *     <li>{@link Random} draws from {@code Randomness.get()} when it is evaluated.</li>
     *     <li>{@link Sample} defaults its seed to a fresh random long.</li>
     *     <li>{@link InferenceFunction} calls an external model.</li>
     *     <li>{@link Now} reads the query's wall clock, which is deliberately not in the key. Coordinator constant
     *     folding turns it into a literal long before a fragment is shipped, so this entry is a backstop against a
     *     fragment where folding did not reach it rather than the common case.</li>
     * </ul>
     */
    private static final List<Class<? extends Expression>> NON_DETERMINISTIC_EXPRESSIONS = List.of(
        Random.class,
        Sample.class,
        InferenceFunction.class,
        Now.class
    );

    private ShardResultCacheVerifier() {}

    /**
     * @return {@code null} when the request is cacheable, otherwise the reason it is not
     */
    @Nullable
    static String notCacheableReason(DataNodeRequest request) {
        // A hit produces no drivers, so a profiled request would show no Lucene operators for the cached shards and
        // misrepresent its own execution. The DSL path refuses profiled requests for the same reason.
        if (request.configuration().profile()) {
            return "profiled request";
        }
        if (request.configuration().explainOnly()) {
            return "explain request";
        }
        if (request.externalSplits().isEmpty() == false) {
            return "external source";
        }
        /*
         * Remote fetch ships doc ids and fetches values later against a context that has to stay retained; a shard
         * served from cache never opens that context. Reduce-node late materialization needs no such check: it only
         * rewrites a TopN reduction, and the fragment-root check below admits nothing but an aggregation.
         */
        if (request.retainSearchContexts()) {
            return "remote fetch";
        }
        if (request.clusterAlias().isEmpty() == false) {
            return "cross-cluster execution";
        }
        if (request.plan() instanceof ExchangeSinkExec sink) {
            if (sink.child() instanceof FragmentExec fragmentExec) {
                return notCacheableFragmentReason(fragmentExec.fragment());
            }
            return "unsupported data node plan [" + sink.child().nodeName() + "]";
        }
        return "unsupported data node plan";
    }

    @Nullable
    private static String notCacheableFragmentReason(LogicalPlan fragment) {
        /*
         * Requiring an aggregation at the root keeps the admissible set to shapes whose per-shard output is small,
         * serializable and order-insensitive. Raw row shapes are excluded because late-materialized doc columns cannot
         * be serialized at all, and sorted shapes because their reduction is what the follow-up per-operator work is
         * about.
         */
        if (fragment.getClass() != Aggregate.class) {
            return "fragment root is not an aggregation [" + fragment.nodeName() + "]";
        }
        Holder<String> reason = new Holder<>();
        fragment.forEachDown(node -> {
            if (reason.get() != null) {
                return;
            }
            if (ALLOWED_FRAGMENT_NODES.contains(node.getClass()) == false) {
                reason.set("unsupported plan node [" + node.nodeName() + "]");
                return;
            }
            if (node instanceof EsRelation relation && relation.indexMode() != IndexMode.STANDARD) {
                reason.set("unsupported index mode [" + relation.indexMode() + "]");
            }
        });
        if (reason.get() != null) {
            return reason.get();
        }
        fragment.forEachExpressionDown(Expression.class, expression -> {
            if (reason.get() != null) {
                return;
            }
            for (Class<? extends Expression> nonDeterministic : NON_DETERMINISTIC_EXPRESSIONS) {
                if (nonDeterministic.isInstance(expression)) {
                    reason.set("non-deterministic expression [" + expression.nodeName() + "]");
                    return;
                }
            }
        });
        return reason.get();
    }
}
