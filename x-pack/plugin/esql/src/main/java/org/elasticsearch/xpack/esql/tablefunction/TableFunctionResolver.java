/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.tablefunction;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.RefCountingListener;
import org.elasticsearch.action.support.ThreadedActionListener;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.xpack.esql.action.PlanningCpuTracker;
import org.elasticsearch.xpack.esql.parser.ParsingException;
import org.elasticsearch.xpack.esql.plan.logical.LeafPlan;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedTableFunctionRelation;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;

/**
 * Turns every {@link UnresolvedTableFunctionRelation} the parser left in a plan into the leaf that reads.
 * <p>
 * The whole of what is common to every table function lives here, and it is four things: resolve the name, check
 * the shape, run the function's own validation, and bind. Everything else a particular function needs — a
 * privilege, a round-trip, a collaborator — belongs to that function, not here. Nothing here reads a function's
 * arguments, holds a collaborator, or checks a privilege.
 * <p>
 * Resolution of the <i>name</i> happens before any of it, so a misspelled function is answered by its own
 * diagnostic at its own position rather than by whatever the next stage would have made of it.
 */
public class TableFunctionResolver {

    /**
     * The most calls one query may make, counted over the whole plan once views have been expanded.
     * <p>
     * A fixed ceiling rather than a setting, and the first thing to refuse a wide source list rather than a
     * backstop behind something else. The configurable branch count ({@code max_branch_count}, 20 by default) is
     * checked on the <i>optimized</i> plan, which is long after every call in this query has been made. The
     * parser's own width check is reached from one place, {@code LogicalPlanBuilder#visitForkCommand}, and is
     * applied to the {@code Fork} node built there - so a {@code FROM} naming more calls than this is refused by
     * no parser check at all. Each call is an outbound request for a function that does I/O, and they go out
     * together rather than in turn, so a source list is a way to spend one query string on as many concurrent
     * requests as it has room for, with nothing refusing it until those requests are done.
     * <p>
     * Set to {@link QueryPragmas#MAX_PARSED_BRANCH_COUNT} for its value and not for its reach: 128 is the number
     * already chosen for how wide a parsed plan may get, and taking it keeps one number here rather than a
     * second one that would drift from it.
     */
    static final int MAX_CALLS_PER_QUERY = QueryPragmas.MAX_PARSED_BRANCH_COUNT;

    private final TableFunctionRegistry registry;
    private final Executor executor;

    /**
     * @param executor the pool planning continues on. A function may complete its listener from anywhere - a
     *                 transport response lands on a transport worker, a field-caps handler on
     *                 {@code search_coordination} - and the caller resumes planning on {@code search} and asserts
     *                 it. Making that the facility's business rather than each function's is the difference
     *                 between a contract and a thing every function has to remember; the sibling
     *                 {@code DatasetResolver} takes the same executor for the same reason.
     */
    public TableFunctionResolver(TableFunctionRegistry registry, Executor executor) {
        this.registry = registry;
        this.executor = executor;
    }

    /**
     * Completes {@code listener} with the plan whose table functions have become reading leaves, or with the plan
     * untouched when it has none — which costs one walk and nothing else, so an ordinary query pays almost nothing.
     */
    public void resolveTableFunctions(LogicalPlan parsed, ProjectMetadata projectMetadata, ActionListener<LogicalPlan> listener) {
        List<UnresolvedTableFunctionRelation> leaves = new ArrayList<>();
        // Distinct by identity, because a plan may reach one leaf from more than one parent: FORK hands every
        // branch the one plan above it -- LogicalPlanBuilder#visitForkCommand applies each branch's factory to the
        // same input -- so FROM f(x) | FORK (..) (..) walks down to one leaf once per branch. Collected per
        // occurrence, a call the user wrote once is asked once per branch: a function doing I/O pays a round trip
        // per branch, and every relation but the last is built and thrown away. Identity rather than equality for
        // the reason the map below is: two calls written separately must each bind, and they can compare equal.
        //
        // Counted rather than merely de-duplicated. Asking once means the one relation is then installed in every
        // place that leaf occupied, and a relation carrying a page cannot be read from more than one of them --
        // see readableRepeatedly, which the count decides to apply.
        Map<UnresolvedTableFunctionRelation, Integer> occurrences = new IdentityHashMap<>();
        parsed.forEachDown(node -> {
            if (node instanceof UnresolvedTableFunctionRelation leaf && occurrences.merge(leaf, 1, Integer::sum) == 1) {
                leaves.add(leaf);
            }
        });
        if (leaves.isEmpty()) {
            listener.onResponse(parsed);
            return;
        }
        if (leaves.size() > MAX_CALLS_PER_QUERY) {
            // Before the shape checks below, and before anything is asked: a query this wide has to lose calls
            // whatever is wrong with the ones it has, and the position is the first call it has no room for, so
            // the message points at where cutting starts.
            listener.onFailure(
                new ParsingException(
                    leaves.get(MAX_CALLS_PER_QUERY).call().source(),
                    "a query may call up to [{}] table functions, found [{}]",
                    MAX_CALLS_PER_QUERY,
                    leaves.size()
                )
            );
            return;
        }

        // Every shape question is settled before any function binds, so a malformed call in a source list is
        // reported as what it is rather than as a failure of whatever the first well-formed call was doing.
        List<TableFunction> functions = new ArrayList<>(leaves.size());
        try {
            for (UnresolvedTableFunctionRelation leaf : leaves) {
                TableFunctionInvocation call = leaf.call();
                TableFunction function = registry.resolve(call.source(), call.name());
                function.signature().check(call);
                function.validate(call);
                functions.add(function);
            }
        } catch (Exception e) {
            listener.onFailure(e);
            return;
        }

        // One slot per leaf, written by that leaf's bind and by nothing else. The binds below run concurrently, so
        // the alternative is a shared structure and a lock around it; distinct slots of an array need neither,
        // which is why the collection is positional and the lookup substitute needs is built from it afterwards.
        LeafPlan[] relations = new LeafPlan[leaves.size()];
        // Forked here and not on the early return above: a query naming no function never leaves the caller's
        // thread, so it pays nothing for this.
        //
        // The rewrite is inside the fork rather than in front of it. The last function to complete releases the
        // last ref, so anything run before the fork runs on that function's thread - a transport worker, for a
        // function backed by a client call - and outside the metering this listener installs. Walking the plan is
        // planning work, so it belongs on the pool planning continues on, which is what the executor names.
        ActionListener<Void> onPlanningPool = new ThreadedActionListener<>(
            executor,
            PlanningCpuTracker.inheritMeteredCpu(listener.map(ignored -> substitute(parsed, leaves, relations, occurrences)))
        );
        try (var refs = new RefCountingListener(onPlanningPool)) {
            for (int i = 0; i < leaves.size(); i++) {
                UnresolvedTableFunctionRelation leaf = leaves.get(i);
                TableFunction function = functions.get(i);
                // Concurrently, not in sequence: a source list naming several functions that each need I/O should
                // cost one round-trip's latency rather than one per leaf.
                //
                // ActionListener.run, not a bare call: acquire() takes the ref before toRelation runs, so a function
                // that throws instead of completing would leave the count above zero for good and this method would
                // never answer its own listener. A function is not required to be no-throw, so the facility carries
                // the cost of one that is not.
                int slot = i;
                ActionListener<LeafPlan> bind = refs.acquire(relation -> relations[slot] = relation);
                ActionListener.run(bind, l -> function.toRelation(leaf.call(), leaf.metadataFields(), projectMetadata, l));
            }
        }
    }

    private static LogicalPlan substitute(
        LogicalPlan parsed,
        List<UnresolvedTableFunctionRelation> leaves,
        LeafPlan[] relations,
        Map<UnresolvedTableFunctionRelation, Integer> occurrences
    ) {
        // Keyed by identity rather than by equality. Two calls written in one query do differ today, because an
        // invocation's first component is its Source and the parser gives each its own - so equality would work.
        // It is identity because the alternative depends on that staying true: were a call's identity ever to stop
        // including its position, an equality-keyed map would silently collapse two leaves onto one relation
        // instance, installing the same attribute ids twice in one plan. Built here rather than filled by the
        // binds: every write to it is on this thread, so it is a plain map.
        Map<UnresolvedTableFunctionRelation, LeafPlan> bound = new IdentityHashMap<>(leaves.size());
        for (int i = 0; i < leaves.size(); i++) {
            UnresolvedTableFunctionRelation leaf = leaves.get(i);
            bound.put(leaf, readableRepeatedly(relations[i], occurrences.get(leaf)));
        }
        return parsed.transformDown(UnresolvedTableFunctionRelation.class, leaf -> {
            LeafPlan relation = bound.get(leaf);
            // Thrown rather than asserted, as TableFunctionSignature's own declaration checks are: assertions are
            // off in production, where a null would fail later as an NPE naming no function, and another unresolved
            // call would be reported to the user as their own syntax being unresolved.
            if (relation == null) {
                throw new IllegalStateException("[" + leaf.call().name() + "] bound no relation for a call in this query");
            }
            if (relation instanceof UnresolvedTableFunctionRelation) {
                throw new IllegalStateException("[" + leaf.call().name() + "] returned another table function call");
            }
            return relation;
        });
    }

    /**
     * Makes a relation that is about to be installed in several places safe to read from each of them.
     * <p>
     * Reached here by asking a call once and installing its relation under every FORK branch, which without
     * {@link LocalRelation#readableRepeatedly()} crashes on the released block or hangs the request.
     * <p>
     * Only where a leaf really does occur more than once, so a call written once and read once pays no copy. And
     * only for {@code LocalRelation}: a leaf a function left unresolved becomes its own relation per occurrence
     * further down, the way a written-out index pattern does, so there is nothing to make safe.
     */
    private static LeafPlan readableRepeatedly(LeafPlan relation, int occurrences) {
        if (occurrences > 1 && relation instanceof LocalRelation local) {
            return local.readableRepeatedly();
        }
        return relation;
    }
}
