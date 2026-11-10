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
import org.elasticsearch.xpack.esql.plan.logical.LeafPlan;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedTableFunctionRelation;

import java.util.ArrayList;
import java.util.Collections;
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
        parsed.forEachDown(node -> {
            if (node instanceof UnresolvedTableFunctionRelation leaf) {
                leaves.add(leaf);
            }
        });
        if (leaves.isEmpty()) {
            listener.onResponse(parsed);
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

        // Keyed by identity rather than by equality. Two calls written in one query do differ today, because an
        // invocation's first component is its Source and the parser gives each its own - so equality would work.
        // It is identity because the alternative depends on that staying true: were a call's identity ever to stop
        // including its position, an equality-keyed map would silently collapse two leaves onto one relation
        // instance, installing the same attribute ids twice in one plan. Synchronized because the binds below run
        // concurrently.
        Map<UnresolvedTableFunctionRelation, LeafPlan> bound = Collections.synchronizedMap(new IdentityHashMap<>(leaves.size()));
        // Forked here and not on the early return above: a query naming no function never leaves the caller's
        // thread, so it pays nothing for this.
        //
        // The rewrite is inside the fork rather than in front of it. The last function to complete releases the
        // last ref, so anything run before the fork runs on that function's thread - a transport worker, for a
        // function backed by a client call - and outside the metering this listener installs. Walking the plan is
        // planning work, so it belongs on the pool planning continues on, which is what the executor names.
        ActionListener<Void> onPlanningPool = new ThreadedActionListener<>(
            executor,
            PlanningCpuTracker.inheritMeteredCpu(listener.map(ignored -> substitute(parsed, bound)))
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
                ActionListener<LeafPlan> bind = refs.acquire(relation -> bound.put(leaf, relation));
                ActionListener.run(bind, l -> function.toRelation(leaf.call(), leaf.metadataFields(), projectMetadata, l));
            }
        }
    }

    private static LogicalPlan substitute(LogicalPlan parsed, Map<UnresolvedTableFunctionRelation, LeafPlan> bound) {
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
}
