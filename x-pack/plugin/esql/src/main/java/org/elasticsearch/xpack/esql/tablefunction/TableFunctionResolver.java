/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.tablefunction;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.RefCountingListener;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.xpack.esql.plan.logical.LeafPlan;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedTableFunctionRelation;

import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

/**
 * Turns every {@link UnresolvedTableFunctionRelation} the parser left in a plan into the leaf that reads.
 * <p>
 * The whole of what is common to every table function lives here, and it is four things: resolve the name, check
 * the shape, run the function's own validation, and bind. Everything else a particular function needs — a
 * privilege, a round-trip, a collaborator — belongs to that function or to the base class it extends, not here.
 * That boundary is the point of the facility, and it is easy to lose: the first function to need something is
 * always the one that looks like it should be served by the shared path.
 * <p>
 * Resolution of the <i>name</i> happens before any of it, so a misspelled function is answered by its own
 * diagnostic at its own position rather than by whatever the next stage would have made of it.
 */
public class TableFunctionResolver {

    private final TableFunctionRegistry registry;

    public TableFunctionResolver(TableFunctionRegistry registry) {
        this.registry = registry;
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

        // Keyed by identity, and it has to be: UnresolvedTableFunctionRelation's equals excludes its own Source, so
        // two leaves in one plan can compare equal, and an equality-keyed map would collapse them onto a single
        // relation instance - the same attribute NameIds installed in two places. Synchronized because the binds
        // below run concurrently.
        Map<UnresolvedTableFunctionRelation, LeafPlan> bound = Collections.synchronizedMap(new IdentityHashMap<>(leaves.size()));
        TableFunctionContext context = new TableFunctionContext(projectMetadata);
        try (
            var refs = new RefCountingListener(
                listener.delegateFailureAndWrap((delegate, ignored) -> delegate.onResponse(substitute(parsed, bound)))
            )
        ) {
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
                ActionListener.run(bind, l -> function.toRelation(leaf.call(), leaf.metadataFields(), context, l));
            }
        }
    }

    private static LogicalPlan substitute(LogicalPlan parsed, Map<UnresolvedTableFunctionRelation, LeafPlan> bound) {
        return parsed.transformDown(UnresolvedTableFunctionRelation.class, leaf -> {
            LeafPlan relation = bound.get(leaf);
            assert relation != null : "a leaf reached the substitution that the binding walk did not see";
            // A function returning another unresolved call would never be rewritten again, and the verifier would
            // report the user's own syntax back at them as unresolved. Nothing in the type system prevents it.
            assert relation instanceof UnresolvedTableFunctionRelation == false
                : "[" + leaf.call().name() + "] returned another table function call";
            return relation;
        });
    }
}
