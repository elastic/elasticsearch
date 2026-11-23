/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.sourcefunction;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.RefCountingListener;
import org.elasticsearch.action.support.ThreadedActionListener;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.metadata.ViewMetadata;
import org.elasticsearch.xpack.esql.action.PlanningCpuTracker;
import org.elasticsearch.xpack.esql.core.capabilities.Unresolvable;
import org.elasticsearch.xpack.esql.parser.ParsingException;
import org.elasticsearch.xpack.esql.plan.logical.LeafPlan;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedSourceFunctionRelation;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;

/**
 * Turns every {@link UnresolvedSourceFunctionRelation} the parser left in a plan into the leaf that reads.
 * <p>
 * The whole of what is common to every source function lives here. Four steps per call — resolve the name, check
 * the shape, run the function's own validation, bind — and two things the facility owns on every query's behalf:
 * the ceiling on how many calls one query may make, and refusing a returned relation that names a view. Everything
 * else a particular function needs — a privilege, a round-trip, a collaborator — belongs to that function, not
 * here. Nothing here reads a function's arguments, holds a collaborator, or itself checks a privilege.
 * <p>
 * What it never does is change what a function handed back. A returned leaf is installed as it came, so nothing
 * here needs to know which kind of relation it is: this checks what it was given and stops there. The checks are
 * the facility's, the relation is the function's.
 * <p>
 * Resolution of the <i>name</i> happens before any of it, so a misspelled function is answered by its own
 * diagnostic at its own position rather than by whatever the next stage would have made of it.
 */
public class SourceFunctionResolver {

    /**
     * The most calls one query may make, counted over the whole plan once views have been expanded — and counted
     * per place a call appears rather than per call written, because a call under a {@code FORK} is asked once per
     * branch and that is what the cluster is being asked to serve.
     * <p>
     * Checked here because nothing later checks it in time: the configurable {@code max_branch_count} is applied
     * to the <i>optimized</i> plan, long after every call in this query has been made, and the parser's own width
     * check only covers the {@code Fork} node. Calls go out together rather than in turn, so a wide source list
     * spends one query string on that many concurrent requests.
     * <p>
     * Set to {@link QueryPragmas#MAX_PARSED_BRANCH_COUNT} for its value and not for its reach: it is the number
     * already chosen for how wide a parsed plan may get, so there is one here rather than a second to drift from it.
     */
    static final int MAX_CALLS_PER_QUERY = QueryPragmas.MAX_PARSED_BRANCH_COUNT;

    private final SourceFunctionRegistry registry;
    private final Executor executor;

    /**
     * @param executor the pool planning continues on. A function may complete its listener from anywhere - a
     *                 transport response lands on a transport worker, a field-caps handler on
     *                 {@code search_coordination} - and the caller resumes planning on {@code search} and asserts
     *                 it. Making that the facility's business rather than each function's is the difference
     *                 between a contract and a thing every function has to remember; the sibling
     *                 {@code DatasetResolver} takes the same executor for the same reason.
     */
    public SourceFunctionResolver(SourceFunctionRegistry registry, Executor executor) {
        this.registry = registry;
        this.executor = executor;
    }

    /**
     * Completes {@code listener} with the plan whose source functions have become reading leaves, or with the plan
     * untouched when it has none — which costs one walk and nothing else, so an ordinary query pays almost nothing.
     */
    public void resolveSourceFunctions(LogicalPlan parsed, ProjectMetadata projectMetadata, ActionListener<LogicalPlan> listener) {
        List<UnresolvedSourceFunctionRelation> leaves = new ArrayList<>();
        // Every place a call appears, not every call a user wrote. A plan reaches one leaf from more than one
        // parent two ways. FORK hands every branch the one plan above it -- LogicalPlanBuilder#visitForkCommand
        // applies each branch's factory to the same input -- so FROM f("x") | FORK (..) (..) walks down to one
        // leaf once per branch. And LetResolver splices a bound plan in by reference rather than by copy, so a
        // LET naming a call and referenced twice arrives the same way. Each of those places is asked on its own.
        //
        // Which is what the rest of the pipeline already does with a fork: Mapper#mapMergePlan maps every branch
        // into its own fragment, so an EVAL above one runs per branch too, and two branches over an index scan it
        // separately. Asking per place rather than once is also what lets a returned relation be installed
        // untouched -- nothing is shared, so nothing has to be made safe to share, and the facility needs no
        // opinion about what a function handed back.
        parsed.forEachDown(node -> {
            if (node instanceof UnresolvedSourceFunctionRelation leaf) {
                leaves.add(leaf);
            }
        });
        if (leaves.isEmpty()) {
            listener.onResponse(parsed);
            return;
        }
        if (leaves.size() > MAX_CALLS_PER_QUERY) {
            // Before the shape checks below, and before anything is asked: a query this wide has to lose calls
            // whatever is wrong with the ones it has. The position is one of the calls past the ceiling rather
            // than a particular one -- leaves are collected in plan order, and a source list is assembled as
            // [patterns, then the subqueries and calls in written order], so the leaf at this index is not
            // reliably the one a reader would count as the first too many. It is a place in the query to start
            // cutting, not the place.
            listener.onFailure(
                new ParsingException(
                    leaves.get(MAX_CALLS_PER_QUERY).call().source(),
                    "a query may call up to [{}] source functions, found [{}]",
                    MAX_CALLS_PER_QUERY,
                    leaves.size()
                )
            );
            return;
        }

        // Every shape question is settled before any function binds, so a malformed call in a source list is
        // reported as what it is rather than as a failure of whatever the first well-formed call was doing.
        List<SourceFunction> functions = new ArrayList<>(leaves.size());
        try {
            for (UnresolvedSourceFunctionRelation leaf : leaves) {
                SourceFunctionInvocation call = leaf.call();
                SourceFunction function = registry.resolve(call.source(), call.name());
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
            PlanningCpuTracker.inheritMeteredCpu(listener.map(ignored -> substitute(parsed, leaves, functions, relations, projectMetadata)))
        );
        try (var refs = new RefCountingListener(onPlanningPool)) {
            for (int i = 0; i < leaves.size(); i++) {
                UnresolvedSourceFunctionRelation leaf = leaves.get(i);
                SourceFunction function = functions.get(i);
                // Concurrently, not in sequence: a source list naming several functions that each need I/O should
                // cost one round-trip's latency rather than one per leaf.
                //
                // ActionListener.run, not a bare call: acquire() takes the ref before toRelation runs, so a function
                // that throws instead of completing would leave the count above zero for good and this method would
                // never answer its own listener. A function is not required to be no-throw, so the facility carries
                // the cost of one that is not.
                //
                // notifyOnce around the ref, because throwing is not the only way to be badly behaved: a function
                // that completes its listener and THEN throws would otherwise have its ref released twice -- once
                // by the completion, once by ActionListener.run's failure path -- and RefCountingRunnable's
                // once-only guard is an assertion, so in production the second release drops the count early,
                // answers this method's listener before the other leaves are bound, and makes the next acquire()
                // throw past a listener that has already been completed.
                int slot = i;
                ActionListener<LeafPlan> bind = ActionListener.notifyOnce(refs.acquire(relation -> relations[slot] = relation));
                ActionListener.run(bind, l -> function.toRelation(leaf.call(), leaf.metadataFields(), projectMetadata, l));
            }
        }
    }

    private static LogicalPlan substitute(
        LogicalPlan parsed,
        List<UnresolvedSourceFunctionRelation> leaves,
        List<SourceFunction> functions,
        LeafPlan[] relations,
        ProjectMetadata projectMetadata
    ) {
        // Keyed by identity rather than by equality, and the difference is not hypothetical in the way it looks.
        // Two calls the user WRITES do differ, because an invocation's first component is its Source and the
        // parser gives each its own. A call that arrives through a view body does not inherit that guarantee: a
        // body is parsed per reference and every Source in it is tagged with the view's name rather than with the
        // reference's position, so two such calls can compare equal. What spares us today is one level up -- a
        // repeated source name is one source, so FROM v, v expands one body and produces one leaf, which
        // SourceFunctionFacilityIT.testOneViewNamedTwiceReadsItsRowsOnce pins. Identity costs nothing and does
        // not depend on that staying true; an equality-keyed map would silently collapse two leaves onto one
        // relation instance, installing the same attribute ids twice in one plan. Built here rather than filled
        // by the binds: every write to it is on this thread, so it is a plain map.
        for (int i = 0; i < leaves.size(); i++) {
            // First, because everything after it dereferences the relation. A function that completes with
            // nothing is an author's mistake, and one worth naming here rather than as a null somewhere later.
            if (relations[i] == null) {
                throw new IllegalStateException("[" + functions.get(i).name() + "] bound no relation for a call in this query");
            }
            if (relations[i] instanceof UnresolvedSourceFunctionRelation) {
                throw new IllegalStateException("[" + leaves.get(i).call().name() + "] returned another source function call");
            }
            checkNoViewIsNamed(functions.get(i), relations[i], projectMetadata);
        }
        // A function is asked once per place a call appears, so each of those places has to come back with its own
        // relation. Nothing in the interface stops one answering twice from a cached instance -- the obvious thing
        // to write for a function whose rows follow from its arguments -- and a relation the engine does not
        // complete cannot stand in two places: its page is released by whichever consumer reads it first, and its
        // attributes are shared too, so one plan would carry the same ids under two parents.
        //
        // Refused rather than repaired, and refused rather than worked around here. Copying the page would be the
        // easy half; the ids would have to be freshened as well, and what a function's attributes mean is the
        // function's own business. A relation the engine completes is exempt, because each occurrence of one
        // becomes its own relation downstream, the way a written-out index pattern does.
        Map<LeafPlan, UnresolvedSourceFunctionRelation> installed = new IdentityHashMap<>(leaves.size());
        for (int i = 0; i < leaves.size(); i++) {
            if (relations[i] instanceof Unresolvable == false && installed.put(relations[i], leaves.get(i)) != null) {
                throw new IllegalStateException(
                    "source function ["
                        + functions.get(i).name()
                        + "] answered more than one place with one relation, which cannot stand in both. A call under"
                        + " several FORK branches or LET references is asked once per place, and each place needs its own"
                );
            }
        }
        // One queue per leaf OBJECT, because that object stands at every place a FORK reached it and the walk
        // below is handed it once per place. Which of its relations lands at which place does not matter -- they
        // are answers to the one call -- so a queue is enough, and nothing depends on the two walks visiting in
        // the same order.
        Map<UnresolvedSourceFunctionRelation, Deque<LeafPlan>> bound = new IdentityHashMap<>(leaves.size());
        for (int i = 0; i < leaves.size(); i++) {
            bound.computeIfAbsent(leaves.get(i), ignored -> new ArrayDeque<>()).add(relations[i]);
        }
        return parsed.transformDown(UnresolvedSourceFunctionRelation.class, leaf -> {
            Deque<LeafPlan> answers = bound.get(leaf);
            LeafPlan relation = answers == null ? null : answers.poll();
            // Thrown rather than asserted: assertions are off in production, where a null would surface later as
            // an NPE naming no function, or as the user's own syntax being reported unresolved. Worded apart from
            // the function-completed-with-null case above, which reads the same from a stack trace and is not the
            // same fault: this one is a leaf the substitution walk reached that the collecting walk did not, so
            // the two walks disagree about the plan rather than a function having answered badly.
            if (relation == null) {
                throw new IllegalStateException("[" + leaf.call().name() + "] reached a call that was never collected");
            }
            return relation;
        });
    }

    /**
     * Refuses a relation that names a view, which is the one thing this resolver's placement cannot serve.
     * <p>
     * A view body is parsed and spliced into the plan before this runs, and that order is deliberate: it is what
     * lets a view body name a source function at all. The cost is here. By the time a function hands a pattern
     * back, the pass that expands views has finished, so a view named in that pattern would never be expanded and
     * the name would be resolved as an index -- leaving the user an unknown-index error about a view they can list.
     * Joining the expansion traversal instead would make it work and is declined on cost and on governance: a view
     * body belongs to a named view, and the settings, identity, privileges and telemetry keyed on that name are
     * things a function's output cannot supply. So this is refused rather than made to work, and it is refused
     * where a reader can see which function did it.
     * <p>
     * This expires. Views are to become plan nodes rather than spliced text, and a node can carry the identity and
     * the authorization context a spliced body cannot, at which point the case stops being special.
     * <p>
     * Only an exact name is refused. A wildcard matches views only when the query asked it to, and a function's
     * pattern is not where that was asked -- so a pattern that merely could have matched one is left alone rather
     * than refused on a setting its author never saw.
     */
    private static void checkNoViewIsNamed(SourceFunction function, LeafPlan relation, ProjectMetadata projectMetadata) {
        // Null where a caller has no cluster state to offer, which is every test driving the facility on its own:
        // with no metadata there are no views, and a check over nothing is not the same as a check that passed.
        if (projectMetadata == null) {
            return;
        }
        if (relation instanceof UnresolvedRelation unresolved) {
            ViewMetadata views = projectMetadata.custom(ViewMetadata.TYPE, ViewMetadata.EMPTY);
            if (views.views().isEmpty()) {
                return;
            }
            // Split because one pattern can be a comma-separated list, which is how a function returning several
            // names returns them -- a list with a view in it is as unserveable as a view on its own.
            for (String named : unresolved.indexPattern().indexPattern().split(",")) {
                String name = named.trim();
                if (views.getView(name) != null) {
                    // A ParsingException rather than an IllegalStateException, because a user reaches this: the
                    // shape this facility recommends for an ENGINE function is to hand its argument back as the
                    // pattern, and every test function here does exactly that, so naming a view in the query is
                    // enough. Raised against the relation's own source, which for such a function is the call.
                    throw new ParsingException(
                        relation.source(),
                        "source function [{}] cannot read view [{}]: nothing after this point expands a view, so a"
                            + " function returns the relation it wants read rather than a view standing for one",
                        function.name(),
                        name
                    );
                }
            }
        }
    }
}
