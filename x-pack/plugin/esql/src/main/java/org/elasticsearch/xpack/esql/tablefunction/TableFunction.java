/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.tablefunction;

import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.util.List;

/**
 * A function that stands where a source pattern stands: {@code FROM foo, bar*, NAME(<arguments>)}.
 * <p>
 * One instance per function, held by {@link TableFunctionRegistry}, which is built at startup and identical on
 * every node. A source list may mix table functions with index patterns and subqueries freely, so a function
 * describes one leaf of such a list and never assumes it is alone in it.
 *
 * <h2>The contract: two stages, and what may pass between them</h2>
 *
 * A table function runs in up to two stages, and which of them it uses is the main thing that distinguishes one
 * from another.
 * <p>
 * <b>The coordinator stage is mandatory.</b> {@link TableFunctionRegistry#resolve} finds the function by name,
 * {@link #signature()} establishes the shape of the call, {@link #validate} establishes whatever else must be true
 * of it, {@link #dataSourceNames} declares what the caller must be authorized for, and {@link #toRelation} binds the
 * call to the leaf that will read.
 * <p>
 * <b>A function does not resolve its own schema, and {@code DATASET} does not.</b> It validates the definition in
 * {@link #toRelation} and hands back an <i>unresolved</i> relation; the listing, the schema reconciliation and the
 * splits all happen afterwards, where they already happen for a registered dataset — {@code EsqlSession}'s
 * pre-analysis, through {@code ExternalSourceResolver}. So the answer to "how does a function return a schema" is
 * that it does not: it returns a relation, and a relation has an output. A function whose schema is known from its
 * arguments returns a resolved relation instead, with the output set.
 * <p>
 * <b>The node stage is optional, and runs wherever the leaf is executed</b> — on a data node, and on the
 * coordinator when it reads its own splits — <b>and may run more than once there</b>, since the rebind is applied
 * at the top of {@code PlannerUtils.localPlan}, again inside its {@code FragmentExec} transform, and again in
 * {@code ComputeService.runCompute}. A function that has one implements {@link BindsOnDataNode} and makes it
 * idempotent, side-effect free and free of blocking I/O: the call travels with the plan and the function derives
 * the read again, from the call and from that node's own cluster state, and from nothing else the plan carried.
 * It is also a consistency check, because the coordinator's listing, schema and splits were computed under its own
 * binding. A data node does not trust a plan's configuration: for {@code FROM <dataset>} it takes the
 * dataset's name and rebuilds the credentials from its own state, which is what stops a crafted plan choosing
 * credentials, a role or an endpoint. A table function has no registered name to play that part, so the call plays
 * it instead.
 * <p>
 * <b>None of that node stage runs yet.</b> {@link BindsOnDataNode#bindOnDataNode} has no caller:
 * {@code DatasetConfigResolver.rebind} keys on a registered dataset's name and returns an unnamed leaf unchanged,
 * {@code ExternalSourceDatasets.collect} reads only that name, and {@code ExternalSourceAuthorizer} takes its
 * empty-names shortcut for a plan whose leaves are all unnamed. So a table-function leaf today reaches a node with
 * the plan's own configuration and is neither re-authorized nor rebound there. What keeps that from being a hole is
 * {@code LocalExecutionPlanner.planExternalSource}, which refuses an unnamed external leaf outright unless this is
 * a snapshot build. Wiring the node stage and re-authorizing the data source on the node are therefore one change,
 * not two, and until it lands this paragraph describes an intention rather than the code.
 *
 * <h2>The rule that makes the two stages composable</h2>
 *
 * <b>Whatever the node stage needs must be derivable from the call.</b> The call is the only thing guaranteed to
 * travel, so a function must not compute something on the coordinator, leave it on the plan, and rely on reading it
 * back. Deriving it twice from one input is the point: two derivations of one configuration would be two things to
 * keep in step, and the second one would be the one an attacker gets to write.
 *
 * <h2>What follows from that</h2>
 *
 * <ul>
 *   <li><b>A function with no node stage does not implement {@link BindsOnDataNode}.</b> Anything built from the
 *       request itself, or generating rows from its arguments, is finished once {@link #toRelation} returns — and
 *       because the node stage is a separate interface, such a function cannot be asked a question it has no
 *       answer to.</li>
 *   <li><b>A function that reads something remote has a node stage, necessarily.</b> Credentials, an endpoint or a
 *       connection cannot ride the plan, so they must be rebuilt locally — which means whatever identifies them has
 *       to be nameable in the call and resolvable from cluster state on any node.</li>
 *   <li><b>Authorization does not travel, and re-authorizing is the facility's duty rather than a function's.</b>
 *       The coordinator's decision is made before the handler runs and is not on the plan, and a node does not
 *       trust that a plan came from a coordinator the caller was entitled to use — the compute action is
 *       security-filtered on its indices, which never mention a data source. So before the node stage runs, the
 *       names {@link #dataSourceNames} returns for the travelling call are authorized again in the caller's own
 *       context, as {@code ExternalSourceAuthorizer} does for a registered dataset. A function never authorizes
 *       anything itself and could not: {@link TableFunctionContext} carries a project and no client, listener or
 *       thread context. So <b>what a function cannot name in {@link #dataSourceNames}, this facility cannot
 *       authorize, and will not read.</b></li>
 *   <li><b>A node-stage refusal is an {@code ElasticsearchStatusException} with status 400.</b> That is what
 *       {@code DataNodeComputeHandler.isDatasetRefusalWhileBehind} recognises as "this node may simply be behind
 *       the coordinator" and waits on. {@code ResourceNotFoundException} extends {@code ElasticsearchException}
 *       rather than that, so refusing with one fails the query instead of waiting for the state to arrive.</li>
 *   <li><b>A leaf's resolution is keyed by its resource, not by its call.</b>
 *       {@code EsqlSession.extractExternalConfigs} gathers configurations into a map keyed by the resource string,
 *       so two calls naming one resource with differing configuration silently keep one of them. Until that
 *       identity changes, such a pair must agree or be refused.</li>
 *   <li><b>A table-function leaf is local to the cluster that planned it.</b> {@code ClusterComputeHandler}
 *       refuses a cross-cluster plan carrying any external leaf, so fan-out belongs to the cross-cluster planner
 *       and not to a leaf.</li>
 *   <li><b>{@link #dataSourceNames} is answered from the call text alone.</b> The facility authorizes before it
 *       binds, so a function whose reach is only known after reading cannot be authorized by this path and must
 *       enforce its own in {@code toRelation}. The facility models the one unit it has and does not pretend
 *       otherwise.</li>
 *   <li><b>The leaf decides what the function is, but not where it runs.</b> Placement follows the leaf's type —
 *       {@code Mapper.mapLeaf} sends an {@code ExternalRelation} to data nodes through a fragment and keeps other
 *       leaves on the coordinator — and splitting is the facility's, discovered from the resolved listing. An
 *       {@code ExternalRelation}'s output schema is replaced by the resolved source's, so a function that wants a
 *       schema of its own must not build one of those.</li>
 * </ul>
 */
public interface TableFunction {

    /**
     * The function's name, lowercase. {@link TableFunctionRegistry} resolves a call to it without regard to
     * case, matching how ES|QL treats every other name a user types.
     */
    String name();

    /**
     * The shape of call this function accepts — its arity, its argument names, and what it does with a trailing
     * {@code {...}}. Checked before {@link #validate}, with wording shared by every table function, so no function
     * writes arity or payload code of its own.
     */
    TableFunctionSignature signature();

    /**
     * Checks whatever is true of this function beyond its {@link #signature()} — for {@code DATASET}, that the data
     * source is one exact name rather than a pattern. The shape already holds by the time this runs, so it can read
     * the arguments it declared without re-counting them.
     * <p>
     * Raises {@link org.elasticsearch.xpack.esql.parser.ParsingException} against the source of the
     * offending part of the call rather than the call as a whole, so the error points at what the user
     * has to change.
     * <p>
     * Empty by default: a function whose shape is its whole contract needs nothing here.
     */
    default void validate(TableFunctionInvocation call) {}

    /**
     * The data sources this call reads, which the coordinator authorizes the caller for before any of it is bound,
     * and which also decide whether the federation gates apply at all. Exact names only — {@link #validate} is what
     * guarantees that, so a pattern never reaches here.
     * <p>
     * Separate from {@link #toRelation} because authorization precedes binding and is batched across every call in
     * the source list: one round-trip for the query, not one per leaf.
     * <p>
     * Empty by default, which is the honest answer for a function that reads no data source — a generator, or
     * something built from the request itself. Such a function is bound with no round-trip, and the federation
     * availability and licence checks do not apply to it, because they are properties of reading a data source
     * rather than of being a table function. A function authorized by some other unit entirely enforces that
     * itself in {@link #toRelation}; this facility models the one unit it has, and does not pretend to model the
     * rest.
     */
    default List<String> dataSourceNames(TableFunctionInvocation call) {
        return List.of();
    }

    /**
     * Binds the call to the leaf that reads it, against {@code context}'s project. Called only after the caller has
     * been authorized for every name {@link #dataSourceNames} returned.
     * <p>
     * Diagnostics propagate as the registration API raises them — a missing data source as a 404, a definition the
     * validator refuses as a 400 — so one definition draws the same error whichever route it arrives by. That costs
     * the position within the query text, which the registration API has no notion of.
     */
    LogicalPlan toRelation(TableFunctionInvocation call, List<NamedExpression> metadataFields, TableFunctionContext context);

}
