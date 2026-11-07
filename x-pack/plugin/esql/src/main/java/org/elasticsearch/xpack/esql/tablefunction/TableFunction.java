/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.tablefunction;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.plan.logical.LeafPlan;

import java.util.List;

/**
 * A function that stands where an index pattern stands: {@code FROM logs-*, NAME(<arguments>)}.
 *
 * <h2>What a table function is</h2>
 *
 * <b>A function from an invocation to a relation.</b> The input is a {@link TableFunctionInvocation} — a name and
 * an ordered argument list, which is everything the user wrote and nothing else. The output is a plan leaf that
 * denotes rows. Then the function is done: resolution, optimization, placement, splitting and execution all happen
 * to that leaf, and no stage after this one has a table-function branch in it.
 * <p>
 * One instance per function, held by {@link TableFunctionRegistry}, built at startup and identical on every node. A
 * source list may mix functions with index patterns and subqueries freely, so a function describes one leaf of such
 * a list and never assumes it is alone in it.
 *
 * <h2>A function does not return a schema</h2>
 *
 * It returns a relation, and a relation has an output — so the question never arises separately. A function whose
 * columns follow from its arguments returns a resolved leaf with its output set; a function whose columns are only
 * knowable by looking returns an <i>unresolved</i> leaf and the ordinary analyzer resolves it, exactly as
 * {@code FROM logs-*} is resolved. Nothing here is a second schema mechanism.
 *
 * <h2>What a function cannot do, and why</h2>
 *
 * <ul>
 *   <li><b>It cannot authorize anything, and must not try.</b> {@link TableFunctionContext} hands it a project and
 *       no client, listener or thread context, so it could not dispatch a check if it wanted to. A function that
 *       reads something needing a privilege declares that by implementing the corresponding marker, and the
 *       facility performs the check — before {@link #toRelation} is ever called. What a function cannot declare,
 *       this facility cannot authorize, and will not read.</li>
 *   <li><b>It cannot choose where it runs.</b> Placement follows the leaf it returned: {@code Mapper#mapLeaf}
 *       sends some leaf types to data nodes through a fragment and keeps others on the coordinator. A function
 *       picks what it denotes, not where that is read.</li>
 *   <li><b>It cannot carry state from here to a data node.</b> Only the leaf travels, so anything a node needs has
 *       to be derivable from what the leaf carries. Two derivations of one configuration would be two things to
 *       keep in step, and the second is the one an attacker gets to write.</li>
 *   <li><b>It cannot know how it was written.</b> Nothing downstream may branch on whether a leaf came from a
 *       function call, which is what keeps it possible for an ordinary index pattern to become one of these later
 *       without the facility changing.</li>
 * </ul>
 *
 * <h2>Writing one</h2>
 *
 * A name, a signature, and the leaf. Most functions should extend a base class rather than implement this
 * directly — {@link CoordinatorRowsFunction} for a function whose rows the coordinator produces — because the
 * bases own the parts that must not vary between functions.
 */
public interface TableFunction {

    /**
     * The function's name, lowercase. {@link TableFunctionRegistry} resolves a call to it without regard to case,
     * matching how ES|QL treats every other name a user types.
     */
    String name();

    /**
     * The shape of invocation this function accepts: its arity, and the kind of each argument. Checked before
     * {@link #validate} and before anything is authorized, with wording shared by every table function, so no
     * function writes arity code of its own and two functions cannot word the same complaint differently.
     * <p>
     * Because the shape is known to hold by the time a function reads its arguments, the accessors on
     * {@link TableFunctionInvocation} may cast without testing.
     */
    TableFunctionSignature signature();

    /**
     * Whatever must be true of an invocation beyond its {@link #signature()}.
     * <p>
     * Raises {@link org.elasticsearch.xpack.esql.parser.ParsingException} against the source of the offending
     * argument rather than of the whole call, so the error points at what the user has to change.
     * <p>
     * Empty by default: a function whose shape is its whole contract needs nothing here.
     */
    default void validate(TableFunctionInvocation call) {}

    /**
     * Completes {@code listener} with the {@link LeafPlan} this invocation denotes, bound against {@code context}'s project.
     * <p>
     * Asynchronous because some functions cannot answer without I/O — a listing, a schema, a round-trip — and the
     * resolver this runs inside is already asynchronous. A function needing none completes inline, and the base
     * classes do that for it, so the simple case never handles a listener.
     * <p>
     * Called only after the shape has been checked, {@link #validate} has passed, and the facility has performed
     * whatever authorization this function declared.
     *
     * @param metadataFields the names from the user's {@code METADATA} clause, threaded to every leaf in the source
     *                       list. A leaf that binds its own metadata consumes them; one that does not may ignore
     *                       them, because the analyzer binds any name absent from a leaf's output to NULL.
     */
    void toRelation(
        TableFunctionInvocation call,
        List<NamedExpression> metadataFields,
        TableFunctionContext context,
        ActionListener<LeafPlan> listener
    );
}
