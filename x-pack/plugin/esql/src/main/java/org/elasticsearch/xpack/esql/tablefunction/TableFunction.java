/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.tablefunction;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.plan.logical.LeafPlan;

import java.util.List;

/**
 * A function that stands where an index pattern stands: {@code FROM logs-*, NAME(<arguments>)}.
 * <p>
 * It is a function from an invocation to a relation. The input is a {@link TableFunctionInvocation} — a name and
 * an ordered argument list, which is everything the user wrote and nothing else. The output is a plan leaf that
 * denotes rows, and then the function is done: no stage after this one has a table-function branch in it.
 * <p>
 * It does not return a schema, because a relation has an output. A function whose columns follow from its
 * arguments returns a resolved leaf; a function whose columns are only knowable by looking returns an
 * <i>unresolved</i> leaf, and the ordinary analyzer resolves it exactly as it resolves {@code FROM logs-*}.
 * <p>
 * Three things a function cannot do, each of which something else owns. It cannot authorize: nothing here checks
 * a privilege, so a function reaching something access-controlled does that itself, and dispatching the action
 * its REST equivalent dispatches is how to do it without inventing a rule. It cannot choose where it runs, since
 * placement follows the leaf it returned. And it cannot know how it was written — nothing downstream may branch
 * on whether a leaf came from a call, which is what would let an index pattern become one of these later.
 * <p>
 * One instance per function, held by {@link TableFunctionRegistry} and identical on every node. A source list may
 * mix functions with patterns and subqueries, so a function describes one leaf and never assumes it is alone.
 */
public interface TableFunction {

    /**
     * The function's name, lowercase. {@link TableFunctionRegistry} resolves a call to it without regard to case,
     * matching how ES|QL treats every other name a user types.
     */
    String name();

    /**
     * The arity and argument kinds this function accepts, checked before {@link #validate} with wording shared by
     * every table function. Because it is known to hold by the time a function reads its arguments, the accessors
     * on {@link TableFunctionInvocation} may cast without testing.
     */
    TableFunctionSignature signature();

    /**
     * Whatever must be true of an invocation beyond its {@link #signature()}. Raises
     * {@link org.elasticsearch.xpack.esql.parser.ParsingException} against the offending argument's source rather
     * than the whole call's, so the error points at what the user has to change.
     */
    default void validate(TableFunctionInvocation call) {}

    /**
     * Completes {@code listener} with the {@link LeafPlan} this invocation denotes.
     * <p>
     * Asynchronous because some functions cannot answer without I/O; one needing none completes inline. Called
     * after the shape has been checked and {@link #validate} has passed for every call in the query, not just
     * this one.
     *
     * @param metadataFields the names from the user's {@code METADATA} clause, threaded to every leaf in the source
     *                       list. A leaf that binds its own metadata consumes them; one that does not may ignore
     *                       them, because the analyzer binds any name absent from a leaf's output to NULL.
     */
    void toRelation(
        TableFunctionInvocation call,
        List<NamedExpression> metadataFields,
        ProjectMetadata projectMetadata,
        ActionListener<LeafPlan> listener
    );
}
