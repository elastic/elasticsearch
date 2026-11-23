/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.sourcefunction;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.plan.logical.LeafPlan;

import java.util.List;

/**
 * A function that stands where an index pattern stands: {@code FROM logs-*, NAME(<arguments>)}.
 * <p>
 * It is a function from an invocation to a relation. The input is a {@link SourceFunctionInvocation} — a name and
 * an ordered argument list, which is everything the user wrote and nothing else. The output is a plan leaf that
 * denotes rows, and then the function is done: no stage after this one has a source-function branch in it.
 * <p>
 * It does not return a schema, because a relation has an output. A function whose columns follow from its
 * arguments returns a resolved leaf; a function whose columns are only knowable by looking returns an
 * <i>unresolved</i> leaf, which the ordinary analyzer then resolves. Not every stage, though: resolution runs
 * immediately after view expansion and before dataset rewriting, so a leaf a function returns reaches the
 * dataset rewrite, view scoping, pre-analysis and field resolution, but not view expansion, which has already
 * run by then. A function wanting a view expanded returns that relation itself, and one naming a view is
 * refused rather than read as an index that does not exist.
 * <p>
 * Three things a function cannot do, each of which something else owns. It cannot authorize: nothing here checks
 * a privilege, so a function reaching something access-controlled does that itself, and dispatching the action
 * its REST equivalent dispatches is how to do it without inventing a rule. It cannot choose where it runs, since
 * placement follows the leaf it returned. And it cannot know how it was written — nothing downstream may branch
 * on whether a leaf came from a call, which is what would let an index pattern become one of these later.
 * <p>
 * One instance per function, held by {@link SourceFunctionRegistry} and identical on every node. A source list may
 * mix functions with patterns and subqueries, so a function describes one leaf and never assumes it is alone.
 */
public interface SourceFunction {

    /**
     * The function's name, lowercase. {@link SourceFunctionRegistry} resolves a call to it without regard to case,
     * matching how ES|QL treats every other name a user types.
     */
    String name();

    /**
     * The arity and argument kinds this function accepts, checked before {@link #validate} with wording shared by
     * every source function. Because it is known to hold by the time a function reads its arguments, the accessors
     * on {@link SourceFunctionInvocation} may cast without testing.
     */
    SourceFunctionSignature signature();

    /**
     * Whatever must be true of an invocation beyond its {@link #signature()}. Raises
     * {@link org.elasticsearch.xpack.esql.parser.ParsingException} against the offending argument's source rather
     * than the whole call's, so the error points at what the user has to change.
     */
    default void validate(SourceFunctionInvocation call) {}

    /**
     * Completes {@code listener} with the {@link LeafPlan} this invocation denotes.
     * <p>
     * Asynchronous because some functions cannot answer without I/O; one needing none completes inline. Called
     * after the shape has been checked and {@link #validate} has passed for every call in the query, not just
     * this one.
     * <p>
     * Asked once for every place a call appears, which is not the same as once per call the user wrote: a plan
     * can reach one leaf from several parents, and each of those places is asked on its own. {@code FORK} is the
     * common way to get there — every branch is handed the one plan above it — but not the only one:
     * {@code LetResolver} splices a bound plan in <i>by reference</i>, so a {@code LET} naming a call and
     * referenced twice reaches this the same way.
     * <p>
     * Asking per place is what the rest of the pipeline already does with a fork, where every branch is planned
     * into its own fragment and an index above one is scanned per branch. So a function that does I/O pays a
     * round trip per place, and what comes back is installed as it came rather than shared between places.
     * <p>
     * Which makes the first line of this javadoc a requirement and not a description: a function <i>of</i> an
     * invocation. Two branches over one written call are answered separately, so a function reading something
     * that changes underneath it may hand them different rows — no more and no less than two branches over an
     * index may see different documents. A function whose answer has to be stable across one query owns making
     * it so.
     * <p>
     * And every place has to come back with its own relation. A function whose rows follow from its arguments is
     * the one tempted to answer twice from a cached instance, and a relation the engine does not complete cannot
     * stand in two places: one page is released by whichever consumer reads it first, and one set of attribute
     * ids would sit under two parents. Both are checked rather than taken on trust, and a new wrapper around a held
     * page or a held column list is refused exactly as returning the same relation twice is. That is refused,
     * naming the function, and it is refused for a relation the
     * engine completes as well: what runs downstream keys a leaf by identity and pairs it with a scope that
     * varies per place, so one object at two places is resolved once and the other place inherits that answer.
     * <p>
     * One requirement comes back onto a function that materializes its rows, and it is not a promise: build the
     * page on {@code PlannerUtils.NON_BREAKING_BLOCK_FACTORY}, as the facility's own test functions do. The page
     * is built while the query is still being planned rather than by the operator that will read it, so an
     * accounted one can outlive anything that would release its circuit-breaker reservation. Bounding a materialized
     * relation properly is a follow-up; until it lands, this is a constraint a function has to meet.
     *
     * @param metadataFields the names from the user's {@code METADATA} clause, threaded to every leaf in the source
     *                       list. A leaf that binds its own metadata consumes them; one that does not may ignore
     *                       them, because the analyzer binds any name absent from a leaf's output to NULL.
     * @param projectMetadata the cluster state a function may need to look something up in, or {@code null} where
     *                       the caller has none to offer, which is every test that drives the facility on its own
     */
    void toRelation(
        SourceFunctionInvocation call,
        List<NamedExpression> metadataFields,
        ProjectMetadata projectMetadata,
        ActionListener<LeafPlan> listener
    );
}
