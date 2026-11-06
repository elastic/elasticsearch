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
 * A function that stands where a source pattern stands: {@code FROM foo, bar*, NAME(<arguments>, {<payload>})}.
 * <p>
 * One instance per function, held by {@link TableFunctionRegistry}. A source list may mix table functions
 * with index patterns and subqueries freely, so a function describes one leaf of such a list and never
 * assumes it is alone in it.
 */
public interface TableFunction {

    /**
     * The function's name, lowercase. {@link TableFunctionRegistry} resolves a call to it without regard to
     * case, matching how ES|QL treats every other name a user types.
     */
    String name();

    /**
     * Checks that this call is one the function could serve — how many arguments it takes, whether it
     * requires a payload, what a payload may contain. A separate question from building the relation,
     * because the shape of a call is only meaningful against the function that defines it, while the
     * caller decides when and whether a leaf gets built.
     * <p>
     * Raises {@link org.elasticsearch.xpack.esql.parser.ParsingException} against the source of the
     * offending part of the call rather than the call as a whole, so the error points at what the user
     * has to change.
     */
    void validate(TableFunctionCall call);

    /**
     * The data sources this call reads, which the coordinator authorizes the caller for before any of it is bound.
     * Exact names only — {@link #validate} is what guarantees that, so a pattern never reaches here.
     * <p>
     * Separate from {@link #toRelation} because authorization precedes binding and is batched across every call in
     * the source list: one round-trip for the query, not one per leaf.
     */
    List<String> dataSourceNames(TableFunctionCall call);

    /**
     * Binds the call to the leaf that reads it, against {@code context}'s project. Called only after the caller has
     * been authorized for every name {@link #dataSourceNames} returned.
     * <p>
     * Diagnostics propagate as the registration API raises them — a missing data source as a 404, a definition the
     * validator refuses as a 400 — so one definition draws the same error whichever route it arrives by. That costs
     * the position within the query text, which the registration API has no notion of.
     */
    LogicalPlan toRelation(TableFunctionCall call, List<NamedExpression> metadataFields, TableFunctionContext context);
}
