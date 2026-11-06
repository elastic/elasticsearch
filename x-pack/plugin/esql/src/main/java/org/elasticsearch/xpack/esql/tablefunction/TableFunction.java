/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.tablefunction;

import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

/**
 * A function that stands where a source pattern stands and yields a relation:
 * {@code FROM foo, bar*, NAME(<arguments>, {<payload>})}.
 * <p>
 * One instance per function, held by {@link TableFunctionRegistry}. A source list may mix table
 * functions with index patterns and subqueries freely, so an implementation yields a leaf that the
 * analyzer can resolve beside the others rather than anything that assumes it is alone.
 */
public interface TableFunction {

    /**
     * The function's name, lowercase. {@link TableFunctionRegistry} resolves a call to it without
     * regard to case, matching how ES|QL treats every other name a user types.
     */
    String name();

    /**
     * The unresolved relation this call stands for.
     * <p>
     * Also where the call's shape is checked — how many arguments it takes, whether it requires a
     * payload, what its payload may contain — because the shape is only meaningful against the function
     * that defines it. Raises {@link org.elasticsearch.xpack.esql.parser.ParsingException} against the
     * source of the offending part of the call, not against the call as a whole, so the error points at
     * what the user has to change.
     */
    LogicalPlan toRelation(TableFunctionCall call);
}
