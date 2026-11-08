/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.tablefunction;

import org.elasticsearch.xpack.esql.core.tree.Source;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * One table-function call exactly as written in a source list. Its name is not yet known to denote anything:
 * the parser never consults {@link TableFunctionRegistry}, so resolution happens later.
 * <p>
 * Named an invocation rather than a call because {@code *FunctionCall} already means a plan node in this
 * tree — {@code PromqlFunctionCall} and {@code HistogramFunctionCall} both extend {@code UnaryPlan} — and
 * this is not one and never will be.
 * <p>
 * One flat argument list, in which a trailing map is the last argument rather than a slot beside the arguments.
 * Why it is shaped that way is on {@link TableFunctionArgument}.
 * <p>
 * A carrier, with no validation of its own: what shape of call a function accepts is
 * {@link TableFunction#signature()}'s question and what else is true of it is {@link TableFunction#validate}'s, and
 * each raises its own diagnostic against the {@link #source()} of whatever was wrong.
 *
 * @param source the whole call, {@code NAME(...)} included, for a diagnostic about the call itself
 * @param name the function name as the user wrote it, for diagnostics; {@link TableFunctionRegistry} resolves
 *             case-insensitively, so this is not a key
 * @param arguments the arguments, in the order written; empty when none were
 */
public record TableFunctionInvocation(Source source, String name, List<TableFunctionArgument> arguments) {

    public TableFunctionInvocation {
        Objects.requireNonNull(source, "source");
        Objects.requireNonNull(name, "name");
        arguments = List.copyOf(arguments);
    }

    /** The {@code i}th argument as a name. Safe once {@link TableFunction#signature()} has checked the call. */
    public String name(int i) {
        return ((TableFunctionArgument.Name) arguments.get(i)).value();
    }

    /** The {@code i}th argument as named values, or {@code null} when the call stopped before it. */
    public Map<String, Object> namedValues(int i) {
        return i < arguments.size() ? ((TableFunctionArgument.NamedValues) arguments.get(i)).values() : null;
    }
}
