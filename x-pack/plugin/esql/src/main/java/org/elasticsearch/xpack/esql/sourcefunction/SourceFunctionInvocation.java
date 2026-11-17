/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.sourcefunction;

import org.elasticsearch.xpack.esql.core.tree.Source;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * One source-function call exactly as written in a source list. Its name is not yet known to denote anything:
 * the parser never consults {@link SourceFunctionRegistry}, so resolution happens later.
 * <p>
 * Named an invocation rather than a call because {@code *FunctionCall} already means a plan node in this
 * tree — {@code PromqlFunctionCall} and {@code HistogramFunctionCall} both extend {@code UnaryPlan} — and
 * this is not one and never will be.
 * <p>
 * One flat argument list, in which a trailing map is the last argument rather than a slot beside the arguments.
 * Why it is shaped that way is on {@link SourceFunctionArgument}.
 * <p>
 * A carrier, with no validation of its own: what shape of call a function accepts is
 * {@link SourceFunction#signature()}'s question and what else is true of it is {@link SourceFunction#validate}'s, and
 * each raises its own diagnostic against the {@link #source()} of whatever was wrong.
 *
 * @param source the whole call, {@code NAME(...)} included, for a diagnostic about the call itself
 * @param name the function name as the user wrote it, for diagnostics; {@link SourceFunctionRegistry} resolves
 *             case-insensitively, so this is not a key
 * @param arguments the arguments, in the order written; empty when none were
 */
public record SourceFunctionInvocation(Source source, String name, List<SourceFunctionArgument> arguments) {

    public SourceFunctionInvocation {
        Objects.requireNonNull(source, "source");
        Objects.requireNonNull(name, "name");
        arguments = List.copyOf(arguments);
    }

    /**
     * The {@code i}th argument as a string, or {@code null} when the call had no argument there. Safe once
     * {@link SourceFunction#signature()} has checked the call, which is what makes the cast sound.
     */
    public String string(int i) {
        return i < arguments.size() ? (String) ((SourceFunctionArgument.Value) arguments.get(i)).value() : null;
    }

    /**
     * The {@code i}th argument as named values, or {@code null} when the call had no argument there, as
     * {@link #string(int)} answers. Either kind of position can be declared optional, so an omitted trailing
     * argument is an answer about the call rather than a fault. Both still throw a {@link ClassCastException} for
     * a position holding the other kind, which the signature has already refused.
     */
    public Map<String, Object> namedValues(int i) {
        return i < arguments.size() ? ((SourceFunctionArgument.NamedValues) arguments.get(i)).values() : null;
    }

    /**
     * The name and how many arguments there were, never what was in them. A record's generated {@code toString}
     * renders every component, and an argument may be a definition carrying credentials, so the generated one
     * would put them wherever anyone writes {@code logger.debug("{}", call)}. The leaf that holds an invocation
     * redacts for the same reason; redacting here means a caller does not have to know to.
     */
    @Override
    public String toString() {
        return "SourceFunctionInvocation[name=" + name + ", arguments=" + arguments.size() + "]";
    }
}
