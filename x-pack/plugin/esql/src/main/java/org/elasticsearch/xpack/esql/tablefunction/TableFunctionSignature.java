/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.tablefunction;

import org.elasticsearch.xpack.esql.parser.ParsingException;

import java.util.List;

/**
 * The arguments a {@link TableFunction} accepts: how many, what each is called, what kind each is, and which may be
 * left out.
 * <p>
 * Declared rather than coded, for the reason the scalar function registry declares arity rather than coding it —
 * {@code FunctionDefinition.Builder#unary} and its siblings install a shared validator, so no scalar function author
 * writes an arity check and none of them can word one differently from the rest. The same risk applies here with more
 * force, because a table function's arguments are part of a query's source list, and a confusing diagnostic there is
 * a confusing diagnostic about where the data came from.
 * <p>
 * A function states its arguments once and writes no shape-checking code at all, which leaves
 * {@link TableFunction#validate} for what is genuinely about that function.
 */
public record TableFunctionSignature(List<Argument> arguments) {

    /** What one argument is called, what kind it is, and whether it may be omitted. */
    public record Argument(String name, Kind kind, boolean required) {}

    /** The kinds the grammar admits, matching {@link TableFunctionArgument}'s two shapes. */
    public enum Kind {
        /** A bare or quoted name. */
        NAME("a name"),
        /** A {@code {...}} of named values. */
        NAMED_VALUES("a {...}");

        private final String describedAs;

        Kind(String describedAs) {
            this.describedAs = describedAs;
        }

        @Override
        public String toString() {
            return describedAs;
        }
    }

    public static TableFunctionSignature of(Argument... arguments) {
        return new TableFunctionSignature(List.of(arguments));
    }

    /** A required argument written as a bare or quoted name. */
    public static Argument name(String name) {
        return new Argument(name, Kind.NAME, true);
    }

    /** A required {@code {...}}. */
    public static Argument namedValues(String name) {
        return new Argument(name, Kind.NAMED_VALUES, true);
    }

    /** The same argument, allowed to be left out. Every argument after an optional one must also be optional. */
    public static Argument optional(Argument argument) {
        return new Argument(argument.name(), argument.kind(), false);
    }

    public TableFunctionSignature {
        arguments = List.copyOf(arguments);
        boolean seenOptional = false;
        for (Argument argument : arguments) {
            // A required argument after an optional one could never be supplied positionally, so refuse the
            // declaration rather than letting a function ship a shape no call can satisfy.
            assert argument.required() == false || seenOptional == false
                : "required argument [" + argument.name() + "] follows an optional one";
            seenOptional |= argument.required() == false;
        }
    }

    /** The argument list as a diagnostic spells it, with optional arguments in brackets. */
    private String describe() {
        StringBuilder sb = new StringBuilder();
        for (Argument argument : arguments) {
            if (sb.isEmpty() == false) {
                sb.append(", ");
            }
            sb.append(argument.required() ? argument.name() : "[" + argument.name() + "]");
        }
        return sb.toString();
    }

    /**
     * Checks {@code call} against this shape, raising against the source of whatever is wrong — the call for a
     * count, the argument itself for a kind — so the error points at what the user has to change. Runs before
     * {@link TableFunction#validate}, so a function's own checks can read the arguments it declared without
     * re-establishing that they are there or what they are.
     */
    public void check(TableFunctionInvocation call) {
        long required = arguments.stream().filter(Argument::required).count();
        int found = call.arguments().size();
        if (found < required || found > arguments.size()) {
            if (arguments.isEmpty()) {
                throw new ParsingException(call.source(), "[{}] takes no arguments, found [{}]", call.name(), found);
            }
            throw new ParsingException(call.source(), "[{}] takes [{}], found [{}]", call.name(), describe(), found);
        }
        for (int i = 0; i < found; i++) {
            Argument expected = arguments.get(i);
            TableFunctionArgument actual = call.arguments().get(i);
            boolean matches = switch (expected.kind()) {
                case NAME -> actual instanceof TableFunctionArgument.Name;
                case NAMED_VALUES -> actual instanceof TableFunctionArgument.NamedValues;
            };
            if (matches == false) {
                throw new ParsingException(actual.source(), "[{}] expects {} for [{}]", call.name(), expected.kind(), expected.name());
            }
        }
    }
}
