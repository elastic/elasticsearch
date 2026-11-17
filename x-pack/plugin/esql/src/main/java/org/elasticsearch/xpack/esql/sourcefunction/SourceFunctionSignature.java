/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.sourcefunction;

import org.elasticsearch.xpack.esql.parser.ParsingException;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * The arguments a {@link SourceFunction} accepts: how many, what each is called, what kind each is, and which may be
 * left out.
 * <p>
 * Declared rather than coded, for the reason the scalar function registry declares arity rather than coding it —
 * {@code FunctionDefinition.Builder#unary} and its siblings install a shared validator, so no scalar function author
 * writes an arity check and none of them can word one differently from the rest. The same risk applies here with more
 * force, because a source function's arguments are part of a query's source list, and a confusing diagnostic there is
 * a confusing diagnostic about where the data came from.
 * <p>
 * A function states its arguments once and writes no shape-checking code at all, which leaves
 * {@link SourceFunction#validate} for what is genuinely about that function.
 */
public record SourceFunctionSignature(List<Argument> arguments) {

    /** What one argument is called, what kind it is, and whether it may be omitted. */
    public record Argument(String name, Kind kind, boolean required) {}

    /**
     * What one argument has to be. The grammar admits a constant or a {@code {...}}, which is two shapes; a kind is
     * narrower than a shape, because a function that wants a name wants a string and not any constant. So each kind
     * names both the shape and, for a constant, the types it accepts — which is what lets a function declare its
     * arguments once and leaves {@link #check} the only place that words the refusal.
     */
    public enum Kind {
        /** A string constant, quoting removed. A name is written as one. */
        STRING("a string"),
        /** A {@code {...}} of named values. */
        NAMED_VALUES("a {...}");

        // Adding a kind here is not only a constant and a switch arm. A payload's values are whitelisted by type
        // in LogicalPlanBuilder.SOURCE_FUNCTION_VALUE_TYPES for reasons that apply just as much to a positional
        // constant -- UNSIGNED_LONG's stored long is a biased representation, and a time duration folds to a
        // java.time.Period -- and nothing filters a positional one, because no kind here admits a number today.
        // Whoever adds one inherits that, and should read that whitelist before deciding what the new kind lets
        // through.

        private final String describedAs;

        Kind(String describedAs) {
            this.describedAs = describedAs;
        }

        @Override
        public String toString() {
            return describedAs;
        }
    }

    public static SourceFunctionSignature of(Argument... arguments) {
        return new SourceFunctionSignature(List.of(arguments));
    }

    /** A required argument written as a string constant. A data source or a resource is one. */
    public static Argument string(String name) {
        return new Argument(name, Kind.STRING, true);
    }

    /** A required {@code {...}} payload, in any position. */
    public static Argument namedValues(String name) {
        return new Argument(name, Kind.NAMED_VALUES, true);
    }

    /**
     * The same argument, allowed to be left out -- but only as part of the trailing run of them, because
     * {@link #check} reads a call positionally. Every argument after an optional one must also be optional, so
     * with {@code [string(a), optional(string(b)), optional(namedValues(c))]} a call writing only {@code a} and
     * {@code c} is refused against {@code b}: there is no way to skip a middle argument and no plan to add one.
     */
    public static Argument optional(Argument argument) {
        return new Argument(argument.name(), argument.kind(), false);
    }

    public SourceFunctionSignature {
        arguments = List.copyOf(arguments);
        // Thrown rather than asserted: assertions are off in production, and this is a shape no call could ever
        // satisfy, so a function declaring it is broken on the first query that names it.
        boolean seenOptional = false;
        Set<String> names = new HashSet<>(arguments.size());
        for (int i = 0; i < arguments.size(); i++) {
            Argument argument = arguments.get(i);
            if (argument.required() && seenOptional) {
                throw new IllegalStateException("required argument [" + argument.name() + "] follows an optional one");
            }
            // A name is how every diagnostic about this argument identifies it, and a call is read positionally, so
            // two arguments sharing one name produce a message pointing at a position the reader cannot pick out.
            if (names.add(argument.name()) == false) {
                throw new IllegalStateException("argument [" + argument.name() + "] is declared twice");
            }
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
     * {@link SourceFunction#validate}, so a function's own checks can read the arguments it declared without
     * re-establishing that they are there or what they are.
     */
    public void check(SourceFunctionInvocation call) {
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
            SourceFunctionArgument actual = call.arguments().get(i);
            boolean matches = switch (expected.kind()) {
                // The value rather than the type, because the value is what a function reads: an array literal of
                // strings folds to a KEYWORD constant holding a List, so a type test would admit it, and a written
                // NULL is a constant too. One single string is the whole of what a name can be.
                case STRING -> actual instanceof SourceFunctionArgument.Value value && value.value() instanceof String;
                case NAMED_VALUES -> actual instanceof SourceFunctionArgument.NamedValues;
            };
            if (matches == false) {
                throw new ParsingException(actual.source(), "[{}] expects {} for [{}]", call.name(), expected.kind(), expected.name());
            }
        }
    }
}
