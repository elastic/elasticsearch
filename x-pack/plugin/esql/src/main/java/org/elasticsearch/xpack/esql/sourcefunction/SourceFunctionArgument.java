/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.sourcefunction;

import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.TreeSet;

/**
 * One argument of a source-function invocation.
 * <p>
 * A flat list of these is the whole invocation, which is how a scalar function's arguments already work:
 * {@code ExpressionBuilder#visitFunctionExpression} appends a trailing map to the same list it collected the other
 * arguments into, so a scalar function sees a map as its last argument rather than as a separate slot. Following
 * that is why a map is an argument here rather than a slot beside the arguments.
 * <p>
 * Two kinds, because the grammar admits two. <b>Not a general expression</b>, which is where this departs from a
 * scalar function and has to: a source list is read before anything is in scope for an expression to refer to, so
 * there would be nothing one could mean. What is left is a constant, and a constant is already folded by the time
 * it arrives here, so a value parameter the request bound is indistinguishable from the same value written out.
 * <p>
 * <b>Not {@code Writeable}.</b> An invocation never leaves the coordinator — it is rewritten into a leaf before the
 * plan is distributed, and {@code UnresolvedSourceFunctionRelation} refuses to serialize for that reason. A function
 * whose leaf needs something at a data node puts that on the leaf, where a reader can see what travels.
 */
public sealed interface SourceFunctionArgument {

    /** Where the argument was written, for the diagnostic a function raises about it. */
    Source source();

    /**
     * One folded constant: its type and its value, with every {@code BytesRef} already rendered as a {@link String},
     * so that a string argument and a string inside a {@link NamedValues} are the same Java type.
     * <p>
     * {@code value} may be {@code null}, for a written {@code NULL}. Refusing it here would report it as a fault of
     * the call rather than of the argument, while {@link SourceFunctionSignature} refuses it against the argument's
     * declared kind and so can name the argument the user has to fix.
     */
    record Value(Source source, DataType type, Object value) implements SourceFunctionArgument {
        public Value {
            Objects.requireNonNull(source, "source");
            Objects.requireNonNull(type, "type");
        }

        /**
         * The type and nothing else, for the reason {@link NamedValues#toString} renders only its keys: a
         * positional argument is as able to carry a credential as one inside a payload -- a signature declares
         * what a position means, and "a data source or a resource" is one declaration away from a token.
         */
        @Override
        public String toString() {
            return "Value[" + type.typeName() + "]";
        }
    }

    /**
     * A {@code {...}} folded to plain values: the double-quoted keys the grammar requires, against strings,
     * numbers, booleans, lists, or a nested map. An explicitly written {@code {}} is an empty map here, which a
     * function may well accept.
     */
    record NamedValues(Source source, Map<String, Object> values) implements SourceFunctionArgument {

        public NamedValues {
            Objects.requireNonNull(source, "source");
            // LinkedHashMap rather than Map.copyOf: written order is what a diagnostic listing keys reports, and an
            // unordered copy would make the same bad input produce a differently ordered message.
            values = Collections.unmodifiableMap(new LinkedHashMap<>(Objects.requireNonNull(values, "values")));
        }

        /**
         * The keys and nothing else. These values are whatever the user wrote inside the braces, which for a
         * definition includes credentials, and a record's generated {@code toString} renders all of them.
         */
        @Override
        public String toString() {
            return "NamedValues" + new TreeSet<>(values.keySet());
        }
    }
}
