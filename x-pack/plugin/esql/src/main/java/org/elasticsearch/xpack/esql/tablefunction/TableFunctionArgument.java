/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.tablefunction;

import org.elasticsearch.xpack.esql.core.tree.Source;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.TreeSet;

/**
 * One argument of a table-function invocation.
 * <p>
 * A flat list of these is the whole invocation, which is how a scalar function's arguments already work:
 * {@code ExpressionBuilder#visitFunctionExpression} appends a trailing map to the same list it collected the other
 * arguments into, so a scalar function sees a map as its last argument rather than as a separate slot. Following
 * that is why a map is an argument here rather than a slot beside the arguments.
 * <p>
 * Two kinds, because the grammar admits two. <b>Not a general expression</b>, which is where this departs from a
 * scalar function and has to: a source list is read before anything is in scope for an expression to refer to, so
 * there would be nothing one could mean.
 * <p>
 * <b>Not {@code Writeable}.</b> An invocation never leaves the coordinator — it is rewritten into a leaf before the
 * plan is distributed, and {@code UnresolvedTableFunctionRelation} refuses to serialize for that reason. A function
 * whose leaf needs something at a data node puts that on the leaf, where a reader can see what travels.
 */
public sealed interface TableFunctionArgument {

    /** Where the argument was written, for the diagnostic a function raises about it. */
    Source source();

    /** A bare identifier, a backquoted one, or a quoted string — with any quoting removed. */
    record Name(Source source, String value) implements TableFunctionArgument {
        public Name {
            Objects.requireNonNull(source, "source");
            Objects.requireNonNull(value, "value");
        }
    }

    /**
     * A {@code {...}} folded to plain values: the double-quoted keys the grammar requires, against strings,
     * numbers, booleans, lists, or a nested map. An explicitly written {@code {}} is an empty map here, which a
     * function may well accept.
     */
    record NamedValues(Source source, Map<String, Object> values) implements TableFunctionArgument {

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
