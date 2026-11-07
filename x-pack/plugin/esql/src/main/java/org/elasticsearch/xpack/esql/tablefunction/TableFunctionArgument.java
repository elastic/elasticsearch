/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.tablefunction;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * One argument of a table-function call.
 * <p>
 * A flat list of these is the whole call, which is how a scalar function's arguments work:
 * {@code ExpressionBuilder.visitFunctionExpression} appends the trailing map to the same list it collected the other
 * arguments into, so a scalar function sees a map as its last argument rather than as a separate slot. Following that
 * is why there is no "payload" concept here — a map is an argument.
 * <p>
 * Two kinds, because the grammar admits two: a bare or quoted name, and a {@code {...}} of named values. Not a
 * general expression, which is where this departs from scalar functions and has to: a source list is read before
 * anything is in scope for an expression to refer to, so there is nothing one could mean.
 * <p>
 * A closed set rather than a {@code NamedWriteable}, since the grammar decides what kinds exist and both ends of the
 * wire compile against the same grammar. A new kind is a new tag below, and the transport version the enclosing
 * request rides already gates the whole list.
 */
public sealed interface TableFunctionArgument extends Writeable {

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
     * A {@code {...}} folded to plain values: the double-quoted keys the grammar requires, against strings, numbers,
     * booleans, lists, or a nested map. An explicitly written {@code {}} is an empty map here, which a function may
     * well accept.
     */
    record NamedValues(Source source, Map<String, Object> values) implements TableFunctionArgument {
        public NamedValues {
            Objects.requireNonNull(source, "source");
            // LinkedHashMap rather than Map.copyOf: written order is what a diagnostic listing keys reports, and an
            // unordered copy would make the same bad input produce a differently ordered message.
            values = Collections.unmodifiableMap(new LinkedHashMap<>(Objects.requireNonNull(values, "values")));
        }
    }

    /** The wire discriminator. Implicitly public, being a member of an interface; not part of the API. */
    enum Tag {
        NAME,
        NAMED_VALUES
    }

    @Override
    default void writeTo(StreamOutput out) throws IOException {
        // Source is not written: it locates the argument in the query text for a diagnostic the coordinator raises,
        // and by the time this reaches a node the call has parsed and validated.
        switch (this) {
            case Name n -> {
                out.writeEnum(Tag.NAME);
                out.writeString(n.value());
            }
            case NamedValues v -> {
                out.writeEnum(Tag.NAMED_VALUES);
                out.writeGenericValue(v.values());
            }
        }
    }

    static TableFunctionArgument readFrom(StreamInput in) throws IOException {
        return switch (in.readEnum(Tag.class)) {
            case NAME -> new Name(Source.EMPTY, in.readString());
            case NAMED_VALUES -> {
                @SuppressWarnings("unchecked")
                Map<String, Object> values = (Map<String, Object>) in.readGenericValue();
                yield new NamedValues(Source.EMPTY, values);
            }
        };
    }
}
