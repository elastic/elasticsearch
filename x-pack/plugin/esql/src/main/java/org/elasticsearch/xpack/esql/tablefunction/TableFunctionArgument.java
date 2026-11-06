/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.tablefunction;

import org.elasticsearch.xpack.esql.core.tree.Source;

import java.util.Objects;

/**
 * One positional argument of a table-function call.
 * <p>
 * Always a name, written bare ({@code my_source}) or quoted ({@code "my source"}) — which is what the
 * grammar's {@code tableFunctionArgument} admits, and what this type follows rather than widening to
 * {@code Expression} for a caller that does not exist. A table function stands where a source pattern
 * stands, and nothing is in scope there for an expression to refer to. A function that one day needs a
 * number takes it in its payload, or the grammar rule changes and so does this.
 *
 * @param source where the argument was written, for the diagnostic a function raises about it
 * @param value the name, with any quoting removed
 */
public record TableFunctionArgument(Source source, String value) {

    public TableFunctionArgument {
        Objects.requireNonNull(source, "source");
        Objects.requireNonNull(value, "value");
    }
}
