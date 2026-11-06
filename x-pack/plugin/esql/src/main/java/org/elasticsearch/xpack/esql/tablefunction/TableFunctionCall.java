/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.tablefunction;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * One table-function call as written in a source list, with its name already resolved against
 * {@link TableFunctionRegistry} and its payload already folded to plain values.
 * <p>
 * A carrier, with no validation of its own: how many arguments a function takes and whether it requires
 * a payload are the function's questions, not this type's, and each raises its own diagnostic against
 * the {@link #source()} of whatever was wrong.
 *
 * @param source the whole call, {@code NAME(...)} included, for a diagnostic about the call itself
 * @param name the function name as the user wrote it, for diagnostics; {@link TableFunctionRegistry}
 *             resolves case-insensitively, so this is not a key
 * @param arguments the positional arguments, in the order written; empty when none were
 * @param payload the trailing {@code {...}}, or {@code null} when none was written. Keys are the
 *                double-quoted strings the grammar requires; a value is a plain Java value or, for a
 *                nested {@code {...}}, another {@code Map<String, Object>}. An explicitly written
 *                {@code {}} folds to an empty map, which is deliberately not the same as {@code null}:
 *                a function may require a payload and still accept an empty one.
 */
public record TableFunctionCall(Source source, String name, List<TableFunctionArgument> arguments, @Nullable Map<String, Object> payload) {

    public TableFunctionCall {
        Objects.requireNonNull(source, "source");
        Objects.requireNonNull(name, "name");
        arguments = List.copyOf(arguments);
        // LinkedHashMap rather than Map.copyOf: written order is what a diagnostic listing keys reports,
        // and an unordered copy would make the same bad payload produce a differently ordered message.
        payload = payload == null ? null : Collections.unmodifiableMap(new LinkedHashMap<>(payload));
    }
}
