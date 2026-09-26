/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.generator.function;

import org.elasticsearch.xpack.esql.generator.Column;
import org.elasticsearch.xpack.esql.generator.GenerativeFunctionSignature;

import java.util.List;

/**
 * Custom argument generator for ES|QL functions that cannot use the generic recursive
 * expression builder for one or more of their parameters — for example, functions that
 * require a literal from a fixed vocabulary, a constant integer index, or a single-byte
 * delimiter.
 * <p>
 * Register implementations in {@link SpecialFunctionGeneratorRegistry}. The function name
 * is the registry key; implementations do not need to store it.
 *
 * <p><b>Contract</b>
 * <ul>
 *   <li>Return {@code null} to signal that this combination cannot be generated; the caller
 *       falls back to a leaf expression.</li>
 *   <li>For parameters that can be arbitrary expressions, call
 *       {@code recurse.recurse(param.type(), columns, allowUnmapped, depthLeft - 1)} and
 *       propagate {@code null} if it returns {@code null}.</li>
 *   <li>For parameters that must be constants or come from a fixed set, produce a literal
 *       directly (e.g. {@code "\"day\""}, {@code "42"}).</li>
 * </ul>
 *
 * <p><b>Example</b>
 * <pre>{@code
 * // date_diff(unit, start, end) — "unit" must be a literal from a fixed vocabulary
 * (name, sig, cols, unmapped, depth, recurse) -> {
 *     String start = recurse.recurse(sig.params().get(1).type(), cols, unmapped, depth - 1);
 *     if (start == null) return null;
 *     String end   = recurse.recurse(sig.params().get(2).type(), cols, unmapped, depth - 1);
 *     if (end == null) return null;
 *     return name + "(\"day\", " + start + ", " + end + ")";
 * }
 * }</pre>
 */
@FunctionalInterface
public interface SpecialFunctionGenerator {

    /**
     * Generates the complete function call string for {@code name} using the given signature.
     *
     * @param name          the function name (e.g. {@code "split"})
     * @param sig           the overload to use (return type already matches the caller's need)
     * @param columns       columns available as leaf inputs
     * @param allowUnmapped whether unmapped field names may appear in leaf expressions
     * @param depthLeft     remaining nesting depth; pass {@code depthLeft - 1} to {@code recurse}
     *                      so total nesting stays bounded
     * @param recurse       callback into the main composite generator for building sub-expressions
     * @return the expression string (e.g. {@code "split(region, \".\")"}), or {@code null} to
     *         fall back to a leaf expression
     */
    String generate(
        String name,
        GenerativeFunctionSignature sig,
        List<Column> columns,
        boolean allowUnmapped,
        int depthLeft,
        Recurser recurse
    );

    /**
     * Callback into the main composite generator.
     * Used by {@link SpecialFunctionGenerator} implementations to recursively build
     * sub-expressions for the parameters they do not need to override.
     */
    @FunctionalInterface
    interface Recurser {
        /**
         * Produces an expression that returns {@code targetType}, recursing into the composite
         * generator at {@code depthLeft}. Returns {@code null} if no such expression can be built.
         */
        String recurse(String targetType, List<Column> columns, boolean allowUnmapped, int depthLeft);
    }
}
