/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.generator.function;

import org.elasticsearch.xpack.esql.generator.Column;
import org.elasticsearch.xpack.esql.generator.GenerativeFunctionCatalog;
import org.elasticsearch.xpack.esql.generator.GenerativeFunctionDefinition;
import org.elasticsearch.xpack.esql.generator.GenerativeFunctionParam;
import org.elasticsearch.xpack.esql.generator.GenerativeFunctionSignature;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.test.ESTestCase.randomFrom;
import static org.elasticsearch.test.ESTestCase.randomIntBetween;
import static org.elasticsearch.xpack.esql.generator.EsqlQueryGenerator.randomName;

/**
 * Generates composite ES|QL function expressions with nested function calls, e.g.
 * {@code to_string(sin(cos(a)))} or {@code length(to_string(round(x, 2)))}.
 * <p>
 * Uses {@link GenerativeFunctionCatalog} to select type-compatible functions at each level of nesting,
 * so every generated expression is well-typed.
 */
public final class CompositeFunctionGenerator {

    private CompositeFunctionGenerator() {}

    /**
     * Generates a composite function expression whose outermost call returns {@code targetType}.
     *
     * @param columns       available input columns
     * @param allowUnmapped if true, may use unmapped field names as leaf arguments
     * @param targetType    the ES|QL type the outermost expression must return
     * @param maxDepth      maximum nesting depth (1 = single function call, 2 = f(g(...)), etc.)
     * @return an expression string, or {@code null} if none can be generated
     */
    public static String compositeExpression(List<Column> columns, boolean allowUnmapped, String targetType, int maxDepth) {
        return generate(targetType, columns, allowUnmapped, maxDepth);
    }

    private static String generate(String targetType, List<Column> columns, boolean allowUnmapped, int depthLeft) {
        Set<String> columnTypes = columns.stream().map(Column::type).collect(Collectors.toSet());

        // At depth 0, always produce a leaf. Otherwise stop early with 30% probability so that
        // the generator still produces composite expressions most of the time.
        if (depthLeft == 0) {
            return leaf(targetType, columns, allowUnmapped);
        }
        if (randomIntBetween(0, 9) < 3) {
            return leaf(targetType, columns, allowUnmapped);
        }

        GenerativeFunctionCatalog catalog = GenerativeFunctionCatalog.getInstance();
        List<GenerativeFunctionDefinition> candidates = catalog.scalarsReturning(targetType);
        if (candidates.isEmpty()) {
            return leaf(targetType, columns, allowUnmapped);
        }

        // Try a few random candidates before giving up and falling back to a leaf
        for (int attempt = 0; attempt < 5; attempt++) {
            GenerativeFunctionDefinition fn = randomFrom(candidates);
            // Keep only signatures that both return targetType (or an equivalent) and can be
            // satisfied from available types. Without the returnType filter, mv_sort-style functions
            // (one signature per input type, each returning that same type) would be randomly
            // picked in a wrong-return-type overload.
            List<GenerativeFunctionSignature> sigs = catalog.satisfiableSignatures(fn, columnTypes)
                .stream()
                .filter(s -> GenerativeFunctionCatalog.equivalentTypes(targetType).contains(s.returnType()))
                .toList();
            if (sigs.isEmpty()) {
                continue;
            }
            GenerativeFunctionSignature sig = randomFrom(sigs);
            String expr = tryBuild(fn.name(), sig, columns, allowUnmapped, depthLeft);
            if (expr != null) {
                return expr;
            }
        }

        return leaf(targetType, columns, allowUnmapped);
    }

    /**
     * Attempts to build a function call {@code name(arg1, arg2, ...)} by generating each argument.
     * Functions with a {@link SpecialFunctionGenerator} registered in
     * {@link SpecialFunctionGeneratorRegistry} use that instead of the generic recursive builder
     * (e.g. to pass a literal delimiter, a constant index, or a value from a fixed vocabulary).
     * Returns {@code null} if any required argument cannot be generated.
     */
    private static String tryBuild(
        String name,
        GenerativeFunctionSignature sig,
        List<Column> columns,
        boolean allowUnmapped,
        int depthLeft
    ) {
        SpecialFunctionGenerator special = SpecialFunctionGeneratorRegistry.forFunction(name);
        if (special != null) {
            return special.generate(name, sig, columns, allowUnmapped, depthLeft, CompositeFunctionGenerator::generate);
        }

        List<String> args = new ArrayList<>();
        for (GenerativeFunctionParam param : sig.params()) {
            if (param.optional()) {
                // Always stop at the first optional param. Optional keyword params are typically
                // literal mode/order/format selectors ("ASC", "DESC", date format strings, etc.)
                // that cannot be satisfied by arbitrary expressions.
                break;
            }
            String arg = generate(param.type(), columns, allowUnmapped, depthLeft - 1);
            if (arg == null) {
                return null;
            }
            args.add(arg);
        }
        if (args.isEmpty()) {
            // Zero-arg call (e.g. pi(), e(), now()) — only valid if there are no required params
            boolean hasRequiredParams = sig.params().stream().anyMatch(p -> p.optional() == false);
            if (hasRequiredParams) {
                return null;
            }
        }
        return name + "(" + String.join(", ", args) + ")";
    }

    /**
     * Leaf: returns a column reference of the target type, or an unmapped field if allowed and no
     * column of the right type exists.
     */
    private static String leaf(String targetType, List<Column> columns, boolean allowUnmapped) {
        String field = randomName(columns, GenerativeFunctionCatalog.equivalentTypes(targetType));
        if (field != null) {
            return field;
        }
        if (allowUnmapped && randomIntBetween(0, 10) < 3) {
            return FunctionGeneratorUtils.maybeUnmappedField(true);
        }
        return null;
    }
}
