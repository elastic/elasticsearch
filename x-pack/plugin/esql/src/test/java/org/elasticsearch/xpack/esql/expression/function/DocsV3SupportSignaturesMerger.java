/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function;

import org.elasticsearch.xpack.esql.core.type.DataType;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.esql.expression.function.AbstractFunctionTestCase.shouldHideSignature;
import static org.elasticsearch.xpack.esql.expression.function.DocsV3Support.getFirstParametersIndexForSignature;

public class DocsV3SupportSignaturesMerger {
    private DocsV3SupportSignaturesMerger() {}

    /**
     * Builds a merged table of allowed signatures.
     * @return A map of return types to:
     *         <br/>A set of signatures, each being:
     *         <br/>A list of parameters, each being:
     *         <br/>A set of allowed types for that signature.
     */
    public static Map<DataType, Set<List<Set<DataType>>>> buildMergedTypesTable(
        List<EsqlFunctionRegistry.ArgSignature> args,
        Set<DocsV3Support.TypeSignature> signatures
    ) {
        var unmergedTypesTable = signaturesToRawTypesTable(args, signatures);
        return mergeTypesTable(unmergedTypesTable);
    }

    /**
     * Builds an unmerged types table:
     * for each return type, a list of rows, where each row is a list of parameter types (length always {@code args.size()}).
     * <p>
     *     Optional args not provided in a signature are represented as {@code null} at that position so all rows have the same length.
     * </p>
     */
    private static Map<DataType, List<List<DataType>>> signaturesToRawTypesTable(
        List<EsqlFunctionRegistry.ArgSignature> args,
        Set<DocsV3Support.TypeSignature> signatures
    ) {
        int rowLength = args.size();
        Map<DataType, List<List<DataType>>> byReturnType = new LinkedHashMap<>();
        for (DocsV3Support.TypeSignature sig : signatures) {
            if (shouldHideSignature(sig.argTypes(), sig.returnType())) {
                continue;
            }
            if (sig.argTypes().size() > rowLength) {
                continue;
            }
            DataType returnType = sig.returnType();
            if (returnType == null) {
                continue;
            }
            List<DataType> row = new ArrayList<>(rowLength);
            for (int i = 0; i < rowLength; i++) {
                row.add(null);
            }
            int start = getFirstParametersIndexForSignature(args, sig);
            for (int i = 0; i < sig.argTypes().size(); i++) {
                row.set(start + i, sig.argTypes().get(i).dataType());
            }
            byReturnType.computeIfAbsent(returnType, k -> new ArrayList<>()).add(row);
        }
        return byReturnType;
    }

    /**
     * Reduces the unmerged types table like a decision table: merge rows that differ in exactly one position,
     * by replacing that position with the union of the values. Repeat until no more rows are merged.
     * The result allows exactly the same function call types as the original signatures.
     */
    private static Map<DataType, Set<List<Set<DataType>>>> mergeTypesTable(Map<DataType, List<List<DataType>>> unmerged) {
        Map<DataType, Set<List<Set<DataType>>>> result = new LinkedHashMap<>();
        for (Map.Entry<DataType, List<List<DataType>>> e : unmerged.entrySet()) {
            result.put(e.getKey(), reduceDecisionTable(e.getValue()));
        }
        return result;
    }

    /**
     * Reduces a list of rows (decision-table style):
     * - Merge two rows if they differ in exactly one position.
     * - Replace that position with the union
     * - Repeat until no more merges are possible.
     * @return A set of reduced rows (order-independent).
     */
    private static Set<List<Set<DataType>>> reduceDecisionTable(List<List<DataType>> rows) {
        if (rows.isEmpty()) {
            return Set.of();
        }

        // Convert the input rows to a list of mutable sets
        int rowLength = rows.getFirst().size();
        List<List<Set<DataType>>> current = new ArrayList<>();
        for (List<DataType> row : rows) {
            assert row.size() == rowLength : "All rows must have the same length (missing optional params must be null)";
            List<Set<DataType>> asSets = new ArrayList<>(rowLength);
            for (int j = 0; j < rowLength; j++) {
                DataType t = row.get(j);
                Set<DataType> set = new HashSet<>();
                if (t != null) {
                    set.add(t);
                }
                asSets.add(set);
            }
            current.add(asSets);
        }

        // Merge the rows
        int iterations = 0;
        boolean changed;
        do {
            if (iterations > 1000) {
                throw new IllegalStateException("Too many iterations while merging rows");
            }

            changed = false;
            for (int a = 0; a < current.size() && !changed; a++) {
                var rowA = current.get(a);
                for (int b = a + 1; b < current.size() && !changed; b++) {
                    var rowB = current.get(b);
                    int diffIndex = singleDiffIndex(rowA, rowB);
                    if (diffIndex >= 0) {
                        rowA.get(diffIndex).addAll(rowB.get(diffIndex));
                        current.remove(b);
                        // This will also break the loops
                        changed = true;
                    }
                }
            }
            iterations++;
        } while (changed);
        return new HashSet<>(current);
    }

    /** Returns the single index where the two rows differ (by set equality), or -1 if 0 or 2+ differences. */
    private static int singleDiffIndex(List<Set<DataType>> rowA, List<Set<DataType>> rowB) {
        int diffAt = -1;
        for (int j = 0; j < rowA.size(); j++) {
            if (rowA.get(j).equals(rowB.get(j)) == false) {
                if (diffAt >= 0) {
                    return -1;
                }
                diffAt = j;
            }
        }
        return diffAt;
    }
}
