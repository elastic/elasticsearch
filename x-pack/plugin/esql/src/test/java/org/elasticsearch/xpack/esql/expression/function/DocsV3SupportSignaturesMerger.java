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
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.esql.expression.function.AbstractFunctionTestCase.shouldHideSignature;
import static org.elasticsearch.xpack.esql.expression.function.DocsV3Support.getFirstParametersIndexForSignature;

public class DocsV3SupportSignaturesMerger {
    private DocsV3SupportSignaturesMerger() {}

    /**
     * A cell in the merged types table, carrying both the set of allowed data types and the appliesTo annotation.
     * Two params are equal only when both parts match, which prevents merging rows with different appliesTo annotations.
     */
    public record ParamCell(Set<DataType> types, Set<FunctionAppliesTo> appliesTo) {
        static final ParamCell EMPTY = new ParamCell(Set.of(), Set.of());
    }

    /**
     * Builds a merged table of allowed signatures.
     * @return A map of return types to a set of merged rows, each row being a list of {@link ParamCell}s.
     */
    public static Map<DataType, Set<List<ParamCell>>> buildMergedTypesTable(
        List<EsqlFunctionRegistry.ArgSignature> args,
        Set<DocsV3Support.TypeSignature> signatures
    ) {
        var unmergedTypesTable = signaturesToRawTypesTable(args, signatures);
        return mergeTypesTable(unmergedTypesTable);
    }

    /**
     * Builds an unmerged types table:
     * for each return type, a set of rows, where each row is a list of {@link ParamCell}s (total length always {@code args.size()}).
     * <p>
     *     Optional args not provided in a signature are represented as {@link ParamCell#EMPTY} so all rows have the same length.
     * </p>
     */
    private static Map<DataType, Set<List<ParamCell>>> signaturesToRawTypesTable(
        List<EsqlFunctionRegistry.ArgSignature> args,
        Set<DocsV3Support.TypeSignature> signatures
    ) {
        int rowLength = args.size();
        Map<DataType, Set<List<ParamCell>>> byReturnType = new LinkedHashMap<>();
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
            List<ParamCell> row = new ArrayList<>(rowLength);
            for (int i = 0; i < rowLength; i++) {
                row.add(ParamCell.EMPTY);
            }
            int start = getFirstParametersIndexForSignature(args, sig);
            for (int i = 0; i < sig.argTypes().size(); i++) {
                DocsV3Support.Param param = sig.argTypes().get(i);
                Set<FunctionAppliesTo> appliesTo = param.appliesTo() != null ? new HashSet<>(param.appliesTo()) : Set.of();
                HashSet<DataType> types = new HashSet<>();
                types.add(param.dataType());
                row.set(start + i, new ParamCell(types, appliesTo));
            }
            byReturnType.computeIfAbsent(returnType, k -> new HashSet<>()).add(row);
        }
        return byReturnType;
    }

    /**
     * Reduces the unmerged types table like a decision table: merge rows that differ in exactly one position,
     * by replacing that position with the union of the values. Repeat until no more rows are merged.
     * The result allows exactly the same function call types as the original signatures.
     */
    private static Map<DataType, Set<List<ParamCell>>> mergeTypesTable(Map<DataType, Set<List<ParamCell>>> unmerged) {
        Map<DataType, Set<List<ParamCell>>> result = new LinkedHashMap<>();
        for (Map.Entry<DataType, Set<List<ParamCell>>> e : unmerged.entrySet()) {
            result.put(e.getKey(), reduceDecisionTable(e.getValue()));
        }
        return result;
    }

    /**
     * Reduces a list of rows (decision-table style):
     * - Merge two rows if they differ in exactly one position and the appliesTo at that position matches.
     * - Replace that position with the union of the type sets (appliesTo is kept as-is).
     * - Repeat until no more merges are possible.
     * @return A set of reduced rows (order-independent).
     */
    private static Set<List<ParamCell>> reduceDecisionTable(Set<List<ParamCell>> unmergedRows) {
        if (unmergedRows.isEmpty()) {
            return Set.of();
        }

        // Deterministically sort the cells: lexicographically on the types, then lexicographically on the appliesTo
        // Done to avoid different results in different runs of the docs generations, given some tables can be reduced in multiple ways
        List<List<ParamCell>> rows = unmergedRows.stream().sorted((a, b) -> {
            for (int i = 0; i < a.size(); i++) {
                ParamCell cellA = a.get(i);
                ParamCell cellB = b.get(i);
                String aTypes = cellA.types().stream().map(DataType::esNameIfPossible).sorted().collect(Collectors.joining(","));
                String bTypes = cellB.types().stream().map(DataType::esNameIfPossible).sorted().collect(Collectors.joining(","));
                int cmp = aTypes.compareTo(bTypes);
                if (cmp != 0) return cmp;

                String aApplies = cellA.appliesTo().stream().map(Object::toString).sorted().collect(Collectors.joining(","));
                String bApplies = cellB.appliesTo().stream().map(Object::toString).sorted().collect(Collectors.joining(","));
                cmp = aApplies.compareTo(bApplies);
                if (cmp != 0) return cmp;
            }
            return 0;
        }).collect(Collectors.toCollection(ArrayList::new));

        int maxIterations = rows.size();
        int iterations = 0;
        boolean changed;
        do {
            if (iterations > maxIterations) {
                throw new IllegalStateException("Too many iterations while merging " + maxIterations + " rows");
            }

            changed = false;
            for (int a = 0; a < rows.size() && !changed; a++) {
                var rowA = rows.get(a);
                for (int b = a + 1; b < rows.size() && !changed; b++) {
                    var rowB = rows.get(b);
                    int diffIndex = singleMergeableDiffIndex(rowA, rowB);
                    if (diffIndex >= 0) {
                        rowA.get(diffIndex).types().addAll(rowB.get(diffIndex).types());
                        rows.remove(b);
                        changed = true;
                    }
                }
            }
            iterations++;
        } while (changed);
        return new HashSet<>(rows);
    }

    /**
     * Returns the single index where the two rows differ and are mergeable, or -1.
     * Two params are "different" when their {@link ParamCell#equals} returns false.
     * A difference is "mergeable" only when the appliesTo lists are equal (only the types differ).
     * Returns -1 if there are 0 differences, 2+ differences, or the single difference has mismatched appliesTo.
     */
    private static int singleMergeableDiffIndex(List<ParamCell> rowA, List<ParamCell> rowB) {
        int diffAt = -1;
        for (int j = 0; j < rowA.size(); j++) {
            if (rowA.get(j).equals(rowB.get(j)) == false) {
                if (diffAt >= 0) {
                    return -1;
                }
                // Empty cell == optional missing parameter. Can't be merged
                if (rowA.get(j).types().isEmpty() || rowB.get(j).types().isEmpty()) {
                    return -1;
                }
                if (rowA.get(j).appliesTo().equals(rowB.get(j).appliesTo()) == false) {
                    return -1;
                }
                diffAt = j;
            }
        }
        return diffAt;
    }
}
