/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function;

import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.esql.expression.function.DocsV3SupportSignaturesMerger.ParamCell;
import static org.hamcrest.Matchers.equalTo;

public class DocsV3SupportSignaturesMergerTests extends ESTestCase {

    /**
     * Four args: optional-required-required-optional.
     * Each has distinct types so that {@link DocsV3Support#getFirstParametersIndexForSignature} can correctly
     * distinguish whether a leading optional is present or missing in 3-arg signatures.
     */
    private static final List<EsqlFunctionRegistry.ArgSignature> ARGS = List.of(
        new EsqlFunctionRegistry.ArgSignature("a", new String[] { "datetime", "double" }, "", true, false),
        new EsqlFunctionRegistry.ArgSignature("b", new String[] { "integer", "long" }, "", false, false),
        new EsqlFunctionRegistry.ArgSignature("c", new String[] { "keyword", "text" }, "", false, false),
        new EsqlFunctionRegistry.ArgSignature("d", new String[] { "date_period", "time_duration" }, "", true, false)
    );

    /**
     * Two required args, used for pivot-row determinism tests.
     */
    private static final List<EsqlFunctionRegistry.ArgSignature> ARGS_2 = List.of(
        new EsqlFunctionRegistry.ArgSignature("x", new String[] { "double", "integer", "long" }, "", false, false),
        new EsqlFunctionRegistry.ArgSignature("y", new String[] { "double", "integer", "long" }, "", false, false)
    );

    private static final FunctionAppliesTo GA_9_1 = TestCaseSupplier.appliesTo(FunctionAppliesToLifecycle.GA, "9.1.0", "", true);
    private static final FunctionAppliesTo PREVIEW_9_3 = TestCaseSupplier.appliesTo(FunctionAppliesToLifecycle.PREVIEW, "9.3.0", "", true);

    public void testNoSignatures() {
        assertMerged(ARGS, Set.of(), Map.of());
        assertMerged(ARGS_2, Set.of(), Map.of());
    }

    public void testSingleSignature() {
        assertMerged(
            ARGS,
            Set.of(sig(DataType.LONG, List.of(DataType.INTEGER, DataType.KEYWORD))),
            Map.of(DataType.LONG, Set.of(List.of(cell(), cell(DataType.INTEGER), cell(DataType.KEYWORD), cell())))
        );
    }

    public void testMergeTwoSignatures() {
        assertMerged(
            ARGS,
            Set.of(
                sig(DataType.LONG, List.of(DataType.INTEGER, DataType.KEYWORD)),
                sig(DataType.LONG, List.of(DataType.INTEGER, DataType.TEXT))
            ),
            Map.of(DataType.LONG, Set.of(List.of(cell(), cell(DataType.INTEGER), cell(DataType.KEYWORD, DataType.TEXT), cell())))
        );
    }

    public void testDontMergeSignaturesMultipleDifferences() {
        assertMerged(
            ARGS,
            Set.of(
                sig(DataType.LONG, List.of(DataType.DATETIME, DataType.INTEGER, DataType.KEYWORD, DataType.DATE_PERIOD)),
                sig(DataType.LONG, List.of(DataType.DATETIME, DataType.INTEGER, DataType.TEXT, DataType.TIME_DURATION))
            ),
            Map.of(
                DataType.LONG,
                Set.of(
                    List.of(cell(DataType.DATETIME), cell(DataType.INTEGER), cell(DataType.KEYWORD), cell(DataType.DATE_PERIOD)),
                    List.of(cell(DataType.DATETIME), cell(DataType.INTEGER), cell(DataType.TEXT), cell(DataType.TIME_DURATION))
                )
            )
        );
    }

    public void testMergeMultipleSignatures() {
        assertMerged(
            ARGS,
            Set.of(
                sig(DataType.LONG, List.of(DataType.DATETIME, DataType.INTEGER, DataType.KEYWORD, DataType.DATE_PERIOD)),
                sig(DataType.LONG, List.of(DataType.DATETIME, DataType.LONG, DataType.KEYWORD, DataType.DATE_PERIOD)),
                sig(DataType.LONG, List.of(DataType.DATETIME, DataType.DOUBLE, DataType.KEYWORD, DataType.DATE_PERIOD))
            ),
            Map.of(
                DataType.LONG,
                Set.of(
                    List.of(
                        cell(DataType.DATETIME),
                        cell(DataType.INTEGER, DataType.LONG, DataType.DOUBLE),
                        cell(DataType.KEYWORD),
                        cell(DataType.DATE_PERIOD)
                    )
                )
            )
        );
    }

    public void testMergeMultipleReturnTypes() {
        assertMerged(
            ARGS,
            Set.of(
                sig(DataType.LONG, List.of(DataType.INTEGER, DataType.KEYWORD)),
                sig(DataType.DOUBLE, List.of(DataType.INTEGER, DataType.KEYWORD)),
                sig(DataType.DOUBLE, List.of(DataType.LONG, DataType.KEYWORD))
            ),
            Map.of(
                DataType.LONG,
                Set.of(List.of(cell(), cell(DataType.INTEGER), cell(DataType.KEYWORD), cell())),
                DataType.DOUBLE,
                Set.of(List.of(cell(), cell(DataType.INTEGER, DataType.LONG), cell(DataType.KEYWORD), cell()))
            )
        );
    }

    public void testAlignmentWithLeadingOptional() {
        assertMerged(
            ARGS,
            Set.of(
                sig(DataType.LONG, List.of(DataType.DATETIME, DataType.INTEGER, DataType.KEYWORD)),
                sig(DataType.LONG, List.of(DataType.DOUBLE, DataType.INTEGER, DataType.KEYWORD)),
                sig(DataType.LONG, List.of(DataType.INTEGER, DataType.KEYWORD, DataType.DATE_PERIOD))
            ),
            Map.of(
                DataType.LONG,
                Set.of(
                    List.of(cell(DataType.DATETIME, DataType.DOUBLE), cell(DataType.INTEGER), cell(DataType.KEYWORD), cell()),
                    List.of(cell(), cell(DataType.INTEGER), cell(DataType.KEYWORD), cell(DataType.DATE_PERIOD))
                )
            )
        );
    }

    public void testMergeWithSameAppliesTo() {
        assertMerged(
            ARGS,
            Set.of(
                sig(DataType.LONG, param(DataType.INTEGER, GA_9_1), param(DataType.KEYWORD)),
                sig(DataType.LONG, param(DataType.LONG, GA_9_1), param(DataType.KEYWORD))
            ),
            Map.of(
                DataType.LONG,
                Set.of(List.of(cell(), cell(Set.of(GA_9_1), DataType.INTEGER, DataType.LONG), cell(DataType.KEYWORD), cell()))
            )
        );
    }

    public void testDontMergeWithDifferentAppliesTo() {
        assertMerged(
            ARGS,
            Set.of(
                sig(DataType.LONG, param(DataType.INTEGER, GA_9_1), param(DataType.KEYWORD)),
                sig(DataType.LONG, param(DataType.LONG, PREVIEW_9_3), param(DataType.KEYWORD))
            ),
            Map.of(
                DataType.LONG,
                Set.of(
                    List.of(cell(), cell(Set.of(GA_9_1), DataType.INTEGER), cell(DataType.KEYWORD), cell()),
                    List.of(cell(), cell(Set.of(PREVIEW_9_3), DataType.LONG), cell(DataType.KEYWORD), cell())
                )
            )
        );
    }

    public void testDontMergeMixedDifferences() {
        assertMerged(
            ARGS,
            Set.of(
                sig(
                    DataType.LONG,
                    param(DataType.DATETIME),
                    param(DataType.INTEGER, GA_9_1),
                    param(DataType.KEYWORD),
                    param(DataType.DATE_PERIOD)
                ),
                sig(
                    DataType.LONG,
                    param(DataType.DATETIME),
                    param(DataType.INTEGER, PREVIEW_9_3),
                    param(DataType.TEXT),
                    param(DataType.DATE_PERIOD)
                )
            ),
            Map.of(
                DataType.LONG,
                Set.of(
                    List.of(
                        cell(DataType.DATETIME),
                        cell(Set.of(GA_9_1), DataType.INTEGER),
                        cell(DataType.KEYWORD),
                        cell(DataType.DATE_PERIOD)
                    ),
                    List.of(
                        cell(DataType.DATETIME),
                        cell(Set.of(PREVIEW_9_3), DataType.INTEGER),
                        cell(DataType.TEXT),
                        cell(DataType.DATE_PERIOD)
                    )
                )
            )
        );
    }

    /**
     * Two signatures that differ only at an optional-parameter position: one provides the trailing optional,
     * the other does not (EMPTY). These must NOT be merged — "no parameter" is not a type that can be unioned.
     */
    public void testDontMergeEmptyWithNonEmpty() {
        assertMerged(
            ARGS,
            Set.of(
                sig(DataType.LONG, List.of(DataType.INTEGER, DataType.KEYWORD)),
                sig(DataType.LONG, List.of(DataType.INTEGER, DataType.KEYWORD, DataType.DATE_PERIOD))
            ),
            Map.of(
                DataType.LONG,
                Set.of(
                    List.of(cell(), cell(DataType.INTEGER), cell(DataType.KEYWORD), cell()),
                    List.of(cell(), cell(DataType.INTEGER), cell(DataType.KEYWORD), cell(DataType.DATE_PERIOD))
                )
            )
        );
    }

    /**
     * Verifies that the merge result is deterministic regardless of input iteration order.
     * The pivot row {@code (double, double)} can merge with either the {@code field=double} group via column 0
     * or the {@code max=double} group via column 1. Sorting rows before reduction must canonicalize the outcome.
     * Exhaustively tests all 5! = 120 permutations of the input signatures.
     */
    public void testDeterministicMergeWithPivotRow() throws Exception {
        List<DocsV3Support.TypeSignature> sigs = List.of(
            sig2(DataType.DOUBLE, DataType.DOUBLE, DataType.DOUBLE),
            sig2(DataType.DOUBLE, DataType.DOUBLE, DataType.INTEGER),
            sig2(DataType.DOUBLE, DataType.DOUBLE, DataType.LONG),
            sig2(DataType.DOUBLE, DataType.INTEGER, DataType.DOUBLE),
            sig2(DataType.DOUBLE, DataType.LONG, DataType.DOUBLE)
        );

        var expected = new AtomicReference<Map<DataType, Set<List<DocsV3SupportSignaturesMerger.ParamCell>>>>();
        forEachPermutation(sigs, newSigs -> {
            Set<DocsV3Support.TypeSignature> sigSet = new LinkedHashSet<>(newSigs);
            Map<DataType, Set<List<DocsV3SupportSignaturesMerger.ParamCell>>> result = DocsV3SupportSignaturesMerger.buildMergedTypesTable(
                ARGS_2,
                sigSet
            );
            if (expected.get() == null) {
                expected.set(result);
            } else {
                assertThat("Merge result should be deterministic regardless of input order: " + newSigs, result, equalTo(expected.get()));
            }
        });
    }

    /**
     * Asserts the exact merge result for the pivot-row scenario.
     * With sorted reduction, {@code (double, double)} merges with {@code (double, integer)} and {@code (double, long)}
     * via column 1 (alphabetically first), leaving {@code (integer, double)} and {@code (long, double)} to merge
     * via column 0.
     */
    public void testPivotRowMergesConsistently() {
        assertMerged(
            ARGS_2,
            Set.of(
                sig2(DataType.DOUBLE, DataType.DOUBLE, DataType.DOUBLE),
                sig2(DataType.DOUBLE, DataType.DOUBLE, DataType.INTEGER),
                sig2(DataType.DOUBLE, DataType.DOUBLE, DataType.LONG),
                sig2(DataType.DOUBLE, DataType.INTEGER, DataType.DOUBLE),
                sig2(DataType.DOUBLE, DataType.LONG, DataType.DOUBLE)
            ),
            Map.of(
                DataType.DOUBLE,
                Set.of(
                    List.of(cell(DataType.DOUBLE), cell(DataType.DOUBLE, DataType.INTEGER, DataType.LONG)),
                    List.of(cell(DataType.INTEGER, DataType.LONG), cell(DataType.DOUBLE))
                )
            )
        );
    }

    private static DocsV3Support.Param param(DataType type, FunctionAppliesTo... appliesTo) {
        return new DocsV3Support.Param(type, List.of(appliesTo));
    }

    private static DocsV3Support.TypeSignature sig(DataType returnType, List<DataType> argTypes) {
        List<DocsV3Support.Param> params = argTypes.stream().map(t -> new DocsV3Support.Param(t, List.of())).collect(Collectors.toList());
        return new DocsV3Support.TypeSignature(params, returnType);
    }

    private static DocsV3Support.TypeSignature sig(DataType returnType, DocsV3Support.Param... params) {
        return new DocsV3Support.TypeSignature(List.of(params), returnType);
    }

    private static DocsV3Support.TypeSignature sig2(DataType returnType, DataType arg1Type, DataType arg2Type) {
        return new DocsV3Support.TypeSignature(
            List.of(new DocsV3Support.Param(arg1Type, List.of()), new DocsV3Support.Param(arg2Type, List.of())),
            returnType
        );
    }

    private static DocsV3SupportSignaturesMerger.ParamCell cell(DataType... types) {
        if (types.length == 0) {
            return ParamCell.EMPTY;
        }
        return new ParamCell(Set.of(types), Set.of());
    }

    private static ParamCell cell(Set<FunctionAppliesTo> appliesTo, DataType... types) {
        return new DocsV3SupportSignaturesMerger.ParamCell(Set.of(types), appliesTo);
    }

    private static void assertMerged(
        List<EsqlFunctionRegistry.ArgSignature> args,
        Set<DocsV3Support.TypeSignature> signatures,
        Map<DataType, Set<List<ParamCell>>> expected
    ) {
        Map<DataType, Set<List<ParamCell>>> actual = DocsV3SupportSignaturesMerger.buildMergedTypesTable(args, signatures);
        assertThat(actual, equalTo(expected));
    }

    private static <T> void forEachPermutation(List<T> items, CheckedConsumer<List<T>, ? extends Exception> consumer) throws Exception {
        if (items.size() <= 1) {
            consumer.accept(new ArrayList<>(items));
            return;
        }
        for (int i = 0; i < items.size(); i++) {
            T head = items.get(i);
            List<T> rest = new ArrayList<>(items);
            rest.remove(i);
            forEachPermutation(rest, tailPerm -> {
                List<T> perm = new ArrayList<>(items.size());
                perm.add(head);
                perm.addAll(tailPerm);
                consumer.accept(perm);
            });
        }
    }
}
