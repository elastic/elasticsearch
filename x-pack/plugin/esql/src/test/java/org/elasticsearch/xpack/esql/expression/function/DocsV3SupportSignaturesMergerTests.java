/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.util.List;
import java.util.Map;
import java.util.Set;
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

    private static final FunctionAppliesTo GA_9_1 = TestCaseSupplier.appliesTo(FunctionAppliesToLifecycle.GA, "9.1.0", "", true);
    private static final FunctionAppliesTo PREVIEW_9_3 = TestCaseSupplier.appliesTo(FunctionAppliesToLifecycle.PREVIEW, "9.3.0", "", true);

    public void testNoSignatures() {
        assertMerged(List.of(), Map.of());
    }

    public void testSingleSignature() {
        assertMerged(
            List.of(sig(DataType.LONG, List.of(DataType.INTEGER, DataType.KEYWORD))),
            Map.of(DataType.LONG, Set.of(List.of(cell(), cell(DataType.INTEGER), cell(DataType.KEYWORD), cell())))
        );
    }

    public void testMergeTwoSignatures() {
        assertMerged(
            List.of(
                sig(DataType.LONG, List.of(DataType.INTEGER, DataType.KEYWORD)),
                sig(DataType.LONG, List.of(DataType.INTEGER, DataType.TEXT))
            ),
            Map.of(DataType.LONG, Set.of(List.of(cell(), cell(DataType.INTEGER), cell(DataType.KEYWORD, DataType.TEXT), cell())))
        );
    }

    public void testDontMergeSignaturesMultipleDifferences() {
        assertMerged(
            List.of(
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
            List.of(
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
            List.of(
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
            List.of(
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
            List.of(
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
            List.of(
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
            List.of(
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

    private static ParamCell cell(DataType... types) {
        if (types.length == 0) {
            return ParamCell.EMPTY;
        }
        return new ParamCell(Set.of(types), Set.of());
    }

    private static ParamCell cell(Set<FunctionAppliesTo> appliesTo, DataType... types) {
        return new ParamCell(Set.of(types), appliesTo);
    }

    private static void assertMerged(List<DocsV3Support.TypeSignature> signatures, Map<DataType, Set<List<ParamCell>>> expected) {
        Map<DataType, Set<List<ParamCell>>> actual = DocsV3SupportSignaturesMerger.buildMergedTypesTable(ARGS, Set.copyOf(signatures));
        assertThat(actual, equalTo(expected));
    }
}
