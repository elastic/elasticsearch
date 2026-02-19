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

    public void testNoSignatures() {
        assertMerged(List.of(), Map.of());
    }

    public void testSingleSignature() {
        assertMerged(
            List.of(sig(DataType.LONG, List.of(DataType.INTEGER, DataType.KEYWORD))),
            Map.of(DataType.LONG, Set.of(List.of(Set.of(), Set.of(DataType.INTEGER), Set.of(DataType.KEYWORD), Set.of())))
        );
    }

    public void testMergeTwoSignatures() {
        assertMerged(
            List.of(
                sig(DataType.LONG, List.of(DataType.INTEGER, DataType.KEYWORD)),
                sig(DataType.LONG, List.of(DataType.INTEGER, DataType.TEXT))
            ),
            Map.of(
                DataType.LONG,
                Set.of(List.of(Set.of(), Set.of(DataType.INTEGER), Set.of(DataType.KEYWORD, DataType.TEXT), Set.of()))
            )
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
                    List.of(Set.of(DataType.DATETIME), Set.of(DataType.INTEGER), Set.of(DataType.KEYWORD), Set.of(DataType.DATE_PERIOD)),
                    List.of(Set.of(DataType.DATETIME), Set.of(DataType.INTEGER), Set.of(DataType.TEXT), Set.of(DataType.TIME_DURATION))
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
                        Set.of(DataType.DATETIME),
                        Set.of(DataType.INTEGER, DataType.LONG, DataType.DOUBLE),
                        Set.of(DataType.KEYWORD),
                        Set.of(DataType.DATE_PERIOD)
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
                Set.of(List.of(Set.of(), Set.of(DataType.INTEGER), Set.of(DataType.KEYWORD), Set.of())),
                DataType.DOUBLE,
                Set.of(List.of(Set.of(), Set.of(DataType.INTEGER, DataType.LONG), Set.of(DataType.KEYWORD), Set.of()))
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
                    List.of(Set.of(DataType.DATETIME, DataType.DOUBLE), Set.of(DataType.INTEGER), Set.of(DataType.KEYWORD), Set.of()),
                    List.of(Set.of(), Set.of(DataType.INTEGER), Set.of(DataType.KEYWORD), Set.of(DataType.DATE_PERIOD))
                )
            )
        );
    }

    private static DocsV3Support.TypeSignature sig(DataType returnType, List<DataType> argTypes) {
        List<DocsV3Support.Param> params = argTypes.stream().map(t -> new DocsV3Support.Param(t, List.of())).collect(Collectors.toList());
        return new DocsV3Support.TypeSignature(params, returnType);
    }

    private static void assertMerged(List<DocsV3Support.TypeSignature> signatures, Map<DataType, Set<List<Set<DataType>>>> expected) {
        Map<DataType, Set<List<Set<DataType>>>> actual = DocsV3SupportSignaturesMerger.buildMergedTypesTable(ARGS, Set.copyOf(signatures));
        assertThat(actual, equalTo(expected));
    }
}
