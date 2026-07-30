/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.lang.annotation.Annotation;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class FunctionSignaturesTests extends ESTestCase {

    public void testExpandConcreteParamsAndReturn() {
        Set<FunctionSignatures.ConcreteSignature> expanded = FunctionSignatures.expand(
            signature(new String[] { "integer", "keyword" }, "long")
        );
        assertThat(expanded, hasSize(1));
        assertThat(
            expanded.iterator().next(),
            equalTo(new FunctionSignatures.ConcreteSignature(List.of(DataType.INTEGER, DataType.KEYWORD), DataType.LONG))
        );
    }

    public void testExpandUnion() {
        Set<FunctionSignatures.ConcreteSignature> expanded = FunctionSignatures.expand(
            signature(new String[] { "integer|long" }, "double")
        );
        assertThat(
            expanded,
            containsInAnyOrder(
                new FunctionSignatures.ConcreteSignature(List.of(DataType.INTEGER), DataType.DOUBLE),
                new FunctionSignatures.ConcreteSignature(List.of(DataType.LONG), DataType.DOUBLE)
            )
        );
    }

    public void testExpandTypeGroup() {
        Set<FunctionSignatures.ConcreteSignature> expanded = FunctionSignatures.expand(signature(new String[] { "STRING" }, "keyword"));
        assertThat(
            expanded,
            containsInAnyOrder(
                new FunctionSignatures.ConcreteSignature(List.of(DataType.KEYWORD), DataType.KEYWORD),
                new FunctionSignatures.ConcreteSignature(List.of(DataType.TEXT), DataType.KEYWORD)
            )
        );
    }

    public void testReturnRefFollowsParamWithNoText() {
        Set<FunctionSignatures.ConcreteSignature> expanded = FunctionSignatures.expand(signature(new String[] { "STRING" }, "$0"));
        assertThat(
            expanded,
            containsInAnyOrder(
                new FunctionSignatures.ConcreteSignature(List.of(DataType.KEYWORD), DataType.KEYWORD),
                new FunctionSignatures.ConcreteSignature(List.of(DataType.TEXT), DataType.KEYWORD)
            )
        );
    }

    public void testReturnRefOutOfRange() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> FunctionSignatures.expand(signature(new String[] { "integer" }, "$1"))
        );
        assertThat(e.getMessage(), containsString("out of range"));
    }

    public void testReturnUnionRejected() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> FunctionSignatures.expand(signature(new String[] { "integer" }, "integer|long"))
        );
        assertThat(e.getMessage(), containsString("not a union"));
    }

    public void testReturnTypeGroupRejected() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> FunctionSignatures.expand(signature(new String[] { "integer" }, "NUMERIC"))
        );
        assertThat(e.getMessage(), containsString("not a type group"));
    }

    public void testUnknownReturnTypeRejected() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> FunctionSignatures.expand(signature(new String[] { "integer" }, "not_a_type"))
        );
        assertThat(e.getMessage(), containsString("not a known data type"));
    }

    public void testUnknownParamTypeRejected() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> FunctionSignatures.expand(signature(new String[] { "not_a_type" }, "integer"))
        );
        assertThat(e.getMessage(), containsString("not a known data type"));
    }

    public void testEmptyUnionPartRejected() {
        // String.split discards a trailing empty segment for "integer|"; use an interior empty part.
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> FunctionSignatures.expand(signature(new String[] { "integer||long" }, "integer"))
        );
        assertThat(e.getMessage(), containsString("empty type"));
    }

    private static Signature signature(String[] params, String returnType) {
        return new Signature() {
            @Override
            public Class<? extends Annotation> annotationType() {
                return Signature.class;
            }

            @Override
            public String[] params() {
                return params;
            }

            @Override
            public String returnType() {
                return returnType;
            }
        };
    }
}
