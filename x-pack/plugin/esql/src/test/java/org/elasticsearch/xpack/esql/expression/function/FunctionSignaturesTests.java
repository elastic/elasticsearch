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

    public void testReturnRefPreservesExactType() {
        Set<FunctionSignatures.ConcreteSignature> expanded = FunctionSignatures.expand(signature(new String[] { "STRING" }, "$0"));
        assertThat(
            expanded,
            containsInAnyOrder(
                new FunctionSignatures.ConcreteSignature(List.of(DataType.KEYWORD), DataType.KEYWORD),
                new FunctionSignatures.ConcreteSignature(List.of(DataType.TEXT), DataType.TEXT)
            )
        );
    }

    public void testReturnRefNoText() {
        Set<FunctionSignatures.ConcreteSignature> expanded = FunctionSignatures.expand(
            signature(new String[] { "long|STRING" }, "$0.noText")
        );
        assertThat(
            expanded,
            containsInAnyOrder(
                new FunctionSignatures.ConcreteSignature(List.of(DataType.LONG), DataType.LONG),
                new FunctionSignatures.ConcreteSignature(List.of(DataType.KEYWORD), DataType.KEYWORD),
                new FunctionSignatures.ConcreteSignature(List.of(DataType.TEXT), DataType.KEYWORD)
            )
        );
    }

    public void testReturnRefUnknownModifierRejected() {
        expectThrows(
            IllegalArgumentException.class,
            containsString("unknown return type reference modifier"),
            () -> FunctionSignatures.expand(signature(new String[] { "integer" }, "$0.noCounter"))
        );
    }

    public void testReturnRefOutOfRange() {
        expectThrows(
            IllegalArgumentException.class,
            containsString("out of range"),
            () -> FunctionSignatures.expand(signature(new String[] { "integer" }, "$1"))
        );
    }

    public void testReturnUnionRejected() {
        expectThrows(
            IllegalArgumentException.class,
            containsString("not a union"),
            () -> FunctionSignatures.expand(signature(new String[] { "integer" }, "integer|long"))
        );
    }

    public void testReturnTypeGroupRejected() {
        expectThrows(
            IllegalArgumentException.class,
            containsString("not a type group"),
            () -> FunctionSignatures.expand(signature(new String[] { "integer" }, "NUMERIC"))
        );
    }

    public void testUnknownReturnTypeRejected() {
        expectThrows(
            IllegalArgumentException.class,
            containsString("not a known data type"),
            () -> FunctionSignatures.expand(signature(new String[] { "integer" }, "not_a_type"))
        );
    }

    public void testUnknownParamTypeRejected() {
        expectThrows(
            IllegalArgumentException.class,
            containsString("not a known data type"),
            () -> FunctionSignatures.expand(signature(new String[] { "not_a_type" }, "integer"))
        );
    }

    public void testEmptyUnionPartRejected() {
        // String.split discards a trailing empty segment for "integer|"; use an interior empty part.
        expectThrows(
            IllegalArgumentException.class,
            containsString("empty type"),
            () -> FunctionSignatures.expand(signature(new String[] { "integer||long" }, "integer"))
        );
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
