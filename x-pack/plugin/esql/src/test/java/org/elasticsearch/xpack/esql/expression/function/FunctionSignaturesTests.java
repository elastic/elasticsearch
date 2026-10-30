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
import java.util.Arrays;
import java.util.HashSet;
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
        assertThat(expanded.iterator().next(), equalTo(concrete(DataType.LONG, DataType.INTEGER, DataType.KEYWORD)));
    }

    public void testExpandUnion() {
        Set<FunctionSignatures.ConcreteSignature> expanded = FunctionSignatures.expand(
            signature(new String[] { "integer|long" }, "double")
        );
        assertThat(expanded, containsInAnyOrder(concrete(DataType.DOUBLE, DataType.INTEGER), concrete(DataType.DOUBLE, DataType.LONG)));
    }

    public void testExpandTypeGroup() {
        Set<FunctionSignatures.ConcreteSignature> expanded = FunctionSignatures.expand(signature(new String[] { "STRING" }, "keyword"));
        assertThat(expanded, containsInAnyOrder(concrete(DataType.KEYWORD, DataType.KEYWORD), concrete(DataType.KEYWORD, DataType.TEXT)));
    }

    public void testReturnRefPreservesExactType() {
        Set<FunctionSignatures.ConcreteSignature> expanded = FunctionSignatures.expand(signature(new String[] { "STRING" }, "$0"));
        assertThat(expanded, containsInAnyOrder(concrete(DataType.KEYWORD, DataType.KEYWORD), concrete(DataType.TEXT, DataType.TEXT)));
    }

    public void testReturnRefNoText() {
        Set<FunctionSignatures.ConcreteSignature> expanded = FunctionSignatures.expand(
            signature(new String[] { "long|STRING" }, "$0.noText")
        );
        assertThat(
            expanded,
            containsInAnyOrder(
                concrete(DataType.LONG, DataType.LONG),
                concrete(DataType.KEYWORD, DataType.KEYWORD),
                concrete(DataType.KEYWORD, DataType.TEXT)
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

    public void testNamedParametersToken() {
        Set<FunctionSignatures.ConcreteSignature> expanded = FunctionSignatures.expand(
            signature(new String[] { "integer", FunctionSignatures.NAMED_PARAMETERS }, "double")
        );
        assertThat(expanded, hasSize(1));
        FunctionSignatures.ConcreteSignature signature = expanded.iterator().next();
        assertThat(signature.returnType(), equalTo(DataType.DOUBLE));
        assertThat(signature.argTypes().get(0), equalTo(FunctionSignatures.SignatureArg.of(DataType.INTEGER)));
        assertThat(signature.argTypes().get(1).namedParameters(), equalTo(true));
        assertThat(signature.argTypes().get(1).typeName(), equalTo(FunctionSignatures.NAMED_PARAMETERS));
    }

    public void testNamedParametersMixedWithDataTypeRejected() {
        expectThrows(
            IllegalArgumentException.class,
            containsString("mixes " + FunctionSignatures.NAMED_PARAMETERS),
            () -> FunctionSignatures.expand(signature(new String[] { "integer|" + FunctionSignatures.NAMED_PARAMETERS }, "double"))
        );
    }

    public void testNamedParametersReturnRejected() {
        expectThrows(
            IllegalArgumentException.class,
            containsString("return type cannot be " + FunctionSignatures.NAMED_PARAMETERS),
            () -> FunctionSignatures.expand(signature(new String[] { "integer" }, FunctionSignatures.NAMED_PARAMETERS))
        );
    }

    public void testNamedParametersReturnReferenceRejected() {
        expectThrows(
            IllegalArgumentException.class,
            containsString("cannot reference " + FunctionSignatures.NAMED_PARAMETERS),
            () -> FunctionSignatures.expand(signature(new String[] { FunctionSignatures.NAMED_PARAMETERS }, "$0"))
        );
    }

    public void testMapParamsUseNamedParametersToken() {
        EsqlFunctionRegistry registry = new EsqlFunctionRegistry();
        Set<Class<?>> seen = new HashSet<>();
        for (FunctionDefinition def : registry.snapshotRegistry().listFunctions()) {
            if (seen.add(def.clazz()) == false) {
                continue;
            }
            FunctionInfo info = EsqlFunctionRegistry.functionInfo(def);
            if (info == null || info.signatures().length == 0) {
                continue;
            }
            AbstractFunctionTestCase.assertNamedParametersMatchMapParams(
                def.name(),
                EsqlFunctionRegistry.description(def).args(),
                FunctionSignatures.declaredSignatures(def)
            );
        }
    }

    public void testEmptyUnionPartRejected() {
        // String.split discards a trailing empty segment for "integer|"; use an interior empty part.
        expectThrows(
            IllegalArgumentException.class,
            containsString("empty type"),
            () -> FunctionSignatures.expand(signature(new String[] { "integer||long" }, "integer"))
        );
    }

    private static FunctionSignatures.ConcreteSignature concrete(DataType returnType, DataType... argTypes) {
        return new FunctionSignatures.ConcreteSignature(
            Arrays.stream(argTypes).map(FunctionSignatures.SignatureArg::of).toList(),
            returnType
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
