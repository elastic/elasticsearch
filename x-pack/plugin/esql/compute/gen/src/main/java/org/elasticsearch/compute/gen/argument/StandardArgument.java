/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.gen.argument;

import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;

import java.util.List;

import javax.lang.model.element.Modifier;

import static org.elasticsearch.compute.gen.Methods.getMethod;
import static org.elasticsearch.compute.gen.Types.BOOLEAN_BLOCK;
import static org.elasticsearch.compute.gen.Types.BYTES_REF;
import static org.elasticsearch.compute.gen.Types.BYTES_REF_BLOCK;
import static org.elasticsearch.compute.gen.Types.DOUBLE_BLOCK;
import static org.elasticsearch.compute.gen.Types.EXPRESSION_EVALUATOR;
import static org.elasticsearch.compute.gen.Types.EXPRESSION_EVALUATOR_FACTORY;
import static org.elasticsearch.compute.gen.Types.INT_BLOCK;
import static org.elasticsearch.compute.gen.Types.LONG_BLOCK;
import static org.elasticsearch.compute.gen.Types.blockType;
import static org.elasticsearch.compute.gen.Types.vectorType;

public record StandardArgument(TypeName type, String name) implements Argument {
    @Override
    public TypeName dataType(boolean blockStyle) {
        if (blockStyle) {
            return isBlockType() ? type : blockType(type);
        }
        return vectorType(type);
    }

    @Override
    public String paramName(boolean blockStyle) {
        return name + (blockStyle ? "Block" : "Vector");
    }

    @Override
    public void declareField(TypeSpec.Builder builder) {
        builder.addField(EXPRESSION_EVALUATOR, name, Modifier.PRIVATE, Modifier.FINAL);
    }

    @Override
    public void declareFactoryField(TypeSpec.Builder builder) {
        builder.addField(EXPRESSION_EVALUATOR_FACTORY, name, Modifier.PRIVATE, Modifier.FINAL);
    }

    @Override
    public void implementCtor(MethodSpec.Builder builder) {
        builder.addParameter(EXPRESSION_EVALUATOR, name);
        builder.addStatement("this.$L = $L", name, name);
    }

    @Override
    public void implementFactoryCtor(MethodSpec.Builder builder) {
        builder.addParameter(EXPRESSION_EVALUATOR_FACTORY, name);
        builder.addStatement("this.$L = $L", name, name);
    }

    @Override
    public String factoryInvocation(MethodSpec.Builder factoryMethodBuilder) {
        return name + ".get(context)";
    }

    @Override
    public void evalToBlock(MethodSpec.Builder builder) {
        TypeName blockType = isBlockType() ? type : blockType(type);
        builder.beginControlFlow("try ($T $LBlock = ($T) $L.eval(page))", blockType, name, blockType, name);
    }

    @Override
    public void closeEvalToBlock(MethodSpec.Builder builder) {
        builder.endControlFlow();
    }

    @Override
    public void resolveVectors(MethodSpec.Builder builder, String invokeBlockEval) {
        builder.addStatement("$T $LVector = $LBlock.asVector()", vectorType(type), name, name);
        builder.beginControlFlow("if ($LVector == null)", name).addStatement(invokeBlockEval).endControlFlow();
    }

    @Override
    public void createScratch(MethodSpec.Builder builder) {
        if (type.equals(BYTES_REF)) {
            builder.addStatement("$T $LScratch = new $T()", BYTES_REF, name, BYTES_REF);
        }
    }

    @Override
    public void skipNull(MethodSpec.Builder builder) {
        skipNull(builder, paramName(true));
    }

    @Override
    public void allBlocksAreNull(MethodSpec.Builder builder) {
        skipNull(builder);
    }

    private boolean isBlockType() {
        return isBlockType(type);
    }

    static boolean isBlockType(TypeName type) {
        return type.equals(INT_BLOCK)
            || type.equals(LONG_BLOCK)
            || type.equals(DOUBLE_BLOCK)
            || type.equals(BOOLEAN_BLOCK)
            || type.equals(BYTES_REF_BLOCK);
    }

    @Override
    public void read(MethodSpec.Builder builder, boolean blockStyle) {
        String params = blockStyle ? paramName(true) + ".getFirstValueIndex(p)" : "p";
        if (type.equals(BYTES_REF)) {
            params += ", " + name + "Scratch";
        }

        builder.addStatement("$T $L = $L.$L($L)", type, name, paramName(blockStyle), getMethod(type), params);
    }

    @Override
    public void buildInvocation(StringBuilder pattern, List<Object> args, boolean blockStyle) {
        pattern.append("$L");
        args.add(name);
    }

    @Override
    public void buildToStringInvocation(StringBuilder pattern, List<Object> args, String prefix) {
        pattern.append(" + $S + $L");
        args.add(prefix + name + "=");
        args.add(name);
    }

    @Override
    public String closeInvocation() {
        return name;
    }

    @Override
    public void sumBaseRamBytesUsed(MethodSpec.Builder builder) {
        builder.addStatement("baseRamBytesUsed += $L.baseRamBytesUsed()", name);
    }

    static void skipNull(MethodSpec.Builder builder, String value) {
        builder.beginControlFlow("if ($N.isNull(p))", value);
        {
            builder.addStatement("result.appendNull()");
            builder.addStatement("continue position");
        }
        builder.endControlFlow();
        builder.beginControlFlow("if ($N.getValueCount(p) != 1)", value);
        {
            builder.beginControlFlow("if ($N.getValueCount(p) > 1)", value);
            {
                builder.addStatement(
                    // TODO: reflection on SingleValueQuery.MULTI_VALUE_WARNING?
                    "warnings().registerException(new $T(\"single-value function encountered multi-value\"))",
                    IllegalArgumentException.class
                );
            }
            builder.endControlFlow();
            builder.addStatement("result.appendNull()");
            builder.addStatement("continue position");
        }
        builder.endControlFlow();
    }
}
