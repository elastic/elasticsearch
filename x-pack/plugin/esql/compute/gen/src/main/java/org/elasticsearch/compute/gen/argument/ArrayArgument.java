/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.gen.argument;

import com.squareup.javapoet.ArrayTypeName;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;

import java.util.Arrays;
import java.util.List;

import javax.lang.model.element.Modifier;

import static org.elasticsearch.compute.gen.Methods.getMethod;
import static org.elasticsearch.compute.gen.Types.EXPRESSION_EVALUATOR;
import static org.elasticsearch.compute.gen.Types.EXPRESSION_EVALUATOR_FACTORY;
import static org.elasticsearch.compute.gen.Types.RELEASABLE;
import static org.elasticsearch.compute.gen.Types.RELEASABLES;
import static org.elasticsearch.compute.gen.Types.blockType;
import static org.elasticsearch.compute.gen.Types.vectorType;

public record ArrayArgument(TypeName type, String name) implements Argument {
    @Override
    public TypeName dataType(boolean blockStyle) {
        if (blockStyle) {
            return ArrayTypeName.of(blockType(type));
        }
        return ArrayTypeName.of(vectorType(type));
    }

    @Override
    public boolean supportsVectorReadAccess() {
        return vectorType(type) != null;
    }

    @Override
    public String paramName(boolean blockStyle) {
        return name + (blockStyle ? "Block" : "Vector") + "s";
    }

    @Override
    public void declareField(TypeSpec.Builder builder) {
        builder.addField(ArrayTypeName.of(EXPRESSION_EVALUATOR), name, Modifier.PRIVATE, Modifier.FINAL);
    }

    @Override
    public void declareFactoryField(TypeSpec.Builder builder) {
        builder.addField(ArrayTypeName.of(EXPRESSION_EVALUATOR_FACTORY), name, Modifier.PRIVATE, Modifier.FINAL);
    }

    @Override
    public void implementCtor(MethodSpec.Builder builder) {
        builder.addParameter(ArrayTypeName.of(EXPRESSION_EVALUATOR), name);
        builder.addStatement("this.$L = $L", name, name);
    }

    @Override
    public void implementFactoryCtor(MethodSpec.Builder builder) {
        builder.addParameter(ArrayTypeName.of(EXPRESSION_EVALUATOR_FACTORY), name);
        builder.addStatement("this.$L = $L", name, name);
    }

    @Override
    public String factoryInvocation(MethodSpec.Builder factoryMethodBuilder) {
        factoryMethodBuilder.addStatement(
            "$T[] $L = Arrays.stream(this.$L).map(a -> a.get(context)).toArray($T[]::new)",
            EXPRESSION_EVALUATOR,
            name,
            name,
            EXPRESSION_EVALUATOR
        );
        return name;
    }

    @Override
    public void evalToBlock(MethodSpec.Builder builder) {
        TypeName blockType = blockType(type);
        builder.addStatement("$T[] $LBlocks = new $T[$L.length]", blockType, name, blockType, name);
        builder.beginControlFlow("try ($T $LRelease = $T.wrap($LBlocks))", RELEASABLE, name, RELEASABLES, name);
        builder.beginControlFlow("for (int i = 0; i < $LBlocks.length; i++)", name);
        {
            builder.addStatement("$LBlocks[i] = ($T)$L[i].eval(page)", name, blockType, name);
        }
        builder.endControlFlow();
    }

    @Override
    public void closeEvalToBlock(MethodSpec.Builder builder) {
        builder.endControlFlow();
    }

    @Override
    public void resolveVectors(MethodSpec.Builder builder, String... invokeBlockEval) {
        assert invokeBlockEval != null && invokeBlockEval.length == 1;
        TypeName vectorType = vectorType(type);
        builder.addStatement("$T[] $LVectors = new $T[$L.length]", vectorType, name, vectorType, name);
        builder.beginControlFlow("for (int i = 0; i < $LBlocks.length; i++)", name);
        builder.addStatement("$LVectors[i] = $LBlocks[i].asVector()", name, name);
        builder.beginControlFlow("if ($LVectors[i] == null)", name).addStatement(invokeBlockEval[0]).endControlFlow();
        builder.endControlFlow();
    }

    @Override
    public void createScratch(MethodSpec.Builder builder) {
        builder.addStatement("$T[] $LValues = new $T[$L.length]", type, name, type, name);
        ClassName scratchType = scratchType();
        if (scratchType != null) {
            builder.addStatement("$T[] $LScratch = new $T[$L.length]", scratchType, name, scratchType, name);
            builder.beginControlFlow("for (int i = 0; i < $L.length; i++)", name);
            builder.addStatement("$LScratch[i] = new $T()", name, scratchType);
            builder.endControlFlow();
        }
    }

    @Override
    public void skipNull(MethodSpec.Builder builder) {
        builder.beginControlFlow("for (int i = 0; i < $L.length; i++)", paramName(true));
        StandardArgument.skipNull(builder, paramName(true) + "[i]");
        builder.endControlFlow();
    }

    @Override
    public void allBlocksAreNull(MethodSpec.Builder builder) {
        // nothing to do
    }

    @Override
    public void read(MethodSpec.Builder builder, boolean blockStyle) {
        builder.addComment("unpack $L into $LValues", paramName(blockStyle), name);
        builder.beginControlFlow("for (int i = 0; i < $L.length; i++)", paramName(blockStyle));
        String lookupVar;
        if (blockStyle) {
            lookupVar = "o";
            builder.addStatement("int o = $LBlocks[i].getFirstValueIndex(p)", name);
        } else {
            lookupVar = "p";
        }
        if (scratchType() != null) {
            builder.addStatement("$LValues[i] = $L[i].$L($L, $LScratch[i])", name, paramName(blockStyle), getMethod(type), lookupVar, name);
        } else {
            builder.addStatement("$LValues[i] = $L[i].$L($L)", name, paramName(blockStyle), getMethod(type), lookupVar);
        }
        builder.endControlFlow();
    }

    @Override
    public void buildInvocation(StringBuilder pattern, List<Object> args, boolean blockStyle) {
        pattern.append("$LValues");
        args.add(name);
    }

    @Override
    public void buildToStringInvocation(StringBuilder pattern, List<Object> args, String prefix) {
        pattern.append(" + $S + $T.toString($L)");
        args.add(prefix + name + "=");
        args.add(Arrays.class);
        args.add(name);
    }

    @Override
    public String closeInvocation() {
        return "() -> Releasables.close(" + name + ")";
    }

    @Override
    public void sumBaseRamBytesUsed(MethodSpec.Builder builder) {
        builder.beginControlFlow("for ($T e : $L)", EXPRESSION_EVALUATOR, name);
        builder.addStatement("baseRamBytesUsed += e.baseRamBytesUsed()");
        builder.endControlFlow();
    }
}
