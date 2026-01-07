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

import org.elasticsearch.compute.gen.Types;

import java.util.List;

import javax.lang.model.element.Modifier;

import static org.elasticsearch.compute.gen.Types.EXPRESSION_EVALUATOR;
import static org.elasticsearch.compute.gen.Types.EXPRESSION_EVALUATOR_FACTORY;

public record BlockArgument(TypeName type, String name) implements Argument {
    @Override
    public TypeName dataType(boolean blockStyle) {
        return type;
    }

    @Override
    public TypeName elementType() {
        return Types.elementType(type());
    }

    @Override
    public String paramName(boolean blockStyle) {
        return name + "Block";
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
        builder.beginControlFlow("try ($T $LBlock = ($T) $L.eval(page))", type, name, type, name);
    }

    @Override
    public void closeEvalToBlock(MethodSpec.Builder builder) {
        builder.endControlFlow();
    }

    @Override
    public void resolveVectors(MethodSpec.Builder builder, String... invokeBlockEval) {
        // nothing to do
    }

    @Override
    public void createScratch(MethodSpec.Builder builder) {
        // nothing to do
    }

    @Override
    public void skipNull(MethodSpec.Builder builder) {
        StandardArgument.skipNull(builder, paramName(true));
    }

    @Override
    public void allBlocksAreNull(MethodSpec.Builder builder) {
        builder.beginControlFlow("if (!$N.isNull(p))", paramName(true));
        {
            builder.addStatement("allBlocksAreNulls = false");
        }
        builder.endControlFlow();
    }

    @Override
    public void read(MethodSpec.Builder builder, boolean blockStyle) {
        // nothing to do
    }

    @Override
    public void buildInvocation(StringBuilder pattern, List<Object> args, boolean blockStyle) {
        pattern.append("$L");
        args.add(paramName(blockStyle));
    }

    @Override
    public void buildToStringInvocation(StringBuilder pattern, List<Object> args, String prefix) {
        pattern.append(" + $S + $L");
        args.add(prefix + name + "=");
        args.add(name);
    }

    @Override
    public void addContinueIfPositionHasNoValueBlock(MethodSpec.Builder builder) {
        // nothing to do
        // block params don't skip any positions as all values must be passed down to the aggregator
    }

    @Override
    public String closeInvocation() {
        return name;
    }

    @Override
    public void sumBaseRamBytesUsed(MethodSpec.Builder builder) {
        builder.addStatement("baseRamBytesUsed += $L.baseRamBytesUsed()", name);
    }

    @Override
    public void startBlockProcessingLoop(MethodSpec.Builder builder) {
        // nothing to do
    }

    @Override
    public void endBlockProcessingLoop(MethodSpec.Builder builder) {
        // nothing to do
    }
}
