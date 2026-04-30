/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.gen.argument;

import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;

import java.util.List;
import java.util.function.Consumer;

import javax.lang.model.element.Modifier;

import static org.elasticsearch.compute.gen.Types.EXPRESSION_EVALUATOR;
import static org.elasticsearch.compute.gen.Types.EXPRESSION_EVALUATOR_FACTORY;
import static org.elasticsearch.compute.gen.Types.LONG_RANGE;
import static org.elasticsearch.compute.gen.Types.LONG_RANGE_BLOCK;

/**
 * Argument handler for {@code LongRangeBlockBuilder.LongRange} typed parameters in {@code @Evaluator} process methods.
 * {@code LongRangeBlock} has no vector equivalent, so only the block evaluation path is generated.
 * Values are read by constructing a {@code LongRange} from the block's {@code getFromBlock()} and {@code getToBlock()} sub-blocks.
 * Multi-valued positions are rejected (null + warning), consistent with all other scalar functions.
 */
public record LongRangeArgument(TypeName type, String name) implements Argument {

    @Override
    public TypeName dataType(boolean blockStyle) {
        if (blockStyle) {
            return LONG_RANGE_BLOCK;
        }
        return null;
    }

    @Override
    public TypeName elementType() {
        return LONG_RANGE;
    }

    @Override
    public boolean supportsVectorReadAccess() {
        return false;
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
        builder.beginControlFlow("try ($T $LBlock = ($T) $L.eval(page))", LONG_RANGE_BLOCK, name, LONG_RANGE_BLOCK, name);
    }

    @Override
    public void closeEvalToBlock(MethodSpec.Builder builder) {
        builder.endControlFlow();
    }

    @Override
    public void resolveVectors(MethodSpec.Builder builder, Consumer<MethodSpec.Builder> onBlock, Consumer<MethodSpec.Builder> onAllNull) {
        // always block path — nothing to resolve
    }

    @Override
    public void createScratch(MethodSpec.Builder builder) {
        // no scratch needed
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
        // Construct a LongRange from the two sub-blocks at the (single) value index for position p
        builder.addStatement(
            "$T $L = new $T($LBlock.getFromBlock().getLong($LBlock.getFirstValueIndex(p)), "
                + "$LBlock.getToBlock().getLong($LBlock.getFirstValueIndex(p)))",
            LONG_RANGE,
            name,
            LONG_RANGE,
            name,
            name,
            name,
            name
        );
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
    public void addContinueIfPositionHasNoValueBlock(MethodSpec.Builder builder) {
        // not used in non-aggregator context
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
        // not used for LongRange — read() handles position-level access directly
    }

    @Override
    public void endBlockProcessingLoop(MethodSpec.Builder builder) {
        // not used for LongRange
    }

    @Override
    public ClassName scratchType() {
        return null;
    }
}
