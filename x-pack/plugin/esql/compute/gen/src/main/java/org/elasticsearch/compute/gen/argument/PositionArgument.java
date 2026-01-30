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

/**
 * The position in a block.
 */
public record PositionArgument(TypeName type, String name) implements Argument {
    @Override
    public TypeName dataType(boolean blockStyle) {
        return TypeName.INT;
    }

    @Override
    public String paramName(boolean blockStyle) {
        // No need to pass it
        return null;
    }

    @Override
    public void declareField(TypeSpec.Builder builder) {
        // Nothing to do
    }

    @Override
    public void declareFactoryField(TypeSpec.Builder builder) {
        // Nothing to do
    }

    @Override
    public void implementCtor(MethodSpec.Builder builder) {
        // Nothing to do
    }

    @Override
    public void implementFactoryCtor(MethodSpec.Builder builder) {
        // Nothing to do
    }

    @Override
    public String factoryInvocation(MethodSpec.Builder factoryMethodBuilder) {
        return null;
    }

    @Override
    public void evalToBlock(MethodSpec.Builder builder) {
        // nothing to do
    }

    @Override
    public void closeEvalToBlock(MethodSpec.Builder builder) {
        // nothing to do
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
        // nothing to do
    }

    @Override
    public void allBlocksAreNull(MethodSpec.Builder builder) {
        // nothing to do
    }

    @Override
    public void read(MethodSpec.Builder builder, boolean blockStyle) {
        // nothing to do
    }

    @Override
    public void buildInvocation(StringBuilder pattern, List<Object> args, boolean blockStyle) {
        pattern.append("p");
    }

    @Override
    public void buildToStringInvocation(StringBuilder pattern, List<Object> args, String prefix) {
        // nothing to do
    }

    @Override
    public String closeInvocation() {
        return null;
    }

    @Override
    public void sumBaseRamBytesUsed(MethodSpec.Builder builder) {}
}
