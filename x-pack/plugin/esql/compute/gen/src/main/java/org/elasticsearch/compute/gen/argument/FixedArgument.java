/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.gen.argument;

import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;

import org.elasticsearch.compute.ann.Fixed;

import java.util.List;
import java.util.function.Function;

import javax.lang.model.element.Modifier;

import static org.elasticsearch.compute.gen.Types.DRIVER_CONTEXT;

public record FixedArgument(TypeName type, String name, boolean includeInToString, Fixed.Scope scope, boolean releasable)
    implements
        Argument {
    @Override
    public TypeName dataType(boolean blockStyle) {
        return type;
    }

    @Override
    public String paramName(boolean blockStyle) {
        // No need to pass it
        return null;
    }

    @Override
    public void declareField(TypeSpec.Builder builder) {
        builder.addField(type, name, Modifier.PRIVATE, Modifier.FINAL);
    }

    @Override
    public void declareFactoryField(TypeSpec.Builder builder) {
        builder.addField(factoryFieldType(), name, Modifier.PRIVATE, Modifier.FINAL);
    }

    @Override
    public void implementCtor(MethodSpec.Builder builder) {
        builder.addParameter(type, name);
        builder.addStatement("this.$L = $L", name, name);
    }

    @Override
    public void implementFactoryCtor(MethodSpec.Builder builder) {
        builder.addParameter(factoryFieldType(), name);
        builder.addStatement("this.$L = $L", name, name);
    }

    private TypeName factoryFieldType() {
        return switch (scope) {
            case SINGLETON -> type;
            case THREAD_LOCAL -> ParameterizedTypeName.get(ClassName.get(Function.class), DRIVER_CONTEXT, type.box());
        };
    }

    @Override
    public String factoryInvocation(MethodSpec.Builder factoryMethodBuilder) {
        return switch (scope) {
            case SINGLETON -> name;
            case THREAD_LOCAL -> name + ".apply(context)";
        };
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
        pattern.append("this.$L");
        args.add(name);
    }

    @Override
    public void buildToStringInvocation(StringBuilder pattern, List<Object> args, String prefix) {
        if (includeInToString) {
            pattern.append(" + $S + $L");
            args.add(prefix + name + "=");
            args.add(name);
        }
    }

    @Override
    public String closeInvocation() {
        return releasable ? name : null;
    }

    @Override
    public void sumBaseRamBytesUsed(MethodSpec.Builder builder) {}
}
