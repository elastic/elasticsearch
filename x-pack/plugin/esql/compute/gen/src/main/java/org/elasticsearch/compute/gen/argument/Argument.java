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

import org.elasticsearch.compute.ann.Fixed;
import org.elasticsearch.compute.ann.Position;
import org.elasticsearch.compute.gen.Types;

import java.util.List;

import javax.lang.model.element.VariableElement;
import javax.lang.model.type.ArrayType;
import javax.lang.model.type.TypeKind;
import javax.lang.model.type.TypeMirror;

import static org.elasticsearch.compute.gen.argument.StandardArgument.isBlockType;

/**
 * An argument to the evaluator or aggregation method.
 */
public interface Argument {
    static Argument fromParameter(javax.lang.model.util.Types types, VariableElement v) {
        TypeName type = TypeName.get(v.asType());
        String name = v.getSimpleName().toString();
        Fixed fixed = v.getAnnotation(Fixed.class);
        if (fixed != null) {
            return new FixedArgument(
                type,
                name,
                fixed.includeInToString(),
                fixed.scope(),
                Types.extendsSuper(types, v.asType(), "org.elasticsearch.core.Releasable")
            );
        }

        Position position = v.getAnnotation(Position.class);
        if (position != null) {
            return new PositionArgument();
        }

        if (type instanceof ClassName c
            && c.simpleName().equals("Builder")
            && c.enclosingClassName() != null
            && c.enclosingClassName().simpleName().endsWith("Block")) {
            return new BuilderArgument(c, name);
        }
        if (v.asType().getKind() == TypeKind.ARRAY) {
            TypeMirror componentType = ((ArrayType) v.asType()).getComponentType();
            return new ArrayArgument(TypeName.get(componentType), name);
        }
        if (isBlockType(type)) {
            return new BlockArgument(type, name);
        }
        return new StandardArgument(type, name);
    }

    /**
     * Type containing the actual data for a page of values for this field. Usually a
     * Block or Vector, but for fixed fields will be the original fixed type.
     */
    TypeName dataType(boolean blockStyle);

    /**
     * The parameter passed to the real evaluation function
     */
    String paramName(boolean blockStyle);

    /**
     * Declare any required fields for the evaluator to implement this type of parameter.
     */
    void declareField(TypeSpec.Builder builder);

    /**
     * Declare any required fields for the evaluator factory to implement this type of parameter.
     */
    void declareFactoryField(TypeSpec.Builder builder);

    /**
     * Implement the ctor for this parameter. Will declare parameters
     * and assign values to declared fields.
     */
    void implementCtor(MethodSpec.Builder builder);

    /**
     * Implement the ctor for the evaluator factory for this parameter.
     * Will declare parameters and assign values to declared fields.
     */
    void implementFactoryCtor(MethodSpec.Builder builder);

    /**
     * Invocation called in the ExpressionEvaluator.Factory#get method to
     * convert from whatever the factory holds to what the evaluator needs,
     * or {@code null} this parameter isn't passed to the evaluator's ctor.
     */
    String factoryInvocation(MethodSpec.Builder factoryMethodBuilder);

    /**
     * Emits code to evaluate this parameter to a Block or array of Blocks
     * and begins a {@code try} block for those refs. Noop if the parameter is {@link Fixed}.
     */
    void evalToBlock(MethodSpec.Builder builder);

    /**
     * Closes the {@code try} block emitted by {@link #evalToBlock} if it made one.
     * Noop otherwise.
     */
    void closeEvalToBlock(MethodSpec.Builder builder);

    /**
     * Emits code to check if this parameter is a vector or a block, and to
     * call the block flavored evaluator if this is a block. Noop if the
     * parameter is {@link Fixed}.
     */
    void resolveVectors(MethodSpec.Builder builder, String invokeBlockEval);

    /**
     * Create any scratch structures needed by {@code eval}.
     */
    void createScratch(MethodSpec.Builder builder);

    /**
     * Skip any null values in blocks containing this field.
     */
    void skipNull(MethodSpec.Builder builder);

    /**
     * Skip any null values in blocks containing this field.
     */
    void allBlocksAreNull(MethodSpec.Builder builder);

    /**
     * Read the value of this parameter to a local variable. For arrays this
     * unpacks the values into an array, otherwise it just reads to a local.
     */
    void read(MethodSpec.Builder builder, boolean blockStyle);

    /**
     * Build the invocation of the process method for this parameter.
     */
    void buildInvocation(StringBuilder pattern, List<Object> args, boolean blockStyle);

    /**
     * Accumulate invocation pattern and arguments to implement {@link Object#toString()}.
     */
    void buildToStringInvocation(StringBuilder pattern, List<Object> args, String prefix);

    /**
     * The string to close this argument or {@code null}.
     */
    String closeInvocation();

    /**
     * Invokes {@code baseRamBytesUsed} on sub-expressions an
     */
    void sumBaseRamBytesUsed(MethodSpec.Builder builder);
}
