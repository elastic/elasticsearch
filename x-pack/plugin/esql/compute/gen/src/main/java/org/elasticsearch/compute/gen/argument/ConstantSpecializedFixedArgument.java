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
import java.util.function.Consumer;
import java.util.function.Function;

import javax.lang.model.element.Modifier;

import static org.elasticsearch.compute.gen.Types.DRIVER_CONTEXT;

/**
 * Variant of {@link FixedArgument} used when {@code @Fixed(jitConstant = true)} is
 * present on the parameter. Causes the codegen to emit:
 * <ul>
 *   <li>An {@code protected abstract <T> name()} accessor on the evaluator class
 *       (no instance field for the constant)</li>
 *   <li>An override of the accessor on a runtime-generated hidden subclass with
 *       the value baked in as {@code static final} (primitive) or class data
 *       loaded via {@code condy} (reference) — see
 *       {@code org.elasticsearch.compute.operator.ConstantMethodResultSpecializer}</li>
 *   <li>The hot per-row loop calls {@code name()} (which the JIT inlines to the
 *       baked constant) instead of reading {@code this.name}</li>
 *   <li>The Factory's {@code get(DriverContext)} method uses the specializer to
 *       materialise the per-value subclass and constructs an instance via
 *       reflection on the no-jit-args ctor</li>
 * </ul>
 */
public record ConstantSpecializedFixedArgument(TypeName type, String name, boolean includeInToString, Fixed.Scope scope)
    implements
        Argument {

    /**
     * Validates that a parameter type + scope combination is compatible with
     * {@code @Fixed(jitConstant=true)}. Throws {@link IllegalArgumentException} if not. Call
     * before constructing the record so a downstream codegen never sees an invalid shape.
     *
     * <p>Two combinations are forbidden:
     * <ul>
     *   <li><b>{@code scope != SINGLETON}</b> — the specializer caches one hidden class per
     *       {@code (base, accessor, value)} triple; thread-local scope would require a
     *       per-thread specialization model the cache doesn't support.</li>
     *   <li><b>Releasable parameter type</b> — the JIT-constant value lives on the spun
     *       subclass's {@code static final} field (or class-data slot), shared across every
     *       evaluator instance built from that specialization. Per-evaluator {@code close()}
     *       on a shared value would corrupt other live evaluators. Forbidden at codegen
     *       rather than papered over with refcounting.</li>
     * </ul>
     */
    public static void validateJitConstantCompatible(String name, Fixed.Scope scope, boolean releasable) {
        if (scope != Fixed.Scope.SINGLETON) {
            throw new IllegalArgumentException(
                "@Fixed(jitConstant=true) requires SINGLETON scope (parameter: " + name + ", scope: " + scope + ")"
            );
        }
        if (releasable) {
            throw new IllegalArgumentException(
                "@Fixed(jitConstant=true) cannot be applied to a Releasable type — the value would be shared "
                    + "across evaluator instances via the specialized subclass's static field, and per-evaluator "
                    + "close() would corrupt other live evaluators (parameter: "
                    + name
                    + ")"
            );
        }
    }

    @Override
    public boolean isJitConstant() {
        return true;
    }

    @Override
    public TypeName dataType(boolean blockStyle) {
        return type;
    }

    @Override
    public String paramName(boolean blockStyle) {
        // not passed to the per-row processing function — accessor is called instead
        return null;
    }

    /** No instance field — value comes via accessor method. */
    @Override
    public void declareField(TypeSpec.Builder builder) {
        // no field for the jit constant
    }

    /** Accessor method declared as abstract on the evaluator. */
    @Override
    public void declareAbstractAccessor(TypeSpec.Builder builder) {
        builder.addMethod(MethodSpec.methodBuilder(name).addModifiers(Modifier.PROTECTED, Modifier.ABSTRACT).returns(type).build());
    }

    /** Factory still holds the value — needed to pass to the specializer. */
    @Override
    public void declareFactoryField(TypeSpec.Builder builder) {
        builder.addField(factoryFieldType(), name, Modifier.PRIVATE, Modifier.FINAL);
    }

    /** Evaluator ctor takes no param for this — value lives on the constant-specialized subclass. */
    @Override
    public void implementCtor(MethodSpec.Builder builder) {
        // no-op
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

    /**
     * Returns null because for jitConstant args we don't pass the value through the
     * per-instance constructor — instead the factory's get() body uses the specializer
     * (see EvaluatorImplementer.factoryGetWithJitConstants).
     */
    @Override
    public String factoryInvocation(MethodSpec.Builder factoryMethodBuilder) {
        return null;
    }

    @Override
    public void evalToBlock(MethodSpec.Builder builder) {}

    @Override
    public void closeEvalToBlock(MethodSpec.Builder builder) {}

    @Override
    public void resolveVectors(MethodSpec.Builder builder, Consumer<MethodSpec.Builder> onBlock, Consumer<MethodSpec.Builder> onAllNull) {}

    @Override
    public void createScratch(MethodSpec.Builder builder) {}

    @Override
    public void skipNull(MethodSpec.Builder builder) {}

    @Override
    public void allBlocksAreNull(MethodSpec.Builder builder) {}

    @Override
    public void read(MethodSpec.Builder builder, boolean blockStyle) {}

    @Override
    public void buildInvocation(StringBuilder pattern, List<Object> args, boolean blockStyle) {
        // call the accessor, e.g. "this.rhs()" — using `name` resolves to method invocation on `this`
        pattern.append("$L()");
        args.add(name);
    }

    @Override
    public void buildToStringInvocation(StringBuilder pattern, List<Object> args, String prefix) {
        // Evaluator side: no field exists, value lives on the constant-specialized subclass — call the accessor.
        if (includeInToString) {
            pattern.append(" + $S + $L()");
            args.add(prefix + name + "=");
            args.add(name);
        }
    }

    @Override
    public void buildToStringInvocationFromFactory(StringBuilder pattern, List<Object> args, String prefix) {
        // Factory side: value still lives as a regular field on the Factory.
        if (includeInToString) {
            pattern.append(" + $S + $L");
            args.add(prefix + name + "=");
            args.add(name);
        }
    }

    @Override
    public String closeInvocation() {
        // Never emits a close() invocation: validateJitConstantCompatible forbids Releasable types.
        return null;
    }

    @Override
    public void sumBaseRamBytesUsed(MethodSpec.Builder builder) {}
}
