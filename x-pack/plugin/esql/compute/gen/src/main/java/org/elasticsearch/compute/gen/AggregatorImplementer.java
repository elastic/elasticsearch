/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.gen;

import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;

import org.elasticsearch.compute.ann.Aggregator;

import java.util.Locale;
import java.util.Optional;

import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.TypeElement;
import javax.lang.model.util.ElementFilter;
import javax.lang.model.util.Elements;

import static org.elasticsearch.compute.gen.Types.AGGREGATOR_FUNCTION;
import static org.elasticsearch.compute.gen.Types.AGGREGATOR_STATE_VECTOR;
import static org.elasticsearch.compute.gen.Types.AGGREGATOR_STATE_VECTOR_BUILDER;
import static org.elasticsearch.compute.gen.Types.BLOCK;
import static org.elasticsearch.compute.gen.Types.DOUBLE_VECTOR;
import static org.elasticsearch.compute.gen.Types.LONG_VECTOR;
import static org.elasticsearch.compute.gen.Types.PAGE;
import static org.elasticsearch.compute.gen.Types.VECTOR;

/**
 * Implements "AggregationFunction" from a class containing static methods
 * annotated with {@link Aggregator}.
 * <p>The goal here is the implement an AggregationFunction who's inner loops
 * don't contain any {@code invokevirtual}s. Instead, we generate a class
 * that calls static methods in the inner loops.
 * <p>A secondary goal is to make the generated code as readable, debuggable,
 * and break-point-able as possible.
 */
public class AggregatorImplementer {
    private final TypeElement declarationType;
    private final ExecutableElement init;
    private final ExecutableElement combine;
    private final ExecutableElement combineValueCount;
    private final ExecutableElement combineStates;
    private final ExecutableElement evaluateFinal;
    private final ClassName implementation;
    private final TypeName stateType;

    public AggregatorImplementer(Elements elements, TypeElement declarationType) {
        this.declarationType = declarationType;

        ExecutableElement init = null;
        ExecutableElement combine = null;
        ExecutableElement combineValueCount = null;
        ExecutableElement combineStates = null;
        ExecutableElement evaluateFinal = null;
        for (ExecutableElement e : ElementFilter.methodsIn(declarationType.getEnclosedElements())) {
            switch (e.getSimpleName().toString()) {
                case "init":
                    init = e;
                    break;
                case "combine":
                    combine = e;
                    break;
                case "combineValueCount":
                    combineValueCount = e;
                    break;
                case "combineStates":
                    combineStates = e;
                    break;
                case "evaluateFinal":
                    evaluateFinal = e;
                    break;
                default: // pass
            }
        }
        this.init = checkStaticMethod("init", init);
        this.combine = checkStaticMethod("combine", combine);
        this.combineValueCount = checkOptionalStaticMethod("combineValueCount", combineValueCount);
        this.combineStates = checkOptionalStaticMethod("combineStates", combineStates);
        this.evaluateFinal = checkOptionalStaticMethod("evaluateFinal", evaluateFinal);

        this.stateType = choseStateType();
        this.implementation = ClassName.get(
            elements.getPackageOf(declarationType).toString(),
            (declarationType.getSimpleName() + "AggregatorFunction").replace("AggregatorAggregator", "Aggregator")
        );
    }

    static ExecutableElement checkStaticMethod(String name, ExecutableElement e) {
        if (e == null) {
            throw new IllegalArgumentException(name + " is required");
        }
        if (false == e.getModifiers().contains(Modifier.STATIC)) {
            throw new IllegalArgumentException(name + " must be static");
        }
        return e;
    }

    static ExecutableElement checkOptionalStaticMethod(String name, ExecutableElement e) {
        if (e == null) {
            return null;
        }
        if (false == e.getModifiers().contains(Modifier.STATIC)) {
            throw new IllegalArgumentException(name + " must be static if it exists");
        }
        return e;
    }

    private TypeName choseStateType() {
        TypeName initReturn = TypeName.get(init.getReturnType());
        if (false == initReturn.isPrimitive()) {
            return initReturn;
        }
        return ClassName.get("org.elasticsearch.compute.aggregation", firstUpper(initReturn.toString()) + "State");
    }

    public static String firstUpper(String s) {
        String head = s.toString().substring(0, 1).toUpperCase(Locale.ROOT);
        String tail = s.toString().substring(1);
        return head + tail;
    }

    public JavaFile sourceFile() {
        JavaFile.Builder builder = JavaFile.builder(implementation.packageName(), type());
        return builder.build();
    }

    private TypeSpec type() {
        TypeSpec.Builder builder = TypeSpec.classBuilder(implementation);
        builder.addJavadoc("{@link $T} implementation for {@link $T}.\n", AGGREGATOR_FUNCTION, declarationType);
        builder.addJavadoc("This class is generated. Do not edit it.");
        builder.addModifiers(Modifier.PUBLIC, Modifier.FINAL);
        builder.addSuperinterface(AGGREGATOR_FUNCTION);
        builder.addField(stateType, "state", Modifier.PRIVATE, Modifier.FINAL);
        builder.addField(TypeName.INT, "channel", Modifier.PRIVATE, Modifier.FINAL);

        builder.addMethod(create());
        builder.addMethod(ctor());
        builder.addMethod(addRawInput());
        builder.addMethod(addRawVector());
        builder.addMethod(addRawBlock());
        builder.addMethod(addIntermediateInput());
        builder.addMethod(evaluateIntermediate());
        builder.addMethod(evaluateFinal());
        builder.addMethod(toStringMethod());
        return builder.build();
    }

    private MethodSpec create() {
        MethodSpec.Builder builder = MethodSpec.methodBuilder("create");
        builder.addModifiers(Modifier.PUBLIC, Modifier.STATIC).returns(implementation).addParameter(TypeName.INT, "channel");
        builder.addStatement("return new $T(channel, $L)", implementation, callInit());
        return builder.build();
    }

    private CodeBlock callInit() {
        CodeBlock.Builder builder = CodeBlock.builder();
        if (init.getReturnType().toString().equals(stateType.toString())) {
            builder.add("$T.init()", declarationType);
        } else {
            builder.add("new $T($T.init())", stateType, declarationType);
        }
        return builder.build();
    }

    private MethodSpec ctor() {
        MethodSpec.Builder builder = MethodSpec.constructorBuilder().addModifiers(Modifier.PUBLIC);
        builder.addParameter(TypeName.INT, "channel");
        builder.addParameter(stateType, "state");
        builder.addStatement("this.channel = channel");
        builder.addStatement("this.state = state");
        return builder.build();
    }

    private MethodSpec addRawInput() {
        MethodSpec.Builder builder = MethodSpec.methodBuilder("addRawInput");
        builder.addAnnotation(Override.class).addModifiers(Modifier.PUBLIC).addParameter(PAGE, "page");
        builder.addStatement("assert channel >= 0");
        builder.addStatement("$T block = page.getBlock(channel)", BLOCK);
        builder.addStatement("$T vector = block.asVector()", ParameterizedTypeName.get(ClassName.get(Optional.class), VECTOR));
        builder.beginControlFlow("if (vector.isPresent())").addStatement("addRawVector(vector.get())");
        builder.nextControlFlow("else").addStatement("addRawBlock(block)").endControlFlow();
        return builder.build();
    }

    private MethodSpec addRawVector() {
        MethodSpec.Builder builder = MethodSpec.methodBuilder("addRawVector");
        builder.addModifiers(Modifier.PRIVATE).addParameter(VECTOR, "vector");
        builder.beginControlFlow("for (int i = 0; i < vector.getPositionCount(); i++)");
        {
            combineRawInput(builder, "vector");
        }
        builder.endControlFlow();
        if (combineValueCount != null) {
            builder.addStatement("$T.combineValueCount(state, vector.getPositionCount())", declarationType);
        }
        return builder.build();
    }

    private MethodSpec addRawBlock() {
        MethodSpec.Builder builder = MethodSpec.methodBuilder("addRawBlock");
        builder.addModifiers(Modifier.PRIVATE).addParameter(BLOCK, "block");
        builder.beginControlFlow("for (int i = 0; i < block.getTotalValueCount(); i++)");
        {
            builder.beginControlFlow("if (block.isNull(i) == false)");
            combineRawInput(builder, "block");
            builder.endControlFlow();
        }
        builder.endControlFlow();
        if (combineValueCount != null) {
            builder.addStatement("$T.combineValueCount(state, block.validPositionCount())", declarationType);
        }
        return builder.build();
    }

    private void combineRawInput(MethodSpec.Builder builder, String blockVariable) {
        TypeName returnType = TypeName.get(combine.getReturnType());
        if (returnType.isPrimitive()) {
            combineRawInputForPrimitive(returnType, builder, blockVariable);
            return;
        }
        if (returnType == TypeName.VOID) {
            combineRawInputForVoid(builder, blockVariable);
            return;
        }
        throw new IllegalArgumentException("combine must return void or a primitive");
    }

    private void combineRawInputForPrimitive(TypeName returnType, MethodSpec.Builder builder, String blockVariable) {
        builder.addStatement(
            "state.$TValue($T.combine(state.$TValue(), $L.get$L(i)))",
            returnType,
            declarationType,
            returnType,
            blockVariable,
            firstUpper(combine.getParameters().get(1).asType().toString())
        );
    }

    private void combineRawInputForVoid(MethodSpec.Builder builder, String blockVariable) {
        builder.addStatement(
            "$T.combine(state, $L.get$L(i))",
            declarationType,
            blockVariable,
            firstUpper(combine.getParameters().get(1).asType().toString())
        );
    }

    private MethodSpec addIntermediateInput() {
        MethodSpec.Builder builder = MethodSpec.methodBuilder("addIntermediateInput");
        builder.addAnnotation(Override.class).addModifiers(Modifier.PUBLIC).addParameter(BLOCK, "block");
        builder.addStatement("assert channel == -1");
        builder.addStatement("$T vector = block.asVector()", ParameterizedTypeName.get(ClassName.get(Optional.class), VECTOR));
        builder.beginControlFlow("if (vector.isEmpty() || vector.get() instanceof $T == false)", AGGREGATOR_STATE_VECTOR);
        {
            builder.addStatement("throw new RuntimeException($S + block)", "expected AggregatorStateBlock, got:");
            builder.endControlFlow();
        }
        builder.addStatement("@SuppressWarnings($S) $T blobVector = ($T) vector.get()", "unchecked", stateBlockType(), stateBlockType());
        builder.addStatement("$T tmpState = new $T()", stateType, stateType);
        builder.beginControlFlow("for (int i = 0; i < block.getPositionCount(); i++)");
        {
            builder.addStatement("blobVector.get(i, tmpState)");
            combineStates(builder);
            builder.endControlFlow();
        }
        return builder.build();
    }

    private void combineStates(MethodSpec.Builder builder) {
        if (combineStates == null) {
            String m = primitiveStateMethod();
            builder.addStatement("state.$L($T.combine(state.$L(), tmpState.$L()))", m, declarationType, m, m);
            return;
        }
        builder.addStatement("$T.combineStates(state, tmpState)", declarationType);
    }

    private String primitiveStateMethod() {
        switch (stateType.toString()) {
            case "org.elasticsearch.compute.aggregation.LongState":
                return "longValue";
            case "org.elasticsearch.compute.aggregation.DoubleState":
                return "doubleValue";
            default:
                throw new IllegalArgumentException(
                    "don't know how to fetch primitive values from " + stateType + ". define combineStates."
                );
        }
    }

    private MethodSpec evaluateIntermediate() {
        MethodSpec.Builder builder = MethodSpec.methodBuilder("evaluateIntermediate");
        builder.addAnnotation(Override.class).addModifiers(Modifier.PUBLIC).returns(BLOCK);
        ParameterizedTypeName stateBlockBuilderType = ParameterizedTypeName.get(
            AGGREGATOR_STATE_VECTOR_BUILDER,
            stateBlockType(),
            stateType
        );
        builder.addStatement(
            "$T builder =\n$T.builderOfAggregatorState($T.class, state.getEstimatedSize())",
            stateBlockBuilderType,
            AGGREGATOR_STATE_VECTOR,
            stateType
        );
        builder.addStatement("builder.add(state)");
        builder.addStatement("return builder.build().asBlock()");
        return builder.build();
    }

    private MethodSpec evaluateFinal() {
        MethodSpec.Builder builder = MethodSpec.methodBuilder("evaluateFinal");
        builder.addAnnotation(Override.class).addModifiers(Modifier.PUBLIC).returns(BLOCK);
        if (evaluateFinal == null) {
            primitiveStateToResult(builder);
        } else {
            builder.addStatement("return $T.evaluateFinal(state)", declarationType);
        }
        return builder.build();
    }

    private void primitiveStateToResult(MethodSpec.Builder builder) {
        switch (stateType.toString()) {
            case "org.elasticsearch.compute.aggregation.LongState":
                builder.addStatement("return new $T(new long[] { state.longValue() }, 1).asBlock()", LONG_VECTOR);
                return;
            case "org.elasticsearch.compute.aggregation.DoubleState":
                builder.addStatement("return new $T(new double[] { state.doubleValue() }, 1).asBlock()", DOUBLE_VECTOR);
                return;
            default:
                throw new IllegalArgumentException("don't know how to convert state to result: " + stateType);
        }
    }

    private MethodSpec toStringMethod() {
        MethodSpec.Builder builder = MethodSpec.methodBuilder("toString");
        builder.addAnnotation(Override.class).addModifiers(Modifier.PUBLIC).returns(String.class);
        builder.addStatement("$T sb = new $T()", StringBuilder.class, StringBuilder.class);
        builder.addStatement("sb.append(getClass().getSimpleName()).append($S)", "[");
        builder.addStatement("sb.append($S).append(channel)", "channel=");
        builder.addStatement("sb.append($S)", "]");
        builder.addStatement("return sb.toString()");
        return builder.build();
    }

    private ParameterizedTypeName stateBlockType() {
        return ParameterizedTypeName.get(AGGREGATOR_STATE_VECTOR, stateType);
    }
}
