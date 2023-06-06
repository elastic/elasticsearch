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
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.VariableElement;
import javax.lang.model.util.Elements;

import static org.elasticsearch.compute.gen.Methods.findMethod;
import static org.elasticsearch.compute.gen.Methods.findMethodArguments;
import static org.elasticsearch.compute.gen.Methods.findRequiredMethod;
import static org.elasticsearch.compute.gen.Types.AGGREGATOR_FUNCTION;
import static org.elasticsearch.compute.gen.Types.AGGREGATOR_STATE_VECTOR;
import static org.elasticsearch.compute.gen.Types.AGGREGATOR_STATE_VECTOR_BUILDER;
import static org.elasticsearch.compute.gen.Types.BIG_ARRAYS;
import static org.elasticsearch.compute.gen.Types.BLOCK;
import static org.elasticsearch.compute.gen.Types.BOOLEAN_BLOCK;
import static org.elasticsearch.compute.gen.Types.BOOLEAN_VECTOR;
import static org.elasticsearch.compute.gen.Types.DOUBLE_BLOCK;
import static org.elasticsearch.compute.gen.Types.DOUBLE_VECTOR;
import static org.elasticsearch.compute.gen.Types.ELEMENT_TYPE;
import static org.elasticsearch.compute.gen.Types.INT_BLOCK;
import static org.elasticsearch.compute.gen.Types.INT_VECTOR;
import static org.elasticsearch.compute.gen.Types.LONG_BLOCK;
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

        this.init = findRequiredMethod(declarationType, new String[] { "init", "initSingle" }, e -> true);
        this.stateType = choseStateType();

        this.combine = findRequiredMethod(declarationType, new String[] { "combine" }, e -> {
            if (e.getParameters().size() == 0) {
                return false;
            }
            TypeName firstParamType = TypeName.get(e.getParameters().get(0).asType());
            return firstParamType.isPrimitive() || firstParamType.toString().equals(stateType.toString());
        });
        this.combineValueCount = findMethod(declarationType, "combineValueCount");
        this.combineStates = findMethod(declarationType, "combineStates");
        this.evaluateFinal = findMethod(declarationType, "evaluateFinal");

        this.implementation = ClassName.get(
            elements.getPackageOf(declarationType).toString(),
            (declarationType.getSimpleName() + "AggregatorFunction").replace("AggregatorAggregator", "Aggregator")
        );
    }

    private TypeName choseStateType() {
        TypeName initReturn = TypeName.get(init.getReturnType());
        if (false == initReturn.isPrimitive()) {
            return initReturn;
        }
        return ClassName.get("org.elasticsearch.compute.aggregation", firstUpper(initReturn.toString()) + "State");
    }

    static String primitiveType(ExecutableElement init, ExecutableElement combine) {
        if (combine != null) {
            // If there's an explicit combine function it's final parameter is the type of the value.
            return combine.getParameters().get(combine.getParameters().size() - 1).asType().toString();
        }
        String initReturn = init.getReturnType().toString();
        switch (initReturn) {
            case "double":
                return "double";
            case "long":
                return "long";
            case "int":
                return "int";
            case "boolean":
                return "boolean";
            default:
                throw new IllegalArgumentException("unknown primitive type for " + initReturn);
        }
    }

    static ClassName valueBlockType(ExecutableElement init, ExecutableElement combine) {
        return switch (primitiveType(init, combine)) {
            case "boolean" -> BOOLEAN_BLOCK;
            case "double" -> DOUBLE_BLOCK;
            case "long" -> LONG_BLOCK;
            case "int" -> INT_BLOCK;
            default -> throw new IllegalArgumentException("unknown block type for " + primitiveType(init, combine));
        };
    }

    static ClassName valueVectorType(ExecutableElement init, ExecutableElement combine) {
        return switch (primitiveType(init, combine)) {
            case "boolean" -> BOOLEAN_VECTOR;
            case "double" -> DOUBLE_VECTOR;
            case "long" -> LONG_VECTOR;
            case "int" -> INT_VECTOR;
            default -> throw new IllegalArgumentException("unknown vector type for " + primitiveType(init, combine));
        };
    }

    public static String firstUpper(String s) {
        String head = s.toString().substring(0, 1).toUpperCase(Locale.ROOT);
        String tail = s.toString().substring(1);
        return head + tail;
    }

    public JavaFile sourceFile() {
        JavaFile.Builder builder = JavaFile.builder(implementation.packageName(), type());
        builder.addFileComment("""
            Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
            or more contributor license agreements. Licensed under the Elastic License
            2.0; you may not use this file except in compliance with the Elastic License
            2.0.""");
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
        builder.addField(Object[].class, "parameters", Modifier.PRIVATE, Modifier.FINAL);

        builder.addMethod(create());
        builder.addMethod(ctor());
        builder.addMethod(addRawInput());
        builder.addMethod(addRawVector());
        builder.addMethod(addRawBlock());
        builder.addMethod(addIntermediateInput());
        builder.addMethod(evaluateIntermediate());
        builder.addMethod(evaluateFinal());
        builder.addMethod(toStringMethod());
        builder.addMethod(close());
        return builder.build();
    }

    private MethodSpec create() {
        MethodSpec.Builder builder = MethodSpec.methodBuilder("create");
        builder.addModifiers(Modifier.PUBLIC, Modifier.STATIC).returns(implementation);
        builder.addParameter(BIG_ARRAYS, "bigArrays").addParameter(TypeName.INT, "channel").addParameter(Object[].class, "parameters");
        builder.addStatement("return new $T(channel, $L, parameters)", implementation, callInit());
        return builder.build();
    }

    private CodeBlock callInit() {
        VariableElement[] initArgs = findMethodArguments(
            init,
            t -> BIG_ARRAYS.equals(TypeName.get(t.asType())) || TypeName.get(Object[].class).equals(TypeName.get(t.asType()))
        );
        assert initArgs.length <= 2 : "Method " + init + " cannot have more than 2 arguments";
        String args = Stream.of(initArgs)
            .map(t -> BIG_ARRAYS.equals(TypeName.get(t.asType())) ? "bigArrays" : "parameters")
            .collect(Collectors.joining(", "));

        CodeBlock.Builder builder = CodeBlock.builder();
        if (init.getReturnType().toString().equals(stateType.toString())) {
            builder.add("$T.$L($L)", declarationType, init.getSimpleName(), args);
        } else {
            builder.add("new $T($T.$L($L))", stateType, declarationType, init.getSimpleName(), args);
        }
        return builder.build();
    }

    private MethodSpec ctor() {
        MethodSpec.Builder builder = MethodSpec.constructorBuilder().addModifiers(Modifier.PUBLIC);
        builder.addParameter(TypeName.INT, "channel");
        builder.addParameter(stateType, "state");
        builder.addParameter(Object[].class, "parameters");
        builder.addStatement("this.channel = channel");
        builder.addStatement("this.state = state");
        builder.addStatement("this.parameters = parameters");
        return builder.build();
    }

    private MethodSpec addRawInput() {
        MethodSpec.Builder builder = MethodSpec.methodBuilder("addRawInput");
        builder.addAnnotation(Override.class).addModifiers(Modifier.PUBLIC).addParameter(PAGE, "page");
        builder.addStatement("$T type = page.getBlock(channel).elementType()", ELEMENT_TYPE);
        builder.beginControlFlow("if (type == $T.NULL)", ELEMENT_TYPE).addStatement("return").endControlFlow();
        builder.addStatement("$T block = page.getBlock(channel)", valueBlockType(init, combine));
        builder.addStatement("$T vector = block.asVector()", valueVectorType(init, combine));
        builder.beginControlFlow("if (vector != null)").addStatement("addRawVector(vector)");
        builder.nextControlFlow("else").addStatement("addRawBlock(block)").endControlFlow();
        return builder.build();
    }

    private MethodSpec addRawVector() {
        MethodSpec.Builder builder = MethodSpec.methodBuilder("addRawVector");
        builder.addModifiers(Modifier.PRIVATE).addParameter(valueVectorType(init, combine), "vector");
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
        builder.addModifiers(Modifier.PRIVATE).addParameter(valueBlockType(init, combine), "block");
        builder.beginControlFlow("for (int p = 0; p < block.getPositionCount(); p++)");
        {
            builder.beginControlFlow("if (block.isNull(p))");
            builder.addStatement("continue");
            builder.endControlFlow();
            builder.addStatement("int start = block.getFirstValueIndex(p)");
            builder.addStatement("int end = start + block.getValueCount(p)");
            builder.beginControlFlow("for (int i = start; i < end; i++)");
            combineRawInput(builder, "block");
            builder.endControlFlow();
        }
        builder.endControlFlow();
        if (combineValueCount != null) {
            builder.addStatement("$T.combineValueCount(state, block.getTotalValueCount())", declarationType);
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
        builder.addStatement("$T vector = block.asVector()", VECTOR);
        builder.beginControlFlow("if (vector == null || vector instanceof $T == false)", AGGREGATOR_STATE_VECTOR);
        {
            builder.addStatement("throw new RuntimeException($S + block)", "expected AggregatorStateBlock, got:");
            builder.endControlFlow();
        }
        builder.addStatement("@SuppressWarnings($S) $T blobVector = ($T) vector", "unchecked", stateBlockType(), stateBlockType());
        builder.addComment("TODO exchange big arrays directly without funny serialization - no more copying");
        builder.addStatement("$T bigArrays = $T.NON_RECYCLING_INSTANCE", BIG_ARRAYS, BIG_ARRAYS);
        builder.addStatement("$T tmpState = $L", stateType, callInit());
        builder.beginControlFlow("for (int i = 0; i < block.getPositionCount(); i++)");
        {
            builder.addStatement("blobVector.get(i, tmpState)");
            combineStates(builder);
            builder.endControlFlow();
        }
        builder.addStatement("tmpState.close()");
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
            case "org.elasticsearch.compute.aggregation.IntState":
                return "intValue";
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
        builder.addStatement("builder.add(state, $T.range(0, 1))", INT_VECTOR);
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
            case "org.elasticsearch.compute.aggregation.IntState":
                builder.addStatement("return $T.newConstantBlockWith(state.intValue(), 1)", INT_BLOCK);
                return;
            case "org.elasticsearch.compute.aggregation.LongState":
                builder.addStatement("return $T.newConstantBlockWith(state.longValue(), 1)", LONG_BLOCK);
                return;
            case "org.elasticsearch.compute.aggregation.DoubleState":
                builder.addStatement("return $T.newConstantBlockWith(state.doubleValue(), 1)", DOUBLE_BLOCK);
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

    private MethodSpec close() {
        MethodSpec.Builder builder = MethodSpec.methodBuilder("close");
        builder.addAnnotation(Override.class).addModifiers(Modifier.PUBLIC);
        builder.addStatement("state.close()");
        return builder.build();
    }

    private ParameterizedTypeName stateBlockType() {
        return ParameterizedTypeName.get(AGGREGATOR_STATE_VECTOR, stateType);
    }
}
