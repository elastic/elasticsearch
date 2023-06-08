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

import static org.elasticsearch.compute.gen.AggregatorImplementer.valueBlockType;
import static org.elasticsearch.compute.gen.AggregatorImplementer.valueVectorType;
import static org.elasticsearch.compute.gen.Methods.findMethod;
import static org.elasticsearch.compute.gen.Methods.findMethodArguments;
import static org.elasticsearch.compute.gen.Methods.findRequiredMethod;
import static org.elasticsearch.compute.gen.Types.AGGREGATOR_STATE_VECTOR;
import static org.elasticsearch.compute.gen.Types.AGGREGATOR_STATE_VECTOR_BUILDER;
import static org.elasticsearch.compute.gen.Types.BIG_ARRAYS;
import static org.elasticsearch.compute.gen.Types.BLOCK;
import static org.elasticsearch.compute.gen.Types.BYTES_REF;
import static org.elasticsearch.compute.gen.Types.GROUPING_AGGREGATOR_FUNCTION;
import static org.elasticsearch.compute.gen.Types.INT_VECTOR;
import static org.elasticsearch.compute.gen.Types.LONG_BLOCK;
import static org.elasticsearch.compute.gen.Types.LONG_VECTOR;
import static org.elasticsearch.compute.gen.Types.PAGE;
import static org.elasticsearch.compute.gen.Types.VECTOR;

/**
 * Implements "GroupingAggregationFunction" from a class containing static methods
 * annotated with {@link Aggregator}.
 * <p>The goal here is the implement an GroupingAggregationFunction who's inner loops
 * don't contain any {@code invokevirtual}s. Instead, we generate a class
 * that calls static methods in the inner loops.
 * <p>A secondary goal is to make the generated code as readable, debuggable,
 * and break-point-able as possible.
 */
public class GroupingAggregatorImplementer {
    private final TypeElement declarationType;
    private final ExecutableElement init;
    private final ExecutableElement combine;
    private final ExecutableElement combineStates;
    private final ExecutableElement evaluateFinal;
    private final ClassName implementation;
    private final TypeName stateType;
    private final boolean valuesIsBytesRef;

    public GroupingAggregatorImplementer(Elements elements, TypeElement declarationType) {
        this.declarationType = declarationType;

        this.init = findRequiredMethod(declarationType, new String[] { "init", "initGrouping" }, e -> true);
        this.stateType = choseStateType();

        this.combine = findRequiredMethod(declarationType, new String[] { "combine" }, e -> {
            if (e.getParameters().size() == 0) {
                return false;
            }
            TypeName firstParamType = TypeName.get(e.getParameters().get(0).asType());
            return firstParamType.isPrimitive() || firstParamType.toString().equals(stateType.toString());
        });
        this.combineStates = findMethod(declarationType, "combineStates");
        this.evaluateFinal = findMethod(declarationType, "evaluateFinal");

        this.implementation = ClassName.get(
            elements.getPackageOf(declarationType).toString(),
            (declarationType.getSimpleName() + "GroupingAggregatorFunction").replace("AggregatorGroupingAggregator", "GroupingAggregator")
        );
        this.valuesIsBytesRef = BYTES_REF.equals(TypeName.get(combine.getParameters().get(combine.getParameters().size() - 1).asType()));
    }

    private TypeName choseStateType() {
        TypeName initReturn = TypeName.get(init.getReturnType());
        if (false == initReturn.isPrimitive()) {
            return initReturn;
        }
        String head = initReturn.toString().substring(0, 1).toUpperCase(Locale.ROOT);
        String tail = initReturn.toString().substring(1);
        return ClassName.get("org.elasticsearch.compute.aggregation", head + tail + "ArrayState");
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
        builder.addJavadoc("{@link $T} implementation for {@link $T}.\n", GROUPING_AGGREGATOR_FUNCTION, declarationType);
        builder.addJavadoc("This class is generated. Do not edit it.");
        builder.addModifiers(Modifier.PUBLIC, Modifier.FINAL);
        builder.addSuperinterface(GROUPING_AGGREGATOR_FUNCTION);
        builder.addField(stateType, "state", Modifier.PRIVATE, Modifier.FINAL);
        builder.addField(TypeName.INT, "channel", Modifier.PRIVATE, Modifier.FINAL);
        builder.addField(Object[].class, "parameters", Modifier.PRIVATE, Modifier.FINAL);

        builder.addMethod(create());
        builder.addMethod(ctor());
        builder.addMethod(addRawInputStartup(LONG_VECTOR));
        builder.addMethod(addRawInputLoop(LONG_VECTOR, valueBlockType(init, combine)));
        builder.addMethod(addRawInputLoop(LONG_VECTOR, valueVectorType(init, combine)));
        builder.addMethod(addRawInputStartup(LONG_BLOCK));
        builder.addMethod(addRawInputLoop(LONG_BLOCK, valueBlockType(init, combine)));
        builder.addMethod(addRawInputLoop(LONG_BLOCK, valueVectorType(init, combine)));
        builder.addMethod(addIntermediateInput());
        builder.addMethod(addIntermediateRowInput());
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
            builder.add("new $T(bigArrays, $T.$L($L))", stateType, declarationType, init.getSimpleName(), args);
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

    private MethodSpec addRawInputStartup(TypeName groupsType) {
        MethodSpec.Builder builder = MethodSpec.methodBuilder("addRawInput");
        builder.addAnnotation(Override.class).addModifiers(Modifier.PUBLIC);
        builder.addParameter(groupsType, "groups").addParameter(PAGE, "page");
        builder.addStatement("$T valuesBlock = page.getBlock(channel)", valueBlockType(init, combine));
        builder.addStatement("assert groups.getPositionCount() == page.getPositionCount()");
        builder.addStatement("$T valuesVector = valuesBlock.asVector()", valueVectorType(init, combine));
        builder.beginControlFlow("if (valuesVector == null)");
        builder.addStatement("addRawInput(groups, valuesBlock)");
        builder.nextControlFlow("else");
        builder.addStatement("addRawInput(groups, valuesVector)");
        builder.endControlFlow();
        return builder.build();
    }

    private MethodSpec addRawInputLoop(TypeName groupsType, TypeName valuesType) {
        boolean groupsIsBlock = groupsType.toString().endsWith("Block");
        boolean valuesIsBlock = valuesType.toString().endsWith("Block");
        MethodSpec.Builder builder = MethodSpec.methodBuilder("addRawInput");
        builder.addModifiers(Modifier.PRIVATE);
        builder.addParameter(groupsType, "groups").addParameter(valuesType, "values");
        if (valuesIsBytesRef) {
            // Add bytes_ref scratch var that will be used for bytes_ref blocks/vectors
            builder.addStatement("$T scratch = new $T()", BYTES_REF, BYTES_REF);
        }
        builder.beginControlFlow("for (int position = 0; position < groups.getPositionCount(); position++)");
        {
            if (groupsIsBlock) {
                builder.beginControlFlow("if (groups.isNull(position))");
                builder.addStatement("continue");
                builder.endControlFlow();
                builder.addStatement("int groupStart = groups.getFirstValueIndex(position)");
                builder.addStatement("int groupEnd = groupStart + groups.getValueCount(position)");
                builder.beginControlFlow("for (int g = groupStart; g < groupEnd; g++)");
                builder.addStatement("int groupId = Math.toIntExact(groups.getLong(g))");
            } else {
                builder.addStatement("int groupId = Math.toIntExact(groups.getLong(position))");
            }

            if (valuesIsBlock) {
                builder.beginControlFlow("if (values.isNull(position))");
                builder.addStatement("state.putNull(groupId)");
                builder.addStatement("continue");
                builder.endControlFlow();
                builder.addStatement("int valuesStart = values.getFirstValueIndex(position)");
                builder.addStatement("int valuesEnd = valuesStart + values.getValueCount(position)");
                builder.beginControlFlow("for (int v = valuesStart; v < valuesEnd; v++)");
                combineRawInput(builder, "values", "v");
                builder.endControlFlow();
            } else {
                combineRawInput(builder, "values", "position");
            }

            if (groupsIsBlock) {
                builder.endControlFlow();
            }
        }
        builder.endControlFlow();
        return builder.build();
    }

    private MethodSpec addRawInputGroupVectorValuesVector() {
        MethodSpec.Builder builder = MethodSpec.methodBuilder("addRawInput");
        builder.addModifiers(Modifier.PRIVATE);
        builder.addParameter(LONG_VECTOR, "groups").addParameter(valueVectorType(init, combine), "values");
        if (valuesIsBytesRef) {
            // Add bytes_ref scratch var that will be used for bytes_ref blocks/vectors
            builder.addStatement("$T scratch = new $T()", BYTES_REF, BYTES_REF);
        }
        builder.beginControlFlow("for (int position = 0; position < groups.getPositionCount(); position++)");
        {
            builder.addStatement("int groupId = Math.toIntExact(groups.getLong(position))");
            combineRawInput(builder, "values", "position");
        }
        builder.endControlFlow();
        return builder.build();
    }

    private MethodSpec addRawInputGroupBlockValuesBlock() {
        MethodSpec.Builder builder = MethodSpec.methodBuilder("addRawInput");
        builder.addModifiers(Modifier.PRIVATE);
        builder.addParameter(LONG_BLOCK, "groups").addParameter(valueBlockType(init, combine), "values");
        if (valuesIsBytesRef) {
            // Add bytes_ref scratch var that will be used for bytes_ref blocks/vectors
            builder.addStatement("$T scratch = new $T()", BYTES_REF, BYTES_REF);
        }
        builder.beginControlFlow("for (int position = 0; position < groups.getPositionCount(); position++)");
        {
            builder.beginControlFlow("if (groups.isNull(position) || values.isNull(position)");
            {
                builder.addStatement("state.putNull(groupId)");
                builder.addStatement("continue");
            }
            builder.endControlFlow();
            builder.addStatement("int groupId = Math.toIntExact(groups.getLong(position))");
            builder.addStatement("int start = values.getFirstValueIndex(position)");
            builder.addStatement("int end = start + values.getValueCount(position)");
            builder.beginControlFlow("for (int i = start; i < end; i++)");
            combineRawInput(builder, "values", "i");
            builder.endControlFlow();
        }
        builder.endControlFlow();
        return builder.build();
    }

    private MethodSpec addRawInputGroupBlockValuesVector() {
        MethodSpec.Builder builder = MethodSpec.methodBuilder("addRawInput");
        builder.addModifiers(Modifier.PRIVATE);
        builder.addParameter(LONG_VECTOR, "groups").addParameter(valueVectorType(init, combine), "values");
        if (valuesIsBytesRef) {
            // Add bytes_ref scratch var that will be used for bytes_ref blocks/vectors
            builder.addStatement("$T scratch = new $T()", BYTES_REF, BYTES_REF);
        }
        builder.beginControlFlow("for (int position = 0; position < groups.getPositionCount(); position++)");
        {
            builder.addStatement("int groupId = Math.toIntExact(groups.getLong(position))");
            combineRawInput(builder, "values", "position");
        }
        builder.endControlFlow();
        return builder.build();
    }

    private void combineRawInput(MethodSpec.Builder builder, String blockVariable, String offsetVariable) {
        if (valuesIsBytesRef) {
            combineRawInputForBytesRef(builder, blockVariable, offsetVariable);
            return;
        }
        TypeName valueType = TypeName.get(combine.getParameters().get(combine.getParameters().size() - 1).asType());
        if (valueType.isPrimitive() == false) {
            throw new IllegalArgumentException("second parameter to combine must be a primitive");
        }
        String secondParameterGetter = "get"
            + valueType.toString().substring(0, 1).toUpperCase(Locale.ROOT)
            + valueType.toString().substring(1);
        TypeName returnType = TypeName.get(combine.getReturnType());
        if (returnType.isPrimitive()) {
            combineRawInputForPrimitive(builder, secondParameterGetter, blockVariable, offsetVariable);
            return;
        }
        if (returnType == TypeName.VOID) {
            combineRawInputForVoid(builder, secondParameterGetter, blockVariable, offsetVariable);
            return;
        }
        throw new IllegalArgumentException("combine must return void or a primitive");
    }

    private void combineRawInputForPrimitive(
        MethodSpec.Builder builder,
        String secondParameterGetter,
        String blockVariable,
        String offsetVariable
    ) {
        builder.addStatement(
            "state.set($T.combine(state.getOrDefault(groupId), $L.$L($L)), groupId)",
            declarationType,
            blockVariable,
            secondParameterGetter,
            offsetVariable
        );
    }

    private void combineRawInputForVoid(
        MethodSpec.Builder builder,
        String secondParameterGetter,
        String blockVariable,
        String offsetVariable
    ) {
        builder.addStatement(
            "$T.combine(state, groupId, $L.$L($L))",
            declarationType,
            blockVariable,
            secondParameterGetter,
            offsetVariable
        );
    }

    private void combineRawInputForBytesRef(MethodSpec.Builder builder, String blockVariable, String offsetVariable) {
        // scratch is a BytesRef var that must have been defined before the iteration starts
        builder.addStatement("$T.combine(state, groupId, $L.getBytesRef($L, scratch))", declarationType, blockVariable, offsetVariable);
    }

    private MethodSpec addIntermediateInput() {
        MethodSpec.Builder builder = MethodSpec.methodBuilder("addIntermediateInput");
        builder.addAnnotation(Override.class).addModifiers(Modifier.PUBLIC);
        builder.addParameter(LONG_VECTOR, "groupIdVector").addParameter(BLOCK, "block");
        builder.addStatement("$T vector = block.asVector()", VECTOR);
        builder.beginControlFlow("if (vector == null || vector instanceof $T == false)", AGGREGATOR_STATE_VECTOR);
        {
            builder.addStatement("throw new RuntimeException($S + block)", "expected AggregatorStateBlock, got:");
            builder.endControlFlow();
        }
        builder.addStatement("@SuppressWarnings($S) $T blobVector = ($T) vector", "unchecked", stateBlockType(), stateBlockType());
        builder.addComment("TODO exchange big arrays directly without funny serialization - no more copying");
        builder.addStatement("$T bigArrays = $T.NON_RECYCLING_INSTANCE", BIG_ARRAYS, BIG_ARRAYS);
        builder.addStatement("$T inState = $L", stateType, callInit());
        builder.addStatement("blobVector.get(0, inState)");
        builder.beginControlFlow("for (int position = 0; position < groupIdVector.getPositionCount(); position++)");
        {
            builder.addStatement("int groupId = Math.toIntExact(groupIdVector.getLong(position))");
            combineStates(builder);
            builder.endControlFlow();
        }
        builder.addStatement("inState.close()");
        return builder.build();
    }

    private void combineStates(MethodSpec.Builder builder) {
        if (combineStates == null) {
            builder.addStatement("state.set($T.combine(state.getOrDefault(groupId), inState.get(position)), groupId)", declarationType);
            return;
        }
        builder.addStatement("$T.combineStates(state, groupId, inState, position)", declarationType);
    }

    private MethodSpec addIntermediateRowInput() {
        MethodSpec.Builder builder = MethodSpec.methodBuilder("addIntermediateRowInput");
        builder.addAnnotation(Override.class).addModifiers(Modifier.PUBLIC);
        builder.addParameter(int.class, "groupId").addParameter(GROUPING_AGGREGATOR_FUNCTION, "input").addParameter(int.class, "position");
        builder.beginControlFlow("if (input.getClass() != getClass())");
        {
            builder.addStatement("throw new IllegalArgumentException($S + getClass() + $S + input.getClass())", "expected ", "; got ");
        }
        builder.endControlFlow();
        builder.addStatement("$T inState = (($T) input).state", stateType, implementation);
        combineStates(builder);
        return builder.build();
    }

    private MethodSpec evaluateIntermediate() {
        MethodSpec.Builder builder = MethodSpec.methodBuilder("evaluateIntermediate");
        builder.addAnnotation(Override.class).addModifiers(Modifier.PUBLIC).returns(BLOCK);
        builder.addParameter(INT_VECTOR, "selected");
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
        builder.addStatement("builder.add(state, selected)");
        builder.addStatement("return builder.build().asBlock()");
        return builder.build();
    }

    private MethodSpec evaluateFinal() {
        MethodSpec.Builder builder = MethodSpec.methodBuilder("evaluateFinal");
        builder.addAnnotation(Override.class).addModifiers(Modifier.PUBLIC).returns(BLOCK);
        builder.addParameter(INT_VECTOR, "selected");
        if (evaluateFinal == null) {
            builder.addStatement("return state.toValuesBlock(selected)");
        } else {
            builder.addStatement("return $T.evaluateFinal(state, selected)", declarationType);
        }
        return builder.build();
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
