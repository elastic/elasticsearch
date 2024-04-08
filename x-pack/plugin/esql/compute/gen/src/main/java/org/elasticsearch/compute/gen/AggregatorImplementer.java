/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.gen;

import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;

import org.elasticsearch.compute.ann.Aggregator;
import org.elasticsearch.compute.ann.IntermediateState;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;

import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.TypeElement;
import javax.lang.model.util.Elements;

import static java.util.stream.Collectors.joining;
import static org.elasticsearch.compute.gen.Methods.findMethod;
import static org.elasticsearch.compute.gen.Methods.findRequiredMethod;
import static org.elasticsearch.compute.gen.Methods.vectorAccessorName;
import static org.elasticsearch.compute.gen.Types.AGGREGATOR_FUNCTION;
import static org.elasticsearch.compute.gen.Types.BIG_ARRAYS;
import static org.elasticsearch.compute.gen.Types.BLOCK;
import static org.elasticsearch.compute.gen.Types.BLOCK_ARRAY;
import static org.elasticsearch.compute.gen.Types.BOOLEAN_BLOCK;
import static org.elasticsearch.compute.gen.Types.BOOLEAN_VECTOR;
import static org.elasticsearch.compute.gen.Types.BYTES_REF;
import static org.elasticsearch.compute.gen.Types.BYTES_REF_BLOCK;
import static org.elasticsearch.compute.gen.Types.BYTES_REF_VECTOR;
import static org.elasticsearch.compute.gen.Types.DOUBLE_BLOCK;
import static org.elasticsearch.compute.gen.Types.DOUBLE_VECTOR;
import static org.elasticsearch.compute.gen.Types.DRIVER_CONTEXT;
import static org.elasticsearch.compute.gen.Types.ELEMENT_TYPE;
import static org.elasticsearch.compute.gen.Types.INTERMEDIATE_STATE_DESC;
import static org.elasticsearch.compute.gen.Types.INT_BLOCK;
import static org.elasticsearch.compute.gen.Types.INT_VECTOR;
import static org.elasticsearch.compute.gen.Types.LIST_AGG_FUNC_DESC;
import static org.elasticsearch.compute.gen.Types.LIST_INTEGER;
import static org.elasticsearch.compute.gen.Types.LONG_BLOCK;
import static org.elasticsearch.compute.gen.Types.LONG_VECTOR;
import static org.elasticsearch.compute.gen.Types.PAGE;
import static org.elasticsearch.compute.gen.Types.blockType;
import static org.elasticsearch.compute.gen.Types.vectorType;

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
    private final ExecutableElement combineIntermediate;
    private final ExecutableElement evaluateFinal;
    private final ClassName implementation;
    private final TypeName stateType;
    private final boolean stateTypeHasSeen;
    private final boolean valuesIsBytesRef;
    private final List<IntermediateStateDesc> intermediateState;
    private final List<Parameter> createParameters;

    public AggregatorImplementer(Elements elements, TypeElement declarationType, IntermediateState[] interStateAnno) {
        this.declarationType = declarationType;

        this.init = findRequiredMethod(declarationType, new String[] { "init", "initSingle" }, e -> true);
        this.stateType = choseStateType();
        stateTypeHasSeen = elements.getAllMembers(elements.getTypeElement(stateType.toString()))
            .stream()
            .anyMatch(e -> e.toString().equals("seen()"));

        this.combine = findRequiredMethod(declarationType, new String[] { "combine" }, e -> {
            if (e.getParameters().size() == 0) {
                return false;
            }
            TypeName firstParamType = TypeName.get(e.getParameters().get(0).asType());
            return firstParamType.isPrimitive() || firstParamType.toString().equals(stateType.toString());
        });
        this.combineValueCount = findMethod(declarationType, "combineValueCount");
        this.combineIntermediate = findMethod(declarationType, "combineIntermediate");
        this.evaluateFinal = findMethod(declarationType, "evaluateFinal");
        this.createParameters = init.getParameters()
            .stream()
            .map(Parameter::from)
            .filter(f -> false == f.type().equals(BIG_ARRAYS))
            .toList();

        this.implementation = ClassName.get(
            elements.getPackageOf(declarationType).toString(),
            (declarationType.getSimpleName() + "AggregatorFunction").replace("AggregatorAggregator", "Aggregator")
        );
        this.valuesIsBytesRef = BYTES_REF.equals(TypeName.get(combine.getParameters().get(combine.getParameters().size() - 1).asType()));
        intermediateState = Arrays.stream(interStateAnno).map(IntermediateStateDesc::newIntermediateStateDesc).toList();
    }

    ClassName implementation() {
        return implementation;
    }

    List<Parameter> createParameters() {
        return createParameters;
    }

    private TypeName choseStateType() {
        TypeName initReturn = TypeName.get(init.getReturnType());
        if (false == initReturn.isPrimitive()) {
            return initReturn;
        }
        return ClassName.get("org.elasticsearch.compute.aggregation", firstUpper(initReturn.toString()) + "State");
    }

    static String valueType(ExecutableElement init, ExecutableElement combine) {
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
        return switch (valueType(init, combine)) {
            case "boolean" -> BOOLEAN_BLOCK;
            case "double" -> DOUBLE_BLOCK;
            case "long" -> LONG_BLOCK;
            case "int" -> INT_BLOCK;
            case "org.apache.lucene.util.BytesRef" -> BYTES_REF_BLOCK;
            default -> throw new IllegalArgumentException("unknown block type for " + valueType(init, combine));
        };
    }

    static ClassName valueVectorType(ExecutableElement init, ExecutableElement combine) {
        return switch (valueType(init, combine)) {
            case "boolean" -> BOOLEAN_VECTOR;
            case "double" -> DOUBLE_VECTOR;
            case "long" -> LONG_VECTOR;
            case "int" -> INT_VECTOR;
            case "org.apache.lucene.util.BytesRef" -> BYTES_REF_VECTOR;
            default -> throw new IllegalArgumentException("unknown vector type for " + valueType(init, combine));
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
        builder.addField(
            FieldSpec.builder(LIST_AGG_FUNC_DESC, "INTERMEDIATE_STATE_DESC", Modifier.PRIVATE, Modifier.STATIC, Modifier.FINAL)
                .initializer(initInterState())
                .build()
        );
        builder.addField(DRIVER_CONTEXT, "driverContext", Modifier.PRIVATE, Modifier.FINAL);
        builder.addField(stateType, "state", Modifier.PRIVATE, Modifier.FINAL);
        builder.addField(LIST_INTEGER, "channels", Modifier.PRIVATE, Modifier.FINAL);

        for (Parameter p : createParameters) {
            builder.addField(p.type(), p.name(), Modifier.PRIVATE, Modifier.FINAL);
        }

        builder.addMethod(create());
        builder.addMethod(ctor());
        builder.addMethod(intermediateStateDesc());
        builder.addMethod(intermediateBlockCount());
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
        builder.addParameter(DRIVER_CONTEXT, "driverContext");
        builder.addParameter(LIST_INTEGER, "channels");
        for (Parameter p : createParameters) {
            builder.addParameter(p.type(), p.name());
        }
        if (createParameters.isEmpty()) {
            builder.addStatement("return new $T(driverContext, channels, $L)", implementation, callInit());
        } else {
            builder.addStatement(
                "return new $T(driverContext, channels, $L, $L)",
                implementation,
                callInit(),
                createParameters.stream().map(p -> p.name()).collect(joining(", "))
            );
        }
        return builder.build();
    }

    private CodeBlock callInit() {
        String initParametersCall = init.getParameters()
            .stream()
            .map(p -> TypeName.get(p.asType()).equals(BIG_ARRAYS) ? "driverContext.bigArrays()" : p.getSimpleName().toString())
            .collect(joining(", "));
        CodeBlock.Builder builder = CodeBlock.builder();
        if (init.getReturnType().toString().equals(stateType.toString())) {
            builder.add("$T.$L($L)", declarationType, init.getSimpleName(), initParametersCall);
        } else {
            builder.add("new $T($T.$L($L))", stateType, declarationType, init.getSimpleName(), initParametersCall);
        }
        return builder.build();
    }

    private CodeBlock initInterState() {
        CodeBlock.Builder builder = CodeBlock.builder();
        builder.add("List.of(");
        boolean addComma = false;
        for (var interState : intermediateState) {
            if (addComma) builder.add(",");
            builder.add("$Wnew $T($S, $T." + interState.elementType() + ")", INTERMEDIATE_STATE_DESC, interState.name(), ELEMENT_TYPE);
            addComma = true;
        }
        builder.add("$W$W)");
        return builder.build();
    }

    private MethodSpec ctor() {
        MethodSpec.Builder builder = MethodSpec.constructorBuilder().addModifiers(Modifier.PUBLIC);
        builder.addParameter(DRIVER_CONTEXT, "driverContext");
        builder.addParameter(LIST_INTEGER, "channels");
        builder.addParameter(stateType, "state");
        builder.addStatement("this.driverContext = driverContext");
        builder.addStatement("this.channels = channels");
        builder.addStatement("this.state = state");

        for (Parameter p : createParameters()) {
            builder.addParameter(p.type(), p.name());
            builder.addStatement("this.$N = $N", p.name(), p.name());
        }
        return builder.build();
    }

    private MethodSpec intermediateStateDesc() {
        MethodSpec.Builder builder = MethodSpec.methodBuilder("intermediateStateDesc");
        builder.addModifiers(Modifier.PUBLIC, Modifier.STATIC).returns(LIST_AGG_FUNC_DESC);
        builder.addStatement("return INTERMEDIATE_STATE_DESC");
        return builder.build();
    }

    private MethodSpec intermediateBlockCount() {
        MethodSpec.Builder builder = MethodSpec.methodBuilder("intermediateBlockCount");
        builder.addAnnotation(Override.class).addModifiers(Modifier.PUBLIC).returns(TypeName.INT);
        builder.addStatement("return INTERMEDIATE_STATE_DESC.size()");
        return builder.build();
    }

    private MethodSpec addRawInput() {
        MethodSpec.Builder builder = MethodSpec.methodBuilder("addRawInput");
        builder.addAnnotation(Override.class).addModifiers(Modifier.PUBLIC).addParameter(PAGE, "page");
        builder.addStatement("$T block = page.getBlock(channels.get(0))", valueBlockType(init, combine));
        builder.addStatement("$T vector = block.asVector()", valueVectorType(init, combine));
        builder.beginControlFlow("if (vector != null)").addStatement("addRawVector(vector)");
        builder.nextControlFlow("else").addStatement("addRawBlock(block)").endControlFlow();
        return builder.build();
    }

    private MethodSpec addRawVector() {
        MethodSpec.Builder builder = MethodSpec.methodBuilder("addRawVector");
        builder.addModifiers(Modifier.PRIVATE).addParameter(valueVectorType(init, combine), "vector");

        if (stateTypeHasSeen) {
            builder.addStatement("state.seen(true)");
        }
        if (valuesIsBytesRef) {
            // Add bytes_ref scratch var that will be used for bytes_ref blocks/vectors
            builder.addStatement("$T scratch = new $T()", BYTES_REF, BYTES_REF);
        }

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

        if (valuesIsBytesRef) {
            // Add bytes_ref scratch var that will only be used for bytes_ref blocks/vectors
            builder.addStatement("$T scratch = new $T()", BYTES_REF, BYTES_REF);
        }
        builder.beginControlFlow("for (int p = 0; p < block.getPositionCount(); p++)");
        {
            builder.beginControlFlow("if (block.isNull(p))");
            builder.addStatement("continue");
            builder.endControlFlow();
            if (stateTypeHasSeen) {
                builder.addStatement("state.seen(true)");
            }
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
        if (valuesIsBytesRef) {
            combineRawInputForBytesRef(builder, blockVariable);
            return;
        }
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

    private void combineRawInputForBytesRef(MethodSpec.Builder builder, String blockVariable) {
        // scratch is a BytesRef var that must have been defined before the iteration starts
        builder.addStatement("$T.combine(state, $L.getBytesRef(i, scratch))", declarationType, blockVariable);
    }

    private MethodSpec addIntermediateInput() {
        MethodSpec.Builder builder = MethodSpec.methodBuilder("addIntermediateInput");
        builder.addAnnotation(Override.class).addModifiers(Modifier.PUBLIC).addParameter(PAGE, "page");
        builder.addStatement("assert channels.size() == intermediateBlockCount()");
        builder.addStatement("assert page.getBlockCount() >= channels.get(0) + intermediateStateDesc().size()");
        for (int i = 0; i < intermediateState.size(); i++) {
            var interState = intermediateState.get(i);
            interState.assignToVariable(builder, i);
            builder.addStatement("assert $L.getPositionCount() == 1", interState.name());
        }
        if (combineIntermediate != null) {
            if (intermediateState.stream().map(IntermediateStateDesc::elementType).anyMatch(n -> n.equals("BYTES_REF"))) {
                builder.addStatement("$T scratch = new $T()", BYTES_REF, BYTES_REF);
            }
            builder.addStatement("$T.combineIntermediate(state, " + intermediateStateRowAccess() + ")", declarationType);
        } else if (hasPrimitiveState()) {
            assert intermediateState.size() == 2;
            assert intermediateState.get(1).name().equals("seen");
            builder.beginControlFlow("if (seen.getBoolean(0))");
            {
                var state = intermediateState.get(0);
                var s = "state.$L($T.combine(state.$L(), " + state.name() + "." + vectorAccessorName(state.elementType()) + "(0)))";
                builder.addStatement(s, primitiveStateMethod(), declarationType, primitiveStateMethod());
                builder.addStatement("state.seen(true)");
                builder.endControlFlow();
            }
        } else {
            throw new IllegalArgumentException("Don't know how to combine intermediate input. Define combineIntermediate");
        }
        return builder.build();
    }

    String intermediateStateRowAccess() {
        return intermediateState.stream().map(desc -> desc.access("0")).collect(joining(", "));
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
                    "don't know how to fetch primitive values from " + stateType + ". define combineIntermediate."
                );
        }
    }

    private MethodSpec evaluateIntermediate() {
        MethodSpec.Builder builder = MethodSpec.methodBuilder("evaluateIntermediate");
        builder.addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .addParameter(BLOCK_ARRAY, "blocks")
            .addParameter(TypeName.INT, "offset")
            .addParameter(DRIVER_CONTEXT, "driverContext");
        builder.addStatement("state.toIntermediate(blocks, offset, driverContext)");
        return builder.build();
    }

    private MethodSpec evaluateFinal() {
        MethodSpec.Builder builder = MethodSpec.methodBuilder("evaluateFinal");
        builder.addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .addParameter(BLOCK_ARRAY, "blocks")
            .addParameter(TypeName.INT, "offset")
            .addParameter(DRIVER_CONTEXT, "driverContext");
        if (stateTypeHasSeen) {
            builder.beginControlFlow("if (state.seen() == false)");
            builder.addStatement("blocks[offset] = driverContext.blockFactory().newConstantNullBlock(1)", BLOCK);
            builder.addStatement("return");
            builder.endControlFlow();
        }
        if (evaluateFinal == null) {
            primitiveStateToResult(builder);
        } else {
            builder.addStatement("blocks[offset] = $T.evaluateFinal(state, driverContext)", declarationType);
        }
        return builder.build();
    }

    private void primitiveStateToResult(MethodSpec.Builder builder) {
        switch (stateType.toString()) {
            case "org.elasticsearch.compute.aggregation.IntState":
                builder.addStatement("blocks[offset] = driverContext.blockFactory().newConstantIntBlockWith(state.intValue(), 1)");
                return;
            case "org.elasticsearch.compute.aggregation.LongState":
                builder.addStatement("blocks[offset] = driverContext.blockFactory().newConstantLongBlockWith(state.longValue(), 1)");
                return;
            case "org.elasticsearch.compute.aggregation.DoubleState":
                builder.addStatement("blocks[offset] = driverContext.blockFactory().newConstantDoubleBlockWith(state.doubleValue(), 1)");
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
        builder.addStatement("sb.append($S).append(channels)", "channels=");
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

    private boolean hasPrimitiveState() {
        return switch (stateType.toString()) {
            case "org.elasticsearch.compute.aggregation.IntState", "org.elasticsearch.compute.aggregation.LongState",
                "org.elasticsearch.compute.aggregation.DoubleState" -> true;
            default -> false;
        };
    }

    record IntermediateStateDesc(String name, String elementType, boolean block) {
        static IntermediateStateDesc newIntermediateStateDesc(IntermediateState state) {
            String type = state.type();
            boolean block = false;
            if (type.toUpperCase(Locale.ROOT).endsWith("_BLOCK")) {
                type = type.substring(0, type.length() - "_BLOCK".length());
                block = true;
            }
            return new IntermediateStateDesc(state.name(), type, block);
        }

        public String access(String position) {
            if (block) {
                return name();
            }
            String s = name() + "." + vectorAccessorName(elementType()) + "(" + position;
            if (elementType().equals("BYTES_REF")) {
                s += ", scratch";
            }
            return s + ")";
        }

        public void assignToVariable(MethodSpec.Builder builder, int offset) {
            builder.addStatement("Block $L = page.getBlock(channels.get($L))", name + "Uncast", offset);
            ClassName blockType = blockType(elementType());
            builder.beginControlFlow("if ($L.areAllValuesNull())", name + "Uncast");
            {
                builder.addStatement("return");
                builder.endControlFlow();
            }
            if (block) {
                builder.addStatement("$T $L = ($T) $L", blockType, name, blockType, name + "Uncast");
            } else {
                builder.addStatement("$T $L = (($T) $L).asVector()", vectorType(elementType), name, blockType, name + "Uncast");
            }
        }
    }
}
