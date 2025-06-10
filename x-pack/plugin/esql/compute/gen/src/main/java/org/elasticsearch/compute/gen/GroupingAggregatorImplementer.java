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
import org.elasticsearch.compute.gen.AggregatorImplementer.AggregationParameter;
import org.elasticsearch.compute.gen.AggregatorImplementer.AggregationState;

import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.TypeElement;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.Elements;

import static java.util.stream.Collectors.joining;
import static org.elasticsearch.compute.gen.AggregatorImplementer.capitalize;
import static org.elasticsearch.compute.gen.Methods.optionalStaticMethod;
import static org.elasticsearch.compute.gen.Methods.requireAnyArgs;
import static org.elasticsearch.compute.gen.Methods.requireAnyType;
import static org.elasticsearch.compute.gen.Methods.requireArgs;
import static org.elasticsearch.compute.gen.Methods.requireName;
import static org.elasticsearch.compute.gen.Methods.requirePrimitiveOrImplements;
import static org.elasticsearch.compute.gen.Methods.requireStaticMethod;
import static org.elasticsearch.compute.gen.Methods.requireType;
import static org.elasticsearch.compute.gen.Methods.requireVoidType;
import static org.elasticsearch.compute.gen.Methods.vectorAccessorName;
import static org.elasticsearch.compute.gen.Types.BIG_ARRAYS;
import static org.elasticsearch.compute.gen.Types.BLOCK;
import static org.elasticsearch.compute.gen.Types.BLOCK_ARRAY;
import static org.elasticsearch.compute.gen.Types.BYTES_REF;
import static org.elasticsearch.compute.gen.Types.DRIVER_CONTEXT;
import static org.elasticsearch.compute.gen.Types.ELEMENT_TYPE;
import static org.elasticsearch.compute.gen.Types.GROUPING_AGGREGATOR_FUNCTION;
import static org.elasticsearch.compute.gen.Types.GROUPING_AGGREGATOR_FUNCTION_ADD_INPUT;
import static org.elasticsearch.compute.gen.Types.INTERMEDIATE_STATE_DESC;
import static org.elasticsearch.compute.gen.Types.INT_BLOCK;
import static org.elasticsearch.compute.gen.Types.INT_VECTOR;
import static org.elasticsearch.compute.gen.Types.LIST_AGG_FUNC_DESC;
import static org.elasticsearch.compute.gen.Types.LIST_INTEGER;
import static org.elasticsearch.compute.gen.Types.LONG_BLOCK;
import static org.elasticsearch.compute.gen.Types.LONG_VECTOR;
import static org.elasticsearch.compute.gen.Types.PAGE;
import static org.elasticsearch.compute.gen.Types.SEEN_GROUP_IDS;
import static org.elasticsearch.compute.gen.Types.WARNINGS;
import static org.elasticsearch.compute.gen.Types.blockType;
import static org.elasticsearch.compute.gen.Types.vectorType;

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
    private final List<TypeMirror> warnExceptions;
    private final ExecutableElement init;
    private final ExecutableElement combine;
    private final List<Parameter> createParameters;
    private final ClassName implementation;
    private final List<AggregatorImplementer.IntermediateStateDesc> intermediateState;
    private final boolean includeTimestampVector;

    private final AggregationState aggState;
    private final AggregationParameter aggParam;

    public GroupingAggregatorImplementer(
        Elements elements,
        TypeElement declarationType,
        IntermediateState[] interStateAnno,
        List<TypeMirror> warnExceptions,
        boolean includeTimestampVector
    ) {
        this.declarationType = declarationType;
        this.warnExceptions = warnExceptions;

        this.init = requireStaticMethod(
            declarationType,
            requirePrimitiveOrImplements(elements, Types.GROUPING_AGGREGATOR_STATE),
            requireName("init", "initGrouping"),
            requireAnyArgs("<arbitrary init arguments>")
        );
        this.aggState = AggregationState.create(elements, init.getReturnType(), warnExceptions.isEmpty() == false, true);

        this.combine = requireStaticMethod(
            declarationType,
            aggState.declaredType().isPrimitive() ? requireType(aggState.declaredType()) : requireVoidType(),
            requireName("combine"),
            combineArgs(aggState, includeTimestampVector)
        );
        // TODO support multiple parameters
        this.aggParam = AggregationParameter.create(combine.getParameters().get(combine.getParameters().size() - 1).asType());

        this.createParameters = init.getParameters()
            .stream()
            .map(Parameter::from)
            .filter(f -> false == f.type().equals(BIG_ARRAYS) && false == f.type().equals(DRIVER_CONTEXT))
            .collect(Collectors.toList());

        this.implementation = ClassName.get(
            elements.getPackageOf(declarationType).toString(),
            (declarationType.getSimpleName() + "GroupingAggregatorFunction").replace("AggregatorGroupingAggregator", "GroupingAggregator")
        );

        this.intermediateState = Arrays.stream(interStateAnno)
            .map(AggregatorImplementer.IntermediateStateDesc::newIntermediateStateDesc)
            .toList();
        this.includeTimestampVector = includeTimestampVector;
    }

    private static Methods.ArgumentMatcher combineArgs(AggregationState aggState, boolean includeTimestampVector) {
        if (aggState.declaredType().isPrimitive()) {
            return requireArgs(requireType(aggState.declaredType()), requireAnyType("<aggregation input column type>"));
        } else if (includeTimestampVector) {
            return requireArgs(
                requireType(aggState.declaredType()),
                requireType(TypeName.INT),
                requireType(TypeName.LONG), // @timestamp
                requireAnyType("<aggregation input column type>")
            );
        } else {
            return requireArgs(
                requireType(aggState.declaredType()),
                requireType(TypeName.INT),
                requireAnyType("<aggregation input column type>")
            );
        }
    }

    public ClassName implementation() {
        return implementation;
    }

    List<Parameter> createParameters() {
        return createParameters;
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
        builder.addJavadoc("This class is generated. Edit {@code " + getClass().getSimpleName() + "} instead.");
        builder.addModifiers(Modifier.PUBLIC, Modifier.FINAL);
        builder.addSuperinterface(GROUPING_AGGREGATOR_FUNCTION);
        builder.addField(
            FieldSpec.builder(LIST_AGG_FUNC_DESC, "INTERMEDIATE_STATE_DESC", Modifier.PRIVATE, Modifier.STATIC, Modifier.FINAL)
                .initializer(initInterState())
                .build()
        );
        builder.addField(aggState.type(), "state", Modifier.PRIVATE, Modifier.FINAL);
        if (warnExceptions.isEmpty() == false) {
            builder.addField(WARNINGS, "warnings", Modifier.PRIVATE, Modifier.FINAL);
        }
        builder.addField(LIST_INTEGER, "channels", Modifier.PRIVATE, Modifier.FINAL);
        builder.addField(DRIVER_CONTEXT, "driverContext", Modifier.PRIVATE, Modifier.FINAL);

        for (Parameter p : createParameters) {
            builder.addField(p.type(), p.name(), Modifier.PRIVATE, Modifier.FINAL);
        }

        builder.addMethod(create());
        builder.addMethod(ctor());
        builder.addMethod(intermediateStateDesc());
        builder.addMethod(intermediateBlockCount());
        builder.addMethod(prepareProcessPage());
        builder.addMethod(addRawInputLoop(INT_VECTOR, blockType(aggParam.type())));
        builder.addMethod(addRawInputLoop(INT_VECTOR, vectorType(aggParam.type())));
        builder.addMethod(addRawInputLoop(INT_BLOCK, blockType(aggParam.type())));
        builder.addMethod(addRawInputLoop(INT_BLOCK, vectorType(aggParam.type())));
        builder.addMethod(selectedMayContainUnseenGroups());
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
        if (warnExceptions.isEmpty() == false) {
            builder.addParameter(WARNINGS, "warnings");
        }
        builder.addParameter(LIST_INTEGER, "channels");
        builder.addParameter(DRIVER_CONTEXT, "driverContext");
        for (Parameter p : createParameters) {
            builder.addParameter(p.type(), p.name());
        }
        if (createParameters.isEmpty()) {
            builder.addStatement(
                "return new $T($Lchannels, $L, driverContext)",
                implementation,
                warnExceptions.isEmpty() ? "" : "warnings, ",
                callInit()
            );
        } else {
            builder.addStatement(
                "return new $T($Lchannels, $L, driverContext, $L)",
                implementation,
                warnExceptions.isEmpty() ? "" : "warnings, ",
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
        if (aggState.declaredType().isPrimitive()) {
            builder.add(
                "new $T(driverContext.bigArrays(), $T.$L($L))",
                aggState.type(),
                declarationType,
                init.getSimpleName(),
                initParametersCall
            );
        } else {
            builder.add("$T.$L($L)", declarationType, init.getSimpleName(), initParametersCall);
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
        if (warnExceptions.isEmpty() == false) {
            builder.addParameter(WARNINGS, "warnings");
        }
        builder.addParameter(LIST_INTEGER, "channels");
        builder.addParameter(aggState.type(), "state");
        builder.addParameter(DRIVER_CONTEXT, "driverContext");
        if (warnExceptions.isEmpty() == false) {
            builder.addStatement("this.warnings = warnings");
        }
        builder.addStatement("this.channels = channels");
        builder.addStatement("this.state = state");
        builder.addStatement("this.driverContext = driverContext");

        for (Parameter p : createParameters) {
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

    /**
     * Prepare to process a single page of results.
     */
    private MethodSpec prepareProcessPage() {
        MethodSpec.Builder builder = MethodSpec.methodBuilder("prepareProcessPage");
        builder.addAnnotation(Override.class).addModifiers(Modifier.PUBLIC).returns(GROUPING_AGGREGATOR_FUNCTION_ADD_INPUT);
        builder.addParameter(SEEN_GROUP_IDS, "seenGroupIds").addParameter(PAGE, "page");

        builder.addStatement("$T valuesBlock = page.getBlock(channels.get(0))", blockType(aggParam.type()));
        builder.addStatement("$T valuesVector = valuesBlock.asVector()", vectorType(aggParam.type()));
        if (includeTimestampVector) {
            builder.addStatement("$T timestampsBlock = page.getBlock(channels.get(1))", LONG_BLOCK);
            builder.addStatement("$T timestampsVector = timestampsBlock.asVector()", LONG_VECTOR);

            builder.beginControlFlow("if (timestampsVector == null) ");
            builder.addStatement("throw new IllegalStateException($S)", "expected @timestamp vector; but got a block");
            builder.endControlFlow();
        }
        builder.beginControlFlow("if (valuesVector == null)");
        String extra = includeTimestampVector ? ", timestampsVector" : "";
        {
            builder.beginControlFlow("if (valuesBlock.mayHaveNulls())");
            builder.addStatement("state.enableGroupIdTracking(seenGroupIds)");
            builder.endControlFlow();
            if (shouldWrapAddInput(blockType(aggParam.type()))) {
                builder.addStatement(
                    "var addInput = $L",
                    addInput(b -> b.addStatement("addRawInput(positionOffset, groupIds, valuesBlock$L)", extra))
                );
                builder.addStatement("return $T.wrapAddInput(addInput, state, valuesBlock)", declarationType);
            } else {
                builder.addStatement(
                    "return $L",
                    addInput(b -> b.addStatement("addRawInput(positionOffset, groupIds, valuesBlock$L)", extra))
                );
            }
        }
        builder.endControlFlow();
        if (shouldWrapAddInput(vectorType(aggParam.type()))) {
            builder.addStatement(
                "var addInput = $L",
                addInput(b -> b.addStatement("addRawInput(positionOffset, groupIds, valuesVector$L)", extra))
            );
            builder.addStatement("return $T.wrapAddInput(addInput, state, valuesVector)", declarationType);
        } else {
            builder.addStatement(
                "return $L",
                addInput(b -> b.addStatement("addRawInput(positionOffset, groupIds, valuesVector$L)", extra))
            );
        }
        return builder.build();
    }

    /**
     * Generate an {@code AddInput} implementation. That's a collection path optimized for the input data.
     */
    private TypeSpec addInput(Consumer<MethodSpec.Builder> addBlock) {
        TypeSpec.Builder builder = TypeSpec.anonymousClassBuilder("");
        builder.addSuperinterface(GROUPING_AGGREGATOR_FUNCTION_ADD_INPUT);

        MethodSpec.Builder block = MethodSpec.methodBuilder("add").addAnnotation(Override.class).addModifiers(Modifier.PUBLIC);
        block.addParameter(TypeName.INT, "positionOffset").addParameter(INT_BLOCK, "groupIds");
        addBlock.accept(block);
        builder.addMethod(block.build());

        MethodSpec.Builder vector = MethodSpec.methodBuilder("add").addAnnotation(Override.class).addModifiers(Modifier.PUBLIC);
        vector.addParameter(TypeName.INT, "positionOffset").addParameter(INT_VECTOR, "groupIds");
        addBlock.accept(vector);
        builder.addMethod(vector.build());

        MethodSpec.Builder close = MethodSpec.methodBuilder("close").addAnnotation(Override.class).addModifiers(Modifier.PUBLIC);
        builder.addMethod(close.build());

        return builder.build();
    }

    /**
     * Generate an {@code addRawInput} method to perform the actual aggregation.
     * @param groupsType The type of the group key, always {@code IntBlock} or {@code IntVector}
     * @param valuesType The type of the values to consume, always a subclass of {@code Block} or a subclass of {@code Vector}
     */
    private MethodSpec addRawInputLoop(TypeName groupsType, TypeName valuesType) {
        boolean groupsIsBlock = groupsType.toString().endsWith("Block");
        boolean valuesIsBlock = valuesType.toString().endsWith("Block");
        MethodSpec.Builder builder = MethodSpec.methodBuilder("addRawInput");
        builder.addModifiers(Modifier.PRIVATE);
        builder.addParameter(TypeName.INT, "positionOffset").addParameter(groupsType, "groups").addParameter(valuesType, "values");
        if (includeTimestampVector) {
            builder.addParameter(LONG_VECTOR, "timestamps");
        }
        if (aggParam.isBytesRef()) {
            // Add bytes_ref scratch var that will be used for bytes_ref blocks/vectors
            builder.addStatement("$T scratch = new $T()", BYTES_REF, BYTES_REF);
        }
        if (aggParam.isArray() && valuesIsBlock == false) {
            builder.addComment("This type does not support vectors because all values are multi-valued");
            return builder.build();
        }

        builder.beginControlFlow("for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++)");
        {
            if (groupsIsBlock) {
                builder.beginControlFlow("if (groups.isNull(groupPosition))");
                builder.addStatement("continue");
                builder.endControlFlow();
                builder.addStatement("int groupStart = groups.getFirstValueIndex(groupPosition)");
                builder.addStatement("int groupEnd = groupStart + groups.getValueCount(groupPosition)");
                builder.beginControlFlow("for (int g = groupStart; g < groupEnd; g++)");
                builder.addStatement("int groupId = groups.getInt(g)");
            } else {
                builder.addStatement("int groupId = groups.getInt(groupPosition)");
            }

            if (warnExceptions.isEmpty() == false) {
                builder.beginControlFlow("if (state.hasFailed(groupId))");
                builder.addStatement("continue");
                builder.endControlFlow();
            }

            if (valuesIsBlock) {
                builder.beginControlFlow("if (values.isNull(groupPosition + positionOffset))");
                builder.addStatement("continue");
                builder.endControlFlow();
                builder.addStatement("int valuesStart = values.getFirstValueIndex(groupPosition + positionOffset)");
                builder.addStatement("int valuesEnd = valuesStart + values.getValueCount(groupPosition + positionOffset)");
                if (aggParam.isArray()) {
                    String arrayType = aggParam.type().toString().replace("[]", "");
                    builder.addStatement("$L[] valuesArray = new $L[valuesEnd - valuesStart]", arrayType, arrayType);
                    builder.beginControlFlow("for (int v = valuesStart; v < valuesEnd; v++)");
                    builder.addStatement("valuesArray[v-valuesStart] = $L.get$L(v)", "values", capitalize(arrayType));
                    builder.endControlFlow();
                    combineRawInputForArray(builder, "valuesArray");
                } else {
                    builder.beginControlFlow("for (int v = valuesStart; v < valuesEnd; v++)");
                    combineRawInput(builder, "values", "v");
                    builder.endControlFlow();
                }
            } else {
                combineRawInput(builder, "values", "groupPosition + positionOffset");
            }

            if (groupsIsBlock) {
                builder.endControlFlow();
            }
        }
        builder.endControlFlow();
        return builder.build();
    }

    private void combineRawInput(MethodSpec.Builder builder, String blockVariable, String offsetVariable) {
        TypeName valueType = aggParam.type();
        TypeName returnType = TypeName.get(combine.getReturnType());

        warningsBlock(builder, () -> {
            if (aggParam.isBytesRef()) {
                combineRawInputForBytesRef(builder, blockVariable, offsetVariable);
            } else if (valueType.isPrimitive() == false) {
                throw new IllegalArgumentException("second parameter to combine must be a primitive, array or BytesRef: " + valueType);
            } else if (returnType.isPrimitive()) {
                combineRawInputForPrimitive(builder, blockVariable, offsetVariable);
            } else if (returnType == TypeName.VOID) {
                combineRawInputForVoid(builder, blockVariable, offsetVariable);
            } else {
                throw new IllegalArgumentException("combine must return void or a primitive");
            }
        });
    }

    private void combineRawInputForBytesRef(MethodSpec.Builder builder, String blockVariable, String offsetVariable) {
        // scratch is a BytesRef var that must have been defined before the iteration starts
        if (includeTimestampVector) {
            if (offsetVariable.contains(" + ")) {
                builder.addStatement("var valuePosition = $L", offsetVariable);
                offsetVariable = "valuePosition";
            }
            builder.addStatement(
                "$T.combine(state, groupId, timestamps.getLong($L), $L.getBytesRef($L, scratch))",
                declarationType,
                offsetVariable,
                blockVariable,
                offsetVariable
            );
        } else {
            builder.addStatement("$T.combine(state, groupId, $L.getBytesRef($L, scratch))", declarationType, blockVariable, offsetVariable);
        }
    }

    private void combineRawInputForPrimitive(MethodSpec.Builder builder, String blockVariable, String offsetVariable) {
        if (includeTimestampVector) {
            if (offsetVariable.contains(" + ")) {
                builder.addStatement("var valuePosition = $L", offsetVariable);
                offsetVariable = "valuePosition";
            }
            builder.addStatement(
                "$T.combine(state, groupId, timestamps.getLong($L), values.get$L($L))",
                declarationType,
                offsetVariable,
                capitalize(aggParam.type().toString()),
                offsetVariable
            );
        } else {
            builder.addStatement(
                "state.set(groupId, $T.combine(state.getOrDefault(groupId), $L.get$L($L)))",
                declarationType,
                blockVariable,
                capitalize(aggParam.type().toString()),
                offsetVariable
            );
        }
    }

    private void combineRawInputForVoid(MethodSpec.Builder builder, String blockVariable, String offsetVariable) {
        if (includeTimestampVector) {
            if (offsetVariable.contains(" + ")) {
                builder.addStatement("var valuePosition = $L", offsetVariable);
                offsetVariable = "valuePosition";
            }
            builder.addStatement(
                "$T.combine(state, groupId, timestamps.getLong($L), values.get$L($L))",
                declarationType,
                offsetVariable,
                capitalize(aggParam.type().toString()),
                offsetVariable
            );
        } else {
            builder.addStatement(
                "$T.combine(state, groupId, $L.get$L($L))",
                declarationType,
                blockVariable,
                capitalize(aggParam.type().toString()),
                offsetVariable
            );
        }
    }

    private void combineRawInputForArray(MethodSpec.Builder builder, String arrayVariable) {
        warningsBlock(builder, () -> builder.addStatement("$T.combine(state, groupId, $L)", declarationType, arrayVariable));
    }

    private boolean shouldWrapAddInput(TypeName valuesType) {
        return optionalStaticMethod(
            declarationType,
            requireType(GROUPING_AGGREGATOR_FUNCTION_ADD_INPUT),
            requireName("wrapAddInput"),
            requireArgs(requireType(GROUPING_AGGREGATOR_FUNCTION_ADD_INPUT), requireType(aggState.declaredType()), requireType(valuesType))
        ) != null;
    }

    private void warningsBlock(MethodSpec.Builder builder, Runnable block) {
        if (warnExceptions.isEmpty() == false) {
            builder.beginControlFlow("try");
        }
        block.run();
        if (warnExceptions.isEmpty() == false) {
            String catchPattern = "catch (" + warnExceptions.stream().map(m -> "$T").collect(Collectors.joining(" | ")) + " e)";
            builder.nextControlFlow(catchPattern, warnExceptions.stream().map(TypeName::get).toArray());
            builder.addStatement("warnings.registerException(e)");
            builder.addStatement("state.setFailed(groupId)");
            builder.endControlFlow();
        }
    }

    private MethodSpec selectedMayContainUnseenGroups() {
        MethodSpec.Builder builder = MethodSpec.methodBuilder("selectedMayContainUnseenGroups");
        builder.addAnnotation(Override.class).addModifiers(Modifier.PUBLIC);
        builder.addParameter(SEEN_GROUP_IDS, "seenGroupIds");
        builder.addStatement("state.enableGroupIdTracking(seenGroupIds)");
        return builder.build();
    }

    private MethodSpec addIntermediateInput() {
        MethodSpec.Builder builder = MethodSpec.methodBuilder("addIntermediateInput");
        builder.addAnnotation(Override.class).addModifiers(Modifier.PUBLIC);
        builder.addParameter(TypeName.INT, "positionOffset");
        builder.addParameter(INT_VECTOR, "groups");
        builder.addParameter(PAGE, "page");

        builder.addStatement("state.enableGroupIdTracking(new $T.Empty())", SEEN_GROUP_IDS);
        builder.addStatement("assert channels.size() == intermediateBlockCount()");
        int count = 0;
        for (var interState : intermediateState) {
            interState.assignToVariable(builder, count);
            count++;
        }
        final String first = intermediateState.get(0).name();
        if (intermediateState.size() > 1) {
            builder.addStatement(
                "assert "
                    + intermediateState.stream()
                        .map(AggregatorImplementer.IntermediateStateDesc::name)
                        .skip(1)
                        .map(s -> first + ".getPositionCount() == " + s + ".getPositionCount()")
                        .collect(joining(" && "))
            );
        }
        if (intermediateState.stream().map(AggregatorImplementer.IntermediateStateDesc::elementType).anyMatch(n -> n.equals("BYTES_REF"))) {
            builder.addStatement("$T scratch = new $T()", BYTES_REF, BYTES_REF);
        }
        builder.beginControlFlow("for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++)");
        {
            builder.addStatement("int groupId = groups.getInt(groupPosition)");
            if (aggState.declaredType().isPrimitive()) {
                if (warnExceptions.isEmpty()) {
                    assert intermediateState.size() == 2;
                    assert intermediateState.get(1).name().equals("seen");
                    builder.beginControlFlow("if (seen.getBoolean(groupPosition + positionOffset))");
                } else {
                    assert intermediateState.size() == 3;
                    assert intermediateState.get(1).name().equals("seen");
                    assert intermediateState.get(2).name().equals("failed");
                    builder.beginControlFlow("if (failed.getBoolean(groupPosition + positionOffset))");
                    {
                        builder.addStatement("state.setFailed(groupId)");
                    }
                    builder.nextControlFlow("else if (seen.getBoolean(groupPosition + positionOffset))");
                }

                warningsBlock(builder, () -> {
                    var name = intermediateState.get(0).name();
                    var vectorAccessor = vectorAccessorName(intermediateState.get(0).elementType());
                    builder.addStatement(
                        "state.set(groupId, $T.combine(state.getOrDefault(groupId), $L.$L(groupPosition + positionOffset)))",
                        declarationType,
                        name,
                        vectorAccessor
                    );
                });
                builder.endControlFlow();
            } else {
                var stateHasBlock = intermediateState.stream().anyMatch(AggregatorImplementer.IntermediateStateDesc::block);
                requireStaticMethod(
                    declarationType,
                    requireVoidType(),
                    requireName("combineIntermediate"),
                    requireArgs(
                        Stream.of(
                            Stream.of(aggState.declaredType(), TypeName.INT), // aggState and groupId
                            intermediateState.stream().map(AggregatorImplementer.IntermediateStateDesc::combineArgType),
                            Stream.of(TypeName.INT).filter(p -> stateHasBlock) // position
                        ).flatMap(Function.identity()).map(Methods::requireType).toArray(Methods.TypeMatcher[]::new)
                    )
                );

                builder.addStatement(
                    "$T.combineIntermediate(state, groupId, "
                        + intermediateState.stream().map(desc -> desc.access("groupPosition + positionOffset")).collect(joining(", "))
                        + (stateHasBlock ? ", groupPosition + positionOffset" : "")
                        + ")",
                    declarationType
                );
            }
            builder.endControlFlow();
        }
        return builder.build();
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
        builder.addStatement("$T inState = (($T) input).state", aggState.type(), implementation);
        builder.addStatement("state.enableGroupIdTracking(new $T.Empty())", SEEN_GROUP_IDS);
        if (aggState.declaredType().isPrimitive()) {
            builder.beginControlFlow("if (inState.hasValue(position))");
            builder.addStatement("state.set(groupId, $T.combine(state.getOrDefault(groupId), inState.get(position)))", declarationType);
            builder.endControlFlow();
        } else {
            requireStaticMethod(
                declarationType,
                requireVoidType(),
                requireName("combineStates"),
                requireArgs(
                    requireType(aggState.declaredType()),
                    requireType(TypeName.INT),
                    requireType(aggState.declaredType()),
                    requireType(TypeName.INT)
                )
            );
            builder.addStatement("$T.combineStates(state, groupId, inState, position)", declarationType);
        }
        return builder.build();
    }

    private MethodSpec evaluateIntermediate() {
        MethodSpec.Builder builder = MethodSpec.methodBuilder("evaluateIntermediate");
        builder.addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .addParameter(BLOCK_ARRAY, "blocks")
            .addParameter(TypeName.INT, "offset")
            .addParameter(INT_VECTOR, "selected");
        builder.addStatement("state.toIntermediate(blocks, offset, selected, driverContext)");
        return builder.build();
    }

    private MethodSpec evaluateFinal() {
        MethodSpec.Builder builder = MethodSpec.methodBuilder("evaluateFinal");
        builder.addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .addParameter(BLOCK_ARRAY, "blocks")
            .addParameter(TypeName.INT, "offset")
            .addParameter(INT_VECTOR, "selected")
            .addParameter(DRIVER_CONTEXT, "driverContext");

        if (aggState.declaredType().isPrimitive()) {
            builder.addStatement("blocks[offset] = state.toValuesBlock(selected, driverContext)");
        } else {
            requireStaticMethod(
                declarationType,
                requireType(BLOCK),
                requireName("evaluateFinal"),
                requireArgs(requireType(aggState.declaredType()), requireType(INT_VECTOR), requireType(DRIVER_CONTEXT))
            );
            builder.addStatement("blocks[offset] = $T.evaluateFinal(state, selected, driverContext)", declarationType);
        }
        return builder.build();
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
}
