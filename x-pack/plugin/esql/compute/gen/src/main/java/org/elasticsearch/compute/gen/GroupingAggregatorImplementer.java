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
import java.util.function.Consumer;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.TypeElement;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.Elements;

import static java.util.stream.Collectors.joining;
import static org.elasticsearch.compute.gen.AggregatorImplementer.valueBlockType;
import static org.elasticsearch.compute.gen.AggregatorImplementer.valueVectorType;
import static org.elasticsearch.compute.gen.Methods.findMethod;
import static org.elasticsearch.compute.gen.Methods.findRequiredMethod;
import static org.elasticsearch.compute.gen.Methods.vectorAccessorName;
import static org.elasticsearch.compute.gen.Types.BIG_ARRAYS;
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
    private final ExecutableElement combineStates;
    private final ExecutableElement evaluateFinal;
    private final ExecutableElement combineIntermediate;
    private final TypeName stateType;
    private final boolean valuesIsBytesRef;
    private final List<Parameter> createParameters;
    private final ClassName implementation;
    private final List<AggregatorImplementer.IntermediateStateDesc> intermediateState;
    private final boolean includeTimestampVector;

    public GroupingAggregatorImplementer(
        Elements elements,
        TypeElement declarationType,
        IntermediateState[] interStateAnno,
        List<TypeMirror> warnExceptions,
        boolean includeTimestampVector
    ) {
        this.declarationType = declarationType;
        this.warnExceptions = warnExceptions;

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
        this.combineIntermediate = findMethod(declarationType, "combineIntermediate");
        this.evaluateFinal = findMethod(declarationType, "evaluateFinal");
        this.valuesIsBytesRef = BYTES_REF.equals(TypeName.get(combine.getParameters().get(combine.getParameters().size() - 1).asType()));
        this.createParameters = init.getParameters()
            .stream()
            .map(Parameter::from)
            .filter(f -> false == f.type().equals(BIG_ARRAYS) && false == f.type().equals(DRIVER_CONTEXT))
            .collect(Collectors.toList());

        this.implementation = ClassName.get(
            elements.getPackageOf(declarationType).toString(),
            (declarationType.getSimpleName() + "GroupingAggregatorFunction").replace("AggregatorGroupingAggregator", "GroupingAggregator")
        );

        intermediateState = Arrays.stream(interStateAnno)
            .map(AggregatorImplementer.IntermediateStateDesc::newIntermediateStateDesc)
            .toList();
        this.includeTimestampVector = includeTimestampVector;
    }

    public ClassName implementation() {
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
        String head = initReturn.toString().substring(0, 1).toUpperCase(Locale.ROOT);
        String tail = initReturn.toString().substring(1);
        if (warnExceptions.isEmpty()) {
            return ClassName.get("org.elasticsearch.compute.aggregation", head + tail + "ArrayState");
        }
        return ClassName.get("org.elasticsearch.compute.aggregation", head + tail + "FallibleArrayState");
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
        builder.addField(
            FieldSpec.builder(LIST_AGG_FUNC_DESC, "INTERMEDIATE_STATE_DESC", Modifier.PRIVATE, Modifier.STATIC, Modifier.FINAL)
                .initializer(initInterState())
                .build()
        );
        builder.addField(stateType, "state", Modifier.PRIVATE, Modifier.FINAL);
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
        builder.addMethod(addRawInputLoop(INT_VECTOR, valueBlockType(init, combine)));
        builder.addMethod(addRawInputLoop(INT_VECTOR, valueVectorType(init, combine)));
        builder.addMethod(addRawInputLoop(INT_BLOCK, valueBlockType(init, combine)));
        builder.addMethod(addRawInputLoop(INT_BLOCK, valueVectorType(init, combine)));
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
        if (init.getReturnType().toString().equals(stateType.toString())) {
            builder.add("$T.$L($L)", declarationType, init.getSimpleName(), initParametersCall);
        } else {
            builder.add(
                "new $T(driverContext.bigArrays(), $T.$L($L))",
                stateType,
                declarationType,
                init.getSimpleName(),
                initParametersCall
            );
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
        builder.addParameter(stateType, "state");
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

        builder.addStatement("$T valuesBlock = page.getBlock(channels.get(0))", valueBlockType(init, combine));
        builder.addStatement("$T valuesVector = valuesBlock.asVector()", valueVectorType(init, combine));
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
            builder.addStatement("return $L", addInput(b -> b.addStatement("addRawInput(positionOffset, groupIds, valuesBlock$L)", extra)));
        }
        builder.endControlFlow();
        builder.addStatement("return $L", addInput(b -> b.addStatement("addRawInput(positionOffset, groupIds, valuesVector$L)", extra)));
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
        String methodName = "addRawInput";
        MethodSpec.Builder builder = MethodSpec.methodBuilder(methodName);
        builder.addModifiers(Modifier.PRIVATE);
        builder.addParameter(TypeName.INT, "positionOffset").addParameter(groupsType, "groups").addParameter(valuesType, "values");
        if (includeTimestampVector) {
            builder.addParameter(LONG_VECTOR, "timestamps");
        }
        if (valuesIsBytesRef) {
            // Add bytes_ref scratch var that will be used for bytes_ref blocks/vectors
            builder.addStatement("$T scratch = new $T()", BYTES_REF, BYTES_REF);
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
                builder.beginControlFlow("for (int v = valuesStart; v < valuesEnd; v++)");
                combineRawInput(builder, "values", "v");
                builder.endControlFlow();
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
        TypeName valueType = TypeName.get(combine.getParameters().get(combine.getParameters().size() - 1).asType());
        String secondParameterGetter = "get"
            + valueType.toString().substring(0, 1).toUpperCase(Locale.ROOT)
            + valueType.toString().substring(1);
        TypeName returnType = TypeName.get(combine.getReturnType());

        if (warnExceptions.isEmpty() == false) {
            builder.beginControlFlow("try");
        }
        if (valuesIsBytesRef) {
            combineRawInputForBytesRef(builder, blockVariable, offsetVariable);
        } else if (includeTimestampVector) {
            combineRawInputWithTimestamp(builder, offsetVariable);
        } else if (valueType.isPrimitive() == false) {
            throw new IllegalArgumentException("second parameter to combine must be a primitive");
        } else if (returnType.isPrimitive()) {
            combineRawInputForPrimitive(builder, secondParameterGetter, blockVariable, offsetVariable);
        } else if (returnType == TypeName.VOID) {
            combineRawInputForVoid(builder, secondParameterGetter, blockVariable, offsetVariable);
        } else {
            throw new IllegalArgumentException("combine must return void or a primitive");
        }
        if (warnExceptions.isEmpty() == false) {
            String catchPattern = "catch (" + warnExceptions.stream().map(m -> "$T").collect(Collectors.joining(" | ")) + " e)";
            builder.nextControlFlow(catchPattern, warnExceptions.stream().map(TypeName::get).toArray());
            builder.addStatement("warnings.registerException(e)");
            builder.addStatement("state.setFailed(groupId)");
            builder.endControlFlow();
        }
    }

    private void combineRawInputForPrimitive(
        MethodSpec.Builder builder,
        String secondParameterGetter,
        String blockVariable,
        String offsetVariable
    ) {
        builder.addStatement(
            "state.set(groupId, $T.combine(state.getOrDefault(groupId), $L.$L($L)))",
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

    private void combineRawInputWithTimestamp(MethodSpec.Builder builder, String offsetVariable) {
        TypeName valueType = TypeName.get(combine.getParameters().get(combine.getParameters().size() - 1).asType());
        String blockType = valueType.toString().substring(0, 1).toUpperCase(Locale.ROOT) + valueType.toString().substring(1);
        if (offsetVariable.contains(" + ")) {
            builder.addStatement("var valuePosition = $L", offsetVariable);
            offsetVariable = "valuePosition";
        }
        builder.addStatement(
            "$T.combine(state, groupId, timestamps.getLong($L), values.get$L($L))",
            declarationType,
            offsetVariable,
            blockType,
            offsetVariable
        );
    }

    private void combineRawInputForBytesRef(MethodSpec.Builder builder, String blockVariable, String offsetVariable) {
        // scratch is a BytesRef var that must have been defined before the iteration starts
        builder.addStatement("$T.combine(state, groupId, $L.getBytesRef($L, scratch))", declarationType, blockVariable, offsetVariable);
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
            if (hasPrimitiveState()) {
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

                if (warnExceptions.isEmpty() == false) {
                    builder.beginControlFlow("try");
                }
                var name = intermediateState.get(0).name();
                var vectorAccessor = vectorAccessorName(intermediateState.get(0).elementType());
                builder.addStatement(
                    "state.set(groupId, $T.combine(state.getOrDefault(groupId), $L.$L(groupPosition + positionOffset)))",
                    declarationType,
                    name,
                    vectorAccessor
                );
                if (warnExceptions.isEmpty() == false) {
                    String catchPattern = "catch (" + warnExceptions.stream().map(m -> "$T").collect(Collectors.joining(" | ")) + " e)";
                    builder.nextControlFlow(catchPattern, warnExceptions.stream().map(TypeName::get).toArray());
                    builder.addStatement("warnings.registerException(e)");
                    builder.addStatement("state.setFailed(groupId)");
                    builder.endControlFlow();
                }
                builder.endControlFlow();
            } else {
                builder.addStatement("$T.combineIntermediate(state, groupId, " + intermediateStateRowAccess() + ")", declarationType);
            }
            builder.endControlFlow();
        }
        return builder.build();
    }

    String intermediateStateRowAccess() {
        String rowAccess = intermediateState.stream().map(desc -> desc.access("groupPosition + positionOffset")).collect(joining(", "));
        if (intermediateState.stream().anyMatch(AggregatorImplementer.IntermediateStateDesc::block)) {
            rowAccess += ", groupPosition + positionOffset";
        }
        return rowAccess;
    }

    private void combineStates(MethodSpec.Builder builder) {
        if (combineStates == null) {
            builder.beginControlFlow("if (inState.hasValue(position))");
            builder.addStatement("state.set(groupId, $T.combine(state.getOrDefault(groupId), inState.get(position)))", declarationType);
            builder.endControlFlow();
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
        builder.addStatement("state.enableGroupIdTracking(new $T.Empty())", SEEN_GROUP_IDS);
        combineStates(builder);
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

        if (evaluateFinal == null) {
            builder.addStatement("blocks[offset] = state.toValuesBlock(selected, driverContext)");
        } else {
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

    private static final Pattern PRIMITIVE_STATE_PATTERN = Pattern.compile(
        "org.elasticsearch.compute.aggregation.(Boolean|Int|Long|Double|Float)(Fallible)?ArrayState"
    );

    private boolean hasPrimitiveState() {
        return PRIMITIVE_STATE_PATTERN.matcher(stateType.toString()).matches();
    }
}
