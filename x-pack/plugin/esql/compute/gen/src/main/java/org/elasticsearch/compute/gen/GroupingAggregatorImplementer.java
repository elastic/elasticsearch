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
import org.elasticsearch.compute.gen.AggregatorImplementer.AggregationState;
import org.elasticsearch.compute.gen.argument.Argument;
import org.elasticsearch.compute.gen.argument.BlockArgument;
import org.elasticsearch.compute.gen.argument.PositionArgument;
import org.elasticsearch.compute.gen.argument.StandardArgument;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.TypeElement;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.Elements;

import static java.util.stream.Collectors.joining;
import static org.elasticsearch.compute.gen.Methods.optionalStaticMethod;
import static org.elasticsearch.compute.gen.Methods.requireAnyArgs;
import static org.elasticsearch.compute.gen.Methods.requireAnyType;
import static org.elasticsearch.compute.gen.Methods.requireArgs;
import static org.elasticsearch.compute.gen.Methods.requireArgsStartsWith;
import static org.elasticsearch.compute.gen.Methods.requireName;
import static org.elasticsearch.compute.gen.Methods.requirePrimitiveOrImplements;
import static org.elasticsearch.compute.gen.Methods.requireStaticMethod;
import static org.elasticsearch.compute.gen.Methods.requireType;
import static org.elasticsearch.compute.gen.Methods.requireVoidType;
import static org.elasticsearch.compute.gen.Methods.vectorAccessorName;
import static org.elasticsearch.compute.gen.Types.BIG_ARRAYS;
import static org.elasticsearch.compute.gen.Types.BLOCK;
import static org.elasticsearch.compute.gen.Types.BLOCK_ARRAY;
import static org.elasticsearch.compute.gen.Types.DRIVER_CONTEXT;
import static org.elasticsearch.compute.gen.Types.ELEMENT_TYPE;
import static org.elasticsearch.compute.gen.Types.GROUPING_AGGREGATOR_EVALUATOR_CONTEXT;
import static org.elasticsearch.compute.gen.Types.GROUPING_AGGREGATOR_FUNCTION;
import static org.elasticsearch.compute.gen.Types.GROUPING_AGGREGATOR_FUNCTION_ADD_INPUT;
import static org.elasticsearch.compute.gen.Types.INTERMEDIATE_STATE_DESC;
import static org.elasticsearch.compute.gen.Types.INT_ARRAY_BLOCK;
import static org.elasticsearch.compute.gen.Types.INT_BIG_ARRAY_BLOCK;
import static org.elasticsearch.compute.gen.Types.INT_BLOCK;
import static org.elasticsearch.compute.gen.Types.INT_VECTOR;
import static org.elasticsearch.compute.gen.Types.LIST_AGG_FUNC_DESC;
import static org.elasticsearch.compute.gen.Types.LIST_INTEGER;
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
    private static final List<ClassName> GROUP_IDS_CLASSES = List.of(INT_ARRAY_BLOCK, INT_BIG_ARRAY_BLOCK, INT_VECTOR);

    private final TypeElement declarationType;
    private final List<TypeMirror> warnExceptions;
    private final ExecutableElement init;
    private final ExecutableElement combine;
    private final List<Parameter> createParameters;
    private final ClassName implementation;
    private final List<AggregatorImplementer.IntermediateStateDesc> intermediateState;

    private final AggregationState aggState;
    private final List<Argument> aggParams;
    private final boolean hasOnlyBlockArguments;
    private final boolean allArgumentsSupportVectors;

    public GroupingAggregatorImplementer(
        Elements elements,
        javax.lang.model.util.Types types,
        TypeElement declarationType,
        IntermediateState[] interStateAnno,
        List<TypeMirror> warnExceptions
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
            combineArgs(aggState)
        );

        this.aggParams = combine.getParameters().stream().skip(aggState.declaredType().isPrimitive() ? 1 : 2).map(v -> {
            Argument a = Argument.fromParameter(types, v);
            if ((a instanceof StandardArgument || a instanceof BlockArgument || a instanceof PositionArgument) == false) {
                throw new IllegalArgumentException("unsupported argument [" + declarationType + "][" + a + "]");
            }
            return a;
        }).filter(a -> a instanceof PositionArgument == false).toList();

        this.hasOnlyBlockArguments = this.aggParams.stream().allMatch(a -> a instanceof BlockArgument);
        this.allArgumentsSupportVectors = aggParams.stream().noneMatch(a -> a.supportsVectorReadAccess() == false);

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
    }

    private static Methods.ArgumentMatcher combineArgs(AggregationState aggState) {
        if (aggState.declaredType().isPrimitive()) {
            return requireArgs(requireType(aggState.declaredType()), requireAnyType("<aggregation input column type>"));
        } else {
            return requireArgsStartsWith(
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
        builder.addMethod(prepareProcessRawInputPage());
        for (ClassName groupIdClass : GROUP_IDS_CLASSES) {
            builder.addMethod(addRawInputLoop(groupIdClass, false));
            if (hasOnlyBlockArguments == false && allArgumentsSupportVectors) {
                builder.addMethod(addRawInputLoop(groupIdClass, true));
            }
            builder.addMethod(addIntermediateInput(groupIdClass));
        }
        builder.addMethod(maybeEnableGroupIdTracking());
        builder.addMethod(selectedMayContainUnseenGroups());
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
     * Prepare to process a single raw input page.
     */
    private MethodSpec prepareProcessRawInputPage() {
        MethodSpec.Builder builder = MethodSpec.methodBuilder("prepareProcessRawInputPage");
        builder.addAnnotation(Override.class).addModifiers(Modifier.PUBLIC).returns(GROUPING_AGGREGATOR_FUNCTION_ADD_INPUT);
        builder.addParameter(SEEN_GROUP_IDS, "seenGroupIds").addParameter(PAGE, "page");

        for (int i = 0; i < aggParams.size(); i++) {
            Argument a = aggParams.get(i);
            builder.addStatement("$T $L = page.getBlock(channels.get($L))", a.dataType(true), a.blockName(), i);
        }

        String groupIdTrackingStatement = "maybeEnableGroupIdTracking(seenGroupIds, "
            + aggParams.stream().map(arg -> arg.blockName()).collect(joining(", "))
            + ")";

        if (allArgumentsSupportVectors) {

            for (Argument a : aggParams) {
                builder.addStatement(
                    "$T $L = $L.asVector()",
                    vectorType(a.elementType()),
                    (a instanceof BlockArgument) ? (a.name() + "Vector") : a.vectorName(),
                    a.blockName()
                );
                builder.beginControlFlow("if ($L == null)", (a instanceof BlockArgument) ? (a.name() + "Vector") : a.vectorName());
                {
                    builder.addStatement(groupIdTrackingStatement);
                    returnAddInput(builder, false);
                }
                builder.endControlFlow();
            }
            returnAddInput(builder, true);
        } else {
            builder.addStatement(groupIdTrackingStatement);
            returnAddInput(builder, false);
        }
        return builder.build();
    }

    private void returnAddInput(MethodSpec.Builder builder, boolean valuesAreVector) {
        if (shouldWrapAddInput(valuesAreVector)) {
            builder.addStatement("var addInput = $L", addInput(valuesAreVector));

            StringBuilder pattern = new StringBuilder("return $T.wrapAddInput(addInput, state");
            List<Object> params = new ArrayList<>();
            params.add(declarationType);
            for (Argument a : aggParams) {
                pattern.append(", $L");
                params.add(valuesAreVector ? a.vectorName() : a.blockName());
            }
            pattern.append(")");
            builder.addStatement(pattern.toString(), params.toArray());
        } else {
            builder.addStatement("return $L", addInput(valuesAreVector));
        }
    }

    private MethodSpec maybeEnableGroupIdTracking() {
        MethodSpec.Builder builder = MethodSpec.methodBuilder("maybeEnableGroupIdTracking");
        builder.addModifiers(Modifier.PRIVATE).returns(TypeName.VOID);
        builder.addParameter(SEEN_GROUP_IDS, "seenGroupIds");
        for (Argument a : aggParams) {
            builder.addParameter(a.dataType(true), a.blockName());
        }

        for (Argument a : aggParams) {
            builder.beginControlFlow("if ($L.mayHaveNulls())", a.blockName());
            builder.addStatement("state.enableGroupIdTracking(seenGroupIds)");
            builder.endControlFlow();
        }

        return builder.build();
    }

    /**
     * Generate an {@code AddInput} implementation. That's a collection path optimized for the input data.
     */
    private TypeSpec addInput(boolean valuesAreVector) {
        TypeSpec.Builder typeBuilder = TypeSpec.anonymousClassBuilder("");
        typeBuilder.addSuperinterface(GROUPING_AGGREGATOR_FUNCTION_ADD_INPUT);

        for (ClassName groupIdsType : GROUP_IDS_CLASSES) {
            MethodSpec.Builder builder = MethodSpec.methodBuilder("add").addAnnotation(Override.class).addModifiers(Modifier.PUBLIC);
            builder.addParameter(TypeName.INT, "positionOffset").addParameter(groupIdsType, "groupIds");

            if (hasOnlyBlockArguments && valuesAreVector) {
                builder.addComment("This type does not support vectors because all values are multi-valued");
                typeBuilder.addMethod(builder.build());
                continue;
            }

            StringBuilder pattern = new StringBuilder("addRawInput(positionOffset, groupIds");
            List<Object> params = new ArrayList<>();
            for (Argument a : aggParams) {
                pattern.append(", $L");
                params.add(valuesAreVector ? a.vectorName() : a.blockName());
            }
            pattern.append(")");
            builder.addStatement(pattern.toString(), params.toArray());

            typeBuilder.addMethod(builder.build());
        }

        MethodSpec.Builder close = MethodSpec.methodBuilder("close").addAnnotation(Override.class).addModifiers(Modifier.PUBLIC);
        typeBuilder.addMethod(close.build());

        return typeBuilder.build();
    }

    /**
     * Generate an {@code addRawInput} method to perform the actual aggregation.
     * @param groupsType The type of the group key, always {@code IntBlock} or {@code IntVector}
     * @param valuesAreVector Are the value a {@code Vector} (true) or a {@code Block} (false)
     */
    private MethodSpec addRawInputLoop(TypeName groupsType, boolean valuesAreVector) {
        boolean groupsIsBlock = groupsType.toString().endsWith("Block");
        MethodSpec.Builder builder = MethodSpec.methodBuilder("addRawInput");
        builder.addModifiers(Modifier.PRIVATE);
        builder.addParameter(TypeName.INT, "positionOffset").addParameter(groupsType, "groups");

        for (Argument a : aggParams) {
            boolean isBlockArgument = a instanceof BlockArgument;
            TypeName typeName = isBlockArgument ? Types.elementType(a.type()) : a.type();
            builder.addParameter(
                valuesAreVector ? vectorType(typeName) : blockType(typeName),
                valuesAreVector ? a.vectorName() : a.blockName()
            );
        }
        for (Argument a : aggParams) {
            if (a.scratchType() != null) {
                // Add scratch var that will be used for some blocks/vectors, e.g. for bytes_ref
                builder.addStatement("$T $L = new $T()", a.scratchType(), a.scratchName(), a.scratchType());
            }
        }

        if (hasOnlyBlockArguments && valuesAreVector) {
            builder.addComment("This type does not support vectors because all values are multi-valued");
            return builder.build();
        }

        builder.beginControlFlow("for (int groupPosition = 0; groupPosition < groups.getPositionCount(); groupPosition++)");
        {
            if (groupsIsBlock) {
                builder.beginControlFlow("if (groups.isNull(groupPosition))");
                builder.addStatement("continue");
                builder.endControlFlow();
            }
            builder.addStatement("int valuesPosition = groupPosition + positionOffset");

            if (valuesAreVector == false && hasOnlyBlockArguments == false) {
                for (Argument a : aggParams) {
                    builder.beginControlFlow("if ($L.isNull(valuesPosition))", a.blockName());
                    builder.addStatement("continue");
                    builder.endControlFlow();
                }
            }
            if (groupsIsBlock) {
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

            if (valuesAreVector) {
                for (Argument a : aggParams) {
                    a.read(builder, a.vectorName(), "valuesPosition");
                }
                combineRawInput(builder);
            } else {
                if (hasOnlyBlockArguments) {
                    String params = aggParams.stream().map(Argument::blockName).collect(joining(", "));
                    warningsBlock(
                        builder,
                        () -> builder.addStatement("$T.combine(state, groupId, valuesPosition, $L)", declarationType, params)
                    );
                } else {
                    for (Argument a : aggParams) {
                        builder.addStatement("int $L = $L.getFirstValueIndex(valuesPosition)", a.startName(), a.blockName());
                        builder.addStatement("int $L = $L + $L.getValueCount(valuesPosition)", a.endName(), a.startName(), a.blockName());
                        builder.beginControlFlow(
                            "for (int $L = $L; $L < $L; $L++)",
                            a.offsetName(),
                            a.startName(),
                            a.offsetName(),
                            a.endName(),
                            a.offsetName()
                        );
                        a.read(builder, a.blockName(), a.offsetName());
                    }
                    combineRawInput(builder);
                    for (Argument a : aggParams) {
                        builder.endControlFlow();
                    }
                }
            }

            if (groupsIsBlock) {
                builder.endControlFlow();
            }
        }
        builder.endControlFlow();
        return builder.build();
    }

    private void combineRawInput(MethodSpec.Builder builder) {
        TypeName returnType = TypeName.get(combine.getReturnType());
        warningsBlock(builder, () -> invokeCombineRawInput(returnType, builder));
    }

    private void invokeCombineRawInput(TypeName returnType, MethodSpec.Builder builder) {
        StringBuilder pattern = new StringBuilder();
        List<Object> params = new ArrayList<>();

        if (returnType.isPrimitive()) {
            pattern.append("state.set(groupId, $T.combine(state.getOrDefault(groupId)");
            params.add(declarationType);
        } else {
            pattern.append("$T.combine(state, groupId");
            params.add(declarationType);
        }
        if (hasOnlyBlockArguments) {
            pattern.append(", p");
        }
        for (Argument a : aggParams) {
            pattern.append(", $L");
            params.add(a.valueName());
        }
        if (returnType.isPrimitive()) {
            pattern.append(")");
        }
        pattern.append(")");
        builder.addStatement(pattern.toString(), params.toArray());
    }

    private boolean shouldWrapAddInput(boolean valuesAreVector) {
        return optionalStaticMethod(
            declarationType,
            requireType(GROUPING_AGGREGATOR_FUNCTION_ADD_INPUT),
            requireName("wrapAddInput"),
            requireArgs(
                Stream.concat(
                    Stream.of(requireType(GROUPING_AGGREGATOR_FUNCTION_ADD_INPUT), requireType(aggState.declaredType())),
                    aggParams.stream().map(a -> {
                        boolean isBlockArgument = a instanceof BlockArgument;
                        TypeName typeName = isBlockArgument ? Types.elementType(a.type()) : a.type();
                        return requireType(valuesAreVector ? vectorType(typeName) : blockType(typeName));
                    })
                ).toArray(Methods.TypeMatcher[]::new)
            )
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

    private MethodSpec addIntermediateInput(TypeName groupsType) {
        boolean groupsIsBlock = groupsType.toString().endsWith("Block");
        MethodSpec.Builder builder = MethodSpec.methodBuilder("addIntermediateInput");
        builder.addAnnotation(Override.class).addModifiers(Modifier.PUBLIC);
        builder.addParameter(TypeName.INT, "positionOffset");
        builder.addParameter(groupsType, "groups");
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
        var bulkCombineIntermediateMethod = optionalStaticMethod(
            declarationType,
            requireVoidType(),
            requireName("combineIntermediate"),
            requireArgs(
                Stream.concat(
                    // aggState, positionOffset, groupIds
                    Stream.of(aggState.declaredType(), TypeName.INT, groupsIsBlock ? INT_BLOCK : INT_VECTOR),
                    intermediateState.stream().map(AggregatorImplementer.IntermediateStateDesc::combineArgType)
                ).map(Methods::requireType).toArray(Methods.TypeMatcher[]::new)
            )
        );
        if (bulkCombineIntermediateMethod != null) {
            var states = intermediateState.stream()
                .map(AggregatorImplementer.IntermediateStateDesc::name)
                .collect(Collectors.joining(", "));
            builder.addStatement("$T.combineIntermediate(state, positionOffset, groups, " + states + ")", declarationType);
        } else {
            intermediateState.forEach(state -> state.addScratchDeclaration(builder));
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

                builder.addStatement("int valuesPosition = groupPosition + positionOffset");
                if (aggState.declaredType().isPrimitive()) {
                    if (warnExceptions.isEmpty()) {
                        assert intermediateState.size() == 2;
                        assert intermediateState.get(1).name().equals("seen");
                        builder.beginControlFlow("if (seen.getBoolean(valuesPosition))");
                    } else {
                        assert intermediateState.size() == 3;
                        assert intermediateState.get(1).name().equals("seen");
                        assert intermediateState.get(2).name().equals("failed");
                        builder.beginControlFlow("if (failed.getBoolean(valuesPosition))");
                        {
                            builder.addStatement("state.setFailed(groupId)");
                        }
                        builder.nextControlFlow("else if (seen.getBoolean(valuesPosition))");
                    }

                    warningsBlock(builder, () -> {
                        var name = intermediateState.get(0).name();
                        var vectorAccessor = vectorAccessorName(intermediateState.get(0).elementType());
                        builder.addStatement(
                            "state.set(groupId, $T.combine(state.getOrDefault(groupId), $L.$L(valuesPosition)))",
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
                            + intermediateState.stream().map(desc -> desc.access("valuesPosition")).collect(joining(", "))
                            + (stateHasBlock ? ", valuesPosition" : "")
                            + ")",
                        declarationType
                    );
                }
                if (groupsIsBlock) {
                    builder.endControlFlow();
                }
                builder.endControlFlow();
            }
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
            .addParameter(GROUPING_AGGREGATOR_EVALUATOR_CONTEXT, "ctx");

        if (aggState.declaredType().isPrimitive()) {
            builder.addStatement("blocks[offset] = state.toValuesBlock(selected, ctx.driverContext())");
        } else {
            requireStaticMethod(
                declarationType,
                requireType(BLOCK),
                requireName("evaluateFinal"),
                requireArgs(
                    requireType(aggState.declaredType()),
                    requireType(INT_VECTOR),
                    requireType(GROUPING_AGGREGATOR_EVALUATOR_CONTEXT)
                )
            );
            builder.addStatement("blocks[offset] = $T.evaluateFinal(state, selected, ctx)", declarationType);
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
