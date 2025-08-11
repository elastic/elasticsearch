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
import org.elasticsearch.compute.gen.Methods.TypeMatcher;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.VariableElement;
import javax.lang.model.type.TypeKind;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.Elements;

import static java.util.stream.Collectors.joining;
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
import static org.elasticsearch.compute.gen.Types.AGGREGATOR_FUNCTION;
import static org.elasticsearch.compute.gen.Types.BIG_ARRAYS;
import static org.elasticsearch.compute.gen.Types.BLOCK;
import static org.elasticsearch.compute.gen.Types.BLOCK_ARRAY;
import static org.elasticsearch.compute.gen.Types.BOOLEAN_VECTOR;
import static org.elasticsearch.compute.gen.Types.BYTES_REF;
import static org.elasticsearch.compute.gen.Types.DRIVER_CONTEXT;
import static org.elasticsearch.compute.gen.Types.ELEMENT_TYPE;
import static org.elasticsearch.compute.gen.Types.INTERMEDIATE_STATE_DESC;
import static org.elasticsearch.compute.gen.Types.LIST_AGG_FUNC_DESC;
import static org.elasticsearch.compute.gen.Types.LIST_INTEGER;
import static org.elasticsearch.compute.gen.Types.PAGE;
import static org.elasticsearch.compute.gen.Types.WARNINGS;
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
    private final List<TypeMirror> warnExceptions;
    private final ExecutableElement init;
    private final ExecutableElement combine;
    private final List<Parameter> createParameters;
    private final ClassName implementation;
    private final List<IntermediateStateDesc> intermediateState;

    private final AggregationState aggState;
    private final List<AggregationParameter> aggParams;

    public AggregatorImplementer(
        Elements elements,
        TypeElement declarationType,
        IntermediateState[] interStateAnno,
        List<TypeMirror> warnExceptions
    ) {
        this.declarationType = declarationType;
        this.warnExceptions = warnExceptions;

        this.init = requireStaticMethod(
            declarationType,
            requirePrimitiveOrImplements(elements, Types.AGGREGATOR_STATE),
            requireName("init", "initSingle"),
            requireAnyArgs("<arbitrary init arguments>")
        );
        this.aggState = AggregationState.create(elements, init.getReturnType(), warnExceptions.isEmpty() == false, false);

        this.combine = requireStaticMethod(
            declarationType,
            aggState.declaredType().isPrimitive() ? requireType(aggState.declaredType()) : requireVoidType(),
            requireName("combine"),
            requireArgsStartsWith(requireType(aggState.declaredType()), requireAnyType("<aggregation input column type>"))
        );
        this.aggParams = combine.getParameters().stream().skip(1).map(AggregationParameter::create).toList();

        this.createParameters = init.getParameters()
            .stream()
            .map(Parameter::from)
            .filter(f -> false == f.type().equals(BIG_ARRAYS) && false == f.type().equals(DRIVER_CONTEXT))
            .toList();

        this.implementation = ClassName.get(
            elements.getPackageOf(declarationType).toString(),
            (declarationType.getSimpleName() + "AggregatorFunction").replace("AggregatorAggregator", "Aggregator")
        );
        this.intermediateState = Arrays.stream(interStateAnno).map(IntermediateStateDesc::newIntermediateStateDesc).toList();
    }

    ClassName implementation() {
        return implementation;
    }

    List<Parameter> createParameters() {
        return createParameters;
    }

    public static String capitalize(String s) {
        return Character.toUpperCase(s.charAt(0)) + s.substring(1);
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
        builder.addJavadoc("This class is generated. Edit {@code " + getClass().getSimpleName() + "} instead.");
        builder.addModifiers(Modifier.PUBLIC, Modifier.FINAL);
        builder.addSuperinterface(AGGREGATOR_FUNCTION);
        builder.addField(
            FieldSpec.builder(LIST_AGG_FUNC_DESC, "INTERMEDIATE_STATE_DESC", Modifier.PRIVATE, Modifier.STATIC, Modifier.FINAL)
                .initializer(initInterState())
                .build()
        );

        if (warnExceptions.isEmpty() == false) {
            builder.addField(WARNINGS, "warnings", Modifier.PRIVATE, Modifier.FINAL);
        }

        builder.addField(DRIVER_CONTEXT, "driverContext", Modifier.PRIVATE, Modifier.FINAL);
        builder.addField(aggState.type, "state", Modifier.PRIVATE, Modifier.FINAL);
        builder.addField(LIST_INTEGER, "channels", Modifier.PRIVATE, Modifier.FINAL);

        for (Parameter p : createParameters) {
            builder.addField(p.type(), p.name(), Modifier.PRIVATE, Modifier.FINAL);
        }

        builder.addMethod(create());
        builder.addMethod(ctor());
        builder.addMethod(intermediateStateDesc());
        builder.addMethod(intermediateBlockCount());
        builder.addMethod(addRawInput());
        builder.addMethod(addRawInputExploded(true));
        builder.addMethod(addRawInputExploded(false));
        builder.addMethod(addRawVector(false));
        builder.addMethod(addRawVector(true));
        builder.addMethod(addRawBlock(false));
        builder.addMethod(addRawBlock(true));
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
        if (warnExceptions.isEmpty() == false) {
            builder.addParameter(WARNINGS, "warnings");
        }
        builder.addParameter(DRIVER_CONTEXT, "driverContext");
        builder.addParameter(LIST_INTEGER, "channels");
        for (Parameter p : createParameters) {
            builder.addParameter(p.type(), p.name());
        }
        if (createParameters.isEmpty()) {
            builder.addStatement(
                "return new $T($LdriverContext, channels, $L)",
                implementation,
                warnExceptions.isEmpty() ? "" : "warnings, ",
                callInit()
            );
        } else {
            builder.addStatement(
                "return new $T($LdriverContext, channels, $L, $L)",
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
            builder.add("new $T($T.$L($L))", aggState.type(), declarationType, init.getSimpleName(), initParametersCall);
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
        builder.addParameter(DRIVER_CONTEXT, "driverContext");
        builder.addParameter(LIST_INTEGER, "channels");
        builder.addParameter(aggState.type, "state");

        if (warnExceptions.isEmpty() == false) {
            builder.addStatement("this.warnings = warnings");
        }
        builder.addStatement("this.driverContext = driverContext");
        builder.addStatement("this.channels = channels");
        builder.addStatement("this.state = state");

        for (Parameter p : createParameters()) {
            p.buildCtor(builder);
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
        builder.addAnnotation(Override.class).addModifiers(Modifier.PUBLIC).addParameter(PAGE, "page").addParameter(BOOLEAN_VECTOR, "mask");
        if (aggState.hasFailed()) {
            builder.beginControlFlow("if (state.failed())");
            builder.addStatement("return");
            builder.endControlFlow();
        }
        builder.beginControlFlow("if (mask.allFalse())");
        builder.addComment("Entire page masked away");
        builder.nextControlFlow("else if (mask.allTrue())");
        builder.addStatement("$L(page)", addRawInputExplodedName(false));
        builder.nextControlFlow("else");
        builder.addStatement("$L(page, mask)", addRawInputExplodedName(true));
        builder.endControlFlow();
        return builder.build();
    }

    private String addRawInputExplodedName(boolean hasMask) {
        return hasMask ? "addRawInputMasked" : "addRawInputNotMasked";
    }

    private MethodSpec addRawInputExploded(boolean hasMask) {
        MethodSpec.Builder builder = MethodSpec.methodBuilder(addRawInputExplodedName(hasMask));
        builder.addModifiers(Modifier.PRIVATE).addParameter(PAGE, "page");
        if (hasMask) {
            builder.addParameter(BOOLEAN_VECTOR, "mask");
        }

        for (int i = 0; i < aggParams.size(); i++) {
            AggregationParameter p = aggParams.get(i);
            builder.addStatement("$T $L = page.getBlock(channels.get($L))", blockType(p.type()), p.blockName(), i);
        }
        for (AggregationParameter p : aggParams) {
            builder.addStatement("$T $L = $L.asVector()", vectorType(p.type()), p.vectorName(), p.blockName());
            builder.beginControlFlow("if ($L == null)", p.vectorName());
            builder.addStatement(
                "addRawBlock("
                    + aggParams.stream().map(AggregationParameter::blockName).collect(joining(", "))
                    + (hasMask ? ", mask" : "")
                    + ")"
            );
            builder.addStatement("return");
            builder.endControlFlow();
        }
        builder.addStatement(
            "addRawVector("
                + aggParams.stream().map(AggregationParameter::vectorName).collect(joining(", "))
                + (hasMask ? ", mask" : "")
                + ")"
        );
        return builder.build();
    }

    private MethodSpec addRawVector(boolean masked) {
        MethodSpec.Builder builder = initAddRaw(true, masked);
        if (aggParams.getFirst().isArray()) {
            builder.addComment("This type does not support vectors because all values are multi-valued");
            return builder.build();
        }
        if (aggState.hasSeen()) {
            builder.addStatement("state.seen(true)");
        }

        builder.beginControlFlow(
            "for (int valuesPosition = 0; valuesPosition < $L.getPositionCount(); valuesPosition++)",
            aggParams.getFirst().vectorName()
        );
        {
            if (masked) {
                builder.beginControlFlow("if (mask.getBoolean(valuesPosition) == false)").addStatement("continue").endControlFlow();
            }
            for (AggregationParameter p : aggParams) {
                p.read(builder, true);
            }
            combineRawInput(builder);

        }
        builder.endControlFlow();
        return builder.build();
    }

    private MethodSpec addRawBlock(boolean masked) {
        MethodSpec.Builder builder = initAddRaw(false, masked);

        builder.beginControlFlow("for (int p = 0; p < $L.getPositionCount(); p++)", aggParams.getFirst().blockName());
        {
            if (masked) {
                builder.beginControlFlow("if (mask.getBoolean(p) == false)").addStatement("continue").endControlFlow();
            }
            for (AggregationParameter p : aggParams) {
                builder.beginControlFlow("if ($L.isNull(p))", p.blockName());
                builder.addStatement("continue");
                builder.endControlFlow();
            }
            if (aggState.hasSeen()) {
                builder.addStatement("state.seen(true)");
            }

            if (aggParams.getFirst().isArray()) {
                if (aggParams.size() > 1) {
                    throw new IllegalArgumentException("array mode not supported for multiple args");
                }
                builder.addStatement("int start = $L.getFirstValueIndex(p)", aggParams.getFirst().blockName());
                builder.addStatement("int end = start + $L.getValueCount(p)", aggParams.getFirst().blockName());
                // TODO move this to the top of the loop
                builder.addStatement(
                    "$L[] valuesArray = new $L[end - start]",
                    aggParams.getFirst().arrayType(),
                    aggParams.getFirst().arrayType()
                );
                builder.beginControlFlow("for (int i = start; i < end; i++)");
                builder.addStatement(
                    "valuesArray[i-start] = $L.get$L(i)",
                    aggParams.getFirst().blockName(),
                    capitalize(aggParams.getFirst().arrayType())
                );
                builder.endControlFlow();
                combineRawInputForArray(builder, "valuesArray");
            } else {
                for (AggregationParameter p : aggParams) {
                    builder.addStatement("int $L = $L.getFirstValueIndex(p)", p.startName(), p.blockName());
                    builder.addStatement("int $L = $L + $L.getValueCount(p)", p.endName(), p.startName(), p.blockName());
                    builder.beginControlFlow(
                        "for (int $L = $L; $L < $L; $L++)",
                        p.offsetName(),
                        p.startName(),
                        p.offsetName(),
                        p.endName(),
                        p.offsetName()
                    );
                    p.read(builder, false);
                }
                combineRawInput(builder);
                for (AggregationParameter p : aggParams) {
                    builder.endControlFlow();
                }
            }
        }
        builder.endControlFlow();
        return builder.build();
    }

    private MethodSpec.Builder initAddRaw(boolean valuesAreVector, boolean masked) {
        MethodSpec.Builder builder = MethodSpec.methodBuilder(valuesAreVector ? "addRawVector" : "addRawBlock");
        builder.addModifiers(Modifier.PRIVATE);
        for (AggregationParameter p : aggParams) {
            builder.addParameter(
                valuesAreVector ? vectorType(p.type) : blockType(p.type),
                valuesAreVector ? p.vectorName() : p.blockName()
            );
        }
        if (masked) {
            builder.addParameter(BOOLEAN_VECTOR, "mask");
        }
        for (AggregationParameter p : aggParams) {
            if (p.isBytesRef()) {
                // Add bytes_ref scratch var that will be used for bytes_ref blocks/vectors
                builder.addStatement("$T $L = new $T()", BYTES_REF, p.scratchName(), BYTES_REF);
            }
        }
        return builder;
    }

    private void combineRawInput(MethodSpec.Builder builder) {
        TypeName returnType = TypeName.get(combine.getReturnType());
        warningsBlock(builder, () -> invokeCombineRawInput(returnType, builder));
    }

    private void invokeCombineRawInput(TypeName returnType, MethodSpec.Builder builder) {
        StringBuilder pattern = new StringBuilder();
        List<Object> params = new ArrayList<>();
        if (returnType.isPrimitive()) {
            pattern.append("state.$TValue($T.combine(state.$TValue()");
            params.add(returnType);
            params.add(declarationType);
            params.add(returnType);
        } else if (returnType == TypeName.VOID) {
            pattern.append("$T.combine(state");
            params.add(declarationType);
        } else {
            throw new IllegalArgumentException("combine must return void or a primitive");
        }
        for (AggregationParameter p : aggParams) {
            pattern.append(", $L");
            params.add(p.valueName());
        }
        if (returnType.isPrimitive()) {
            pattern.append(")");
        }
        pattern.append(")");
        builder.addStatement(pattern.toString(), params.toArray());
    }

    private void combineRawInputForArray(MethodSpec.Builder builder, String arrayVariable) {
        warningsBlock(builder, () -> builder.addStatement("$T.combine(state, $L)", declarationType, arrayVariable));
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
            builder.addStatement("state.failed(true)");
            builder.addStatement("return");
            builder.endControlFlow();
        }
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
        if (aggState.declaredType().isPrimitive()) {
            if (warnExceptions.isEmpty()) {
                assert intermediateState.size() == 2;
                assert intermediateState.get(1).name().equals("seen");
                builder.beginControlFlow("if (seen.getBoolean(0))");
            } else {
                assert intermediateState.size() == 3;
                assert intermediateState.get(1).name().equals("seen");
                assert intermediateState.get(2).name().equals("failed");
                builder.beginControlFlow("if (failed.getBoolean(0))");
                {
                    builder.addStatement("state.failed(true)");
                    builder.addStatement("state.seen(true)");
                }
                builder.nextControlFlow("else if (seen.getBoolean(0))");
            }

            warningsBlock(builder, () -> {
                var primitiveStateMethod = switch (aggState.declaredType().toString()) {
                    case "boolean" -> "booleanValue";
                    case "int" -> "intValue";
                    case "long" -> "longValue";
                    case "double" -> "doubleValue";
                    case "float" -> "floatValue";
                    default -> throw new IllegalArgumentException("Unexpected primitive type: [" + aggState.declaredType() + "]");
                };
                var state = intermediateState.get(0);
                var s = "state.$L($T.combine(state.$L(), " + state.name() + "." + vectorAccessorName(state.elementType()) + "(0)))";
                builder.addStatement(s, primitiveStateMethod, declarationType, primitiveStateMethod);
                builder.addStatement("state.seen(true)");
            });
            builder.endControlFlow();
        } else {
            requireStaticMethod(
                declarationType,
                requireVoidType(),
                requireName("combineIntermediate"),
                requireArgs(
                    Stream.concat(
                        Stream.of(aggState.declaredType()), // aggState
                        intermediateState.stream().map(IntermediateStateDesc::combineArgType) // intermediate state
                    ).map(Methods::requireType).toArray(TypeMatcher[]::new)
                )
            );
            if (intermediateState.stream().map(IntermediateStateDesc::elementType).anyMatch(n -> n.equals("BYTES_REF"))) {
                builder.addStatement("$T scratch = new $T()", BYTES_REF, BYTES_REF);
            }
            builder.addStatement("$T.combineIntermediate(state, " + intermediateStateRowAccess() + ")", declarationType);
        }
        return builder.build();
    }

    String intermediateStateRowAccess() {
        return intermediateState.stream().map(desc -> desc.access("0")).collect(joining(", "));
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
        if (aggState.hasSeen() || aggState.hasFailed()) {
            builder.beginControlFlow(
                "if ($L)",
                Stream.concat(
                    Stream.of("state.seen() == false").filter(c -> aggState.hasSeen()),
                    Stream.of("state.failed()").filter(c -> aggState.hasFailed())
                ).collect(joining(" || "))
            );
            builder.addStatement("blocks[offset] = driverContext.blockFactory().newConstantNullBlock(1)", BLOCK);
            builder.addStatement("return");
            builder.endControlFlow();
        }
        if (aggState.declaredType().isPrimitive()) {
            builder.addStatement(switch (aggState.declaredType().toString()) {
                case "boolean" -> "blocks[offset] = driverContext.blockFactory().newConstantBooleanBlockWith(state.booleanValue(), 1)";
                case "int" -> "blocks[offset] = driverContext.blockFactory().newConstantIntBlockWith(state.intValue(), 1)";
                case "long" -> "blocks[offset] = driverContext.blockFactory().newConstantLongBlockWith(state.longValue(), 1)";
                case "double" -> "blocks[offset] = driverContext.blockFactory().newConstantDoubleBlockWith(state.doubleValue(), 1)";
                case "float" -> "blocks[offset] = driverContext.blockFactory().newConstantFloatBlockWith(state.floatValue(), 1)";
                default -> throw new IllegalArgumentException("Unexpected primitive type: [" + aggState.declaredType() + "]");
            });
        } else {
            requireStaticMethod(
                declarationType,
                requireType(BLOCK),
                requireName("evaluateFinal"),
                requireArgs(requireType(aggState.declaredType()), requireType(DRIVER_CONTEXT))
            );
            builder.addStatement("blocks[offset] = $T.evaluateFinal(state, driverContext)", declarationType);
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

        public TypeName combineArgType() {
            var type = Types.fromString(elementType);
            return block ? blockType(type) : type;
        }
    }

    /**
     * This represents the type returned by init method used to keep aggregation state
     * @param declaredType declared state type as returned by init method
     * @param type actual type used (we have some predefined state types for primitive values)
     */
    public record AggregationState(TypeName declaredType, TypeName type, boolean hasSeen, boolean hasFailed) {

        public static AggregationState create(Elements elements, TypeMirror mirror, boolean hasFailures, boolean isArray) {
            var declaredType = TypeName.get(mirror);
            var stateType = declaredType.isPrimitive()
                ? ClassName.get("org.elasticsearch.compute.aggregation", primitiveStateStoreClassname(declaredType, hasFailures, isArray))
                : declaredType;
            return new AggregationState(
                declaredType,
                stateType,
                hasMethod(elements, stateType, "seen()"),
                hasMethod(elements, stateType, "failed()")
            );
        }

        private static String primitiveStateStoreClassname(TypeName declaredType, boolean hasFailures, boolean isArray) {
            var name = capitalize(declaredType.toString());
            if (hasFailures) {
                name += "Fallible";
            }
            if (isArray) {
                name += "Array";
            }
            return name + "State";
        }
    }

    public record AggregationParameter(String name, TypeName type, boolean isArray) {
        public static AggregationParameter create(VariableElement v) {
            return new AggregationParameter(
                v.getSimpleName().toString(),
                TypeName.get(v.asType()),
                Objects.equals(v.asType().getKind(), TypeKind.ARRAY)
            );
        }

        public String blockName() {
            return name + "Block";
        }

        public String vectorName() {
            return name + "Vector";
        }

        public String scratchName() {
            if (isBytesRef() == false) {
                throw new IllegalStateException("can't build scratch for non-BytesRef");
            }
            return name + "Scratch";
        }

        public String valueName() {
            return name + "Value";
        }

        public String startName() {
            return name + "Start";
        }

        public String endName() {
            return name + "End";
        }

        public String offsetName() {
            return name + "Offset";
        }

        public String arrayType() {
            return type.toString().replace("[]", "");
        }

        public String readMethod() {
            String type = this.type.toString();
            int lastDot = type.lastIndexOf('.');
            return "get" + capitalize(lastDot >= 0 ? type.substring(lastDot + 1) : type);
        }

        public void read(MethodSpec.Builder builder, boolean vector) {
            StringBuilder pattern = new StringBuilder("$T $L = $L.$L(");
            List<Object> params = new ArrayList<>();
            params.add(type);
            params.add(valueName());
            params.add(vector ? vectorName() : blockName());
            params.add(readMethod());
            if (vector) {
                pattern.append("valuesPosition");
            } else {
                pattern.append("$L");
                params.add(offsetName());
            }
            if (isBytesRef()) {
                pattern.append(", $L");
                params.add(scratchName());
            }
            pattern.append(")");
            builder.addStatement(pattern.toString(), params.toArray());
        }

        public boolean isBytesRef() {
            return Objects.equals(type, BYTES_REF);
        }
    }

    private static boolean hasMethod(Elements elements, TypeName type, String name) {
        return elements.getAllMembers(elements.getTypeElement(type.toString())).stream().anyMatch(e -> e.toString().equals(name));
    }
}
