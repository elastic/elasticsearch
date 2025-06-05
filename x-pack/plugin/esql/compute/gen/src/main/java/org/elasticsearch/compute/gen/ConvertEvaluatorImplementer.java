/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.gen;

import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.TypeElement;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.Elements;

import static org.elasticsearch.compute.gen.Methods.buildFromFactory;
import static org.elasticsearch.compute.gen.Methods.getMethod;
import static org.elasticsearch.compute.gen.Types.ABSTRACT_CONVERT_FUNCTION_EVALUATOR;
import static org.elasticsearch.compute.gen.Types.BLOCK;
import static org.elasticsearch.compute.gen.Types.BYTES_REF;
import static org.elasticsearch.compute.gen.Types.BYTES_REF_VECTOR_BUILDER;
import static org.elasticsearch.compute.gen.Types.DRIVER_CONTEXT;
import static org.elasticsearch.compute.gen.Types.EXPRESSION_EVALUATOR;
import static org.elasticsearch.compute.gen.Types.EXPRESSION_EVALUATOR_FACTORY;
import static org.elasticsearch.compute.gen.Types.INT_VECTOR;
import static org.elasticsearch.compute.gen.Types.ORDINALS_BYTES_REF_VECTOR;
import static org.elasticsearch.compute.gen.Types.SOURCE;
import static org.elasticsearch.compute.gen.Types.VECTOR;
import static org.elasticsearch.compute.gen.Types.blockType;
import static org.elasticsearch.compute.gen.Types.builderType;
import static org.elasticsearch.compute.gen.Types.vectorType;

public class ConvertEvaluatorImplementer {

    private final TypeElement declarationType;
    private final EvaluatorImplementer.ProcessFunction processFunction;
    private final boolean canProcessOrdinals;
    private final ClassName implementation;
    private final TypeName argumentType;
    private final List<TypeMirror> warnExceptions;

    public ConvertEvaluatorImplementer(
        Elements elements,
        javax.lang.model.util.Types types,
        ExecutableElement processFunction,
        String extraName,
        List<TypeMirror> warnExceptions
    ) {
        this.declarationType = (TypeElement) processFunction.getEnclosingElement();
        this.processFunction = new EvaluatorImplementer.ProcessFunction(types, processFunction, warnExceptions);
        this.canProcessOrdinals = warnExceptions.isEmpty()
            && this.processFunction.returnType().equals(BYTES_REF)
            && this.processFunction.args.get(0) instanceof EvaluatorImplementer.StandardProcessFunctionArg s
            && s.type().equals(BYTES_REF);

        if (this.processFunction.args.get(0) instanceof EvaluatorImplementer.StandardProcessFunctionArg == false) {
            throw new IllegalArgumentException("first argument must be the field to process");
        }
        for (int a = 1; a < this.processFunction.args.size(); a++) {
            if (this.processFunction.args.get(a) instanceof EvaluatorImplementer.FixedProcessFunctionArg == false) {
                throw new IllegalArgumentException("fixed function args supported after the first");
                // TODO support more function types when we need them
            }
        }

        this.argumentType = TypeName.get(processFunction.getParameters().get(0).asType());
        this.warnExceptions = warnExceptions;

        this.implementation = ClassName.get(
            elements.getPackageOf(declarationType).toString(),
            declarationType.getSimpleName() + extraName + "Evaluator"
        );
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
        builder.addJavadoc("{@link $T} implementation for {@link $T}.\n", EXPRESSION_EVALUATOR, declarationType);
        builder.addJavadoc("This class is generated. Edit {@code " + getClass().getSimpleName() + "} instead.");
        builder.addModifiers(Modifier.PUBLIC, Modifier.FINAL);
        builder.superclass(ABSTRACT_CONVERT_FUNCTION_EVALUATOR);

        for (EvaluatorImplementer.ProcessFunctionArg a : processFunction.args) {
            a.declareField(builder);
        }
        builder.addMethod(ctor());
        builder.addMethod(next());
        builder.addMethod(evalVector());
        builder.addMethod(evalValue(true));
        builder.addMethod(evalBlock());
        builder.addMethod(evalValue(false));
        if (canProcessOrdinals) {
            builder.addMethod(evalOrdinals());
        }
        builder.addMethod(processFunction.toStringMethod(implementation));
        builder.addMethod(processFunction.close());
        builder.addType(factory());
        return builder.build();
    }

    private MethodSpec ctor() {
        MethodSpec.Builder builder = MethodSpec.constructorBuilder().addModifiers(Modifier.PUBLIC);
        builder.addParameter(SOURCE, "source");
        builder.addStatement("super(driverContext, source)");
        for (EvaluatorImplementer.ProcessFunctionArg a : processFunction.args) {
            a.implementCtor(builder);
        }
        builder.addParameter(DRIVER_CONTEXT, "driverContext");
        return builder.build();
    }

    private MethodSpec next() {
        MethodSpec.Builder builder = MethodSpec.methodBuilder("next").addAnnotation(Override.class).addModifiers(Modifier.PUBLIC);
        builder.returns(EXPRESSION_EVALUATOR);
        builder.addStatement("return $N", ((EvaluatorImplementer.StandardProcessFunctionArg) processFunction.args.get(0)).name());
        return builder.build();
    }

    private MethodSpec evalVector() {
        MethodSpec.Builder builder = MethodSpec.methodBuilder("evalVector").addAnnotation(Override.class).addModifiers(Modifier.PUBLIC);
        builder.addParameter(VECTOR, "v").returns(BLOCK);

        TypeName vectorType = vectorType(argumentType);
        builder.addStatement("$T vector = ($T) v", vectorType, vectorType);
        if (canProcessOrdinals) {
            builder.addStatement("$T ordinals = vector.asOrdinals()", ORDINALS_BYTES_REF_VECTOR);
            builder.beginControlFlow("if (ordinals != null)");
            {
                builder.addStatement("return evalOrdinals(ordinals)");
            }
            builder.endControlFlow();
        }

        builder.addStatement("int positionCount = v.getPositionCount()");

        String scratchPadName = argumentType.equals(BYTES_REF) ? "scratchPad" : null;
        if (argumentType.equals(BYTES_REF)) {
            builder.addStatement("BytesRef $N = new BytesRef()", scratchPadName);
        }

        builder.beginControlFlow("if (vector.isConstant())");
        {
            catchingWarnExceptions(builder, () -> {
                var constVectType = processFunction.resultDataType(true);
                builder.addStatement(
                    "return driverContext.blockFactory().newConstant$TWith($N, positionCount)",
                    constVectType,
                    evalValueCall("vector", "0", scratchPadName)
                );
            }, () -> builder.addStatement("return driverContext.blockFactory().newConstantNullBlock(positionCount)"));
        }
        builder.endControlFlow();

        ClassName resultBuilderType = builderType(processFunction.resultDataType(true));
        builder.beginControlFlow(
            "try ($T builder = driverContext.blockFactory().$L(positionCount))",
            resultBuilderType,
            buildFromFactory(resultBuilderType)
        );
        {
            builder.beginControlFlow("for (int p = 0; p < positionCount; p++)");
            {
                catchingWarnExceptions(
                    builder,
                    () -> builder.addStatement(
                        "builder.$L($N)",
                        processFunction.appendMethod(),
                        evalValueCall("vector", "p", scratchPadName)
                    ),
                    () -> builder.addStatement("builder.appendNull()")
                );
            }
            builder.endControlFlow();
            builder.addStatement("return builder.build()");
        }
        builder.endControlFlow();

        return builder.build();
    }

    private void catchingWarnExceptions(MethodSpec.Builder builder, Runnable whileCatching, Runnable ifCaught) {
        if (warnExceptions.isEmpty()) {
            whileCatching.run();
            return;
        }
        builder.beginControlFlow("try");
        whileCatching.run();
        builder.nextControlFlow(
            "catch (" + warnExceptions.stream().map(m -> "$T").collect(Collectors.joining(" | ")) + "  e)",
            warnExceptions.stream().map(m -> TypeName.get(m)).toArray()
        );
        builder.addStatement("registerException(e)");
        ifCaught.run();
        builder.endControlFlow();
    }

    private MethodSpec evalBlock() {
        MethodSpec.Builder builder = MethodSpec.methodBuilder("evalBlock").addAnnotation(Override.class).addModifiers(Modifier.PUBLIC);
        builder.addParameter(BLOCK, "b").returns(BLOCK);

        TypeName blockType = blockType(argumentType);
        builder.addStatement("$T block = ($T) b", blockType, blockType);
        builder.addStatement("int positionCount = block.getPositionCount()");
        TypeName resultBuilderType = builderType(processFunction.resultDataType(true));
        builder.beginControlFlow(
            "try ($T builder = driverContext.blockFactory().$L(positionCount))",
            resultBuilderType,
            buildFromFactory(resultBuilderType)
        );
        String scratchPadName = argumentType.equals(BYTES_REF) ? "scratchPad" : null;
        if (argumentType.equals(BYTES_REF)) {
            builder.addStatement("BytesRef $N = new BytesRef()", scratchPadName);
        }

        String appendMethod = processFunction.appendMethod();
        builder.beginControlFlow("for (int p = 0; p < positionCount; p++)");
        {
            builder.addStatement("int valueCount = block.getValueCount(p)");
            builder.addStatement("int start = block.getFirstValueIndex(p)");
            builder.addStatement("int end = start + valueCount");
            builder.addStatement("boolean positionOpened = false");
            builder.addStatement("boolean valuesAppended = false");
            builder.beginControlFlow("for (int i = start; i < end; i++)");
            {
                catchingWarnExceptions(builder, () -> {
                    builder.addStatement("$T value = $N", processFunction.returnType(), evalValueCall("block", "i", scratchPadName));
                    builder.beginControlFlow("if (positionOpened == false && valueCount > 1)");
                    {
                        builder.addStatement("builder.beginPositionEntry()");
                        builder.addStatement("positionOpened = true");
                    }
                    builder.endControlFlow();
                    builder.addStatement("builder.$N(value)", appendMethod);
                    builder.addStatement("valuesAppended = true");
                }, () -> {});
            }
            builder.endControlFlow();
            builder.beginControlFlow("if (valuesAppended == false)");
            {
                builder.addStatement("builder.appendNull()");
            }
            builder.nextControlFlow("else if (positionOpened)");
            {
                builder.addStatement("builder.endPositionEntry()");
            }
            builder.endControlFlow();
        }
        builder.endControlFlow();

        builder.addStatement("return builder.build()");
        builder.endControlFlow();

        return builder.build();
    }

    private String evalValueCall(String container, String index, String scratchPad) {
        StringBuilder builder = new StringBuilder("evalValue(");
        builder.append(container);
        builder.append(", ");
        builder.append(index);
        if (scratchPad != null) {
            builder.append(", ");
            builder.append(scratchPad);
        }
        builder.append(")");
        return builder.toString();
    }

    private MethodSpec evalValue(boolean forVector) {
        MethodSpec.Builder builder = MethodSpec.methodBuilder("evalValue")
            .addModifiers(Modifier.PRIVATE)
            .returns(processFunction.returnType());

        if (forVector) {
            builder.addParameter(vectorType(argumentType), "container");
        } else {
            builder.addParameter(blockType(argumentType), "container");
        }
        builder.addParameter(TypeName.INT, "index");
        if (argumentType.equals(BYTES_REF)) {
            builder.addParameter(BYTES_REF, "scratchPad");
            builder.addStatement("$T value = container.$N(index, scratchPad)", argumentType, getMethod(argumentType));
        } else {
            builder.addStatement("$T value = container.$N(index)", argumentType, getMethod(argumentType));
        }

        StringBuilder pattern = new StringBuilder();
        List<Object> args = new ArrayList<>();
        pattern.append("return $T.$N(value");
        args.add(declarationType);
        args.add(processFunction.function.getSimpleName());
        for (int a = 1; a < processFunction.args.size(); a++) {
            pattern.append(", ");
            processFunction.args.get(a).buildInvocation(pattern, args, false /* block style parameter should be unused */);
        }
        pattern.append(")");
        builder.addStatement(pattern.toString(), args.toArray());
        return builder.build();
    }

    private MethodSpec evalOrdinals() {
        MethodSpec.Builder builder = MethodSpec.methodBuilder("evalOrdinals").addModifiers(Modifier.PRIVATE);
        builder.addParameter(ORDINALS_BYTES_REF_VECTOR, "v").returns(BLOCK);

        builder.addStatement("int positionCount = v.getDictionaryVector().getPositionCount()");
        builder.addStatement("BytesRef scratchPad = new BytesRef()");
        builder.beginControlFlow(
            "try ($T builder = driverContext.blockFactory().newBytesRefVectorBuilder(positionCount))",
            BYTES_REF_VECTOR_BUILDER
        );
        {
            builder.beginControlFlow("for (int p = 0; p < positionCount; p++)");
            {
                builder.addStatement("builder.appendBytesRef($N)", evalValueCall("v.getDictionaryVector()", "p", "scratchPad"));
            }
            builder.endControlFlow();
            builder.addStatement("$T ordinals = v.getOrdinalsVector()", INT_VECTOR);
            builder.addStatement("ordinals.incRef()");
            builder.addStatement("return new $T(ordinals, builder.build()).asBlock()", ORDINALS_BYTES_REF_VECTOR);
        }
        builder.endControlFlow();

        return builder.build();
    }

    private TypeSpec factory() {
        TypeSpec.Builder builder = TypeSpec.classBuilder("Factory");
        builder.addSuperinterface(EXPRESSION_EVALUATOR_FACTORY);
        builder.addModifiers(Modifier.PUBLIC, Modifier.STATIC);

        builder.addField(SOURCE, "source", Modifier.PRIVATE, Modifier.FINAL);
        processFunction.args.forEach(a -> a.declareFactoryField(builder));

        builder.addMethod(processFunction.factoryCtor());
        builder.addMethod(processFunction.factoryGet(implementation));
        builder.addMethod(processFunction.toStringMethod(implementation));
        return builder.build();
    }
}
