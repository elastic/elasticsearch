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

import static org.elasticsearch.compute.gen.Methods.appendMethod;
import static org.elasticsearch.compute.gen.Methods.buildFromFactory;
import static org.elasticsearch.compute.gen.Methods.getMethod;
import static org.elasticsearch.compute.gen.Types.ABSTRACT_CONVERT_FUNCTION_EVALUATOR;
import static org.elasticsearch.compute.gen.Types.BLOCK;
import static org.elasticsearch.compute.gen.Types.BYTES_REF;
import static org.elasticsearch.compute.gen.Types.DRIVER_CONTEXT;
import static org.elasticsearch.compute.gen.Types.EXPRESSION_EVALUATOR;
import static org.elasticsearch.compute.gen.Types.EXPRESSION_EVALUATOR_FACTORY;
import static org.elasticsearch.compute.gen.Types.SOURCE;
import static org.elasticsearch.compute.gen.Types.VECTOR;
import static org.elasticsearch.compute.gen.Types.blockType;
import static org.elasticsearch.compute.gen.Types.builderType;
import static org.elasticsearch.compute.gen.Types.vectorType;

public class ConvertEvaluatorImplementer {

    private final TypeElement declarationType;
    private final ExecutableElement processFunction;
    private final String extraName;
    private final ClassName implementation;
    private final TypeName argumentType;
    private final TypeName resultType;
    private final List<TypeMirror> warnExceptions;

    public ConvertEvaluatorImplementer(
        Elements elements,
        ExecutableElement processFunction,
        String extraName,
        List<TypeMirror> warnExceptions
    ) {
        this.declarationType = (TypeElement) processFunction.getEnclosingElement();
        this.processFunction = processFunction;
        if (processFunction.getParameters().size() != 1) {
            throw new IllegalArgumentException("processing function should have exactly one parameter");
        }
        this.extraName = extraName;
        this.argumentType = TypeName.get(processFunction.getParameters().get(0).asType());
        this.resultType = TypeName.get(processFunction.getReturnType());
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

        builder.addMethod(ctor());
        builder.addMethod(name());
        builder.addMethod(evalVector());
        builder.addMethod(evalValue(true));
        builder.addMethod(evalBlock());
        builder.addMethod(evalValue(false));
        builder.addType(factory());
        return builder.build();
    }

    private MethodSpec ctor() {
        MethodSpec.Builder builder = MethodSpec.constructorBuilder().addModifiers(Modifier.PUBLIC);
        builder.addParameter(EXPRESSION_EVALUATOR, "field");
        builder.addParameter(SOURCE, "source");
        builder.addParameter(DRIVER_CONTEXT, "driverContext");
        builder.addStatement("super(driverContext, field, source)");
        return builder.build();
    }

    private MethodSpec name() {
        MethodSpec.Builder builder = MethodSpec.methodBuilder("name").addModifiers(Modifier.PUBLIC);
        builder.addAnnotation(Override.class).returns(String.class);
        builder.addStatement("return $S", declarationType.getSimpleName() + extraName);
        return builder.build();
    }

    private MethodSpec evalVector() {
        MethodSpec.Builder builder = MethodSpec.methodBuilder("evalVector").addAnnotation(Override.class).addModifiers(Modifier.PUBLIC);
        builder.addParameter(VECTOR, "v").returns(BLOCK);

        TypeName vectorType = vectorType(argumentType);
        builder.addStatement("$T vector = ($T) v", vectorType, vectorType);
        builder.addStatement("int positionCount = v.getPositionCount()");

        String scratchPadName = argumentType.equals(BYTES_REF) ? "scratchPad" : null;
        if (argumentType.equals(BYTES_REF)) {
            builder.addStatement("BytesRef $N = new BytesRef()", scratchPadName);
        }

        builder.beginControlFlow("if (vector.isConstant())");
        {
            catchingWarnExceptions(builder, () -> {
                var constVectType = blockType(resultType);
                builder.addStatement(
                    "return driverContext.blockFactory().newConstant$TWith($N, positionCount)",
                    constVectType,
                    evalValueCall("vector", "0", scratchPadName)
                );
            }, () -> builder.addStatement("return driverContext.blockFactory().newConstantNullBlock(positionCount)"));
        }
        builder.endControlFlow();

        ClassName resultBuilderType = builderType(blockType(resultType));
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
                    () -> builder.addStatement("builder.$L($N)", appendMethod(resultType), evalValueCall("vector", "p", scratchPadName)),
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
        TypeName resultBuilderType = builderType(blockType(resultType));
        builder.beginControlFlow(
            "try ($T builder = driverContext.blockFactory().$L(positionCount))",
            resultBuilderType,
            buildFromFactory(resultBuilderType)
        );
        String scratchPadName = argumentType.equals(BYTES_REF) ? "scratchPad" : null;
        if (argumentType.equals(BYTES_REF)) {
            builder.addStatement("BytesRef $N = new BytesRef()", scratchPadName);
        }

        String appendMethod = appendMethod(resultType);
        builder.beginControlFlow("for (int p = 0; p < positionCount; p++)");
        {
            builder.addStatement("int valueCount = block.getValueCount(p)");
            builder.addStatement("int start = block.getFirstValueIndex(p)");
            builder.addStatement("int end = start + valueCount");
            builder.addStatement("boolean positionOpened = false");
            builder.addStatement("boolean valuesAppended = false");
            // builder.addStatement("builder.beginPositionEntry()");
            builder.beginControlFlow("for (int i = start; i < end; i++)");
            {
                catchingWarnExceptions(builder, () -> {
                    builder.addStatement("$T value = $N", resultType, evalValueCall("block", "i", scratchPadName));
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
            .addModifiers(Modifier.PRIVATE, Modifier.STATIC)
            .returns(resultType);

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

        builder.addStatement("return $T.$N(value)", declarationType, processFunction.getSimpleName());

        return builder.build();
    }

    private TypeSpec factory() {
        TypeSpec.Builder builder = TypeSpec.classBuilder("Factory");
        builder.addSuperinterface(EXPRESSION_EVALUATOR_FACTORY);
        builder.addModifiers(Modifier.PUBLIC, Modifier.STATIC);

        builder.addField(SOURCE, "source", Modifier.PRIVATE, Modifier.FINAL);
        builder.addField(EXPRESSION_EVALUATOR_FACTORY, "field", Modifier.PRIVATE, Modifier.FINAL);

        builder.addMethod(factoryCtor());
        builder.addMethod(factoryGet());
        builder.addMethod(factoryToString());
        return builder.build();
    }

    private MethodSpec factoryCtor() {
        MethodSpec.Builder builder = MethodSpec.constructorBuilder().addModifiers(Modifier.PUBLIC);
        builder.addParameter(EXPRESSION_EVALUATOR_FACTORY, "field");
        builder.addParameter(SOURCE, "source");
        builder.addStatement("this.field = field");
        builder.addStatement("this.source = source");
        return builder.build();
    }

    private MethodSpec factoryGet() {
        MethodSpec.Builder builder = MethodSpec.methodBuilder("get").addAnnotation(Override.class);
        builder.addModifiers(Modifier.PUBLIC);
        builder.addParameter(DRIVER_CONTEXT, "context");
        builder.returns(implementation);

        List<String> args = new ArrayList<>();
        args.add("field.get(context)");
        args.add("source");
        args.add("context");
        builder.addStatement("return new $T($L)", implementation, args.stream().collect(Collectors.joining(", ")));
        return builder.build();
    }

    private MethodSpec factoryToString() {
        MethodSpec.Builder builder = MethodSpec.methodBuilder("toString").addAnnotation(Override.class);
        builder.addModifiers(Modifier.PUBLIC);
        builder.returns(String.class);
        builder.addStatement("return $S + field + $S", declarationType.getSimpleName() + extraName + "Evaluator[field=", "]");
        return builder.build();
    }
}
