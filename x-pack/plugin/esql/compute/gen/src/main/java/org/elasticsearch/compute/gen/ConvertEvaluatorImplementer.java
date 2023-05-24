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

import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.TypeElement;
import javax.lang.model.util.Elements;

import static org.elasticsearch.compute.gen.Methods.appendMethod;
import static org.elasticsearch.compute.gen.Methods.getMethod;
import static org.elasticsearch.compute.gen.Types.ABSTRACT_CONVERT_FUNCTION_EVALUATOR;
import static org.elasticsearch.compute.gen.Types.BIG_ARRAYS;
import static org.elasticsearch.compute.gen.Types.BLOCK;
import static org.elasticsearch.compute.gen.Types.BYTES_REF;
import static org.elasticsearch.compute.gen.Types.BYTES_REF_ARRAY;
import static org.elasticsearch.compute.gen.Types.EXPRESSION_EVALUATOR;
import static org.elasticsearch.compute.gen.Types.VECTOR;
import static org.elasticsearch.compute.gen.Types.arrayVectorType;
import static org.elasticsearch.compute.gen.Types.blockType;
import static org.elasticsearch.compute.gen.Types.constantVectorType;
import static org.elasticsearch.compute.gen.Types.vectorType;

public class ConvertEvaluatorImplementer {

    private final TypeElement declarationType;
    private final ExecutableElement processFunction;
    private final ClassName implementation;
    private final TypeName argumentType;
    private final TypeName resultType;

    public ConvertEvaluatorImplementer(Elements elements, ExecutableElement processFunction, String extraName) {
        this.declarationType = (TypeElement) processFunction.getEnclosingElement();
        this.processFunction = processFunction;
        if (processFunction.getParameters().size() != 1) {
            throw new IllegalArgumentException("processing function should have exactly one parameter");
        }
        this.argumentType = TypeName.get(processFunction.getParameters().get(0).asType());
        this.resultType = TypeName.get(processFunction.getReturnType());

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
        builder.addJavadoc("This class is generated. Do not edit it.");
        builder.addModifiers(Modifier.PUBLIC, Modifier.FINAL);
        builder.superclass(ABSTRACT_CONVERT_FUNCTION_EVALUATOR);

        builder.addMethod(ctor());
        builder.addMethod(name());
        builder.addMethod(evalVector());
        builder.addMethod(evalValue(true));
        builder.addMethod(evalBlock());
        builder.addMethod(evalValue(false));
        return builder.build();
    }

    private MethodSpec ctor() {
        MethodSpec.Builder builder = MethodSpec.constructorBuilder().addModifiers(Modifier.PUBLIC);
        builder.addParameter(EXPRESSION_EVALUATOR, "field");
        builder.addStatement("super($N)", "field");
        return builder.build();
    }

    private MethodSpec name() {
        MethodSpec.Builder builder = MethodSpec.methodBuilder("name").addModifiers(Modifier.PUBLIC);
        builder.addAnnotation(Override.class).returns(String.class);
        builder.addStatement("return $S", declarationType.getSimpleName());
        return builder.build();
    }

    private MethodSpec evalVector() {
        MethodSpec.Builder builder = MethodSpec.methodBuilder("evalVector").addAnnotation(Override.class).addModifiers(Modifier.PUBLIC);
        builder.addParameter(VECTOR, "v").returns(VECTOR);

        TypeName vectorType = vectorType(argumentType);
        builder.addStatement("$T vector = ($T) v", vectorType, vectorType);
        builder.addStatement("int positionCount = v.getPositionCount()");

        String scratchPadName = null;
        if (argumentType.equals(BYTES_REF)) {
            scratchPadName = "scratchPad";
            builder.addStatement("BytesRef $N = new BytesRef()", scratchPadName);
        }

        builder.beginControlFlow("if (vector.isConstant())");
        {
            var constVectType = constantVectorType(resultType);
            builder.addStatement("return new $T($N, positionCount)", constVectType, evalValueCall("vector", "0", scratchPadName));
        }
        builder.endControlFlow();

        if (resultType.equals(BYTES_REF)) {
            builder.addStatement(
                "$T values = new $T(positionCount, $T.NON_RECYCLING_INSTANCE)", // TODO: see note MvEvaluatorImplementer
                BYTES_REF_ARRAY,
                BYTES_REF_ARRAY,
                BIG_ARRAYS
            );
        } else {
            builder.addStatement("$T[] values = new $T[positionCount]", resultType, resultType);
        }
        builder.beginControlFlow("for (int p = 0; p < positionCount; p++)");
        {
            if (resultType.equals(BYTES_REF)) {
                builder.addStatement("values.append($N)", evalValueCall("vector", "p", scratchPadName));
            } else {
                builder.addStatement("values[p] = $N", evalValueCall("vector", "p", scratchPadName));
            }
        }
        builder.endControlFlow();

        builder.addStatement("return new $T(values, positionCount)", arrayVectorType(resultType));

        return builder.build();
    }

    private MethodSpec evalBlock() {
        MethodSpec.Builder builder = MethodSpec.methodBuilder("evalBlock").addAnnotation(Override.class).addModifiers(Modifier.PUBLIC);
        builder.addParameter(BLOCK, "b").returns(BLOCK);

        TypeName blockType = blockType(argumentType);
        builder.addStatement("$T block = ($T) b", blockType, blockType);
        builder.addStatement("int positionCount = block.getPositionCount()");
        TypeName resultBlockType = blockType(resultType);
        builder.addStatement("$T.Builder builder = $T.newBlockBuilder(positionCount)", resultBlockType, resultBlockType);
        String scratchPadName = null;
        if (argumentType.equals(BYTES_REF)) {
            scratchPadName = "scratchPad";
            builder.addStatement("BytesRef $N = new BytesRef()", scratchPadName);
        }

        String appendMethod = appendMethod(resultType);
        builder.beginControlFlow("for (int p = 0; p < positionCount; p++)");
        {
            builder.addStatement("int valueCount = block.getValueCount(p)");
            builder.beginControlFlow("if (valueCount == 0)");
            {
                builder.addStatement("builder.appendNull()");
                builder.addStatement("continue");
            }
            builder.endControlFlow();

            builder.addStatement("int start = block.getFirstValueIndex(p)");
            builder.addStatement("int end = start + valueCount");
            builder.addStatement("builder.beginPositionEntry()");
            builder.beginControlFlow("for (int i = start; i < end; i++)");
            {
                builder.addStatement("builder.$N($N)", appendMethod, evalValueCall("block", "i", scratchPadName));
            }
            builder.endControlFlow();
            builder.addStatement("builder.endPositionEntry()");
        }
        builder.endControlFlow();

        builder.addStatement("return builder.build()");

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
}
