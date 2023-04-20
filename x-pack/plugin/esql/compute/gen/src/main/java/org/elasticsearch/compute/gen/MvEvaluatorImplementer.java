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
import static org.elasticsearch.compute.gen.Types.ABSTRACT_MULTIVALUE_FUNCTION_EVALUATOR;
import static org.elasticsearch.compute.gen.Types.BIG_ARRAYS;
import static org.elasticsearch.compute.gen.Types.BLOCK;
import static org.elasticsearch.compute.gen.Types.BYTES_REF;
import static org.elasticsearch.compute.gen.Types.BYTES_REF_ARRAY;
import static org.elasticsearch.compute.gen.Types.EXPRESSION_EVALUATOR;
import static org.elasticsearch.compute.gen.Types.VECTOR;
import static org.elasticsearch.compute.gen.Types.arrayVectorType;
import static org.elasticsearch.compute.gen.Types.blockType;

public class MvEvaluatorImplementer {
    private final TypeElement declarationType;
    private final ExecutableElement processFunction;
    private final ClassName implementation;
    private final TypeName fieldType;

    public MvEvaluatorImplementer(Elements elements, ExecutableElement processFunction, String extraName) {
        this.declarationType = (TypeElement) processFunction.getEnclosingElement();
        this.processFunction = processFunction;
        this.fieldType = TypeName.get(processFunction.getParameters().get(0).asType());

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
        builder.superclass(ABSTRACT_MULTIVALUE_FUNCTION_EVALUATOR);

        builder.addMethod(ctor());
        builder.addMethod(name());
        builder.addMethod(eval("evalNullable", true));
        builder.addMethod(eval("evalNotNullable", false));
        return builder.build();
    }

    private MethodSpec ctor() {
        MethodSpec.Builder builder = MethodSpec.constructorBuilder().addModifiers(Modifier.PUBLIC);
        builder.addParameter(EXPRESSION_EVALUATOR, "field");
        builder.addStatement("super($L)", "field");
        return builder.build();
    }

    private MethodSpec name() {
        MethodSpec.Builder builder = MethodSpec.methodBuilder("name").addModifiers(Modifier.PUBLIC);
        builder.addAnnotation(Override.class).returns(String.class);
        builder.addStatement("return $S", declarationType.getSimpleName());
        return builder.build();
    }

    private MethodSpec eval(String name, boolean nullable) {
        MethodSpec.Builder builder = MethodSpec.methodBuilder(name).addModifiers(Modifier.PUBLIC);
        builder.addAnnotation(Override.class).returns(nullable ? BLOCK : VECTOR).addParameter(BLOCK, "fieldVal");
        TypeName blockType = blockType(fieldType);

        if (fieldType.equals(BYTES_REF)) {
            builder.addStatement("$T firstScratch = new $T()", BYTES_REF, BYTES_REF);
            builder.addStatement("$T nextScratch = new $T()", BYTES_REF, BYTES_REF);
        }

        builder.addStatement("$T v = ($T) fieldVal", blockType, blockType);
        builder.addStatement("int positionCount = v.getPositionCount()");
        if (nullable) {
            builder.addStatement("$T.Builder builder = $T.newBlockBuilder(positionCount)", blockType, blockType);
        } else if (fieldType.equals(BYTES_REF)) {
            builder.addStatement(
                "$T values = new $T(positionCount, $T.NON_RECYCLING_INSTANCE)",  // TODO blocks should use recycling array
                BYTES_REF_ARRAY,
                BYTES_REF_ARRAY,
                BIG_ARRAYS
            );
        } else {
            builder.addStatement("$T[] values = new $T[positionCount]", fieldType, fieldType);
        }

        builder.beginControlFlow("for (int p = 0; p < positionCount; p++)");
        {
            builder.addStatement("int valueCount = v.getValueCount(p)");
            if (nullable) {
                builder.beginControlFlow("if (valueCount == 0)");
                builder.addStatement("builder.appendNull()");
                builder.addStatement("continue");
                builder.endControlFlow();
            }
            builder.addStatement("int first = v.getFirstValueIndex(p)");
            builder.addStatement("int end = first + valueCount");

            fetch(builder, "value", "first", "firstScratch");
            builder.beginControlFlow("for (int i = first + 1; i < end; i++)");
            {
                fetch(builder, "next", "i", "nextScratch");
                if (fieldType.equals(BYTES_REF)) {
                    builder.addStatement("$T.$L(value, next)", declarationType, processFunction.getSimpleName());
                } else {
                    builder.addStatement("value = $T.$L(value, next)", declarationType, processFunction.getSimpleName());
                }
            }
            builder.endControlFlow();
            if (nullable) {
                builder.addStatement("builder.$L(value)", appendMethod(fieldType));
            } else if (fieldType.equals(BYTES_REF)) {
                builder.addStatement("values.append(value)");
            } else {
                builder.addStatement("values[p] = value");
            }
        }
        builder.endControlFlow();
        if (nullable) {
            builder.addStatement("return builder.build()");
        } else {
            builder.addStatement("return new $T(values, positionCount)", arrayVectorType(fieldType));
        }
        return builder.build();
    }

    private void fetch(MethodSpec.Builder builder, String into, String index, String scratchName) {
        if (fieldType.equals(BYTES_REF)) {
            builder.addStatement("$T $L = v.getBytesRef($L, $L)", fieldType, into, index, scratchName);
        } else {
            builder.addStatement("$T $L = v.$L($L)", fieldType, into, getMethod(fieldType), index);
        }
    }
}
