/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.gen;

import com.squareup.javapoet.ArrayTypeName;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;

import org.elasticsearch.compute.ann.Fixed;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.VariableElement;
import javax.lang.model.type.ArrayType;
import javax.lang.model.type.TypeKind;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.Elements;

import static org.elasticsearch.compute.gen.Types.EXPRESSION;
import static org.elasticsearch.compute.gen.Types.EXPRESSION_EVALUATOR;
import static org.elasticsearch.compute.gen.Types.PAGE;

public class EvaluatorImplementer {
    private final TypeElement declarationType;
    private final ExecutableElement processFunction;
    private final ClassName implementation;

    public EvaluatorImplementer(Elements elements, ExecutableElement processFunction, String extraName) {
        this.declarationType = (TypeElement) processFunction.getEnclosingElement();
        this.processFunction = processFunction;

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
        builder.addSuperinterface(EXPRESSION_EVALUATOR);

        for (VariableElement v : processFunction.getParameters()) {
            if (v.getAnnotation(Fixed.class) == null) {
                String name = v.getSimpleName().toString();
                TypeName type = EXPRESSION_EVALUATOR;
                if (v.asType().getKind() == TypeKind.ARRAY) {
                    builder.addField(TypeName.get(v.asType()), name + "Val", Modifier.PRIVATE, Modifier.FINAL);
                    type = ArrayTypeName.of(type);
                }
                builder.addField(type, name, Modifier.PRIVATE, Modifier.FINAL);
            } else {
                builder.addField(TypeName.get(v.asType()), v.getSimpleName().toString(), Modifier.PRIVATE, Modifier.FINAL);
            }
        }

        builder.addMethod(ctor());
        builder.addMethod(fold());
        builder.addMethod(computeRow());
        builder.addMethod(toStringMethod());
        return builder.build();
    }

    private MethodSpec ctor() {
        MethodSpec.Builder builder = MethodSpec.constructorBuilder().addModifiers(Modifier.PUBLIC);
        for (VariableElement v : processFunction.getParameters()) {
            String name = v.getSimpleName().toString();
            if (v.getAnnotation(Fixed.class) == null) {
                TypeName type = EXPRESSION_EVALUATOR;
                if (v.asType().getKind() == TypeKind.ARRAY) {
                    TypeMirror componentType = ((ArrayType) v.asType()).getComponentType();
                    builder.addStatement("this.$LVal = new $T[$L.length]", name, componentType, name);
                    type = ArrayTypeName.of(type);
                }
                builder.addParameter(type, name);
                builder.addStatement("this.$L = $L", name, name);
            } else {
                builder.addParameter(TypeName.get(v.asType()), name);
                builder.addStatement("this.$L = $L", name, name);
            }
        }
        return builder.build();
    }

    private MethodSpec fold() {
        MethodSpec.Builder builder = MethodSpec.methodBuilder("fold")
            .addModifiers(Modifier.STATIC)
            .returns(TypeName.get(processFunction.getReturnType()).box());

        for (VariableElement v : processFunction.getParameters()) {
            String name = v.getSimpleName().toString();
            if (v.getAnnotation(Fixed.class) != null) {
                builder.addParameter(TypeName.get(v.asType()), name);
                continue;
            }
            if (v.asType().getKind() == TypeKind.ARRAY) {
                TypeMirror componentType = ((ArrayType) v.asType()).getComponentType();
                builder.addParameter(ParameterizedTypeName.get(ClassName.get(List.class), EXPRESSION), name);
                builder.addStatement("$T $LVal = new $T[$L.size()]", v.asType(), name, componentType, name);
                builder.beginControlFlow("for (int i = 0; i < $LVal.length; i++)", name);
                builder.addStatement("$LVal[i] = ($T) $L.get(i).fold()", name, componentType, name);
                builder.beginControlFlow("if ($LVal[i] == null)", name).addStatement("return null").endControlFlow();
                builder.endControlFlow();
                continue;
            }
            builder.addParameter(EXPRESSION, name);
            builder.addStatement("Object $LVal = $L.fold()", name, name);
            builder.beginControlFlow("if ($LVal == null)", name).addStatement("return null").endControlFlow();
        }

        invokeProcess(builder);
        return builder.build();
    }

    private MethodSpec computeRow() {
        MethodSpec.Builder builder = MethodSpec.methodBuilder("computeRow").addAnnotation(Override.class);
        builder.addModifiers(Modifier.PUBLIC).returns(Object.class).addParameter(PAGE, "page").addParameter(int.class, "position");

        for (VariableElement v : processFunction.getParameters()) {
            String name = v.getSimpleName().toString();
            if (v.getAnnotation(Fixed.class) == null) {
                if (v.asType().getKind() == TypeKind.ARRAY) {
                    TypeMirror componentType = ((ArrayType) v.asType()).getComponentType();
                    builder.beginControlFlow("for (int i = 0; i < $LVal.length; i++)", name);
                    builder.addStatement("$LVal[i] = ($T) $L[i].computeRow(page, position)", name, componentType, name);
                    builder.beginControlFlow("if ($LVal[i] == null)", name).addStatement("return null").endControlFlow();
                    builder.endControlFlow();
                } else {
                    builder.addStatement("Object $LVal = $L.computeRow(page, position)", name, name);
                    builder.beginControlFlow("if ($LVal == null)", name).addStatement("return null").endControlFlow();
                }
            }
        }

        invokeProcess(builder);
        return builder.build();
    }

    private MethodSpec toStringMethod() {
        MethodSpec.Builder builder = MethodSpec.methodBuilder("toString").addAnnotation(Override.class);
        builder.addModifiers(Modifier.PUBLIC).returns(String.class);

        StringBuilder pattern = new StringBuilder();
        pattern.append("return $S");
        List<Object> args = new ArrayList<>();
        args.add(implementation.simpleName() + "[");
        for (VariableElement v : processFunction.getParameters()) {
            Fixed fixed = v.getAnnotation(Fixed.class);
            if (fixed != null && false == fixed.includeInToString()) {
                continue;
            }
            args.add((args.size() > 2 ? ", " : "") + v.getSimpleName() + "=");
            if (v.asType().getKind() == TypeKind.ARRAY) {
                pattern.append(" + $S + $T.toString($L)");
                args.add(Arrays.class);
            } else {
                pattern.append(" + $S + $L");
            }
            args.add(v.getSimpleName());
        }
        pattern.append(" + $S");
        args.add("]");
        builder.addStatement(pattern.toString(), args.toArray());
        return builder.build();
    }

    private void invokeProcess(MethodSpec.Builder builder) {
        StringBuilder pattern = new StringBuilder();
        List<Object> args = new ArrayList<>();
        pattern.append("return $T.$N(");
        args.add(declarationType);
        args.add(processFunction.getSimpleName());
        for (VariableElement v : processFunction.getParameters()) {
            if (args.size() > 2) {
                pattern.append(", ");
            }
            if (v.getAnnotation(Fixed.class) == null) {
                if (v.asType().getKind() == TypeKind.ARRAY) {
                    pattern.append("$LVal");
                    args.add(v.getSimpleName());
                } else {
                    pattern.append("($T) $LVal");
                    args.add(v.asType());
                    args.add(v.getSimpleName());
                }
            } else {
                pattern.append("$L");
                args.add(v.getSimpleName());
            }
        }
        builder.addStatement(pattern.append(")").toString(), args.toArray());
    }
}
