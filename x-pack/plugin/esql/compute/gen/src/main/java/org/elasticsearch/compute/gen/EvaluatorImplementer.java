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

import java.util.stream.Collectors;

import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.VariableElement;
import javax.lang.model.util.Elements;

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
            builder.addField(EXPRESSION_EVALUATOR, v.getSimpleName().toString(), Modifier.PRIVATE, Modifier.FINAL);
        }

        builder.addMethod(ctor());
        builder.addMethod(process());
        builder.addMethod(computeRow());
        builder.addMethod(toStringMethod());
        return builder.build();
    }

    private MethodSpec ctor() {
        MethodSpec.Builder builder = MethodSpec.constructorBuilder().addModifiers(Modifier.PUBLIC);
        for (VariableElement v : processFunction.getParameters()) {
            String name = v.getSimpleName().toString();
            builder.addParameter(EXPRESSION_EVALUATOR, name);
            builder.addStatement("this.$L = $L", name, name);
        }
        return builder.build();
    }

    private MethodSpec process() {
        MethodSpec.Builder builder = MethodSpec.methodBuilder("process")
            .addModifiers(Modifier.STATIC)
            .returns(TypeName.get(processFunction.getReturnType()).box());

        for (VariableElement v : processFunction.getParameters()) {
            String name = v.getSimpleName().toString();
            builder.addParameter(Object.class, name + "Val");
            builder.beginControlFlow("if ($LVal == null)", name).addStatement("return null").endControlFlow();
        }

        StringBuilder pattern = new StringBuilder();
        pattern.append("return $T.$N(");
        int i = 0;
        Object[] args = new Object[2 + 2 * processFunction.getParameters().size()];
        args[i++] = declarationType;
        args[i++] = processFunction.getSimpleName();
        for (VariableElement v : processFunction.getParameters()) {
            if (i > 3) {
                pattern.append(", ");
            }
            pattern.append("($T) $LVal");
            args[i++] = v.asType();
            args[i++] = v.getSimpleName();
        }

        builder.addStatement(pattern.append(")").toString(), args);
        return builder.build();
    }

    private MethodSpec computeRow() {
        MethodSpec.Builder builder = MethodSpec.methodBuilder("computeRow").addAnnotation(Override.class);
        builder.addModifiers(Modifier.PUBLIC).returns(Object.class).addParameter(PAGE, "page").addParameter(int.class, "position");

        for (VariableElement v : processFunction.getParameters()) {
            String name = v.getSimpleName().toString();
            builder.addStatement("Object $LVal = $L.computeRow(page, position)", name, name);
        }

        builder.addStatement(
            "return process(" + processFunction.getParameters().stream().map(p -> "$LVal").collect(Collectors.joining(", ")) + ")",
            processFunction.getParameters().stream().map(p -> p.getSimpleName()).toArray()
        );
        return builder.build();
    }

    private MethodSpec toStringMethod() {
        MethodSpec.Builder builder = MethodSpec.methodBuilder("toString").addAnnotation(Override.class);
        builder.addModifiers(Modifier.PUBLIC).returns(String.class);

        StringBuilder pattern = new StringBuilder();
        pattern.append("return $S");
        int i = 0;
        Object[] args = new Object[2 + 2 * processFunction.getParameters().size()];
        args[i++] = implementation.simpleName() + "[";
        for (VariableElement v : processFunction.getParameters()) {
            pattern.append(" + $S + $L");
            args[i++] = (i > 2 ? ", " : "") + v.getSimpleName() + "=";
            args[i++] = v.getSimpleName();
        }
        pattern.append(" + $S");
        args[i] = "]";
        builder.addStatement(pattern.toString(), args);
        return builder.build();
    }
}
