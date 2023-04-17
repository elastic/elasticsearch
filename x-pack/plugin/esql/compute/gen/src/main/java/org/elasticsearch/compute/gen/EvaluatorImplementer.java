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
import java.util.stream.Collectors;

import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.VariableElement;
import javax.lang.model.type.ArrayType;
import javax.lang.model.type.TypeKind;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.Elements;

import static org.elasticsearch.compute.gen.Methods.appendMethod;
import static org.elasticsearch.compute.gen.Methods.getMethod;
import static org.elasticsearch.compute.gen.Types.BLOCK;
import static org.elasticsearch.compute.gen.Types.BYTES_REF;
import static org.elasticsearch.compute.gen.Types.EXPRESSION;
import static org.elasticsearch.compute.gen.Types.EXPRESSION_EVALUATOR;
import static org.elasticsearch.compute.gen.Types.PAGE;
import static org.elasticsearch.compute.gen.Types.VECTOR;
import static org.elasticsearch.compute.gen.Types.blockType;
import static org.elasticsearch.compute.gen.Types.vectorType;

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
            builder.addField(typeForParameter(v, EXPRESSION_EVALUATOR), v.getSimpleName().toString(), Modifier.PRIVATE, Modifier.FINAL);
        }

        builder.addMethod(ctor());
        builder.addMethod(fold());
        builder.addMethod(eval());
        builder.addMethod(realEval(BLOCK, "Block", blockType(TypeName.get(processFunction.getReturnType())), true, "newBlockBuilder"));
        builder.addMethod(realEval(VECTOR, "Vector", vectorType(TypeName.get(processFunction.getReturnType())), false, "newVectorBuilder"));
        builder.addMethod(toStringMethod());
        return builder.build();
    }

    private MethodSpec ctor() {
        MethodSpec.Builder builder = MethodSpec.constructorBuilder().addModifiers(Modifier.PUBLIC);
        for (VariableElement v : processFunction.getParameters()) {
            String name = v.getSimpleName().toString();
            builder.addParameter(typeForParameter(v, EXPRESSION_EVALUATOR), name);
            builder.addStatement("this.$L = $L", name, name);
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
                switch (componentType.getKind()) {
                    case INT -> builder.addStatement("$LVal[i] = ((Number) $L.get(i).fold()).intValue()", name, name);
                    case LONG -> builder.addStatement("$LVal[i] = ((Number) $L.get(i).fold()).longValue()", name, name);
                    case DOUBLE -> builder.addStatement("$LVal[i] = ((Number) $L.get(i).fold()).doubleValue()", name, name);
                    default -> builder.addStatement("$LVal[i] = ($T) $L.get(i).fold()", name, componentType, name);
                }

                builder.beginControlFlow("if ($LVal[i] == null)", name).addStatement("return null").endControlFlow();
                builder.endControlFlow();
                continue;
            }
            builder.addParameter(EXPRESSION, name);
            builder.addStatement("Object $LVal = $L.fold()", name, name);
            builder.beginControlFlow("if ($LVal == null)", name).addStatement("return null").endControlFlow();
        }

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
                switch (v.asType().getKind()) {
                    case ARRAY -> {
                        pattern.append("$LVal");
                        args.add(v.getSimpleName());
                    }
                    case INT -> {
                        pattern.append("((Number) $LVal).intValue()");
                        args.add(v.getSimpleName());
                    }
                    case LONG -> {
                        pattern.append("((Number) $LVal).longValue()");
                        args.add(v.getSimpleName());
                    }
                    case DOUBLE -> {
                        pattern.append("((Number) $LVal).doubleValue()");
                        args.add(v.getSimpleName());
                    }
                    default -> {
                        pattern.append("($T) $LVal");
                        args.add(v.asType());
                        args.add(v.getSimpleName());
                    }
                }
            } else {
                pattern.append("$L");
                args.add(v.getSimpleName());
            }
        }
        builder.addStatement(pattern.append(")").toString(), args.toArray());
        return builder.build();
    }

    private MethodSpec eval() {
        MethodSpec.Builder builder = MethodSpec.methodBuilder("eval").addAnnotation(Override.class);
        builder.addModifiers(Modifier.PUBLIC).returns(BLOCK).addParameter(PAGE, "page");

        for (VariableElement v : processFunction.getParameters()) {
            if (v.getAnnotation(Fixed.class) != null) {
                continue;
            }
            String name = v.getSimpleName().toString();
            if (v.asType().getKind() == TypeKind.ARRAY) {
                TypeMirror componentType = ((ArrayType) v.asType()).getComponentType();
                TypeName blockType = blockType(TypeName.get(componentType));
                builder.addStatement("$T[] $LBlocks = new $T[$L.length]", blockType, name, blockType, name);
                builder.beginControlFlow("for (int i = 0; i < $LBlocks.length; i++)", name);
                {
                    builder.addStatement("Block block = $L[i].eval(page)", name);
                    builder.beginControlFlow("if (block.areAllValuesNull())");
                    builder.addStatement("return Block.constantNullBlock(page.getPositionCount())");
                    builder.endControlFlow();
                    builder.addStatement("$LBlocks[i] = ($T) block", name, blockType);
                }
                builder.endControlFlow();
            } else {
                TypeName blockType = blockType(TypeName.get(v.asType()));
                builder.addStatement("Block $LUncastBlock = $L.eval(page)", name, name);
                builder.beginControlFlow("if ($LUncastBlock.areAllValuesNull())", name);
                builder.addStatement("return Block.constantNullBlock(page.getPositionCount())");
                builder.endControlFlow();
                builder.addStatement("$T $LBlock = ($T) $LUncastBlock", blockType, name, blockType, name);
            }
        }
        for (VariableElement v : processFunction.getParameters()) {
            String name = v.getSimpleName().toString();
            if (v.getAnnotation(Fixed.class) != null) {
                continue;
            }
            if (v.asType().getKind() == TypeKind.ARRAY) {
                TypeMirror componentType = ((ArrayType) v.asType()).getComponentType();
                TypeName vectorType = vectorType(TypeName.get(componentType));
                builder.addStatement("$T[] $LVectors = new $T[$L.length]", vectorType, name, vectorType, name);
                builder.beginControlFlow("for (int i = 0; i < $LBlocks.length; i++)", name);
                builder.addStatement("$LVectors[i] = $LBlocks[i].asVector()", name, name);
                builder.beginControlFlow("if ($LVectors[i] == null)", name).addStatement(invokeNextEval("Block")).endControlFlow();
                builder.endControlFlow();
            } else {
                builder.addStatement("$T $LVector = $LBlock.asVector()", typeForParameter(v, VECTOR), name, name);
                builder.beginControlFlow("if ($LVector == null)", name).addStatement(invokeNextEval("Block")).endControlFlow();
            }
        }
        builder.addStatement(invokeNextEval("Vector") + ".asBlock()");
        return builder.build();
    }

    private String invokeNextEval(String flavor) {
        return "return eval(page.getPositionCount(), " + processFunction.getParameters().stream().map(v -> {
            String name = v.getSimpleName().toString();
            if (v.getAnnotation(Fixed.class) != null) {
                return name;
            }
            if (v.asType().getKind() == TypeKind.ARRAY) {
                return name + flavor + "s";
            }
            return name + flavor;
        }).collect(Collectors.joining(", ")) + ")";
    }

    private String nameForParameter(VariableElement v, String flavor) {
        if (v.getAnnotation(Fixed.class) != null) {
            return v.getSimpleName().toString();
        }
        return v.getSimpleName() + flavor + (v.asType().getKind() == TypeKind.ARRAY ? "s" : "");
    }

    private TypeName typeForParameter(VariableElement v, TypeName flavor) {
        if (v.getAnnotation(Fixed.class) != null) {
            return TypeName.get(v.asType());
        }
        if (v.asType().getKind() == TypeKind.ARRAY) {
            TypeMirror componentType = ((ArrayType) v.asType()).getComponentType();
            return ArrayTypeName.of(typeParameterForMirror(componentType, flavor));
        }
        return typeParameterForMirror(v.asType(), flavor);
    }

    private TypeName typeParameterForMirror(TypeMirror mirror, TypeName flavor) {
        if (flavor.equals(BLOCK)) {
            return blockType(TypeName.get(mirror));
        }
        if (flavor.equals(VECTOR)) {
            return vectorType(TypeName.get(mirror));
        }
        return flavor;
    }

    private MethodSpec realEval(
        TypeName typeFlavor,
        String nameFlavor,
        TypeName resultType,
        boolean blockStyle,
        String resultBuilderMethod
    ) {
        MethodSpec.Builder builder = MethodSpec.methodBuilder("eval");
        builder.addModifiers(Modifier.PUBLIC).returns(resultType);
        builder.addParameter(TypeName.INT, "positionCount");

        for (VariableElement v : processFunction.getParameters()) {
            builder.addParameter(typeForParameter(v, typeFlavor), nameForParameter(v, nameFlavor));
        }

        builder.addStatement("$T.Builder result = $T.$L(positionCount)", resultType, resultType, resultBuilderMethod);

        // Create any scratch variables we need
        for (VariableElement v : processFunction.getParameters()) {
            if (TypeName.get(v.asType()).equals(BYTES_REF)) {
                builder.addStatement("BytesRef $LScratch = new BytesRef()", v.getSimpleName().toString());
            }
            if (v.asType().getKind() == TypeKind.ARRAY) {
                TypeMirror componentType = ((ArrayType) v.asType()).getComponentType();
                String name = v.getSimpleName().toString();
                builder.addStatement("$T[] $LValues = new $T[$L.length]", componentType, name, componentType, name);
                if (TypeName.get(componentType).equals(BYTES_REF)) {
                    builder.addStatement("$T[] $LScratch = new $T[$L.length]", componentType, name, componentType, name);
                    builder.beginControlFlow("for (int i = 0; i < $L.length; i++)", v.getSimpleName());
                    builder.addStatement("$LScratch[i] = new BytesRef()", v.getSimpleName());
                    builder.endControlFlow();
                }
            }
        }

        builder.beginControlFlow("position: for (int p = 0; p < positionCount; p++)");
        {
            if (blockStyle) {
                for (VariableElement v : processFunction.getParameters()) {
                    if (v.getAnnotation(Fixed.class) != null) {
                        continue;
                    }
                    String name = nameForParameter(v, nameFlavor);
                    if (v.asType().getKind() != TypeKind.ARRAY) {
                        skipNull(builder, name);
                        continue;
                    }
                    builder.beginControlFlow("for (int i = 0; i < $L.length; i++)", v.getSimpleName());
                    skipNull(builder, name + "[i]");
                    builder.endControlFlow();
                }
            }

            for (VariableElement v : processFunction.getParameters()) {
                if (v.getAnnotation(Fixed.class) != null || v.asType().getKind() != TypeKind.ARRAY) {
                    continue;
                }
                String name = nameForParameter(v, nameFlavor);
                builder.beginControlFlow("for (int i = 0; i < $L.length; i++)", v.getSimpleName());
                TypeMirror componentType = ((ArrayType) v.asType()).getComponentType();
                String lookupVar;
                if (blockStyle) {
                    lookupVar = "o";
                    builder.addStatement("int o = $LBlocks[i].getFirstValueIndex(p)", v.getSimpleName());
                } else {
                    lookupVar = "p";
                }
                if (TypeName.get(componentType).equals(BYTES_REF)) {
                    builder.addStatement(
                        "$LValues[i] = $L[i].getBytesRef($L, $LScratch[i])",
                        v.getSimpleName(),
                        name,
                        lookupVar,
                        v.getSimpleName()
                    );
                } else {
                    builder.addStatement(
                        "$LValues[i] = $L[i].$L($L)",
                        v.getSimpleName(),
                        name,
                        getMethod(TypeName.get(v.asType())),
                        lookupVar
                    );
                }
                builder.endControlFlow();
            }
        }

        StringBuilder pattern = new StringBuilder();
        List<Object> args = new ArrayList<>();
        pattern.append("result.$L($T.$N(");
        args.add(appendMethod(TypeName.get(processFunction.getReturnType())));
        args.add(declarationType);
        args.add(processFunction.getSimpleName());
        for (VariableElement v : processFunction.getParameters()) {
            if (args.size() > 3) {
                pattern.append(", ");
            }
            if (v.getAnnotation(Fixed.class) != null) {
                pattern.append("$L");
                args.add(v.getSimpleName().toString());
                continue;
            }
            String name = nameForParameter(v, nameFlavor);
            if (v.asType().getKind() == TypeKind.ARRAY) {
                pattern.append("$LValues");
                args.add(v.getSimpleName());
                continue;
            }
            if (TypeName.get(v.asType()).equals(BYTES_REF)) {
                if (blockStyle) {
                    pattern.append("$L.getBytesRef($L.getFirstValueIndex(p), $LScratch)");
                    args.add(name);
                } else {
                    pattern.append("$L.getBytesRef(p, $LScratch)");
                }
                args.add(name);
                args.add(v.getSimpleName().toString());
                continue;
            }
            if (blockStyle) {
                pattern.append("$L.$L($L.getFirstValueIndex(p))");
            } else {
                pattern.append("$L.$L(p)");
            }
            args.add(name);
            args.add(getMethod(TypeName.get(v.asType())));
            if (blockStyle) {
                args.add(name);
            }
        }
        builder.addStatement(pattern.append("))").toString(), args.toArray());
        builder.endControlFlow();
        builder.addStatement("return result.build()");
        return builder.build();
    }

    private void skipNull(MethodSpec.Builder builder, String value) {
        builder.beginControlFlow("if ($N.isNull(p) || $N.getValueCount(p) != 1)", value, value);
        {
            builder.addStatement("result.appendNull()");
            builder.addStatement("continue position");
        }
        builder.endControlFlow();
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
}
