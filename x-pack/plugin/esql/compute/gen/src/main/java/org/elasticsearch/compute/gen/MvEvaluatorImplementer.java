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

import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.VariableElement;
import javax.lang.model.type.TypeKind;
import javax.lang.model.util.Elements;

import static org.elasticsearch.compute.gen.Methods.appendMethod;
import static org.elasticsearch.compute.gen.Methods.findMethod;
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
    private final FinishFunction finishFunction;
    private final ClassName implementation;
    private final TypeName workType;
    private final TypeName fieldType;
    private final TypeName resultType;

    public MvEvaluatorImplementer(Elements elements, ExecutableElement processFunction, String extraName, String finishMethodName) {
        this.declarationType = (TypeElement) processFunction.getEnclosingElement();
        this.processFunction = processFunction;
        if (processFunction.getParameters().size() != 2) {
            throw new IllegalArgumentException("process should have exactly two parameters");
        }
        this.workType = TypeName.get(processFunction.getParameters().get(0).asType());
        this.fieldType = TypeName.get(processFunction.getParameters().get(1).asType());

        if (finishMethodName.equals("")) {
            this.resultType = workType;
            this.finishFunction = null;
            if (false == workType.equals(fieldType)) {
                throw new IllegalArgumentException(
                    "the [finish] enum value is required because the first and second arguments differ in type"
                );
            }
        } else {
            ExecutableElement fn = findMethod(
                declarationType,
                new String[] { finishMethodName },
                m -> TypeName.get(m.getParameters().get(0).asType()).equals(workType)
            );
            if (fn == null) {
                throw new IllegalArgumentException("Couldn't find " + declarationType + "#" + finishMethodName + "(" + workType + "...)");
            }
            this.resultType = TypeName.get(fn.getReturnType());
            this.finishFunction = new FinishFunction(fn);
        }

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

        builder.addStatement("$T v = ($T) fieldVal", blockType, blockType);
        builder.addStatement("int positionCount = v.getPositionCount()");
        if (nullable) {
            TypeName resultBlockType = blockType(resultType);
            builder.addStatement("$T.Builder builder = $T.newBlockBuilder(positionCount)", resultBlockType, resultBlockType);
        } else if (resultType.equals(BYTES_REF)) {
            builder.addStatement(
                "$T values = new $T(positionCount, $T.NON_RECYCLING_INSTANCE)",  // TODO blocks should use recycling array
                BYTES_REF_ARRAY,
                BYTES_REF_ARRAY,
                BIG_ARRAYS
            );
        } else {
            builder.addStatement("$T[] values = new $T[positionCount]", resultType, resultType);
        }

        if (false == workType.equals(fieldType)) {
            builder.addStatement("$T work = new $T()", workType, workType);
        }
        if (fieldType.equals(BYTES_REF)) {
            if (workType.equals(fieldType)) {
                builder.addStatement("$T firstScratch = new $T()", BYTES_REF, BYTES_REF);
                builder.addStatement("$T nextScratch = new $T()", BYTES_REF, BYTES_REF);
            } else {
                builder.addStatement("$T valueScratch = new $T()", BYTES_REF, BYTES_REF);
            }
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

            if (workType.equals(fieldType)) {
                // process function evaluates pairwise
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
                if (finishFunction == null) {
                    builder.addStatement("$T result = value", resultType);
                } else {
                    finishFunction.call(builder, "value");
                }
            } else {
                builder.beginControlFlow("for (int i = first; i < end; i++)");
                {
                    fetch(builder, "value", "i", "valueScratch");
                    builder.addStatement("$T.$L(work, value)", declarationType, processFunction.getSimpleName());
                }
                builder.endControlFlow();
                finishFunction.call(builder, "work");
            }

            if (nullable) {
                builder.addStatement("builder.$L(result)", appendMethod(resultType));
            } else if (fieldType.equals(BYTES_REF)) {
                builder.addStatement("values.append(result)");
            } else {
                builder.addStatement("values[p] = result");
            }
        }
        builder.endControlFlow();
        if (nullable) {
            builder.addStatement("return builder.build()");
        } else {
            builder.addStatement("return new $T(values, positionCount)", arrayVectorType(resultType));
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

    private class FinishFunction {
        private final String invocationPattern;
        private final List<Object> invocationArgs = new ArrayList<>();

        private FinishFunction(ExecutableElement fn) {
            StringBuilder pattern = new StringBuilder().append("$T result = $T.$L($work$");
            invocationArgs.add(resultType);
            invocationArgs.add(declarationType);
            invocationArgs.add(fn.getSimpleName());

            for (int p = 1; p < fn.getParameters().size(); p++) {
                VariableElement param = fn.getParameters().get(p);
                if (p == 0) {
                    if (false == TypeName.get(param.asType()).equals(workType)) {
                        throw new IllegalArgumentException(
                            "First argument of " + declarationType + "#" + fn.getSimpleName() + " must have type " + workType
                        );
                    }
                    continue;
                }
                if (param.getSimpleName().toString().equals("valueCount")) {
                    if (param.asType().getKind() != TypeKind.INT) {
                        throw new IllegalArgumentException("count argument must have type [int]");
                    }
                    pattern.append(", valueCount");
                    continue;
                }
                throw new IllegalArgumentException("unsupported parameter " + param);
            }
            invocationPattern = pattern.append(")").toString();
        }

        private void call(MethodSpec.Builder builder, String workName) {
            builder.addStatement(invocationPattern.replace("$work$", workName), invocationArgs.toArray());
        }
    }
}
