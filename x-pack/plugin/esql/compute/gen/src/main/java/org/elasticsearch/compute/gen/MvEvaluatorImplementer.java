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
import java.util.function.Consumer;
import java.util.stream.Collectors;

import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.VariableElement;
import javax.lang.model.type.TypeKind;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.Elements;

import static org.elasticsearch.compute.gen.Methods.appendMethod;
import static org.elasticsearch.compute.gen.Methods.findMethod;
import static org.elasticsearch.compute.gen.Methods.getMethod;
import static org.elasticsearch.compute.gen.Types.ABSTRACT_MULTIVALUE_FUNCTION_EVALUATOR;
import static org.elasticsearch.compute.gen.Types.ABSTRACT_NULLABLE_MULTIVALUE_FUNCTION_EVALUATOR;
import static org.elasticsearch.compute.gen.Types.BLOCK_REF;
import static org.elasticsearch.compute.gen.Types.BYTES_REF;
import static org.elasticsearch.compute.gen.Types.DRIVER_CONTEXT;
import static org.elasticsearch.compute.gen.Types.EXPRESSION_EVALUATOR;
import static org.elasticsearch.compute.gen.Types.SOURCE;
import static org.elasticsearch.compute.gen.Types.WARNINGS;
import static org.elasticsearch.compute.gen.Types.blockType;
import static org.elasticsearch.compute.gen.Types.vectorType;

public class MvEvaluatorImplementer {
    private final TypeElement declarationType;

    /**
     * Function specifying how each value in a multivalued field is processed.
     */
    private final ExecutableElement processFunction;

    /**
     * Optional function "finishing" the processing of a multivalued field. It
     * converts {@link #workType} into {@link #resultType}. If {@code null} then
     * {@link #workType} <strong>is</strong> {@link #resultType} and the work
     * is returned unchanged.
     */
    private final FinishFunction finishFunction;

    /**
     * Optional function to process single valued fields. This is often used
     * when the {@link #fieldType} isn't the same as the {@link #resultType}
     * and will implement the conversion. If this is unspecified then single
     * value fields are returned as is.
     */
    private final SingleValueFunction singleValueFunction;

    /**
     * Optional function to process {@code Block}s where all multivalued fields
     * are ascending, which is how Lucene loads them so it's quite common. If
     * specified then the implementation will use this method to process the
     * multivalued field instead of {@link #processFunction}.
     */
    private final AscendingFunction ascendingFunction;

    private final List<TypeMirror> warnExceptions;
    private final ClassName implementation;
    private final TypeName workType;
    private final TypeName fieldType;
    private final TypeName resultType;

    public MvEvaluatorImplementer(
        Elements elements,
        ExecutableElement processFunction,
        String extraName,
        String finishMethodName,
        String singleValueMethodName,
        String ascendingMethodName,
        List<TypeMirror> warnExceptions
    ) {
        this.declarationType = (TypeElement) processFunction.getEnclosingElement();
        this.processFunction = processFunction;
        if (processFunction.getParameters().size() != 2) {
            throw new IllegalArgumentException("process should have exactly two parameters");
        }
        this.workType = TypeName.get(processFunction.getParameters().get(0).asType());
        this.fieldType = TypeName.get(processFunction.getParameters().get(1).asType());
        this.finishFunction = FinishFunction.from(declarationType, finishMethodName, workType, fieldType);
        this.resultType = this.finishFunction == null ? this.workType : this.finishFunction.resultType;
        this.singleValueFunction = SingleValueFunction.from(declarationType, singleValueMethodName, resultType, fieldType);
        this.ascendingFunction = AscendingFunction.from(this, declarationType, ascendingMethodName);
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
        builder.addJavadoc("This class is generated. Do not edit it.");
        builder.addModifiers(Modifier.PUBLIC, Modifier.FINAL);
        if (warnExceptions.isEmpty()) {
            builder.superclass(ABSTRACT_MULTIVALUE_FUNCTION_EVALUATOR);
        } else {
            builder.superclass(ABSTRACT_NULLABLE_MULTIVALUE_FUNCTION_EVALUATOR);

            builder.addField(WARNINGS, "warnings", Modifier.PRIVATE, Modifier.FINAL);
        }
        builder.addField(DRIVER_CONTEXT, "driverContext", Modifier.PRIVATE, Modifier.FINAL);

        builder.addMethod(ctor());
        builder.addMethod(name());
        builder.addMethod(eval("evalNullable", true));
        if (warnExceptions.isEmpty()) {
            builder.addMethod(eval("evalNotNullable", false));
        }
        if (singleValueFunction != null) {
            builder.addMethod(evalSingleValued("evalSingleValuedNullable", true));
            if (warnExceptions.isEmpty()) {
                builder.addMethod(evalSingleValued("evalSingleValuedNotNullable", false));
            }
        }
        if (ascendingFunction != null) {
            builder.addMethod(evalAscending("evalAscendingNullable", true));
            builder.addMethod(evalAscending("evalAscendingNotNullable", false));
        }
        return builder.build();
    }

    private MethodSpec ctor() {
        MethodSpec.Builder builder = MethodSpec.constructorBuilder().addModifiers(Modifier.PUBLIC);
        if (warnExceptions.isEmpty() == false) {
            builder.addParameter(SOURCE, "source");
        }
        builder.addParameter(EXPRESSION_EVALUATOR, "field");
        builder.addStatement("super($L)", "field");
        if (warnExceptions.isEmpty() == false) {
            builder.addStatement("this.warnings = new Warnings(source)");
        }
        builder.addParameter(DRIVER_CONTEXT, "driverContext");
        builder.addStatement("this.driverContext = driverContext");
        return builder.build();
    }

    private MethodSpec name() {
        MethodSpec.Builder builder = MethodSpec.methodBuilder("name").addModifiers(Modifier.PUBLIC);
        builder.addAnnotation(Override.class).returns(String.class);
        builder.addStatement("return $S", declarationType.getSimpleName());
        return builder.build();
    }

    private MethodSpec evalShell(
        String name,
        boolean override,
        boolean nullable,
        String javadoc,
        Consumer<MethodSpec.Builder> preflight,
        Consumer<MethodSpec.Builder> body
    ) {
        MethodSpec.Builder builder = MethodSpec.methodBuilder(name);
        builder.returns(BLOCK_REF).addParameter(BLOCK_REF, "ref");
        if (override) {
            builder.addAnnotation(Override.class).addModifiers(Modifier.PUBLIC);
        } else {
            builder.addModifiers(Modifier.PRIVATE);
        }
        builder.addJavadoc(javadoc);
        TypeName blockType = blockType(fieldType);

        preflight.accept(builder);

        builder.beginControlFlow("try (ref)");
        builder.addStatement("$T v = ($T) ref.block()", blockType, blockType);
        builder.addStatement("int positionCount = v.getPositionCount()");
        if (nullable) {
            TypeName resultBlockType = blockType(resultType);
            builder.addStatement(
                "$T.Builder builder = $T.newBlockBuilder(positionCount, driverContext.blockFactory())",
                resultBlockType,
                resultBlockType
            );
        } else if (resultType.equals(BYTES_REF)) {
            TypeName resultVectorType = vectorType(resultType);
            builder.addStatement(
                "$T.Builder builder = $T.newVectorBuilder(positionCount, driverContext.blockFactory())",
                resultVectorType,
                resultVectorType
            );
        } else {
            TypeName resultVectorType = vectorType(resultType);
            builder.addStatement(
                "$T.FixedBuilder builder = $T.newVectorFixedBuilder(positionCount, driverContext.blockFactory())",
                resultVectorType,
                resultVectorType
            );
        }

        if (false == workType.equals(fieldType) && workType.isPrimitive() == false) {
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
            if (warnExceptions.isEmpty() == false) {
                builder.beginControlFlow("try");
                body.accept(builder);
                String catchPattern = "catch (" + warnExceptions.stream().map(m -> "$T").collect(Collectors.joining(" | ")) + " e)";
                builder.nextControlFlow(catchPattern, warnExceptions.stream().map(TypeName::get).toArray());
                builder.addStatement("warnings.registerException(e)");
                builder.addStatement("builder.appendNull()");
                builder.endControlFlow();
            } else {
                body.accept(builder);
            }
        }
        builder.endControlFlow();

        builder.addStatement("return Block.Ref.floating(builder.build()$L)", nullable ? "" : ".asBlock()");
        builder.endControlFlow();
        return builder.build();
    }

    private MethodSpec eval(String name, boolean nullable) {
        String javadoc = "Evaluate blocks containing at least one multivalued field.";
        return evalShell(name, true, nullable, javadoc, builder -> {
            if (ascendingFunction == null) {
                return;
            }
            builder.beginControlFlow("if (ref.block().mvSortedAscending())");
            builder.addStatement("return $L(ref)", name.replace("eval", "evalAscending"));
            builder.endControlFlow();
        }, builder -> {
            builder.addStatement("int first = v.getFirstValueIndex(p)");

            if (singleValueFunction != null) {
                builder.beginControlFlow("if (valueCount == 1)");
                fetch(builder, "value", fieldType, "first", workType.equals(fieldType) ? "firstScratch" : "valueScratch");
                singleValueFunction.call(builder);
                writeResult(builder);
                builder.addStatement("continue");
                builder.endControlFlow();
            }

            builder.addStatement("int end = first + valueCount");
            if (workType.equals(fieldType) || workType.isPrimitive()) {
                // process function evaluates pairwise
                fetch(builder, "value", workType, "first", "firstScratch");
                builder.beginControlFlow("for (int i = first + 1; i < end; i++)");
                {
                    if (fieldType.equals(BYTES_REF)) {
                        fetch(builder, "next", workType, "i", "nextScratch");
                        builder.addStatement("$T.$L(value, next)", declarationType, processFunction.getSimpleName());
                    } else {
                        fetch(builder, "next", fieldType, "i", "nextScratch");
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
                    fetch(builder, "value", fieldType, "i", "valueScratch");
                    builder.addStatement("$T.$L(work, value)", declarationType, processFunction.getSimpleName());
                }
                builder.endControlFlow();
                finishFunction.call(builder, "work");
            }
            writeResult(builder);
        });
    }

    private MethodSpec evalSingleValued(String name, boolean nullable) {
        String javadoc = "Evaluate blocks containing only single valued fields.";
        return evalShell(name, true, nullable, javadoc, builder -> {}, builder -> {
            builder.addStatement("assert valueCount == 1");
            builder.addStatement("int first = v.getFirstValueIndex(p)");
            fetch(builder, "value", fieldType, "first", workType.equals(fieldType) ? "firstScratch" : "valueScratch");
            singleValueFunction.call(builder);
            writeResult(builder);
        });
    }

    private void fetch(MethodSpec.Builder builder, String into, TypeName intoType, String index, String scratchName) {
        if (intoType.equals(BYTES_REF)) {
            builder.addStatement("$T $L = v.getBytesRef($L, $L)", intoType, into, index, scratchName);
        } else if (intoType.equals(fieldType) == false && intoType.isPrimitive()) {
            builder.addStatement("$T $L = ($T) v.$L($L)", intoType, into, intoType, getMethod(fieldType), index);
        } else {
            builder.addStatement("$T $L = v.$L($L)", intoType, into, getMethod(fieldType), index);
        }
    }

    private MethodSpec evalAscending(String name, boolean nullable) {
        String javadoc = "Evaluate blocks containing at least one multivalued field and all multivalued fields are in ascending order.";
        return evalShell(name, false, nullable, javadoc, builder -> {}, builder -> {
            builder.addStatement("int first = v.getFirstValueIndex(p)");
            ascendingFunction.call(builder);
            writeResult(builder);
        });
    }

    private void writeResult(MethodSpec.Builder builder) {
        if (fieldType.equals(BYTES_REF)) {
            builder.addStatement("builder.appendBytesRef(result)");
        } else {
            builder.addStatement("builder.$L(result)", appendMethod(resultType));
        }
    }

    /**
     * Function "finishing" the computation on a multivalued field. It converts {@link #workType} into {@link #resultType}.
     */
    private static class FinishFunction {
        static FinishFunction from(TypeElement declarationType, String name, TypeName workType, TypeName fieldType) {
            if (name.equals("")) {
                if (false == workType.equals(fieldType)) {
                    throw new IllegalArgumentException(
                        "the [finish] enum value is required because the first and second arguments differ in type"
                    );
                }
                return null;
            }
            ExecutableElement fn = findMethod(
                declarationType,
                new String[] { name },
                m -> TypeName.get(m.getParameters().get(0).asType()).equals(workType)
            );
            if (fn == null) {
                throw new IllegalArgumentException("Couldn't find " + declarationType + "#" + name + "(" + workType + "...)");
            }
            TypeName resultType = TypeName.get(fn.getReturnType());
            return new FinishFunction(declarationType, fn, resultType, workType);
        }

        private final TypeName resultType;
        private final String invocationPattern;
        private final List<Object> invocationArgs = new ArrayList<>();

        private FinishFunction(TypeElement declarationType, ExecutableElement fn, TypeName resultType, TypeName workType) {
            this.resultType = resultType;
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

    /**
     * Function handling single valued fields.
     */
    private static class SingleValueFunction {
        static SingleValueFunction from(TypeElement declarationType, String name, TypeName resultType, TypeName fieldType) {
            if (name.equals("")) {
                return null;
            }
            ExecutableElement fn = findMethod(
                declarationType,
                new String[] { name },
                m -> m.getParameters().size() == 1 && TypeName.get(m.getParameters().get(0).asType()).equals(fieldType)
            );
            if (fn == null) {
                throw new IllegalArgumentException("Couldn't find " + declarationType + "#" + name + "(" + fieldType + ")");
            }
            return new SingleValueFunction(declarationType, resultType, fn);
        }

        private final List<Object> invocationArgs = new ArrayList<>();

        private SingleValueFunction(TypeElement declarationType, TypeName resultType, ExecutableElement fn) {
            invocationArgs.add(resultType);
            invocationArgs.add(declarationType);
            invocationArgs.add(fn.getSimpleName());
        }

        private void call(MethodSpec.Builder builder) {
            builder.addStatement("$T result = $T.$L(value)", invocationArgs.toArray());
        }
    }

    /**
     * Function handling blocks of ascending values.
     */
    private class AscendingFunction {
        static AscendingFunction from(MvEvaluatorImplementer impl, TypeElement declarationType, String name) {
            if (name.equals("")) {
                return null;
            }

            // check for index lookup
            ExecutableElement fn = findMethod(
                declarationType,
                new String[] { name },
                m -> m.getParameters().size() == 1 && m.getParameters().get(0).asType().getKind() == TypeKind.INT
            );
            if (fn != null) {
                return impl.new AscendingFunction(fn, false);
            }
            fn = findMethod(
                declarationType,
                new String[] { name },
                m -> m.getParameters().size() == 3
                    && m.getParameters().get(1).asType().getKind() == TypeKind.INT
                    && m.getParameters().get(2).asType().getKind() == TypeKind.INT
            );
            if (fn == null) {
                throw new IllegalArgumentException("Couldn't find " + declarationType + "#" + name + "(block, int, int)");
            }
            return impl.new AscendingFunction(fn, true);
        }

        private final List<Object> invocationArgs = new ArrayList<>();
        private final boolean blockMode;

        private AscendingFunction(ExecutableElement fn, boolean blockMode) {
            this.blockMode = blockMode;
            if (blockMode) {
                invocationArgs.add(resultType);
            }
            invocationArgs.add(declarationType);
            invocationArgs.add(fn.getSimpleName());
        }

        private void call(MethodSpec.Builder builder) {
            if (blockMode) {
                builder.addStatement("$T result = $T.$L(v, first, valueCount)", invocationArgs.toArray());
            } else {
                builder.addStatement("int idx = $T.$L(valueCount)", invocationArgs.toArray());
                fetch(builder, "result", resultType, "first + idx", workType.equals(fieldType) ? "firstScratch" : "valueScratch");
            }
        }
    }
}
