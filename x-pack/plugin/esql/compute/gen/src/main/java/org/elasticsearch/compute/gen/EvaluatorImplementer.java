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
import static org.elasticsearch.compute.gen.Types.blockType;
import static org.elasticsearch.compute.gen.Types.vectorType;

public class EvaluatorImplementer {
    private final TypeElement declarationType;
    private final ProcessFunction processFunction;
    private final ClassName implementation;

    public EvaluatorImplementer(Elements elements, ExecutableElement processFunction, String extraName) {
        this.declarationType = (TypeElement) processFunction.getEnclosingElement();
        this.processFunction = new ProcessFunction(processFunction);

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

        processFunction.args.stream().forEach(a -> builder.addField(a.fieldType(), a.name(), Modifier.PRIVATE, Modifier.FINAL));

        builder.addMethod(ctor());
        builder.addMethod(fold());
        builder.addMethod(eval());
        builder.addMethod(realEval(blockType(processFunction.resultType), true, "newBlockBuilder"));
        builder.addMethod(realEval(vectorType(processFunction.resultType), false, "newVectorBuilder"));
        builder.addMethod(toStringMethod());
        return builder.build();
    }

    private MethodSpec ctor() {
        MethodSpec.Builder builder = MethodSpec.constructorBuilder().addModifiers(Modifier.PUBLIC);
        processFunction.args.stream().forEach(a -> builder.addParameter(a.fieldType(), a.name()));
        processFunction.args.stream().forEach(a -> builder.addStatement("this.$L = $L", a.name(), a.name()));
        return builder.build();
    }

    private MethodSpec fold() {
        MethodSpec.Builder builder = MethodSpec.methodBuilder("fold")
            .addModifiers(Modifier.STATIC)
            .returns(processFunction.resultType.box());

        for (VariableElement v : processFunction.function.getParameters()) {
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
        args.add(processFunction.function.getSimpleName());
        for (VariableElement v : processFunction.function.getParameters()) {
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

        processFunction.args.stream().forEach(a -> a.evalToBlock(builder));
        String invokeBlockEval = invokeRealEval(true);
        processFunction.args.stream().forEach(a -> a.resolveVectors(builder, invokeBlockEval));
        builder.addStatement(invokeRealEval(false) + ".asBlock()");
        return builder.build();
    }

    private String invokeRealEval(boolean blockStyle) {
        return "return eval(page.getPositionCount(), "
            + processFunction.args.stream().map(a -> a.paramName(blockStyle)).collect(Collectors.joining(", "))
            + ")";
    }

    private MethodSpec realEval(TypeName resultType, boolean blockStyle, String resultBuilderMethod) {
        MethodSpec.Builder builder = MethodSpec.methodBuilder("eval");
        builder.addModifiers(Modifier.PUBLIC).returns(resultType);
        builder.addParameter(TypeName.INT, "positionCount");

        processFunction.args.stream().forEach(a -> builder.addParameter(a.dataType(blockStyle), a.paramName(blockStyle)));
        builder.addStatement("$T.Builder result = $T.$L(positionCount)", resultType, resultType, resultBuilderMethod);
        processFunction.args.stream().forEach(a -> a.createScratch(builder));

        builder.beginControlFlow("position: for (int p = 0; p < positionCount; p++)");
        {
            if (blockStyle) {
                processFunction.args.stream().forEach(a -> a.skipNull(builder));
            }
            processFunction.args.stream().forEach(a -> a.unpackValues(builder, blockStyle));

            StringBuilder pattern = new StringBuilder();
            List<Object> args = new ArrayList<>();
            pattern.append("result.$L($T.$N(");
            args.add(appendMethod(processFunction.resultType));
            args.add(declarationType);
            args.add(processFunction.function.getSimpleName());
            processFunction.args.stream().forEach(a -> {
                if (args.size() > 3) {
                    pattern.append(", ");
                }
                a.buildInvocation(pattern, args, blockStyle);
            });
            builder.addStatement(pattern.append("))").toString(), args.toArray());
        }
        builder.endControlFlow();
        builder.addStatement("return result.build()");
        return builder.build();
    }

    private static void skipNull(MethodSpec.Builder builder, String value) {
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
        List<Object> args = new ArrayList<>();
        pattern.append("return $S");
        args.add(implementation.simpleName() + "[");
        processFunction.args.stream().forEach(a -> a.buildToStringInvocation(pattern, args, args.size() > 2 ? ", " : ""));
        pattern.append(" + $S");
        args.add("]");
        builder.addStatement(pattern.toString(), args.toArray());
        return builder.build();
    }

    private interface ProcessFunctionArg {
        String name();

        /**
         * Type of the field on the Evaluator object. It can produce values of {@link #dataType}
         * by calling the code emitted by {@link #evalToBlock}.
         */
        TypeName fieldType();

        /**
         * Type containing the actual data for a page of values for this field. Usually a
         * Block or Vector, but for fixed fields will be the original fixed type.
         */
        TypeName dataType(boolean blockStyle);

        /**
         * The parameter passed to the real evaluation function
         */
        String paramName(boolean blockStyle);

        /**
         * Emits code to evaluate this parameter to a Block or array of Blocks.
         * Noop if the parameter is {@link Fixed}.
         */
        void evalToBlock(MethodSpec.Builder builder);

        /**
         * Emits code to check if this parameter is a vector or a block, and to
         * call the block flavored evaluator if this is a block. Noop if the
         * parameter is {@link Fixed}.
         */
        void resolveVectors(MethodSpec.Builder builder, String invokeBlockEval);

        /**
         * Create any scratch structures needed by {@link EvaluatorImplementer#realEval}.
         */
        void createScratch(MethodSpec.Builder builder);

        /**
         * Skip any null values in blocks containing this field.
         */
        void skipNull(MethodSpec.Builder builder);

        /**
         * Unpacks values from blocks and repacks them into an appropriate local. Noop
         * except for arrays.
         */
        void unpackValues(MethodSpec.Builder builder, boolean blockStyle);

        /**
         * Build the invocation of the process method for this parameter.
         */
        void buildInvocation(StringBuilder pattern, List<Object> args, boolean blockStyle);

        void buildToStringInvocation(StringBuilder pattern, List<Object> args, String prefix);
    }

    private record StandardProcessFunctionArg(TypeName type, String name) implements ProcessFunctionArg {
        @Override
        public String name() {
            return name;
        }

        @Override
        public TypeName fieldType() {
            return EXPRESSION_EVALUATOR;
        }

        @Override
        public TypeName dataType(boolean blockStyle) {
            if (blockStyle) {
                return blockType(type);
            }
            return vectorType(type);
        }

        @Override
        public String paramName(boolean blockStyle) {
            return name + (blockStyle ? "Block" : "Vector");
        }

        @Override
        public void evalToBlock(MethodSpec.Builder builder) {
            TypeName blockType = blockType(type);
            builder.addStatement("Block $LUncastBlock = $L.eval(page)", name, name);
            builder.beginControlFlow("if ($LUncastBlock.areAllValuesNull())", name);
            builder.addStatement("return Block.constantNullBlock(page.getPositionCount())");
            builder.endControlFlow();
            builder.addStatement("$T $LBlock = ($T) $LUncastBlock", blockType, name, blockType, name);
        }

        @Override
        public void resolveVectors(MethodSpec.Builder builder, String invokeBlockEval) {
            builder.addStatement("$T $LVector = $LBlock.asVector()", vectorType(type), name, name);
            builder.beginControlFlow("if ($LVector == null)", name).addStatement(invokeBlockEval).endControlFlow();
        }

        @Override
        public void createScratch(MethodSpec.Builder builder) {
            if (type.equals(BYTES_REF)) {
                builder.addStatement("BytesRef $LScratch = new BytesRef()", name);
            }
        }

        @Override
        public void skipNull(MethodSpec.Builder builder) {
            EvaluatorImplementer.skipNull(builder, paramName(true));
        }

        @Override
        public void unpackValues(MethodSpec.Builder builder, boolean blockStyle) {
            // nothing to do
        }

        @Override
        public void buildInvocation(StringBuilder pattern, List<Object> args, boolean blockStyle) {
            if (type.equals(BYTES_REF)) {
                if (blockStyle) {
                    pattern.append("$L.getBytesRef($L.getFirstValueIndex(p), $LScratch)");
                    args.add(paramName(true));
                } else {
                    pattern.append("$L.getBytesRef(p, $LScratch)");
                }
                args.add(paramName(blockStyle));
                args.add(name);
                return;
            }
            if (blockStyle) {
                pattern.append("$L.$L($L.getFirstValueIndex(p))");
            } else {
                pattern.append("$L.$L(p)");
            }
            args.add(paramName(blockStyle));
            args.add(getMethod(type));
            if (blockStyle) {
                args.add(paramName(true));
            }
        }

        @Override
        public void buildToStringInvocation(StringBuilder pattern, List<Object> args, String prefix) {
            pattern.append(" + $S + $L");
            args.add(prefix + name + "=");
            args.add(name);
        }
    }

    private record ArrayProcessFunctionArg(TypeName componentType, String name) implements ProcessFunctionArg {
        @Override
        public String name() {
            return name;
        }

        @Override
        public TypeName fieldType() {
            return ArrayTypeName.of(EXPRESSION_EVALUATOR);
        }

        @Override
        public TypeName dataType(boolean blockStyle) {
            if (blockStyle) {
                return ArrayTypeName.of(blockType(componentType));
            }
            return ArrayTypeName.of(vectorType(componentType));
        }

        @Override
        public String paramName(boolean blockStyle) {
            return name + (blockStyle ? "Block" : "Vector") + "s";
        }

        @Override
        public void evalToBlock(MethodSpec.Builder builder) {
            TypeName blockType = blockType(componentType);
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
        }

        @Override
        public void resolveVectors(MethodSpec.Builder builder, String invokeBlockEval) {
            TypeName vectorType = vectorType(componentType);
            builder.addStatement("$T[] $LVectors = new $T[$L.length]", vectorType, name, vectorType, name);
            builder.beginControlFlow("for (int i = 0; i < $LBlocks.length; i++)", name);
            builder.addStatement("$LVectors[i] = $LBlocks[i].asVector()", name, name);
            builder.beginControlFlow("if ($LVectors[i] == null)", name).addStatement(invokeBlockEval).endControlFlow();
            builder.endControlFlow();
        }

        @Override
        public void createScratch(MethodSpec.Builder builder) {
            builder.addStatement("$T[] $LValues = new $T[$L.length]", componentType, name, componentType, name);
            if (componentType.equals(BYTES_REF)) {
                builder.addStatement("$T[] $LScratch = new $T[$L.length]", componentType, name, componentType, name);
                builder.beginControlFlow("for (int i = 0; i < $L.length; i++)", name);
                builder.addStatement("$LScratch[i] = new BytesRef()", name);
                builder.endControlFlow();
            }
        }

        @Override
        public void skipNull(MethodSpec.Builder builder) {
            builder.beginControlFlow("for (int i = 0; i < $L.length; i++)", paramName(true));
            EvaluatorImplementer.skipNull(builder, paramName(true) + "[i]");
            builder.endControlFlow();
        }

        @Override
        public void unpackValues(MethodSpec.Builder builder, boolean blockStyle) {
            builder.addComment("unpack $L into $LValues", paramName(blockStyle), name);
            builder.beginControlFlow("for (int i = 0; i < $L.length; i++)", paramName(blockStyle));
            String lookupVar;
            if (blockStyle) {
                lookupVar = "o";
                builder.addStatement("int o = $LBlocks[i].getFirstValueIndex(p)", name);
            } else {
                lookupVar = "p";
            }
            if (componentType.equals(BYTES_REF)) {
                builder.addStatement("$LValues[i] = $L[i].getBytesRef($L, $LScratch[i])", name, paramName(blockStyle), lookupVar, name);
            } else {
                builder.addStatement("$LValues[i] = $L[i].$L($L)", name, paramName(blockStyle), getMethod(componentType), lookupVar);
            }
            builder.endControlFlow();
        }

        @Override
        public void buildInvocation(StringBuilder pattern, List<Object> args, boolean blockStyle) {
            pattern.append("$LValues");
            args.add(name);
        }

        @Override
        public void buildToStringInvocation(StringBuilder pattern, List<Object> args, String prefix) {
            pattern.append(" + $S + $T.toString($L)");
            args.add(prefix + name + "=");
            args.add(Arrays.class);
            args.add(name);
        }
    }

    private record FixedProcessFunctionArg(TypeName type, String name, boolean includeInToString) implements ProcessFunctionArg {
        @Override
        public String name() {
            return name;
        }

        @Override
        public TypeName fieldType() {
            return type;
        }

        @Override
        public TypeName dataType(boolean blockStyle) {
            return type;
        }

        @Override
        public String paramName(boolean blockStyle) {
            return name;
        }

        @Override
        public void evalToBlock(MethodSpec.Builder builder) {
            // nothing to do
        }

        @Override
        public void resolveVectors(MethodSpec.Builder builder, String invokeBlockEval) {
            // nothing to do
        }

        @Override
        public void createScratch(MethodSpec.Builder builder) {
            // nothing to do
        }

        @Override
        public void skipNull(MethodSpec.Builder builder) {
            // nothing to do
        }

        @Override
        public void unpackValues(MethodSpec.Builder builder, boolean blockStyle) {
            // nothing to do
        }

        @Override
        public void buildInvocation(StringBuilder pattern, List<Object> args, boolean blockStyle) {
            pattern.append("$L");
            args.add(name);
        }

        @Override
        public void buildToStringInvocation(StringBuilder pattern, List<Object> args, String prefix) {
            if (includeInToString) {
                pattern.append(" + $S + $L");
                args.add(prefix + name + "=");
                args.add(name);
            }
        }
    }

    private static class ProcessFunction {
        private final ExecutableElement function;
        private final List<ProcessFunctionArg> args;
        private final TypeName resultType;

        private ProcessFunction(ExecutableElement function) {
            this.function = function;
            args = new ArrayList<>();
            for (VariableElement v : function.getParameters()) {
                TypeName type = TypeName.get(v.asType());
                String name = v.getSimpleName().toString();
                Fixed fixed = v.getAnnotation(Fixed.class);
                if (fixed != null) {
                    args.add(new FixedProcessFunctionArg(type, name, fixed.includeInToString()));
                    continue;
                }
                if (v.asType().getKind() == TypeKind.ARRAY) {
                    TypeMirror componentType = ((ArrayType) v.asType()).getComponentType();
                    args.add(new ArrayProcessFunctionArg(TypeName.get(componentType), name));
                    continue;
                }
                args.add(new StandardProcessFunctionArg(type, name));
            }
            resultType = TypeName.get(function.getReturnType());
        }
    }
}
