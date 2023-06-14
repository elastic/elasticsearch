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
import static org.elasticsearch.compute.gen.Types.EXPRESSION_EVALUATOR;
import static org.elasticsearch.compute.gen.Types.PAGE;
import static org.elasticsearch.compute.gen.Types.blockType;
import static org.elasticsearch.compute.gen.Types.vectorType;

public class EvaluatorImplementer {
    private final TypeElement declarationType;
    private final ProcessFunction processFunction;
    private final ClassName implementation;

    public EvaluatorImplementer(Elements elements, ExecutableElement processFunction, String extraName, List<TypeMirror> warnExceptions) {
        this.declarationType = (TypeElement) processFunction.getEnclosingElement();
        this.processFunction = new ProcessFunction(processFunction, warnExceptions);

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

        processFunction.args.stream().forEach(a -> a.declareField(builder));

        builder.addMethod(ctor());
        builder.addMethod(eval());
        builder.addMethod(realEval(true));
        builder.addMethod(realEval(false));
        builder.addMethod(toStringMethod());
        return builder.build();
    }

    private MethodSpec ctor() {
        MethodSpec.Builder builder = MethodSpec.constructorBuilder().addModifiers(Modifier.PUBLIC);
        processFunction.args.stream().forEach(a -> a.implementCtor(builder));
        return builder.build();
    }

    private MethodSpec eval() {
        MethodSpec.Builder builder = MethodSpec.methodBuilder("eval").addAnnotation(Override.class);
        builder.addModifiers(Modifier.PUBLIC).returns(BLOCK).addParameter(PAGE, "page");

        processFunction.args.stream().forEach(a -> a.evalToBlock(builder));
        String invokeBlockEval = invokeRealEval(true);
        processFunction.args.stream().forEach(a -> a.resolveVectors(builder, invokeBlockEval));
        builder.addStatement(invokeRealEval(false));
        return builder.build();
    }

    private String invokeRealEval(boolean blockStyle) {
        return "return eval(page.getPositionCount(), "
            + processFunction.args.stream().map(a -> a.paramName(blockStyle)).filter(a -> a != null).collect(Collectors.joining(", "))
            + ")"
            + (processFunction.resultDataType(blockStyle).simpleName().endsWith("Vector") ? ".asBlock()" : "");
    }

    private MethodSpec realEval(boolean blockStyle) {
        ClassName resultDataType = processFunction.resultDataType(blockStyle);
        MethodSpec.Builder builder = MethodSpec.methodBuilder("eval");
        builder.addModifiers(Modifier.PUBLIC).returns(resultDataType);
        builder.addParameter(TypeName.INT, "positionCount");

        processFunction.args.stream().forEach(a -> {
            if (a.paramName(blockStyle) != null) {
                builder.addParameter(a.dataType(blockStyle), a.paramName(blockStyle));
            }
        });
        builder.addStatement(
            "$T.Builder result = $T.$L(positionCount)",
            resultDataType,
            resultDataType,
            resultDataType.simpleName().endsWith("Vector") ? "newVectorBuilder" : "newBlockBuilder"
        );
        processFunction.args.stream().forEach(a -> a.createScratch(builder));

        builder.beginControlFlow("position: for (int p = 0; p < positionCount; p++)");
        {
            if (blockStyle) {
                processFunction.args.stream().forEach(a -> a.skipNull(builder));
            }
            processFunction.args.stream().forEach(a -> a.unpackValues(builder, blockStyle));

            StringBuilder pattern = new StringBuilder();
            List<Object> args = new ArrayList<>();
            pattern.append("$T.$N(");
            args.add(declarationType);
            args.add(processFunction.function.getSimpleName());
            processFunction.args.stream().forEach(a -> {
                if (args.size() > 2) {
                    pattern.append(", ");
                }
                a.buildInvocation(pattern, args, blockStyle);
            });
            pattern.append(")");
            String builtPattern;
            if (processFunction.builderArg == null) {
                builtPattern = "result.$L(" + pattern + ")";
                args.add(0, appendMethod(resultDataType));
            } else {
                builtPattern = pattern.toString();
            }

            if (processFunction.warnExceptions.isEmpty() == false) {
                builder.beginControlFlow("try");
            }
            builder.addStatement(builtPattern, args.toArray());
            if (processFunction.warnExceptions.isEmpty() == false) {
                String catchPattern = "catch ("
                    + processFunction.warnExceptions.stream().map(m -> "$T").collect(Collectors.joining(" | "))
                    + " e)";
                builder.nextControlFlow(catchPattern, processFunction.warnExceptions.stream().map(m -> TypeName.get(m)).toArray());
                builder.addStatement("result.appendNull()");
                builder.endControlFlow();
            }
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
         * Declare any required fields on the type for this parameter.
         */
        void declareField(TypeSpec.Builder builder);

        /**
         * Implement the ctor for this parameter. Will declare parameters
         * and assign values to declared fields.
         */
        void implementCtor(MethodSpec.Builder builder);

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
        public void declareField(TypeSpec.Builder builder) {
            builder.addField(EXPRESSION_EVALUATOR, name, Modifier.PRIVATE, Modifier.FINAL);
        }

        @Override
        public void implementCtor(MethodSpec.Builder builder) {
            builder.addParameter(EXPRESSION_EVALUATOR, name);
            builder.addStatement("this.$L = $L", name, name);
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
                builder.addStatement("$T $LScratch = new $T()", BYTES_REF, name, BYTES_REF);
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
        public void declareField(TypeSpec.Builder builder) {
            builder.addField(ArrayTypeName.of(EXPRESSION_EVALUATOR), name, Modifier.PRIVATE, Modifier.FINAL);
        }

        @Override
        public void implementCtor(MethodSpec.Builder builder) {
            builder.addParameter(ArrayTypeName.of(EXPRESSION_EVALUATOR), name);
            builder.addStatement("this.$L = $L", name, name);
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
                builder.addStatement("$LScratch[i] = new $T()", name, BYTES_REF);
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
        public TypeName dataType(boolean blockStyle) {
            return type;
        }

        @Override
        public String paramName(boolean blockStyle) {
            // No need to pass it
            return null;
        }

        @Override
        public void declareField(TypeSpec.Builder builder) {
            builder.addField(type, name, Modifier.PRIVATE, Modifier.FINAL);
        }

        @Override
        public void implementCtor(MethodSpec.Builder builder) {
            builder.addParameter(type, name);
            builder.addStatement("this.$L = $L", name, name);
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

    private record BuilderProcessFunctionArg(ClassName type, String name) implements ProcessFunctionArg {
        @Override
        public TypeName dataType(boolean blockStyle) {
            return type;
        }

        @Override
        public String paramName(boolean blockStyle) {
            // never passed as a parameter
            return null;
        }

        @Override
        public void declareField(TypeSpec.Builder builder) {
            // Nothing to declare
        }

        @Override
        public void implementCtor(MethodSpec.Builder builder) {
            // Nothing to do
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
            args.add("result");
        }

        @Override
        public void buildToStringInvocation(StringBuilder pattern, List<Object> args, String prefix) {
            // Don't want to include
        }
    }

    private static class ProcessFunction {
        private final ExecutableElement function;
        private final List<ProcessFunctionArg> args;
        private final BuilderProcessFunctionArg builderArg;
        private final List<TypeMirror> warnExceptions;

        private ProcessFunction(ExecutableElement function, List<TypeMirror> warnExceptions) {
            this.function = function;
            args = new ArrayList<>();
            BuilderProcessFunctionArg builderArg = null;
            for (VariableElement v : function.getParameters()) {
                TypeName type = TypeName.get(v.asType());
                String name = v.getSimpleName().toString();
                Fixed fixed = v.getAnnotation(Fixed.class);
                if (fixed != null) {
                    args.add(new FixedProcessFunctionArg(type, name, fixed.includeInToString()));
                    continue;
                }
                if (type instanceof ClassName c
                    && c.simpleName().equals("Builder")
                    && c.enclosingClassName() != null
                    && c.enclosingClassName().simpleName().endsWith("Block")) {
                    if (builderArg != null) {
                        throw new IllegalArgumentException("only one builder allowed");
                    }
                    builderArg = new BuilderProcessFunctionArg(c, name);
                    args.add(builderArg);
                    continue;
                }
                if (v.asType().getKind() == TypeKind.ARRAY) {
                    TypeMirror componentType = ((ArrayType) v.asType()).getComponentType();
                    args.add(new ArrayProcessFunctionArg(TypeName.get(componentType), name));
                    continue;
                }
                args.add(new StandardProcessFunctionArg(type, name));
            }
            this.builderArg = builderArg;
            this.warnExceptions = warnExceptions;
        }

        private ClassName resultDataType(boolean blockStyle) {
            if (builderArg != null) {
                return builderArg.type.enclosingClassName();
            }
            boolean useBlockStyle = blockStyle || warnExceptions.isEmpty() == false;
            return useBlockStyle ? blockType(TypeName.get(function.getReturnType())) : vectorType(TypeName.get(function.getReturnType()));
        }
    }
}
