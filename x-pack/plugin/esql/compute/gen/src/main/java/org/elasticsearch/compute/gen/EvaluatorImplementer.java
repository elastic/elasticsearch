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
import java.util.function.Function;
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
import static org.elasticsearch.compute.gen.Methods.buildFromFactory;
import static org.elasticsearch.compute.gen.Methods.getMethod;
import static org.elasticsearch.compute.gen.Types.BLOCK;
import static org.elasticsearch.compute.gen.Types.BOOLEAN_BLOCK;
import static org.elasticsearch.compute.gen.Types.BYTES_REF;
import static org.elasticsearch.compute.gen.Types.BYTES_REF_BLOCK;
import static org.elasticsearch.compute.gen.Types.DOUBLE_BLOCK;
import static org.elasticsearch.compute.gen.Types.DRIVER_CONTEXT;
import static org.elasticsearch.compute.gen.Types.EXPRESSION_EVALUATOR;
import static org.elasticsearch.compute.gen.Types.EXPRESSION_EVALUATOR_FACTORY;
import static org.elasticsearch.compute.gen.Types.INT_BLOCK;
import static org.elasticsearch.compute.gen.Types.LONG_BLOCK;
import static org.elasticsearch.compute.gen.Types.PAGE;
import static org.elasticsearch.compute.gen.Types.RELEASABLE;
import static org.elasticsearch.compute.gen.Types.RELEASABLES;
import static org.elasticsearch.compute.gen.Types.SOURCE;
import static org.elasticsearch.compute.gen.Types.WARNINGS;
import static org.elasticsearch.compute.gen.Types.blockType;
import static org.elasticsearch.compute.gen.Types.builderType;
import static org.elasticsearch.compute.gen.Types.elementType;
import static org.elasticsearch.compute.gen.Types.vectorFixedBuilderType;
import static org.elasticsearch.compute.gen.Types.vectorType;

public class EvaluatorImplementer {
    private final TypeElement declarationType;
    private final ProcessFunction processFunction;
    private final ClassName implementation;
    private final boolean processOutputsMultivalued;

    public EvaluatorImplementer(
        Elements elements,
        javax.lang.model.util.Types types,
        ExecutableElement processFunction,
        String extraName,
        List<TypeMirror> warnExceptions
    ) {
        this.declarationType = (TypeElement) processFunction.getEnclosingElement();
        this.processFunction = new ProcessFunction(elements, types, processFunction, warnExceptions);

        this.implementation = ClassName.get(
            elements.getPackageOf(declarationType).toString(),
            declarationType.getSimpleName() + extraName + "Evaluator"
        );
        this.processOutputsMultivalued = this.processFunction.hasBlockType && (this.processFunction.builderArg != null);
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
        builder.addType(factory());

        builder.addField(WARNINGS, "warnings", Modifier.PRIVATE, Modifier.FINAL);
        processFunction.args.stream().forEach(a -> a.declareField(builder));
        builder.addField(DRIVER_CONTEXT, "driverContext", Modifier.PRIVATE, Modifier.FINAL);

        builder.addMethod(ctor());
        builder.addMethod(eval());

        if (processOutputsMultivalued) {
            if (processFunction.args.stream().anyMatch(x -> x instanceof FixedProcessFunctionArg == false)) {
                builder.addMethod(realEval(true));
            }
        } else {
            if (processFunction.args.stream().anyMatch(x -> x instanceof FixedProcessFunctionArg == false)) {
                builder.addMethod(realEval(true));
            }
            builder.addMethod(realEval(false));
        }
        builder.addMethod(toStringMethod());
        builder.addMethod(close());
        return builder.build();
    }

    private MethodSpec ctor() {
        MethodSpec.Builder builder = MethodSpec.constructorBuilder().addModifiers(Modifier.PUBLIC);
        builder.addParameter(SOURCE, "source");
        processFunction.args.stream().forEach(a -> a.implementCtor(builder));

        builder.addParameter(DRIVER_CONTEXT, "driverContext");
        builder.addStatement("this.driverContext = driverContext");
        builder.addStatement("this.warnings = Warnings.createWarnings(driverContext.warningsMode(), source)");
        return builder.build();
    }

    private MethodSpec eval() {
        MethodSpec.Builder builder = MethodSpec.methodBuilder("eval").addAnnotation(Override.class);
        builder.addModifiers(Modifier.PUBLIC).returns(BLOCK).addParameter(PAGE, "page");
        processFunction.args.stream().forEach(a -> a.evalToBlock(builder));
        String invokeBlockEval = invokeRealEval(true);
        if (processOutputsMultivalued) {
            builder.addStatement(invokeBlockEval);
        } else {
            processFunction.args.stream().forEach(a -> a.resolveVectors(builder, invokeBlockEval));
            builder.addStatement(invokeRealEval(false));
        }
        processFunction.args.stream().forEach(a -> a.closeEvalToBlock(builder));
        return builder.build();
    }

    private String invokeRealEval(boolean blockStyle) {
        StringBuilder builder = new StringBuilder("return eval(page.getPositionCount()");

        String params = processFunction.args.stream()
            .map(a -> a.paramName(blockStyle))
            .filter(a -> a != null)
            .collect(Collectors.joining(", "));
        if (params.length() > 0) {
            builder.append(", ");
            builder.append(params);
        }
        builder.append(")");
        if (processFunction.resultDataType(blockStyle).simpleName().endsWith("Vector")) {
            builder.append(".asBlock()");
        }
        return builder.toString();
    }

    private MethodSpec realEval(boolean blockStyle) {
        ClassName resultDataType = processFunction.resultDataType(blockStyle);
        MethodSpec.Builder builder = MethodSpec.methodBuilder("eval");
        builder.addModifiers(Modifier.PUBLIC).returns(resultDataType);
        builder.addParameter(TypeName.INT, "positionCount");

        boolean vectorize = false;
        if (blockStyle == false && processFunction.warnExceptions.isEmpty() && processOutputsMultivalued == false) {
            ClassName type = processFunction.resultDataType(false);
            vectorize = type.simpleName().startsWith("BytesRef") == false;
        }

        TypeName builderType = vectorize ? vectorFixedBuilderType(elementType(resultDataType)) : builderType(resultDataType);
        builder.beginControlFlow(
            "try($T result = driverContext.blockFactory().$L(positionCount))",
            builderType,
            buildFromFactory(builderType)
        );
        {
            processFunction.args.stream().forEach(a -> {
                if (a.paramName(blockStyle) != null) {
                    builder.addParameter(a.dataType(blockStyle), a.paramName(blockStyle));
                }
            });

            processFunction.args.stream().forEach(a -> a.createScratch(builder));

            builder.beginControlFlow("position: for (int p = 0; p < positionCount; p++)");
            {
                if (blockStyle) {
                    if (processOutputsMultivalued == false) {
                        processFunction.args.stream().forEach(a -> a.skipNull(builder));
                    } else {
                        builder.addStatement("boolean allBlocksAreNulls = true");
                        // allow block type inputs to be null
                        processFunction.args.stream().forEach(a -> {
                            if (a instanceof StandardProcessFunctionArg as) {
                                as.skipNull(builder);
                            } else if (a instanceof BlockProcessFunctionArg ab) {
                                builder.beginControlFlow("if (!$N.isNull(p))", ab.paramName(blockStyle));
                                {
                                    builder.addStatement("allBlocksAreNulls = false");
                                }
                                builder.endControlFlow();
                            }
                        });

                        builder.beginControlFlow("if (allBlocksAreNulls)");
                        {
                            builder.addStatement("result.appendNull()");
                            builder.addStatement("continue position");
                        }
                        builder.endControlFlow();
                    }
                }
                processFunction.args.stream().forEach(a -> a.unpackValues(builder, blockStyle));

                StringBuilder pattern = new StringBuilder();
                List<Object> args = new ArrayList<>();
                pattern.append(processOutputsMultivalued ? "$T.$N(result, p, " : "$T.$N(");
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
                    builtPattern = vectorize ? "result.$L(p, " + pattern + ")" : "result.$L(" + pattern + ")";
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
                    builder.addStatement("warnings.registerException(e)");
                    builder.addStatement("result.appendNull()");
                    builder.endControlFlow();
                }
            }
            builder.endControlFlow();
            builder.addStatement("return result.build()");
        }
        builder.endControlFlow();

        return builder.build();
    }

    private static void skipNull(MethodSpec.Builder builder, String value) {
        builder.beginControlFlow("if ($N.isNull(p))", value);
        {
            builder.addStatement("result.appendNull()");
            builder.addStatement("continue position");
        }
        builder.endControlFlow();
        builder.beginControlFlow("if ($N.getValueCount(p) != 1)", value);
        {
            builder.beginControlFlow("if ($N.getValueCount(p) > 1)", value);
            {
                builder.addStatement(
                    // TODO: reflection on SingleValueQuery.MULTI_VALUE_WARNING?
                    "warnings.registerException(new $T(\"single-value function encountered multi-value\"))",
                    IllegalArgumentException.class
                );
            }
            builder.endControlFlow();
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

    private MethodSpec close() {
        MethodSpec.Builder builder = MethodSpec.methodBuilder("close").addAnnotation(Override.class);
        builder.addModifiers(Modifier.PUBLIC);

        List<String> invocations = processFunction.args.stream().map(ProcessFunctionArg::closeInvocation).filter(s -> s != null).toList();
        if (invocations.isEmpty() == false) {
            builder.addStatement(
                "$T.closeExpectNoException(" + invocations.stream().collect(Collectors.joining(", ")) + ")",
                Types.RELEASABLES
            );
        }
        return builder.build();
    }

    private TypeSpec factory() {
        TypeSpec.Builder builder = TypeSpec.classBuilder("Factory");
        builder.addSuperinterface(EXPRESSION_EVALUATOR_FACTORY);
        builder.addModifiers(Modifier.STATIC);

        builder.addField(SOURCE, "source", Modifier.PRIVATE, Modifier.FINAL);
        processFunction.args.stream().forEach(a -> a.declareFactoryField(builder));

        builder.addMethod(factoryCtor());
        builder.addMethod(factoryGet());
        builder.addMethod(toStringMethod());

        return builder.build();
    }

    private MethodSpec factoryCtor() {
        MethodSpec.Builder builder = MethodSpec.constructorBuilder().addModifiers(Modifier.PUBLIC);
        builder.addParameter(SOURCE, "source");
        builder.addStatement("this.source = source");
        processFunction.args.stream().forEach(a -> a.implementFactoryCtor(builder));

        return builder.build();
    }

    private MethodSpec factoryGet() {
        MethodSpec.Builder builder = MethodSpec.methodBuilder("get").addAnnotation(Override.class);
        builder.addModifiers(Modifier.PUBLIC);
        builder.addParameter(DRIVER_CONTEXT, "context");
        builder.returns(implementation);

        List<String> args = new ArrayList<>();
        args.add("source");
        for (ProcessFunctionArg arg : processFunction.args) {
            String invocation = arg.factoryInvocation(builder);
            if (invocation != null) {
                args.add(invocation);
            }
        }
        args.add("context");
        builder.addStatement("return new $T($L)", implementation, args.stream().collect(Collectors.joining(", ")));
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
         * Declare any required fields for the evaluator to implement this type of parameter.
         */
        void declareField(TypeSpec.Builder builder);

        /**
         * Declare any required fields for the evaluator factory to implement this type of parameter.
         */
        void declareFactoryField(TypeSpec.Builder builder);

        /**
         * Implement the ctor for this parameter. Will declare parameters
         * and assign values to declared fields.
         */
        void implementCtor(MethodSpec.Builder builder);

        /**
         * Implement the ctor for the evaluator factory for this parameter.
         * Will declare parameters and assign values to declared fields.
         */
        void implementFactoryCtor(MethodSpec.Builder builder);

        /**
         * Invocation called in the ExpressionEvaluator.Factory#get method to
         * convert from whatever the factory holds to what the evaluator needs,
         * or {@code null} this parameter isn't passed to the evaluator's ctor.
         */
        String factoryInvocation(MethodSpec.Builder factoryMethodBuilder);

        /**
         * Emits code to evaluate this parameter to a Block or array of Blocks
         * and begins a {@code try} block for those refs. Noop if the parameter is {@link Fixed}.
         */
        void evalToBlock(MethodSpec.Builder builder);

        /**
         * Closes the {@code try} block emitted by {@link #evalToBlock} if it made one.
         * Noop otherwise.
         */
        void closeEvalToBlock(MethodSpec.Builder builder);

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

        /**
         * Accumulate invocation pattern and arguments to implement {@link Object#toString()}.
         */
        void buildToStringInvocation(StringBuilder pattern, List<Object> args, String prefix);

        /**
         * The string to close this argument or {@code null}.
         */
        String closeInvocation();
    }

    private record StandardProcessFunctionArg(TypeName type, String name) implements ProcessFunctionArg {
        @Override
        public TypeName dataType(boolean blockStyle) {
            if (blockStyle) {
                return isBlockType() ? type : blockType(type);
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
        public void declareFactoryField(TypeSpec.Builder builder) {
            builder.addField(EXPRESSION_EVALUATOR_FACTORY, name, Modifier.PRIVATE, Modifier.FINAL);
        }

        @Override
        public void implementCtor(MethodSpec.Builder builder) {
            builder.addParameter(EXPRESSION_EVALUATOR, name);
            builder.addStatement("this.$L = $L", name, name);
        }

        @Override
        public void implementFactoryCtor(MethodSpec.Builder builder) {
            builder.addParameter(EXPRESSION_EVALUATOR_FACTORY, name);
            builder.addStatement("this.$L = $L", name, name);
        }

        @Override
        public String factoryInvocation(MethodSpec.Builder factoryMethodBuilder) {
            return name + ".get(context)";
        }

        @Override
        public void evalToBlock(MethodSpec.Builder builder) {
            TypeName blockType = isBlockType() ? type : blockType(type);
            builder.beginControlFlow("try ($T $LBlock = ($T) $L.eval(page))", blockType, name, blockType, name);
        }

        @Override
        public void closeEvalToBlock(MethodSpec.Builder builder) {
            builder.endControlFlow();
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

        private boolean isBlockType() {
            return EvaluatorImplementer.isBlockType(type);
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
                if (isBlockType()) {
                    pattern.append("$L");
                } else {
                    pattern.append("$L.$L($L.getFirstValueIndex(p))");
                }
            } else {
                pattern.append("$L.$L(p)");
            }
            args.add(paramName(blockStyle));
            String method = isBlockType() ? null : getMethod(type);
            if (method != null) {
                args.add(method);
                if (blockStyle) {
                    args.add(paramName(true));
                }
            }
        }

        @Override
        public void buildToStringInvocation(StringBuilder pattern, List<Object> args, String prefix) {
            pattern.append(" + $S + $L");
            args.add(prefix + name + "=");
            args.add(name);
        }

        @Override
        public String closeInvocation() {
            return name;
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
        public void declareFactoryField(TypeSpec.Builder builder) {
            builder.addField(ArrayTypeName.of(EXPRESSION_EVALUATOR_FACTORY), name, Modifier.PRIVATE, Modifier.FINAL);
        }

        @Override
        public void implementCtor(MethodSpec.Builder builder) {
            builder.addParameter(ArrayTypeName.of(EXPRESSION_EVALUATOR), name);
            builder.addStatement("this.$L = $L", name, name);
        }

        @Override
        public void implementFactoryCtor(MethodSpec.Builder builder) {
            builder.addParameter(ArrayTypeName.of(EXPRESSION_EVALUATOR_FACTORY), name);
            builder.addStatement("this.$L = $L", name, name);
        }

        @Override
        public String factoryInvocation(MethodSpec.Builder factoryMethodBuilder) {
            factoryMethodBuilder.addStatement(
                "$T[] $L = Arrays.stream(this.$L).map(a -> a.get(context)).toArray($T[]::new)",
                EXPRESSION_EVALUATOR,
                name,
                name,
                EXPRESSION_EVALUATOR
            );
            return name;
        }

        @Override
        public void evalToBlock(MethodSpec.Builder builder) {
            TypeName blockType = blockType(componentType);
            builder.addStatement("$T[] $LBlocks = new $T[$L.length]", blockType, name, blockType, name);
            builder.beginControlFlow("try ($T $LRelease = $T.wrap($LBlocks))", RELEASABLE, name, RELEASABLES, name);
            builder.beginControlFlow("for (int i = 0; i < $LBlocks.length; i++)", name);
            {
                builder.addStatement("$LBlocks[i] = ($T)$L[i].eval(page)", name, blockType, name);
            }
            builder.endControlFlow();
        }

        @Override
        public void closeEvalToBlock(MethodSpec.Builder builder) {
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

        @Override
        public String closeInvocation() {
            return "() -> Releasables.close(" + name + ")";
        }
    }

    private record FixedProcessFunctionArg(TypeName type, String name, boolean includeInToString, boolean build, boolean releasable)
        implements
            ProcessFunctionArg {
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
        public void declareFactoryField(TypeSpec.Builder builder) {
            builder.addField(factoryFieldType(), name, Modifier.PRIVATE, Modifier.FINAL);
        }

        @Override
        public void implementCtor(MethodSpec.Builder builder) {
            builder.addParameter(type, name);
            builder.addStatement("this.$L = $L", name, name);
        }

        @Override
        public void implementFactoryCtor(MethodSpec.Builder builder) {
            builder.addParameter(factoryFieldType(), name);
            builder.addStatement("this.$L = $L", name, name);
        }

        private TypeName factoryFieldType() {
            return build ? ParameterizedTypeName.get(ClassName.get(Function.class), DRIVER_CONTEXT, type.box()) : type;
        }

        @Override
        public String factoryInvocation(MethodSpec.Builder factoryMethodBuilder) {
            return build ? name + ".apply(context)" : name;
        }

        @Override
        public void evalToBlock(MethodSpec.Builder builder) {
            // nothing to do
        }

        @Override
        public void closeEvalToBlock(MethodSpec.Builder builder) {
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
            pattern.append("this.$L");
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

        @Override
        public String closeInvocation() {
            return releasable ? name : null;
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
        public void declareFactoryField(TypeSpec.Builder builder) {
            // Nothing to declare
        }

        @Override
        public void implementCtor(MethodSpec.Builder builder) {
            // Nothing to do
        }

        @Override
        public void implementFactoryCtor(MethodSpec.Builder builder) {
            // Nothing to do
        }

        @Override
        public String factoryInvocation(MethodSpec.Builder factoryMethodBuilder) {
            return null; // Not used in the factory
        }

        @Override
        public void evalToBlock(MethodSpec.Builder builder) {
            // nothing to do
        }

        @Override
        public void closeEvalToBlock(MethodSpec.Builder builder) {
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

        @Override
        public String closeInvocation() {
            return null;
        }
    }

    private record BlockProcessFunctionArg(TypeName type, String name) implements ProcessFunctionArg {
        @Override
        public TypeName dataType(boolean blockStyle) {
            return type;
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
        public void declareFactoryField(TypeSpec.Builder builder) {
            builder.addField(EXPRESSION_EVALUATOR_FACTORY, name, Modifier.PRIVATE, Modifier.FINAL);
        }

        @Override
        public void implementCtor(MethodSpec.Builder builder) {
            builder.addParameter(EXPRESSION_EVALUATOR, name);
            builder.addStatement("this.$L = $L", name, name);
        }

        @Override
        public void implementFactoryCtor(MethodSpec.Builder builder) {
            builder.addParameter(EXPRESSION_EVALUATOR_FACTORY, name);
            builder.addStatement("this.$L = $L", name, name);
        }

        @Override
        public String factoryInvocation(MethodSpec.Builder factoryMethodBuilder) {
            return name + ".get(context)";
        }

        @Override
        public void evalToBlock(MethodSpec.Builder builder) {
            builder.beginControlFlow("try ($T $LBlock = ($T) $L.eval(page))", type, name, type, name);
        }

        @Override
        public void closeEvalToBlock(MethodSpec.Builder builder) {
            builder.endControlFlow();
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
            EvaluatorImplementer.skipNull(builder, paramName(true));
        }

        @Override
        public void unpackValues(MethodSpec.Builder builder, boolean blockStyle) {
            // nothing to do
        }

        @Override
        public void buildInvocation(StringBuilder pattern, List<Object> args, boolean blockStyle) {
            pattern.append("$L");
            args.add(paramName(blockStyle));
        }

        @Override
        public void buildToStringInvocation(StringBuilder pattern, List<Object> args, String prefix) {
            pattern.append(" + $S + $L");
            args.add(prefix + name + "=");
            args.add(name);
        }

        @Override
        public String closeInvocation() {
            return name;
        }
    }

    private static class ProcessFunction {
        private final ExecutableElement function;
        private final List<ProcessFunctionArg> args;
        private final BuilderProcessFunctionArg builderArg;
        private final List<TypeMirror> warnExceptions;

        private boolean hasBlockType;

        private ProcessFunction(
            Elements elements,
            javax.lang.model.util.Types types,
            ExecutableElement function,
            List<TypeMirror> warnExceptions
        ) {
            this.function = function;
            args = new ArrayList<>();
            BuilderProcessFunctionArg builderArg = null;
            hasBlockType = false;
            for (VariableElement v : function.getParameters()) {
                TypeName type = TypeName.get(v.asType());
                String name = v.getSimpleName().toString();
                Fixed fixed = v.getAnnotation(Fixed.class);
                if (fixed != null) {
                    args.add(
                        new FixedProcessFunctionArg(
                            type,
                            name,
                            fixed.includeInToString(),
                            fixed.build(),
                            Types.extendsSuper(types, v.asType(), "org.elasticsearch.core.Releasable")
                        )
                    );
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
                if (isBlockType(type)) {
                    if (builderArg != null && args.size() == 2 && hasBlockType == false) {
                        args.clear();
                        hasBlockType = true;
                    }
                    args.add(new BlockProcessFunctionArg(type, name));
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

    static boolean isBlockType(TypeName type) {
        return type.equals(INT_BLOCK)
            || type.equals(LONG_BLOCK)
            || type.equals(DOUBLE_BLOCK)
            || type.equals(BOOLEAN_BLOCK)
            || type.equals(BYTES_REF_BLOCK);
    }
}
