/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.gen;

import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;

import org.elasticsearch.compute.gen.argument.Argument;
import org.elasticsearch.compute.gen.argument.BlockArgument;
import org.elasticsearch.compute.gen.argument.BuilderArgument;
import org.elasticsearch.compute.gen.argument.FixedArgument;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.VariableElement;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.Elements;

import static org.elasticsearch.compute.gen.Methods.buildFromFactory;
import static org.elasticsearch.compute.gen.Types.BLOCK;
import static org.elasticsearch.compute.gen.Types.DRIVER_CONTEXT;
import static org.elasticsearch.compute.gen.Types.EXPRESSION_EVALUATOR;
import static org.elasticsearch.compute.gen.Types.EXPRESSION_EVALUATOR_FACTORY;
import static org.elasticsearch.compute.gen.Types.PAGE;
import static org.elasticsearch.compute.gen.Types.RAM_USAGE_ESIMATOR;
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
    private final boolean vectorsUnsupported;
    private final boolean allNullsIsNull;

    public EvaluatorImplementer(
        Elements elements,
        javax.lang.model.util.Types types,
        ExecutableElement processFunction,
        String extraName,
        List<TypeMirror> warnExceptions,
        boolean allNullsIsNull
    ) {
        this.declarationType = (TypeElement) processFunction.getEnclosingElement();
        this.processFunction = new ProcessFunction(types, processFunction, warnExceptions);

        this.implementation = ClassName.get(
            elements.getPackageOf(declarationType).toString(),
            declarationType.getSimpleName() + extraName + "Evaluator"
        );
        this.processOutputsMultivalued = this.processFunction.hasBlockType;
        boolean anyParameterNotSupportingVectors = this.processFunction.args.stream().anyMatch(a -> a.supportsVectorReadAccess() == false);
        vectorsUnsupported = processOutputsMultivalued || anyParameterNotSupportingVectors;
        this.allNullsIsNull = allNullsIsNull;
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
        builder.addJavadoc("This class is generated. Edit {@code " + getClass().getSimpleName() + "} instead.");
        builder.addModifiers(Modifier.PUBLIC, Modifier.FINAL);
        builder.addSuperinterface(EXPRESSION_EVALUATOR);
        builder.addField(baseRamBytesUsed(implementation));
        builder.addType(factory());

        builder.addField(SOURCE, "source", Modifier.PRIVATE, Modifier.FINAL);
        processFunction.args.forEach(a -> a.declareField(builder));
        builder.addField(DRIVER_CONTEXT, "driverContext", Modifier.PRIVATE, Modifier.FINAL);

        builder.addField(WARNINGS, "warnings", Modifier.PRIVATE);

        builder.addMethod(ctor());
        builder.addMethod(eval());
        builder.addMethod(processFunction.baseRamBytesUsed());

        if (vectorsUnsupported) {
            if (processFunction.args.stream().anyMatch(x -> x instanceof FixedArgument == false)) {
                builder.addMethod(realEval(true));
            }
        } else {
            if (processFunction.args.stream().anyMatch(x -> x instanceof FixedArgument == false)) {
                builder.addMethod(realEval(true));
            }
            builder.addMethod(realEval(false));
        }
        builder.addMethod(processFunction.toStringMethod(implementation));
        builder.addMethod(processFunction.close());
        builder.addMethod(warnings());
        return builder.build();
    }

    static FieldSpec baseRamBytesUsed(ClassName implementation) {
        FieldSpec.Builder builder = FieldSpec.builder(
            TypeName.LONG,
            "BASE_RAM_BYTES_USED",
            Modifier.PRIVATE,
            Modifier.STATIC,
            Modifier.FINAL
        );
        builder.initializer("$T.shallowSizeOfInstance($T.class)", RAM_USAGE_ESIMATOR, implementation);

        return builder.build();
    }

    private MethodSpec ctor() {
        MethodSpec.Builder builder = MethodSpec.constructorBuilder().addModifiers(Modifier.PUBLIC);
        builder.addParameter(SOURCE, "source");
        builder.addStatement("this.source = source");
        processFunction.args.forEach(a -> a.implementCtor(builder));
        builder.addParameter(DRIVER_CONTEXT, "driverContext");
        builder.addStatement("this.driverContext = driverContext");
        return builder.build();
    }

    private MethodSpec eval() {
        MethodSpec.Builder builder = MethodSpec.methodBuilder("eval").addAnnotation(Override.class);
        builder.addModifiers(Modifier.PUBLIC).returns(BLOCK).addParameter(PAGE, "page");
        processFunction.args.forEach(a -> a.evalToBlock(builder));
        String invokeBlockEval = invokeRealEval(true);
        if (vectorsUnsupported) {
            builder.addStatement(invokeBlockEval);
        } else {
            processFunction.args.forEach(a -> a.resolveVectors(builder, invokeBlockEval));
            builder.addStatement(invokeRealEval(false));
        }
        processFunction.args.forEach(a -> a.closeEvalToBlock(builder));
        return builder.build();
    }

    private String invokeRealEval(boolean blockStyle) {
        StringBuilder builder = new StringBuilder("return eval(page.getPositionCount()");

        String params = processFunction.args.stream()
            .map(a -> a.paramName(blockStyle))
            .filter(Objects::nonNull)
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
            processFunction.args.forEach(a -> {
                if (a.paramName(blockStyle) != null) {
                    builder.addParameter(a.dataType(blockStyle), a.paramName(blockStyle));
                }
            });

            processFunction.args.forEach(a -> a.createScratch(builder));

            builder.beginControlFlow("position: for (int p = 0; p < positionCount; p++)");
            {
                if (blockStyle) {
                    if (processOutputsMultivalued == false) {
                        processFunction.args.forEach(a -> a.skipNull(builder));
                    } else if (allNullsIsNull) {
                        builder.addStatement("boolean allBlocksAreNulls = true");
                        // allow block type inputs to be null
                        processFunction.args.forEach(a -> a.allBlocksAreNull(builder));

                        builder.beginControlFlow("if (allBlocksAreNulls)");
                        {
                            builder.addStatement("result.appendNull()");
                            builder.addStatement("continue position");
                        }
                        builder.endControlFlow();
                    }
                } else {
                    assert allNullsIsNull : "allNullsIsNull == false is only supported for block style.";
                }
                processFunction.args.forEach(a -> a.read(builder, blockStyle));

                StringBuilder pattern = new StringBuilder();
                List<Object> args = new ArrayList<>();
                pattern.append("$T.$N(");
                args.add(declarationType);
                args.add(processFunction.function.getSimpleName());
                pattern.append(processFunction.args.stream().map(argument -> {
                    var invocation = new StringBuilder();
                    argument.buildInvocation(invocation, args, blockStyle);
                    return invocation.toString();
                }).collect(Collectors.joining(", ")));
                pattern.append(")");
                String builtPattern;
                if (processFunction.builderArg == null) {
                    if (vectorize) {
                        builtPattern = "result.$L(p, " + pattern + ")";
                    } else {
                        builtPattern = "result.$L(" + pattern + ")";
                    }
                    args.addFirst(processFunction.appendMethod());
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
                    builder.nextControlFlow(catchPattern, processFunction.warnExceptions.stream().map(TypeName::get).toArray());
                    builder.addStatement("warnings().registerException(e)");
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

    static MethodSpec warnings() {
        MethodSpec.Builder builder = MethodSpec.methodBuilder("warnings");
        builder.addModifiers(Modifier.PRIVATE).returns(WARNINGS);
        builder.beginControlFlow("if (warnings == null)");
        builder.addStatement("""
            this.warnings = Warnings.createWarnings(
                driverContext.warningsMode(),
                source.source().getLineNumber(),
                source.source().getColumnNumber(),
                source.text()
            )""");
        builder.endControlFlow();
        builder.addStatement("return warnings");
        return builder.build();
    }

    private TypeSpec factory() {
        TypeSpec.Builder builder = TypeSpec.classBuilder("Factory");
        builder.addSuperinterface(EXPRESSION_EVALUATOR_FACTORY);
        builder.addModifiers(Modifier.STATIC);

        builder.addField(SOURCE, "source", Modifier.PRIVATE, Modifier.FINAL);
        processFunction.args.forEach(a -> a.declareFactoryField(builder));

        builder.addMethod(processFunction.factoryCtor());
        builder.addMethod(processFunction.factoryGet(implementation));
        builder.addMethod(processFunction.toStringMethod(implementation));

        return builder.build();
    }

    static class ProcessFunction {
        final ExecutableElement function;
        final List<Argument> args;
        private final BuilderArgument builderArg;
        private final List<TypeMirror> warnExceptions;

        private boolean hasBlockType;

        ProcessFunction(javax.lang.model.util.Types types, ExecutableElement function, List<TypeMirror> warnExceptions) {
            this.function = function;
            args = new ArrayList<>();
            BuilderArgument builderArg = null;
            hasBlockType = false;
            for (VariableElement v : function.getParameters()) {
                Argument arg = Argument.fromParameter(types, v);
                if (arg instanceof BuilderArgument ba) {
                    if (builderArg != null) {
                        throw new IllegalArgumentException("only one builder allowed");
                    }
                    builderArg = ba;
                } else if (arg instanceof BlockArgument) {
                    hasBlockType = true;
                }
                args.add(arg);
            }
            this.builderArg = builderArg;
            this.warnExceptions = warnExceptions;
        }

        TypeName returnType() {
            return TypeName.get(function.getReturnType());
        }

        ClassName resultDataType(boolean blockStyle) {
            if (builderArg != null) {
                return builderArg.type().enclosingClassName();
            }
            boolean useBlockStyle = blockStyle || warnExceptions.isEmpty() == false;
            return useBlockStyle ? blockType(returnType()) : vectorType(returnType());
        }

        String appendMethod() {
            return Methods.appendMethod(returnType());
        }

        @Override
        public String toString() {
            return "ProcessFunction{"
                + "function="
                + function
                + ", args="
                + args
                + ", builderArg="
                + builderArg
                + ", warnExceptions="
                + warnExceptions
                + ", hasBlockType="
                + hasBlockType
                + '}';
        }

        MethodSpec toStringMethod(ClassName implementation) {
            MethodSpec.Builder builder = MethodSpec.methodBuilder("toString").addAnnotation(Override.class);
            builder.addModifiers(Modifier.PUBLIC).returns(String.class);

            StringBuilder pattern = new StringBuilder();
            List<Object> args = new ArrayList<>();
            pattern.append("return $S");
            args.add(implementation.simpleName() + "[");
            this.args.forEach(a -> a.buildToStringInvocation(pattern, args, args.size() > 2 ? ", " : ""));
            pattern.append(" + $S");
            args.add("]");
            builder.addStatement(pattern.toString(), args.toArray());
            return builder.build();
        }

        MethodSpec factoryCtor() {
            MethodSpec.Builder builder = MethodSpec.constructorBuilder().addModifiers(Modifier.PUBLIC);
            builder.addParameter(SOURCE, "source");
            builder.addStatement("this.source = source");
            args.forEach(a -> a.implementFactoryCtor(builder));
            return builder.build();
        }

        MethodSpec factoryGet(ClassName implementation) {
            MethodSpec.Builder builder = MethodSpec.methodBuilder("get").addAnnotation(Override.class);
            builder.addModifiers(Modifier.PUBLIC);
            builder.addParameter(DRIVER_CONTEXT, "context");
            builder.returns(implementation);

            List<String> args = new ArrayList<>();
            args.add("source");
            for (Argument arg : this.args) {
                String invocation = arg.factoryInvocation(builder);
                if (invocation != null) {
                    args.add(invocation);
                }
            }
            args.add("context");
            builder.addStatement("return new $T($L)", implementation, String.join(", ", args));
            return builder.build();
        }

        MethodSpec close() {
            MethodSpec.Builder builder = MethodSpec.methodBuilder("close").addAnnotation(Override.class);
            builder.addModifiers(Modifier.PUBLIC);

            List<String> invocations = args.stream().map(Argument::closeInvocation).filter(Objects::nonNull).toList();
            if (invocations.isEmpty() == false) {
                builder.addStatement("$T.closeExpectNoException(" + String.join(", ", invocations) + ")", Types.RELEASABLES);
            }
            return builder.build();
        }

        MethodSpec baseRamBytesUsed() {
            MethodSpec.Builder builder = MethodSpec.methodBuilder("baseRamBytesUsed").addAnnotation(Override.class);
            builder.addModifiers(Modifier.PUBLIC).returns(TypeName.LONG);

            builder.addStatement("long baseRamBytesUsed = BASE_RAM_BYTES_USED");
            for (Argument arg : args) {
                arg.sumBaseRamBytesUsed(builder);
            }
            builder.addStatement("return baseRamBytesUsed");
            return builder.build();
        }
    }
}
