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
import org.elasticsearch.compute.gen.argument.JitConstantFixedArgument;

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
import static org.elasticsearch.compute.gen.Types.JIT_CONSTANT_SPINNER;
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
        boolean returnTypeWithoutVectorSupport = vectorType(elementType(this.processFunction.resultDataType(true))) == null;
        vectorsUnsupported = processOutputsMultivalued || anyParameterNotSupportingVectors || returnTypeWithoutVectorSupport;
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
        // When any argument is @Fixed(jitConstant=true), the class becomes non-final + abstract so
        // JitConstantSpinner can produce per-value hidden subclasses overriding the abstract
        // accessor methods with the value baked in as a JIT-time constant.
        boolean hasJitConstant = processFunction.args.stream().anyMatch(Argument::isJitConstant);
        if (hasJitConstant) {
            builder.addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT);
        } else {
            builder.addModifiers(Modifier.PUBLIC, Modifier.FINAL);
        }
        builder.addSuperinterface(EXPRESSION_EVALUATOR);
        builder.addField(baseRamBytesUsed(implementation));
        builder.addType(factory());

        builder.addField(SOURCE, "source", Modifier.PRIVATE, Modifier.FINAL);
        processFunction.args.forEach(a -> a.declareField(builder));
        processFunction.args.forEach(a -> a.declareAbstractAccessor(builder));
        builder.addField(DRIVER_CONTEXT, "driverContext", Modifier.PRIVATE, Modifier.FINAL);

        if (hasJitConstant) {
            builder.addType(standard());
            // pathLabel() default; the Standard subclass overrides it, the spun subclass
            // inherits this default. Read by toString() to make spun vs Standard visible
            // in ESQL PROFILE output without any class-name string matching.
            builder.addMethod(
                MethodSpec.methodBuilder("pathLabel")
                    .addModifiers(Modifier.PROTECTED)
                    .returns(String.class)
                    .addStatement("return $S", "jit-folded")
                    .build()
            );
        }

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
            // TODO: consider passing an onAllNull to skip processing when all values are null
            processFunction.args.forEach(a -> a.resolveVectors(builder, b -> b.addStatement(invokeBlockEval), null));
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
        builder.addStatement("this.warnings = Warnings.createWarnings(driverContext.warningsMode(), source)");
        builder.endControlFlow();
        builder.addStatement("return warnings");
        return builder.build();
    }

    /**
     * Concrete non-spun subclass for the abstract jit-constant evaluator. Used when
     * {@code JitConstantSpinner} returns {@code Optional.empty()} (admission filter
     * rejected the spin for a first-time key). Stores the constant in a regular instance
     * field — no JIT-time constant folding occurs — but the per-row work still executes
     * correctly. Named {@code Standard} because this is the deliberate non-JIT path for
     * low-cardinality constants, not a failure-mode standard.
     */
    private TypeSpec standard() {
        JitConstantFixedArgument jit = processFunction.args.stream()
            .filter(Argument::isJitConstant)
            .map(a -> (JitConstantFixedArgument) a)
            .findFirst()
            .orElseThrow();

        TypeSpec.Builder builder = TypeSpec.classBuilder("Standard")
            .addModifiers(Modifier.PUBLIC, Modifier.STATIC, Modifier.FINAL)
            .superclass(implementation);

        builder.addJavadoc(
            "Concrete non-spun subclass used when {@link $T} returns {@code Optional.empty()}\n"
                + "(admission filter rejected the spin). The constant lives in a regular\n"
                + "instance field — no JIT-time constant folding, but the per-row work\n"
                + "runs correctly. The Factory chooses between this and the spun subclass.\n",
            JIT_CONSTANT_SPINNER
        );

        // Instance field for the jit constant.
        builder.addField(jit.type(), jit.name(), Modifier.PRIVATE, Modifier.FINAL);

        // Constructor mirrors the abstract base's ctor + adds the jit param.
        // We use the same signal factoryGet uses to determine "is this arg a ctor param":
        // non-null factoryInvocation. BuilderArgument returns null, so it's excluded
        // (it's a process-method param, not a ctor param).
        MethodSpec.Builder ctor = MethodSpec.constructorBuilder().addModifiers(Modifier.PUBLIC);
        List<String> superArgs = new ArrayList<>();
        ctor.addParameter(SOURCE, "source");
        superArgs.add("source");
        MethodSpec.Builder probe = MethodSpec.constructorBuilder();
        for (Argument a : processFunction.args) {
            if (a.isJitConstant()) {
                continue;
            }
            if (a.factoryInvocation(probe) == null) {
                continue;
            }
            a.declareCtorParam(ctor);
            superArgs.add(a.name());
        }
        ctor.addParameter(jit.type(), jit.name());
        ctor.addParameter(DRIVER_CONTEXT, "driverContext");
        superArgs.add("driverContext");
        ctor.addStatement("super($L)", String.join(", ", superArgs));
        ctor.addStatement("this.$L = $L", jit.name(), jit.name());
        builder.addMethod(ctor.build());

        // Override the abstract accessor to return the field.
        builder.addMethod(
            MethodSpec.methodBuilder(jit.name())
                .addAnnotation(Override.class)
                .addModifiers(Modifier.PROTECTED, Modifier.FINAL)
                .returns(jit.type())
                .addStatement("return $L", jit.name())
                .build()
        );

        // Override pathLabel() to identify this as the non-spun path in profile output.
        builder.addMethod(
            MethodSpec.methodBuilder("pathLabel")
                .addAnnotation(Override.class)
                .addModifiers(Modifier.PROTECTED, Modifier.FINAL)
                .returns(String.class)
                .addStatement("return $S", "standard")
                .build()
        );

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
        builder.addMethod(processFunction.factoryToStringMethod(implementation));

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
            return buildToStringMethod(implementation, /* fromFactory */ false);
        }

        MethodSpec factoryToStringMethod(ClassName implementation) {
            return buildToStringMethod(implementation, /* fromFactory */ true);
        }

        private MethodSpec buildToStringMethod(ClassName implementation, boolean fromFactory) {
            MethodSpec.Builder builder = MethodSpec.methodBuilder("toString").addAnnotation(Override.class);
            builder.addModifiers(Modifier.PUBLIC).returns(String.class);

            StringBuilder pattern = new StringBuilder();
            List<Object> args = new ArrayList<>();
            pattern.append("return $S");
            args.add(implementation.simpleName() + "[");
            for (Argument a : this.args) {
                String prefix = args.size() > 2 ? ", " : "";
                if (fromFactory) {
                    a.buildToStringInvocationFromFactory(pattern, args, prefix);
                } else {
                    a.buildToStringInvocation(pattern, args, prefix);
                }
            }
            pattern.append(" + $S");
            args.add("]");
            // For jit-constant evaluators, append " (jit-folded)" or " (standard)" via the
            // pathLabel() override so the ESQL PROFILE output distinguishes the spun path
            // from the Standard path. Factory's toString doesn't get the marker — the
            // factory itself isn't an evaluator; it constructs one per Driver.
            boolean hasJitConstant = this.args.stream().anyMatch(Argument::isJitConstant);
            if (hasJitConstant && fromFactory == false) {
                pattern.append(" + $S + pathLabel() + $S");
                args.add(" (");
                args.add(")");
            }
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

            // Collect non-jit ctor args in order
            List<String> ctorArgs = new ArrayList<>();
            ctorArgs.add("source");
            for (Argument arg : this.args) {
                String invocation = arg.factoryInvocation(builder);
                if (invocation != null) ctorArgs.add(invocation);
            }
            ctorArgs.add("context");

            JitConstantFixedArgument jit = null;
            for (Argument a : this.args) {
                if (a.isJitConstant()) {
                    if (jit != null) {
                        throw new IllegalStateException(
                            "@Fixed(jitConstant=true) supported on at most one parameter per @Evaluator method"
                        );
                    }
                    jit = (JitConstantFixedArgument) a;
                }
            }

            if (jit == null) {
                builder.addStatement("return new $T($L)", implementation, String.join(", ", ctorArgs));
                return builder.build();
            }

            // Spinner-based construction with admission-aware standard.
            // The spinner may return Optional.empty() if the admission filter rejected
            // this constant (first-time key, count < threshold). In that case we route
            // to the Standard nested class — same evaluator, regular instance field for
            // the constant, no JIT folding. The Factory hides this from callers.
            String spinMethod = primitiveSpinnerMethod(jit.type());
            if (spinMethod != null) {
                builder.addStatement(
                    "$T<$T<? extends $T>> spunClassOpt = $T.$L($T.class, $S, this.$L)",
                    ClassName.get(java.util.Optional.class),
                    ClassName.get(Class.class),
                    implementation,
                    JIT_CONSTANT_SPINNER,
                    spinMethod,
                    implementation,
                    jit.name(),
                    jit.name()
                );
            } else {
                builder.addStatement(
                    "$T<$T<? extends $T>> spunClassOpt = $T.referenceConstantSubclass($T.class, $S, $T.class, this.$L)",
                    ClassName.get(java.util.Optional.class),
                    ClassName.get(Class.class),
                    implementation,
                    JIT_CONSTANT_SPINNER,
                    implementation,
                    jit.name(),
                    jit.type(),
                    jit.name()
                );
            }
            builder.beginControlFlow("if (spunClassOpt.isPresent())");
            builder.addStatement("$T<? extends $T> spunClass = spunClassOpt.get()", ClassName.get(Class.class), implementation);
            builder.beginControlFlow("try");
            builder.addStatement("return ($T) spunClass.getConstructors()[0].newInstance($L)", implementation, String.join(", ", ctorArgs));
            builder.nextControlFlow(
                "catch ($T | $T | $T e)",
                ClassName.get(InstantiationException.class),
                ClassName.get(IllegalAccessException.class),
                ClassName.get(java.lang.reflect.InvocationTargetException.class)
            );
            builder.addStatement(
                "throw new $T($S, e)",
                ClassName.get(IllegalStateException.class),
                "failed to construct JIT-spun evaluator for " + implementation.simpleName()
            );
            builder.endControlFlow(); // try-catch
            builder.endControlFlow(); // if isPresent

            // Standard path. ctorArgs are "source", non-jit args (via factoryInvocation),
            // "context". The Standard ctor inserts the jit value just before "context".
            List<String> standardArgs = new ArrayList<>(ctorArgs);
            standardArgs.add(standardArgs.size() - 1, "this." + jit.name());
            builder.addStatement("return new $T($L)", implementation.nestedClass("Standard"), String.join(", ", standardArgs));
            return builder.build();
        }

        /** Returns the JitConstantSpinner method name for a primitive type, or null for references. */
        private static String primitiveSpinnerMethod(com.squareup.javapoet.TypeName type) {
            if (type == TypeName.LONG) return "longConstantSubclass";
            if (type == TypeName.INT) return "intConstantSubclass";
            if (type == TypeName.DOUBLE) return "doubleConstantSubclass";
            return null;
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
