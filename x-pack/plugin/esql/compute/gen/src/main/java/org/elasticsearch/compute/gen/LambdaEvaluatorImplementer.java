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

import java.util.function.Consumer;

import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.TypeElement;
import javax.lang.model.type.TypeKind;
import javax.lang.model.util.Elements;

import static org.elasticsearch.compute.gen.Methods.buildFromFactory;
import static org.elasticsearch.compute.gen.Types.BLOCK;
import static org.elasticsearch.compute.gen.Types.DRIVER_CONTEXT;
import static org.elasticsearch.compute.gen.Types.EXPRESSION_EVALUATOR;
import static org.elasticsearch.compute.gen.Types.EXPRESSION_EVALUATOR_FACTORY;
import static org.elasticsearch.compute.gen.Types.PAGE;
import static org.elasticsearch.compute.gen.Types.RELEASABLES;

/**
 * Implements "LambdaEvaluator" from a class annotated with {@code @LambdaEvaluator}.
 * <p>
 * The generated evaluator applies a lambda expression to every element of a (possibly
 * multivalued) field: it flattens the field into one row per value, builds the lambda
 * body's input page (replicating only the upstream blocks the body references, recipe
 * given by the {@code outerChannels} constructor argument, with the flattened field as
 * the last channel — the lambda parameter), evaluates the body once over that page, and
 * calls the annotated {@code process} method once per original position with the row
 * range corresponding to that position's values.
 * </p>
 * <p>
 * See the {@code LambdaEvaluator} annotation javadoc for the supported {@code process}
 * shapes and the combine contract.
 * </p>
 */
public class LambdaEvaluatorImplementer {
    private static final TypeName INT_ARRAY = ArrayTypeName.of(TypeName.INT);

    private final TypeElement declarationType;
    private final ExecutableElement processFunction;
    private final ClassName implementation;
    /** Type of the result-building first parameter, e.g. {@code BooleanBlock.Builder}. */
    private final TypeName builderType;
    /** Type of the lambda body's output block parameter, e.g. {@code BooleanBlock}. */
    private final TypeName bodyBlockType;
    /** Type of the flattened field block parameter, or {@code null} when the kernel doesn't take it. */
    private final TypeName fieldBlockType;

    public LambdaEvaluatorImplementer(Elements elements, ExecutableElement processFunction, String extraName) {
        this.declarationType = (TypeElement) processFunction.getEnclosingElement();
        this.processFunction = processFunction;

        int paramCount = processFunction.getParameters().size();
        if (paramCount != 4 && paramCount != 5) {
            throw new IllegalArgumentException(
                "process must have 4 or 5 parameters: (builder, [field,] body, start, end); got " + paramCount
            );
        }
        if (processFunction.getParameters().get(paramCount - 2).asType().getKind() != TypeKind.INT
            || processFunction.getParameters().get(paramCount - 1).asType().getKind() != TypeKind.INT) {
            throw new IllegalArgumentException("process must end with two int parameters (start, end)");
        }
        this.builderType = TypeName.get(processFunction.getParameters().get(0).asType());
        if (paramCount == 4) {
            this.fieldBlockType = null;
            this.bodyBlockType = TypeName.get(processFunction.getParameters().get(1).asType());
        } else {
            this.fieldBlockType = TypeName.get(processFunction.getParameters().get(1).asType());
            this.bodyBlockType = TypeName.get(processFunction.getParameters().get(2).asType());
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
        builder.addJavadoc("This class is generated. Edit {@code " + getClass().getSimpleName() + "} instead.");
        builder.addModifiers(Modifier.PUBLIC, Modifier.FINAL);
        builder.addSuperinterface(EXPRESSION_EVALUATOR);
        builder.addField(EvaluatorImplementer.baseRamBytesUsed(implementation));

        builder.addField(EXPRESSION_EVALUATOR, "field", Modifier.PRIVATE, Modifier.FINAL);
        builder.addField(EXPRESSION_EVALUATOR, "lambda", Modifier.PRIVATE, Modifier.FINAL);
        builder.addField(INT_ARRAY, "outerChannels", Modifier.PRIVATE, Modifier.FINAL);
        builder.addField(DRIVER_CONTEXT, "driverContext", Modifier.PRIVATE, Modifier.FINAL);

        builder.addMethod(ctor());
        builder.addMethod(eval());
        builder.addMethod(evalNotExpanded());
        builder.addMethod(evalExpanded());
        builder.addMethod(expandingFilter());
        builder.addMethod(baseRamBytesUsed());
        builder.addMethod(toStringMethod());
        builder.addMethod(close());
        builder.addType(factory());
        return builder.build();
    }

    private MethodSpec ctor() {
        MethodSpec.Builder builder = MethodSpec.constructorBuilder().addModifiers(Modifier.PUBLIC);
        builder.addParameter(EXPRESSION_EVALUATOR, "field");
        builder.addParameter(EXPRESSION_EVALUATOR, "lambda");
        builder.addParameter(INT_ARRAY, "outerChannels");
        builder.addParameter(DRIVER_CONTEXT, "driverContext");
        builder.addStatement("this.field = field");
        builder.addStatement("this.lambda = lambda");
        builder.addStatement("this.outerChannels = outerChannels");
        builder.addStatement("this.driverContext = driverContext");
        return builder.build();
    }

    private MethodSpec eval() {
        MethodSpec.Builder builder = MethodSpec.methodBuilder("eval").addAnnotation(Override.class);
        builder.addModifiers(Modifier.PUBLIC).returns(BLOCK).addParameter(PAGE, "page");
        builder.beginControlFlow("try ($T fieldBlock = field.eval(page))", BLOCK);
        {
            builder.beginControlFlow("if (fieldBlock.areAllValuesNull())");
            builder.addStatement("return driverContext.blockFactory().newConstantNullBlock(page.getPositionCount())");
            builder.endControlFlow();
            builder.beginControlFlow("if (fieldBlock.mayHaveMultivaluedFields() == false && fieldBlock.mayHaveNulls() == false)");
            builder.addStatement("return evalNotExpanded(page, fieldBlock)");
            builder.endControlFlow();
            builder.addStatement("return evalExpanded(page, fieldBlock)");
        }
        builder.endControlFlow();
        return builder.build();
    }

    /**
     * Shared shell of the two eval paths: builds the lambda body's input page out of the blocks
     * prepared by {@code prepareInnerBlocks}, evaluates the body, runs the combine loop emitted
     * by {@code combine} and releases the page in all cases.
     */
    private MethodSpec evalShell(
        String name,
        String javadoc,
        Consumer<MethodSpec.Builder> prepareInnerBlocks,
        Consumer<MethodSpec.Builder> combine
    ) {
        MethodSpec.Builder builder = MethodSpec.methodBuilder(name).addModifiers(Modifier.PRIVATE);
        builder.returns(BLOCK).addParameter(PAGE, "page").addParameter(BLOCK, "fieldBlock");
        builder.addJavadoc(javadoc);
        builder.addStatement("$T[] inner = new $T[outerChannels.length + 1]", BLOCK, BLOCK);
        builder.addStatement("$T innerPage = null", PAGE);
        builder.beginControlFlow("try");
        {
            prepareInnerBlocks.accept(builder);
            builder.addStatement("innerPage = new $T(inner)", PAGE);
            builder.beginControlFlow(
                "try ($T lambdaBlock = ($T) lambda.eval(innerPage); $T result = driverContext.blockFactory().$L(fieldBlock.getPositionCount()))",
                bodyBlockType,
                bodyBlockType,
                builderType,
                buildFromFactory(builderType)
            );
            {
                combine.accept(builder);
                builder.addStatement("return result.build()");
            }
            builder.endControlFlow();
        }
        builder.nextControlFlow("finally");
        {
            builder.beginControlFlow("if (innerPage != null)");
            builder.addStatement("innerPage.releaseBlocks()");
            builder.nextControlFlow("else");
            builder.addStatement("$T.closeExpectNoException(inner)", RELEASABLES);
            builder.endControlFlow();
        }
        builder.endControlFlow();
        return builder.build();
    }

    private MethodSpec evalNotExpanded() {
        String javadoc = """
            Fast path: every field position holds exactly one non-null value, so the lambda body
            is evaluated over the page's own shape with the field itself as the parameter block.""";
        return evalShell("evalNotExpanded", javadoc, builder -> {
            builder.beginControlFlow("for (int c = 0; c < outerChannels.length; c++)");
            builder.addStatement("$T b = page.getBlock(outerChannels[c])", BLOCK);
            builder.addStatement("b.incRef()");
            builder.addStatement("inner[c] = b");
            builder.endControlFlow();
            builder.addStatement("fieldBlock.incRef()");
            builder.addStatement("inner[outerChannels.length] = fieldBlock");
        }, builder -> {
            builder.beginControlFlow("for (int p = 0; p < fieldBlock.getPositionCount(); p++)");
            addProcessCall(builder, "p", "p + 1", "fieldBlock");
            builder.endControlFlow();
        });
    }

    private MethodSpec evalExpanded() {
        String javadoc = """
            Expanded path: multivalued field positions are flattened into one row per value (null
            positions become a single null row) and the lambda body is evaluated once over the
            flattened shape. The combine method is invoked with each original position's row range;
            null field positions produce a null result without invoking it.""";
        return evalShell("evalExpanded", javadoc, builder -> {
            builder.beginControlFlow("if (outerChannels.length > 0)");
            builder.addStatement("int[] expandingFilter = expandingFilter(fieldBlock)");
            builder.beginControlFlow("for (int c = 0; c < outerChannels.length; c++)");
            builder.addStatement("inner[c] = page.getBlock(outerChannels[c]).filter(true, expandingFilter)");
            builder.endControlFlow();
            builder.endControlFlow();
            builder.addStatement("inner[outerChannels.length] = fieldBlock.expand()");
        }, builder -> {
            builder.addStatement("int row = 0");
            builder.beginControlFlow("for (int p = 0; p < fieldBlock.getPositionCount(); p++)");
            {
                builder.addStatement("int valueCount = fieldBlock.getValueCount(p)");
                builder.beginControlFlow("if (valueCount == 0)");
                builder.addStatement("result.appendNull()");
                builder.addComment("null field positions expand to a single null row");
                builder.addStatement("row++");
                builder.addStatement("continue");
                builder.endControlFlow();
                addProcessCall(builder, "row", "row + valueCount", "inner[outerChannels.length]");
                builder.addStatement("row += valueCount");
            }
            builder.endControlFlow();
        });
    }

    private void addProcessCall(MethodSpec.Builder builder, String start, String end, String fieldBlockExpression) {
        if (fieldBlockType == null) {
            builder.addStatement("$T.$L(result, lambdaBlock, $L, $L)", declarationType, processFunction.getSimpleName(), start, end);
        } else {
            builder.addStatement(
                "$T.$L(result, ($T) $L, lambdaBlock, $L, $L)",
                declarationType,
                processFunction.getSimpleName(),
                fieldBlockType,
                fieldBlockExpression,
                start,
                end
            );
        }
    }

    private MethodSpec expandingFilter() {
        MethodSpec.Builder builder = MethodSpec.methodBuilder("expandingFilter").addModifiers(Modifier.PRIVATE, Modifier.STATIC);
        builder.returns(INT_ARRAY).addParameter(BLOCK, "fieldBlock");
        builder.addJavadoc("""
            Maps each row of the flattened field to the original position it came from, used to
            row-replicate the upstream blocks the lambda body references. Null field positions
            occupy a single row, mirroring {@link $T#expand}.""", BLOCK);
        builder.addStatement("int rows = 0");
        builder.beginControlFlow("for (int p = 0; p < fieldBlock.getPositionCount(); p++)");
        builder.addStatement("int valueCount = fieldBlock.getValueCount(p)");
        builder.addStatement("rows += valueCount == 0 ? 1 : valueCount");
        builder.endControlFlow();
        builder.addStatement("int[] expandingFilter = new int[rows]");
        builder.addStatement("int row = 0");
        builder.beginControlFlow("for (int p = 0; p < fieldBlock.getPositionCount(); p++)");
        builder.addStatement("int valueCount = fieldBlock.getValueCount(p)");
        builder.beginControlFlow("if (valueCount == 0)");
        builder.addStatement("valueCount = 1");
        builder.endControlFlow();
        builder.addStatement("$T.fill(expandingFilter, row, row + valueCount, p)", ClassName.get("java.util", "Arrays"));
        builder.addStatement("row += valueCount");
        builder.endControlFlow();
        builder.addStatement("return expandingFilter");
        return builder.build();
    }

    private MethodSpec baseRamBytesUsed() {
        MethodSpec.Builder builder = MethodSpec.methodBuilder("baseRamBytesUsed").addAnnotation(Override.class);
        builder.addModifiers(Modifier.PUBLIC).returns(TypeName.LONG);
        builder.addStatement("return BASE_RAM_BYTES_USED + field.baseRamBytesUsed() + lambda.baseRamBytesUsed()");
        return builder.build();
    }

    private MethodSpec toStringMethod() {
        MethodSpec.Builder builder = MethodSpec.methodBuilder("toString").addAnnotation(Override.class);
        builder.addModifiers(Modifier.PUBLIC).returns(String.class);
        builder.addStatement("return $S + field + $S + lambda + $S", implementation.simpleName() + "[field=", ", lambda=", "]");
        return builder.build();
    }

    private MethodSpec close() {
        MethodSpec.Builder builder = MethodSpec.methodBuilder("close").addAnnotation(Override.class);
        builder.addModifiers(Modifier.PUBLIC);
        builder.addStatement("$T.closeExpectNoException(field, lambda)", RELEASABLES);
        return builder.build();
    }

    private TypeSpec factory() {
        TypeSpec.Builder builder = TypeSpec.classBuilder("Factory");
        builder.addSuperinterface(EXPRESSION_EVALUATOR_FACTORY);
        builder.addModifiers(Modifier.PUBLIC, Modifier.STATIC);

        builder.addField(EXPRESSION_EVALUATOR_FACTORY, "field", Modifier.PRIVATE, Modifier.FINAL);
        builder.addField(EXPRESSION_EVALUATOR_FACTORY, "lambda", Modifier.PRIVATE, Modifier.FINAL);
        builder.addField(INT_ARRAY, "outerChannels", Modifier.PRIVATE, Modifier.FINAL);

        MethodSpec.Builder ctor = MethodSpec.constructorBuilder().addModifiers(Modifier.PUBLIC);
        ctor.addParameter(EXPRESSION_EVALUATOR_FACTORY, "field");
        ctor.addParameter(EXPRESSION_EVALUATOR_FACTORY, "lambda");
        ctor.addParameter(INT_ARRAY, "outerChannels");
        ctor.addStatement("this.field = field");
        ctor.addStatement("this.lambda = lambda");
        ctor.addStatement("this.outerChannels = outerChannels");
        builder.addMethod(ctor.build());

        MethodSpec.Builder get = MethodSpec.methodBuilder("get").addAnnotation(Override.class);
        get.addModifiers(Modifier.PUBLIC);
        get.addParameter(DRIVER_CONTEXT, "context");
        get.returns(implementation);
        get.addStatement("return new $T(field.get(context), lambda.get(context), outerChannels, context)", implementation);
        builder.addMethod(get.build());

        MethodSpec.Builder toString = MethodSpec.methodBuilder("toString").addAnnotation(Override.class);
        toString.addModifiers(Modifier.PUBLIC).returns(String.class);
        toString.addStatement("return $S + field + $S + lambda + $S", implementation.simpleName() + "[field=", ", lambda=", "]");
        builder.addMethod(toString.build());

        return builder.build();
    }
}
