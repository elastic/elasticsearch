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

import org.elasticsearch.compute.ann.Aggregator;
import org.elasticsearch.compute.ann.GroupingAggregator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.lang.model.element.Modifier;
import javax.lang.model.element.TypeElement;
import javax.lang.model.util.Elements;

import static org.elasticsearch.compute.gen.Types.AGGREGATOR_FUNCTION_SUPPLIER;
import static org.elasticsearch.compute.gen.Types.DRIVER_CONTEXT;
import static org.elasticsearch.compute.gen.Types.LIST_INTEGER;
import static org.elasticsearch.compute.gen.Types.STRING;

/**
 * Implements "AggregationFunctionSupplier" from a class annotated with both
 * {@link Aggregator} and {@link GroupingAggregator}.
 */
public class AggregatorFunctionSupplierImplementer {
    private final TypeElement declarationType;
    private final AggregatorImplementer aggregatorImplementer;
    private final GroupingAggregatorImplementer groupingAggregatorImplementer;
    private final boolean hasWarnings;
    private final List<Parameter> createParameters;
    private final ClassName implementation;

    public AggregatorFunctionSupplierImplementer(
        Elements elements,
        TypeElement declarationType,
        AggregatorImplementer aggregatorImplementer,
        GroupingAggregatorImplementer groupingAggregatorImplementer,
        boolean hasWarnings
    ) {
        this.declarationType = declarationType;
        this.aggregatorImplementer = aggregatorImplementer;
        this.groupingAggregatorImplementer = groupingAggregatorImplementer;
        this.hasWarnings = hasWarnings;

        Set<Parameter> createParameters = new LinkedHashSet<>();
        if (aggregatorImplementer != null) {
            createParameters.addAll(aggregatorImplementer.createParameters());
        }
        if (groupingAggregatorImplementer != null) {
            createParameters.addAll(groupingAggregatorImplementer.createParameters());
        }
        this.createParameters = new ArrayList<>(createParameters);
        this.createParameters.add(0, new Parameter(LIST_INTEGER, "channels"));

        this.implementation = ClassName.get(
            elements.getPackageOf(declarationType).toString(),
            (declarationType.getSimpleName() + "AggregatorFunctionSupplier").replace("AggregatorAggregator", "Aggregator")
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
        builder.addJavadoc("{@link $T} implementation for {@link $T}.\n", AGGREGATOR_FUNCTION_SUPPLIER, declarationType);
        builder.addJavadoc("This class is generated. Do not edit it.");
        builder.addModifiers(Modifier.PUBLIC, Modifier.FINAL);
        builder.addSuperinterface(AGGREGATOR_FUNCTION_SUPPLIER);

        if (hasWarnings) {
            builder.addField(TypeName.INT, "warningsLineNumber");
            builder.addField(TypeName.INT, "warningsColumnNumber");
            builder.addField(STRING, "warningsSourceText");
        }
        createParameters.stream().forEach(p -> p.declareField(builder));
        builder.addMethod(ctor());
        if (aggregatorImplementer != null) {
            builder.addMethod(aggregator());
        } else {
            builder.addMethod(unsupportedNonGroupingAggregator());
        }
        builder.addMethod(groupingAggregator());
        builder.addMethod(describe());
        return builder.build();
    }

    private MethodSpec ctor() {
        MethodSpec.Builder builder = MethodSpec.constructorBuilder().addModifiers(Modifier.PUBLIC);
        if (hasWarnings) {
            builder.addParameter(TypeName.INT, "warningsLineNumber");
            builder.addParameter(TypeName.INT, "warningsColumnNumber");
            builder.addParameter(STRING, "warningsSourceText");
            builder.addStatement("this.warningsLineNumber = warningsLineNumber");
            builder.addStatement("this.warningsColumnNumber = warningsColumnNumber");
            builder.addStatement("this.warningsSourceText = warningsSourceText");
        }
        createParameters.stream().forEach(p -> p.buildCtor(builder));
        return builder.build();
    }

    private MethodSpec unsupportedNonGroupingAggregator() {
        MethodSpec.Builder builder = MethodSpec.methodBuilder("aggregator")
            .addParameter(DRIVER_CONTEXT, "driverContext")
            .returns(Types.AGGREGATOR_FUNCTION);
        builder.addAnnotation(Override.class).addModifiers(Modifier.PUBLIC);
        builder.addStatement("throw new UnsupportedOperationException($S)", "non-grouping aggregator is not supported");
        return builder.build();
    }

    private MethodSpec aggregator() {
        MethodSpec.Builder builder = MethodSpec.methodBuilder("aggregator");
        builder.addAnnotation(Override.class).addModifiers(Modifier.PUBLIC);
        builder.addParameter(DRIVER_CONTEXT, "driverContext");
        builder.returns(aggregatorImplementer.implementation());

        if (hasWarnings) {
            builder.addStatement(
                "var warnings = Warnings.createWarnings(driverContext.warningsMode(), "
                    + "warningsLineNumber, warningsColumnNumber, warningsSourceText)"
            );
        }

        builder.addStatement(
            "return $T.create($L)",
            aggregatorImplementer.implementation(),
            Stream.concat(
                Stream.concat(hasWarnings ? Stream.of("warnings") : Stream.of(), Stream.of("driverContext, channels")),
                aggregatorImplementer.createParameters().stream().map(Parameter::name)
            ).collect(Collectors.joining(", "))
        );

        return builder.build();
    }

    private MethodSpec groupingAggregator() {
        MethodSpec.Builder builder = MethodSpec.methodBuilder("groupingAggregator");
        builder.addAnnotation(Override.class).addModifiers(Modifier.PUBLIC);
        builder.addParameter(DRIVER_CONTEXT, "driverContext");
        builder.returns(groupingAggregatorImplementer.implementation());

        if (hasWarnings) {
            builder.addStatement(
                "var warnings = Warnings.createWarnings(driverContext.warningsMode(), "
                    + "warningsLineNumber, warningsColumnNumber, warningsSourceText)"
            );
        }

        builder.addStatement(
            "return $T.create($L)",
            groupingAggregatorImplementer.implementation(),
            Stream.concat(
                Stream.concat(hasWarnings ? Stream.of("warnings") : Stream.of(), Stream.of("channels, driverContext")),
                groupingAggregatorImplementer.createParameters().stream().map(Parameter::name)
            ).collect(Collectors.joining(", "))
        );
        return builder.build();
    }

    private MethodSpec describe() {
        MethodSpec.Builder builder = MethodSpec.methodBuilder("describe").returns(String.class);
        builder.addAnnotation(Override.class).addModifiers(Modifier.PUBLIC);

        String name = declarationType.getSimpleName().toString();
        name = name.replace("BytesRef", "Byte"); // The hack expects one word types so let's make BytesRef into Byte
        String[] parts = name.split("(?=\\p{Upper})");
        if (false == parts[parts.length - 1].equals("Aggregator") || parts.length < 3) {
            throw new IllegalArgumentException("Can't generate description for " + declarationType.getSimpleName());
        }

        String operation = Arrays.stream(parts, 0, parts.length - 2).map(s -> s.toLowerCase(Locale.ROOT)).collect(Collectors.joining("_"));
        String type = parts[parts.length - 2];

        builder.addStatement("return $S", operation + " of " + type.toLowerCase(Locale.ROOT) + "s");
        return builder.build();
    }
}
