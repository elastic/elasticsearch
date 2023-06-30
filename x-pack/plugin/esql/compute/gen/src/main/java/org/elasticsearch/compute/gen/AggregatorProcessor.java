/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.gen;

import com.squareup.javapoet.JavaFile;

import org.elasticsearch.compute.ann.Aggregator;
import org.elasticsearch.compute.ann.GroupingAggregator;
import org.elasticsearch.compute.ann.IntermediateState;

import java.io.IOException;
import java.io.Writer;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;

import javax.annotation.processing.Completion;
import javax.annotation.processing.Filer;
import javax.annotation.processing.ProcessingEnvironment;
import javax.annotation.processing.Processor;
import javax.annotation.processing.RoundEnvironment;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.AnnotationMirror;
import javax.lang.model.element.Element;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.TypeElement;
import javax.tools.Diagnostic;
import javax.tools.JavaFileObject;

/**
 * Glues the {@link AggregatorImplementer} into the jdk's annotation
 * processing framework.
 */
public class AggregatorProcessor implements Processor {
    private ProcessingEnvironment env;

    @Override
    public Set<String> getSupportedOptions() {
        return Set.of();
    }

    @Override
    public Set<String> getSupportedAnnotationTypes() {
        return Set.of(Aggregator.class.getName(), GroupingAggregator.class.getName());
    }

    @Override
    public SourceVersion getSupportedSourceVersion() {
        return SourceVersion.RELEASE_17;
    }

    @Override
    public void init(ProcessingEnvironment processingEnvironment) {
        this.env = processingEnvironment;
    }

    @Override
    public Iterable<? extends Completion> getCompletions(
        Element element,
        AnnotationMirror annotationMirror,
        ExecutableElement executableElement,
        String s
    ) {
        return List.of();
    }

    @Override
    public boolean process(Set<? extends TypeElement> set, RoundEnvironment roundEnvironment) {
        Set<TypeElement> annotatedClasses = Collections.newSetFromMap(new IdentityHashMap<>());
        for (TypeElement ann : set) {
            for (Element aggClass : roundEnvironment.getElementsAnnotatedWith(ann)) {
                annotatedClasses.add((TypeElement) aggClass);
            }
        }
        for (TypeElement aggClass : annotatedClasses) {
            AggregatorImplementer implementer = null;
            if (aggClass.getAnnotation(Aggregator.class) != null) {
                IntermediateState[] intermediateState = aggClass.getAnnotation(Aggregator.class).value();
                implementer = new AggregatorImplementer(env.getElementUtils(), aggClass, intermediateState);
                write(aggClass, "aggregator", implementer.sourceFile(), env);
            }
            GroupingAggregatorImplementer groupingAggregatorImplementer = null;
            if (aggClass.getAnnotation(Aggregator.class) != null) {
                assert aggClass.getAnnotation(GroupingAggregator.class) != null;
                IntermediateState[] intermediateState = aggClass.getAnnotation(GroupingAggregator.class).value();
                if (intermediateState.length == 0) {
                    intermediateState = aggClass.getAnnotation(Aggregator.class).value();
                }

                groupingAggregatorImplementer = new GroupingAggregatorImplementer(env.getElementUtils(), aggClass, intermediateState);
                write(aggClass, "grouping aggregator", groupingAggregatorImplementer.sourceFile(), env);
            }
            if (implementer != null && groupingAggregatorImplementer != null) {
                write(
                    aggClass,
                    "aggregator function supplier",
                    new AggregatorFunctionSupplierImplementer(env.getElementUtils(), aggClass, implementer, groupingAggregatorImplementer)
                        .sourceFile(),
                    env
                );
            }
        }
        return true;
    }

    /**
     * Just like {@link JavaFile#writeTo(Filer)} but on windows it replaces {@code \n} with {@code \r\n}.
     */
    public static void write(Object origination, String what, JavaFile file, ProcessingEnvironment env) {
        try {
            String fileName = file.packageName + "." + file.typeSpec.name;
            JavaFileObject filerSourceFile = env.getFiler()
                .createSourceFile(fileName, file.typeSpec.originatingElements.toArray(Element[]::new));
            try (Writer w = filerSourceFile.openWriter()) {
                if (System.getProperty("line.separator").equals("\n")) {
                    file.writeTo(w);
                } else {
                    w.write(file.toString().replace("\n", System.getProperty("line.separator")));
                }
            }
        } catch (IOException e) {
            env.getMessager().printMessage(Diagnostic.Kind.ERROR, "failed generating " + what + " for " + origination);
            throw new RuntimeException(e);
        }
    }
}
