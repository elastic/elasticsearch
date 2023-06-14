/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.gen;

import org.elasticsearch.compute.ann.GroupingAggregator;

import java.util.List;
import java.util.Set;

import javax.annotation.processing.Completion;
import javax.annotation.processing.ProcessingEnvironment;
import javax.annotation.processing.Processor;
import javax.annotation.processing.RoundEnvironment;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.AnnotationMirror;
import javax.lang.model.element.Element;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.TypeElement;

/**
 * Glues the {@link GroupingAggregatorImplementer} into the jdk's annotation
 * processing framework.
 */
public class GroupingAggregatorProcessor implements Processor {
    private ProcessingEnvironment env;

    @Override
    public Set<String> getSupportedOptions() {
        return Set.of();
    }

    @Override
    public Set<String> getSupportedAnnotationTypes() {
        return Set.of(GroupingAggregator.class.getName());
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
        for (TypeElement ann : set) {
            for (Element aggClass : roundEnvironment.getElementsAnnotatedWith(ann)) {
                AggregatorProcessor.write(
                    aggClass,
                    "grouping aggregation",
                    new GroupingAggregatorImplementer(env.getElementUtils(), (TypeElement) aggClass).sourceFile(),
                    env
                );
            }
        }
        return true;
    }
}
